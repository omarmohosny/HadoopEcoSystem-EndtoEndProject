#!/usr/bin/env bash
# Acceptance tests for the systemd-managed stack. Run as any user with sudo -u hadoop rights:
#   bash /home/hadoop/kafka/tests/test_stack.sh            # all checks incl. end-to-end
#   bash /home/hadoop/kafka/tests/test_stack.sh --no-e2e   # service checks only
set -u
PASS=0; FAIL=0
ok()   { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad()  { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
check(){ local name="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$name"; else bad "$name"; fi; }
H() { sudo -u hadoop env -i HOME=/home/hadoop bash -c 'set -a; . /home/hadoop/kafka/systemd/hadoop-stack.env; set +a; '"$1"; }
UNITS="hadoop-hdfs hadoop-yarn hiveserver2 kafka kafka-connect system-log-classifier spark-system-logs"

echo "== systemd units"
for u in $UNITS; do
  check "$u is enabled" test "$(systemctl is-enabled $u 2>/dev/null)" = enabled
  check "$u is active"  test "$(systemctl is-active  $u 2>/dev/null)" = active
done

echo "== startup ordering"
# Race seen at boot 2026-10-04 01:19: safemode check ran before the NameNode RPC port opened -> unit failed
check "hadoop-hdfs waits for NameNode RPC :9000 before the safemode check" bash -c '
  systemctl cat hadoop-hdfs 2>/dev/null | grep "^ExecStartPost=" | head -2 > /tmp/.hdfs_post.$$
  grep -q "/dev/tcp/localhost/9000" <(sed -n 1p /tmp/.hdfs_post.$$) && grep -q "safemode wait" <(sed -n 2p /tmp/.hdfs_post.$$); rc=$?; rm -f /tmp/.hdfs_post.$$; exit $rc'

echo "== processes are owned by systemd (no manual leftovers)"
for spec in "hadoop-hdfs:proc_namenode" "hadoop-hdfs:proc_datanode" "hadoop-yarn:proc_resourcemanager" \
            "hadoop-yarn:proc_nodemanager" "hiveserver2:HiveServer2" "kafka:kafka\.Kafka " \
            "kafka-connect:ConnectStandalone" "system-log-classifier:system_log_classifier\.py" \
            "spark-system-logs:system_logs_to_hive\.py"; do
  unit=${spec%%:*}; pat=${spec#*:}
  pid=$(pgrep -u hadoop -f "$pat" | head -1)
  check "$pat runs under $unit.service" bash -c "[ -n '$pid' ] && grep -q '/$unit.service' /proc/$pid/cgroup"
done

echo "== /tmp cleanup exclusions"
TMPFILES_CONF=${TMPFILES_CONF:-/etc/tmpfiles.d/kafka.conf}
for p in /tmp/kraft-combined-logs /tmp/connect.offsets /tmp/hadoop-hadoop /tmp/hadoop-yarn-hadoop '/tmp/hadoop-hadoop-*.pid'; do
  check "tmpfiles excludes $p" grep -qxF "x $p" "$TMPFILES_CONF"
done

echo "== selinux"
# ExecStart targets must be bin_t so systemd may run them and they transition to unconfined_service_t
for f in /home/hadoop/hadoop/bin/hdfs /home/hadoop/hadoop/bin/yarn /home/hadoop/hive/bin/hive \
         /home/hadoop/spark/bin/spark-submit /home/hadoop/kafka/bin/kafka-server-start.sh \
         /home/hadoop/kafka/bin/connect-standalone.sh; do
  check "$(basename $f) is labelled bin_t" bash -c "sudo -u hadoop stat -c %C $f | grep -q ':bin_t:'"
done
# Files systemd appends service output to must be var_log_t
for f in /home/hadoop/kafka/logs/classifier.log /home/hadoop/kafka/logs/spark_system_logs.log \
         /home/hadoop/hive/hiveserver2.out /home/hadoop/kafka/logs.txt; do
  check "$(basename $f) is labelled var_log_t" bash -c "sudo -u hadoop stat -L -c %C $f | grep -q ':var_log_t:'"
done
# Every service process should run in the standard domain for third-party services
for pat in proc_namenode proc_datanode proc_secondarynamenode proc_resourcemanager proc_nodemanager \
           HiveServer2 'kafka\.Kafka ' ConnectStandalone 'system_log_classifier\.py' 'org\.apache\.spark\.deploy\.SparkSubmit'; do
  pid=$(pgrep -u hadoop -f "$pat" | head -1)
  check "$pat runs as unconfined_service_t" bash -c "[ -n '$pid' ] && ps -o label= -p $pid | grep -q ':unconfined_service_t:'"
done
check "boolean logging_syslogd_list_non_security_dirs is on" bash -c 'getsebool logging_syslogd_list_non_security_dirs | grep -q "on$"'
# systemd-captured service output must live under /var/log/hadoop-stack (systemd may create files there;
# it may not create them under /home, which broke these units at boot under SELinux)
LOGDIR=/var/log/hadoop-stack
check "$LOGDIR exists, owned by hadoop, labelled var_log_t" bash -c "[ \"\$(stat -c '%U' $LOGDIR)\" = hadoop ] && stat -c %C $LOGDIR | grep -q ':var_log_t:'"
for spec in system-log-classifier:classifier.log:/home/hadoop/kafka/logs/classifier.log \
            spark-system-logs:spark_system_logs.log:/home/hadoop/kafka/logs/spark_system_logs.log \
            hiveserver2:hiveserver2.out:/home/hadoop/hive/hiveserver2.out; do
  IFS=: read -r unit file old <<< "$spec"
  for stream in StandardOutput StandardError; do
    check "$unit $stream -> append:$LOGDIR/$file" bash -c "systemctl cat $unit 2>/dev/null | grep -qx '$stream=append:$LOGDIR/$file'"
  done
  check "$LOGDIR/$file exists, owned by hadoop, var_log_t" bash -c "[ \"\$(stat -c '%U' $LOGDIR/$file)\" = hadoop ] && stat -c %C $LOGDIR/$file | grep -q ':var_log_t:'"
  check "$old is a symlink to $LOGDIR/$file" bash -c "[ \"\$(sudo -u hadoop readlink $old)\" = $LOGDIR/$file ]"
done
# No SELinux denial logged (setroubleshoot -> rsyslog -> logs.txt) since the stack last started
SINCE=$(for u in $UNITS; do systemctl show -p InactiveExitTimestamp --value $u; done | xargs -I{} date -d {} +%s 2>/dev/null | sort -n | head -1)
DENIALS=$(sudo -u hadoop grep -hE "^[A-Z][a-z]{2} +[0-9]+ [0-9:]{8} severity=[a-z]+ hostname=[^ ]+ program=setroubleshoot message=SELinux is preventing" /home/hadoop/kafka/logs.txt | python3 -c '
import sys, time
since = int(sys.argv[1]); year = time.localtime().tm_year
for line in sys.stdin:
    try:
        t = time.mktime(time.strptime(f"{year} " + " ".join(line.split()[:3]), "%Y %b %d %H:%M:%S"))
    except ValueError:
        continue
    if t >= since:
        print(line.split("message=", 1)[-1].split(".#012")[0].split(". For complete")[0].strip())
' "${SINCE:-0}" | sort -u)
if [ -z "$DENIALS" ]; then ok "no SELinux denials since the stack started ($(date -d @${SINCE:-0} '+%F %T'))"
else bad "SELinux denials since the stack started:"; echo "$DENIALS" | sed 's/^/          /'; fi

echo "== log source hygiene (rsyslog -> logs.txt)"
# The pipeline must not ingest its own console output (feedback loop) nor authpriv/sudo lines (sensitive)
RSYSLOG_SINCE=$(date -d "$(systemctl show -p ActiveEnterTimestamp --value rsyslog)" +%s 2>/dev/null || echo 0)
since_count() {  # since_count <program regex>: lines from matching programs written after rsyslog started
  sudo -u hadoop grep -hE "^[A-Z][a-z]{2} +[0-9]+ [0-9:]{8} severity=[a-z]+ hostname=[^ ]+ program=($1) " /home/hadoop/kafka/logs.txt | python3 -c '
import sys, time
since = int(sys.argv[1]); year = time.localtime().tm_year; n = 0
for line in sys.stdin:
    try:
        t = time.mktime(time.strptime(f"{year} " + " ".join(line.split()[:3]), "%Y %b %d %H:%M:%S"))
    except ValueError:
        continue
    n += t >= since
print(n)' "$RSYSLOG_SINCE"
}
N=$(since_count 'kafka-server-start\.sh|connect-standalone\.sh'); check "no Kafka/Connect console lines ingested since rsyslog start ($N found)" test "$N" -eq 0
N=$(since_count 'sudo|su|unix_chkpwd'); check "no sudo/authpriv lines ingested since rsyslog start ($N found)" test "$N" -eq 0
MARK="HYGIENE_PROBE_$(date +%s)"; logger -p user.notice "$MARK"; sleep 3
check "ordinary syslog messages still reach logs.txt" sudo -u hadoop grep -q "$MARK" /home/hadoop/kafka/logs.txt

echo "== network exposure"
# Kafka Connect REST can create connectors that read/write files as hadoop: loopback only
check "Connect REST :8083 listens on loopback only" bash -c '
  l=$(ss -Hlnt "sport = :8083" | awk "{print \$4}"); [ -n "$l" ] && ! grep -qvE "^(127\.0\.0\.1|\[::1\]|\[::ffff:127\.0\.0\.1\]):8083$" <<< "$l"'

echo "== service health"
check "kafka answers on 9092"     H 'kafka-broker-api-versions.sh --bootstrap-server localhost:9092'
check "connect REST lists local-file-source" bash -c 'curl -s localhost:8083/connectors | grep -q local-file-source'
check "hdfs has a live datanode, safemode off" H 'hdfs dfsadmin -report 2>/dev/null | grep -q "Live datanodes (1)" && hdfs dfsadmin -safemode get 2>/dev/null | grep -q OFF'
check "yarn has a RUNNING node"   H 'yarn node -list 2>/dev/null | grep -q RUNNING'
check "hive answers a query"      H 'beeline -u jdbc:hive2://localhost:10000 -n hadoop --silent=true -e "SELECT 1;" 2>/dev/null | grep -q 1'

if [ "${1:-}" != "--no-e2e" ]; then
  echo "== end-to-end: logger -> ... -> Hive"
  RUN="STACKTEST_$(date +%H%M%S)"
  for s in info warning err crit debug; do logger -p "user.$s" "${RUN}_$s"; done
  declare -A EXP=([info]=normal [warning]=warning [err]=error [crit]=critical [debug]=debug)
  got=""
  for _ in $(seq 1 12); do
    sleep 10
    got=$(H "beeline -u jdbc:hive2://localhost:10000 -n hadoop --silent=true --outputformat=csv2 --showHeader=false \
      -e \"SELECT message, severity FROM default.system_logs WHERE message LIKE '${RUN}_%';\" 2>/dev/null" | grep "^${RUN}_")
    [ "$(echo "$got" | grep -c .)" -ge 5 ] && break
  done
  for s in info warning err crit debug; do
    check "${RUN}_$s landed in Hive partition ${EXP[$s]}" bash -c "echo '$got' | grep -qx '${RUN}_$s,${EXP[$s]}'"
  done
fi

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
