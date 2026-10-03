#!/usr/bin/env bash
# Give the stack correct SELinux labels so it works under Enforcing (SELinux stays ON).
#   - start scripts       -> bin_t      (systemd may exec them; services run as unconfined_service_t)
#   - systemd append logs -> var_log_t  (systemd may open them for StandardOutput=append:)
#   - rsyslog may traverse /home/hadoop/kafka to reach logs.txt (boolean)
# Rules are persistent (semanage) and survive relabels. Then the stack + rsyslog are restarted.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Run with: sudo $0"; exit 1; }

UNITS_STOP_ORDER="spark-system-logs system-log-classifier kafka-connect kafka hiveserver2 hadoop-yarn hadoop-hdfs"
UNITS_START_ORDER="hadoop-hdfs hadoop-yarn hiveserver2 kafka kafka-connect system-log-classifier spark-system-logs"
BIN_DIRS="/home/hadoop/hadoop/bin /home/hadoop/hadoop/sbin /home/hadoop/hadoop/libexec
          /home/hadoop/kafka/bin /home/hadoop/hive/bin /home/hadoop/spark/bin /home/hadoop/spark/sbin"
LOG_FILES="/home/hadoop/kafka/logs.txt /home/hadoop/kafka/logs/classifier.log
           /home/hadoop/kafka/logs/spark_system_logs.log /home/hadoop/hive/hiveserver2.out"

add_rule() {  # add or modify a persistent file-context rule
  semanage fcontext -a -t "$1" "$2" 2>/dev/null || semanage fcontext -m -t "$1" "$2"
  echo "  $1  $2"
}

echo "[1/4] Adding persistent file-context rules"
for d in $BIN_DIRS;   do add_rule bin_t     "$d(/.*)?"; done
for f in $LOG_FILES;  do add_rule var_log_t "$f"; done

echo "[2/4] Applying labels (restorecon)"
restorecon -R $BIN_DIRS
restorecon $LOG_FILES
for f in /home/hadoop/hadoop/bin/hdfs /home/hadoop/kafka/bin/kafka-server-start.sh $LOG_FILES; do
  printf '  %-60s %s\n' "$f" "$(stat -c %C "$f")"
done

echo "[3/4] Allowing rsyslog to traverse non-security directories (persistent boolean)"
setsebool -P logging_syslogd_list_non_security_dirs 1
getsebool logging_syslogd_list_non_security_dirs | sed 's/^/  /'

echo "[4/4] Restarting the stack, then rsyslog, so every process starts with the new labels"
for u in $UNITS_STOP_ORDER;  do systemctl stop  "$u.service"; done
for u in $UNITS_START_ORDER; do echo "  starting $u"; systemctl start "$u.service"; done
systemctl restart rsyslog.service
sleep 20
for u in $UNITS_START_ORDER rsyslog; do printf '  %-24s %s\n' "$u" "$(systemctl is-active $u)"; done

echo
echo "SELinux mode: $(getenforce)   Host: $(hostname)  IP: $(hostname -I | awk '{print $1}')  machine-id: $(cat /etc/machine-id)"
