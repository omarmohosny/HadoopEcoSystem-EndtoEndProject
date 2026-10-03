#!/usr/bin/env bash
# Install systemd units for the whole stack and hand over from manually started processes.
#   HDFS -> YARN -> HiveServer2
#   Kafka -> Kafka Connect -> classifier -> Spark streaming (-> HDFS/Hive)
set -euo pipefail
SRC=/home/hadoop/kafka/systemd
[ "$(id -u)" -eq 0 ] || { echo "Run with: sudo $0"; exit 1; }

UNITS_STOP_ORDER="spark-system-logs system-log-classifier kafka-connect kafka hiveserver2 hadoop-yarn hadoop-hdfs"
UNITS_START_ORDER="hadoop-hdfs hadoop-yarn hiveserver2 kafka kafka-connect system-log-classifier spark-system-logs"

as_hadoop() {  # run a command as hadoop with the stack environment
  runuser -u hadoop -- bash -c 'set -a; . /etc/hadoop-stack.env; set +a; exec "$@"' _ "$@"
}
stop_proc() {  # stop_proc <label> <pgrep pattern> <signal> <seconds>
  pgrep -u hadoop -f "$2" >/dev/null || { echo "  $1: not running"; return 0; }
  echo "  $1: stopping"
  pkill "-$3" -u hadoop -f "$2" || true
  for _ in $(seq 1 "$4"); do pgrep -u hadoop -f "$2" >/dev/null || return 0; sleep 1; done
  echo "ERROR: $1 still running after $4s - aborting before starting services"; exit 1
}

echo "[1/5] Installing unit files, environment file and tmpfiles exclusion"
install -m 644 "$SRC"/hadoop-stack.env /etc/hadoop-stack.env
for u in $UNITS_START_ORDER; do install -m 644 "$SRC/$u.service" /etc/systemd/system/; done
install -m 644 "$SRC"/kafka-tmpfiles.conf /etc/tmpfiles.d/kafka.conf
# systemd-captured service output (systemd may create files here under SELinux, not under /home)
install -d -m 755 -o hadoop -g hadoop /var/log/hadoop-stack
restorecon -R /var/log/hadoop-stack
systemctl daemon-reload
for u in $UNITS_START_ORDER; do systemctl enable "$u.service" 2>&1 | grep -v '^Created symlink' || true; done

echo "[2/5] Stopping units from any previous install"
for u in $UNITS_STOP_ORDER; do systemctl stop "$u.service" 2>/dev/null || true; done

echo "[3/5] Stopping manually started processes (pipeline first, then storage)"
stop_proc "spark stream"   'system_logs_to_hive\.py'                             TERM 90
stop_proc "classifier"     'scripts/system_log_classifier\.py'                   INT  30
stop_proc "kafka connect"  'org\.apache\.kafka\.connect\.cli\.ConnectStandalone' TERM 60
stop_proc "kafka"          'kafka\.Kafka .*server\.properties'                   TERM 120
stop_proc "hiveserver2"    'org\.apache\.hive\.service\.server\.HiveServer2'     TERM 90
for d in nodemanager resourcemanager; do
  pgrep -u hadoop -f "proc_$d" >/dev/null && { echo "  $d: stopping"; as_hadoop yarn --daemon stop "$d" || true; }
  stop_proc "$d" "proc_$d" TERM 60
done
for d in secondarynamenode datanode namenode; do
  pgrep -u hadoop -f "proc_$d" >/dev/null && { echo "  $d: stopping"; as_hadoop hdfs --daemon stop "$d" || true; }
  stop_proc "$d" "proc_$d" TERM 90
done

echo "[4/5] Starting services in dependency order"
for u in $UNITS_START_ORDER; do
  echo "  starting $u"
  systemctl start "$u.service"
done
sleep 20

echo "[5/5] Status"
for u in $UNITS_START_ORDER; do printf '  %-24s %s / %s\n' "$u" "$(systemctl is-active $u)" "$(systemctl is-enabled $u)"; done
echo
echo "Host: $(hostname)  IP: $(hostname -I | awk '{print $1}')  machine-id: $(cat /etc/machine-id)"
