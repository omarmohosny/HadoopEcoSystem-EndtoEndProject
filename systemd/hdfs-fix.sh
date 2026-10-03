#!/usr/bin/env bash
# Install the fixed hadoop-hdfs unit (waits for NameNode RPC :9000 before 'safemode wait')
# and bring HDFS plus everything that depends on it back up.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Run with: sudo $0"; exit 1; }
install -m 644 /home/hadoop/kafka/systemd/hadoop-hdfs.service /etc/systemd/system/
systemctl daemon-reload
systemctl reset-failed hadoop-hdfs.service || true
for u in hadoop-hdfs hadoop-yarn hiveserver2 spark-system-logs; do echo "starting $u"; systemctl start "$u.service"; done
sleep 15
for u in hadoop-hdfs hadoop-yarn hiveserver2 kafka kafka-connect system-log-classifier spark-system-logs; do printf '  %-24s %s\n' "$u" "$(systemctl is-active $u)"; done
echo; echo "Next: sudo reboot  (verifies the boot race is gone).  machine-id: $(cat /etc/machine-id)"
