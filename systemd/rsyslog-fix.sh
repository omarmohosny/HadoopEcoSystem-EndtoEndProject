#!/usr/bin/env bash
# Install the filtered rsyslog rule: stop re-ingesting Kafka/Connect console output (feedback loop)
# and stop shipping authpriv (sudo/su/PAM) lines into the pipeline. Other destinations are untouched.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Run with: sudo $0"; exit 1; }
SRC=/home/hadoop/kafka/rsyslog/kafka-system-logs.conf
DST=/etc/rsyslog.d/kafka-system-logs.conf
BAK="$DST.bak.$(date +%Y%m%d_%H%M%S)"

cp -p "$DST" "$BAK"; echo "backup: $BAK"
install -m 644 "$SRC" "$DST"
restorecon "$DST"
if ! rsyslogd -N1 >/dev/null 2>&1; then
  echo "ERROR: rsyslog config invalid - restoring backup"; cp -p "$BAK" "$DST"; rsyslogd -N1; exit 1
fi
echo "rsyslog config valid"
systemctl restart rsyslog
sleep 2
echo "rsyslog: $(systemctl is-active rsyslog)  since $(systemctl show -p ActiveEnterTimestamp --value rsyslog)"
echo "machine-id: $(cat /etc/machine-id)"
