#!/usr/bin/env bash
# Move systemd-captured service output from /home to /var/log/hadoop-stack.
# Under SELinux, systemd may create/append files in var_log_t dirs but not in user_home_t dirs,
# which made classifier, Spark stream and HiveServer2 fail to start at boot under Enforcing.
# Old paths become symlinks so `tail -f` on them keeps working. Log contents are preserved.
set -euo pipefail
[ "$(id -u)" -eq 0 ] || { echo "Run with: sudo $0"; exit 1; }
SRC=/home/hadoop/kafka/systemd
LOGDIR=/var/log/hadoop-stack
UNITS="spark-system-logs system-log-classifier hiveserver2"
MOVES="/home/hadoop/kafka/logs/classifier.log:classifier.log
/home/hadoop/kafka/logs/spark_system_logs.log:spark_system_logs.log
/home/hadoop/hive/hiveserver2.out:hiveserver2.out"

echo "[1/5] Stopping the three affected services"
for u in $UNITS; do systemctl stop "$u.service"; done

echo "[2/5] Creating $LOGDIR"
install -d -m 755 -o hadoop -g hadoop "$LOGDIR"

echo "[3/5] Moving logs and leaving symlinks at the old paths"
while IFS=: read -r old file; do
  new="$LOGDIR/$file"
  if [ -f "$old" ] && [ ! -L "$old" ]; then
    cat "$old" >> "$new"; rm -f "$old"
    echo "  moved   $old -> $new ($(stat -c %s "$new") bytes)"
  fi
  [ -e "$new" ] || install -m 644 -o hadoop -g hadoop /dev/null "$new"
  chown hadoop:hadoop "$new"
  runuser -u hadoop -- ln -sfn "$new" "$old"
  echo "  symlink $old -> $(readlink "$old")"
done <<< "$MOVES"
restorecon -R "$LOGDIR"
ls -lZ "$LOGDIR" | tail -n +2 | sed 's/^/  /'

echo "[4/5] Installing updated unit files"
for u in $UNITS; do install -m 644 "$SRC/$u.service" /etc/systemd/system/; done
systemctl daemon-reload

echo "[5/5] Starting services"
for u in hiveserver2 system-log-classifier spark-system-logs; do systemctl start "$u.service"; done
sleep 15
for u in hiveserver2 system-log-classifier spark-system-logs; do printf '  %-24s %s\n' "$u" "$(systemctl is-active $u)"; done
echo
echo "Next: reboot to verify boot-time behaviour (sudo reboot).  Host: $(hostname)  machine-id: $(cat /etc/machine-id)"
