#!/usr/bin/env bash
# Validate severity-topic purity: FIRST 5 / LAST 5 / UNEXPECTED per topic.
# Accepts both record formats seen on system-logs-raw:
#   rsyslog lines  ->  severity=<sev>
#   app JSON lines ->  "level":"<SEV>"  (escaped as \"level\":\"<SEV>\" inside the Connect payload)
set -u
cd /home/hadoop/kafka
mkdir -p validation
REPORT="validation/system_logs_validation_$(date +%Y%m%d_%H%M%S).txt"
SNAP=$(mktemp -d)
trap 'rm -rf "$SNAP"' EXIT

declare -A EXPECTED=(
  [normal]='info|notice'
  [warning]='warning|warn'
  [error]='err|error'
  [critical]='crit|critical|alert|emerg'
  [debug]='debug'
)
TOTAL_BAD=0

{
echo "System logs validation report - $(date '+%F %T')"
for t in normal warning error critical debug; do
  topic="system-logs-$t"
  sev="${EXPECTED[$t]}"
  pat="severity=($sev)\b|\\\\?\"level\\\\?\":\\\\?\"($sev)\\\\?\""
  # One snapshot per topic so all three sections describe the same data
  bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic "$topic" --from-beginning --timeout-ms 8000 2>/dev/null > "$SNAP/$t"
  total=$(wc -l < "$SNAP/$t")
  bad=$(grep -Eiv "$pat" "$SNAP/$t" | wc -l)
  TOTAL_BAD=$((TOTAL_BAD + bad))
  echo
  echo "============================================================"
  echo "TOPIC: $topic"
  echo "EXPECTED SEVERITY: ${sev//|/ | }"
  echo "RECORDS: $total   UNEXPECTED: $bad   RESULT: $([ "$bad" -eq 0 ] && echo PASS || echo FAIL)"
  echo "============================================================"
  echo
  echo "----- FIRST 5 EXPECTED RECORDS -----"
  grep -Ei "$pat" "$SNAP/$t" | head -5
  echo
  echo "----- LAST 5 EXPECTED RECORDS -----"
  grep -Ei "$pat" "$SNAP/$t" | tail -5
  echo
  echo "----- UNEXPECTED RECORDS -----"
  grep -Eiv "$pat" "$SNAP/$t"
done
echo
echo "OVERALL: $([ "$TOTAL_BAD" -eq 0 ] && echo PASS || echo "FAIL ($TOTAL_BAD unexpected records)")"
} > "$REPORT"

cp "$REPORT" validation/system_logs_validation_latest.txt
echo "Report: /home/hadoop/kafka/$REPORT"
grep -E '^(TOPIC|RECORDS|OVERALL)' "$REPORT"
