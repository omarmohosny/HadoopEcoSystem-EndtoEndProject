#!/usr/bin/env bash
# Acceptance tests for the curated layer (Hive table over Spark batch output).
#   bash /home/hadoop/kafka/tests/test_curated.sh
set -u
PASS=0; FAIL=0
ok()  { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
H() { sudo -u hadoop env -i HOME=/home/hadoop bash -c 'set -a; . /home/hadoop/kafka/systemd/hadoop-stack.env; set +a; '"$1"; }
q() {
  local out rc
  out=$(sudo -u hadoop env -i HOME=/home/hadoop bash -c 'set -a; . /home/hadoop/kafka/systemd/hadoop-stack.env; set +a
    beeline -u jdbc:hive2://localhost:10000 -n hadoop --silent=true --outputformat=csv2 --showHeader=false -e "$1" 2>/dev/null' _ "$1")
  rc=$?
  printf '%s\n' "$out" | grep -vE '^(SLF4J|WARN|WARNING|INFO)' | grep . || true
  return $rc
}
expect_empty() {
  local out rc
  out=$(q "$2"); rc=$?
  if [ $rc -ne 0 ]; then bad "$1 (hive error, exit $rc)"; return; fi
  if [ -z "$out" ]; then ok "$1"; else bad "$1"; echo "$out" | head -5 | sed 's/^/          /'; fi
}
TODAY=$(date +%F)
NOISE="program LIKE 'kafka-%' OR program LIKE 'connect-%' OR program IN ('sudo','su','unix_chkpwd')"
DKEY="coalesce(raw_key, concat_ws(':', 'kafka', kafka_topic, cast(kafka_partition AS STRING), cast(kafka_offset AS STRING)))"
EDATE="date_format(coalesce(event_time, kafka_timestamp),'yyyy-MM-dd')"

echo "== table"
if q "SHOW TABLES IN default LIKE 'system_logs_curated';" | grep -qx system_logs_curated; then ok "table system_logs_curated exists"; else bad "table system_logs_curated exists"; fi

echo "== correctness"
expect_empty "no duplicate dedup_key" \
  "SELECT dedup_key FROM default.system_logs_curated GROUP BY dedup_key HAVING COUNT(*) > 1 LIMIT 5;"
expect_empty "no self-ingested or auth noise" \
  "SELECT program, COUNT(*) FROM default.system_logs_curated WHERE $NOISE GROUP BY program;"
expect_empty "completed days: curated rows == distinct noise-free source records" "
  SELECT s.d, s.n, c.n FROM
    (SELECT $EDATE d, COUNT(DISTINCT $DKEY) n FROM default.system_logs
     WHERE NOT (coalesce(program,'') LIKE 'kafka-%' OR coalesce(program,'') LIKE 'connect-%' OR coalesce(program,'') IN ('sudo','su','unix_chkpwd'))
     GROUP BY $EDATE) s
  LEFT JOIN (SELECT event_date, COUNT(*) n FROM default.system_logs_curated GROUP BY event_date) c
  ON s.d = c.event_date WHERE s.d < '$TODAY' AND (c.n IS NULL OR c.n <> s.n);"
expect_empty "every row has event_ts, event_date, severity and source" \
  "SELECT event_date FROM default.system_logs_curated WHERE event_ts IS NULL OR event_date IS NULL OR severity IS NULL OR source IS NULL LIMIT 5;"

echo "== layout"
FILES=$(H 'hdfs dfs -count /user/hive/warehouse/system_logs_curated/event_date=*/severity=* 2>/dev/null' | awk '{print $2, $4}')
if [ -z "$FILES" ]; then bad "curated partitions exist on HDFS"
else
  ok "curated partitions exist on HDFS ($(echo "$FILES" | wc -l) partitions)"
  OVER=$(echo "$FILES" | awk '$1 > 4')
  if [ -z "$OVER" ]; then ok "at most 4 files per partition"; else bad "partitions with > 4 files:"; echo "$OVER" | head -3 | sed 's/^/          /'; fi
fi
expect_empty "Hive has a partition registered for every curated date" "
  SELECT d FROM (SELECT DISTINCT $EDATE d FROM default.system_logs) s
  LEFT JOIN (SELECT DISTINCT event_date FROM default.system_logs_curated) c ON s.d = c.event_date
  WHERE c.event_date IS NULL AND s.d < '$TODAY';"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
