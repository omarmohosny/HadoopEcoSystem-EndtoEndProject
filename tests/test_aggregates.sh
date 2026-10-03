#!/usr/bin/env bash
# Acceptance tests for the aggregation layer (Hive tables over Spark batch output).
#   bash /home/hadoop/kafka/tests/test_aggregates.sh
set -u
PASS=0; FAIL=0
ok()  { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
q() {  # run a HiveQL query, print csv rows without header; returns beeline's exit status
  local out rc
  out=$(sudo -u hadoop env -i HOME=/home/hadoop bash -c 'set -a; . /home/hadoop/kafka/systemd/hadoop-stack.env; set +a
    beeline -u jdbc:hive2://localhost:10000 -n hadoop --silent=true --outputformat=csv2 --showHeader=false -e "$1" 2>/dev/null' _ "$1")
  rc=$?
  printf '%s\n' "$out" | grep -vE '^(SLF4J|WARN|WARNING|INFO)' | grep . || true
  return $rc
}
expect_empty() {  # expect_empty <name> <query returning offending rows>; a Hive error is a failure
  local out rc
  out=$(q "$2"); rc=$?
  if [ $rc -ne 0 ]; then bad "$1 (hive error, exit $rc)"; return; fi
  if [ -z "$out" ]; then ok "$1"; else bad "$1"; echo "$out" | head -5 | sed 's/^/          /'; fi
}
TODAY=$(date +%F)

echo "== tables"
for t in system_logs_hourly system_logs_daily_sources; do
  if q "SHOW TABLES IN default LIKE '$t';" | grep -qx "$t"; then ok "table $t exists"; else bad "table $t exists"; fi
done

echo "== partitions cover every source date"
expect_empty "hourly has a partition for every system_logs date" "
  SELECT s.d FROM (SELECT DISTINCT date_format(coalesce(event_time, kafka_timestamp),'yyyy-MM-dd') d FROM default.system_logs) s
  LEFT JOIN (SELECT DISTINCT event_date FROM default.system_logs_hourly) h ON s.d = h.event_date
  WHERE h.event_date IS NULL AND s.d < '$TODAY';"
expect_empty "daily_sources has a partition for every hourly date" "
  SELECT h.event_date FROM (SELECT DISTINCT event_date FROM default.system_logs_hourly) h
  LEFT JOIN (SELECT DISTINCT event_date FROM default.system_logs_daily_sources) d ON h.event_date = d.event_date
  WHERE d.event_date IS NULL;"

echo "== reconciliation"
expect_empty "hourly total == daily total for every date" "
  SELECT h.event_date, h.n, d.n FROM
    (SELECT event_date, SUM(event_count) n FROM default.system_logs_hourly GROUP BY event_date) h
  FULL OUTER JOIN
    (SELECT event_date, SUM(total_events) n FROM default.system_logs_daily_sources GROUP BY event_date) d
  ON h.event_date = d.event_date WHERE h.n IS NULL OR d.n IS NULL OR h.n <> d.n;"
expect_empty "completed days: hourly total == curated rows" "
  SELECT c.event_date, c.n, h.n FROM
    (SELECT event_date, COUNT(*) n FROM default.system_logs_curated GROUP BY event_date) c
  LEFT JOIN (SELECT event_date, SUM(event_count) n FROM default.system_logs_hourly GROUP BY event_date) h
  ON c.event_date = h.event_date WHERE c.event_date < '$TODAY' AND (h.n IS NULL OR h.n <> c.n);"
expect_empty "today: hourly total <= curated rows" "
  SELECT c.n, h.n FROM
    (SELECT COUNT(*) n FROM default.system_logs_curated WHERE event_date = '$TODAY') c
  CROSS JOIN (SELECT COALESCE(SUM(event_count),0) n FROM default.system_logs_hourly WHERE event_date = '$TODAY') h
  WHERE h.n > c.n;"
expect_empty "aggregates contain no self-ingested or auth noise" "
  SELECT source, SUM(event_count) FROM default.system_logs_hourly
  WHERE source LIKE 'kafka-%' OR source LIKE 'connect-%' OR source IN ('sudo','su','unix_chkpwd') GROUP BY source;"

echo "== derived values"
expect_empty "error_rank is 1..n with no gaps or duplicates per day" "
  SELECT event_date FROM default.system_logs_daily_sources GROUP BY event_date
  HAVING MIN(error_rank) <> 1 OR MAX(error_rank) <> COUNT(*) OR COUNT(DISTINCT error_rank) <> COUNT(*);"
expect_empty "rank 1 has the most error_events each day" "
  SELECT d.event_date FROM default.system_logs_daily_sources d
  JOIN (SELECT event_date, MAX(error_events) m FROM default.system_logs_daily_sources GROUP BY event_date) x
  ON d.event_date = x.event_date WHERE d.error_rank = 1 AND d.error_events <> x.m;"
expect_empty "error_rate within [0,1] and == error_events/total_events" "
  SELECT source, event_date FROM default.system_logs_daily_sources
  WHERE error_rate < 0 OR error_rate > 1 OR abs(error_rate - error_events / total_events) > 0.0001;"
expect_empty "no NULL source or severity in hourly" "
  SELECT event_date FROM default.system_logs_hourly WHERE source IS NULL OR severity IS NULL LIMIT 5;"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
