#!/usr/bin/env bash
# Acceptance tests: YARN/Tez memory must fit the VM, and Hive-on-Tez must still work.
#   bash /home/hadoop/kafka/tests/test_yarn_memory.sh
set -u
PASS=0; FAIL=0
ok()  { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
check(){ local name="$1"; shift; if "$@" >/dev/null 2>&1; then ok "$name"; else bad "$name"; fi; }
H() { sudo -u hadoop env -i HOME=/home/hadoop bash -c 'set -a; . /home/hadoop/kafka/systemd/hadoop-stack.env; set +a; '"$1"; }
metric() { curl -s localhost:8088/ws/v1/cluster/metrics | python3 -c "import json,sys;print(json.load(sys.stdin)['clusterMetrics']['$1'])"; }

YARN_MB_MAX=3072      # ceiling for YARN on this 7.5 GB VM (steady services use ~3.8 GB)
YARN_MB_MIN=2048      # floor: AM + a few 512 MB tasks must fit
CONTAINER_MB=512      # Tez AM / task size
YARN_BUDGET_MB=2560   # what a live query may hold at peak

echo "== resourcemanager view"
TOTAL_MB=$(metric totalMB); TOTAL_VC=$(metric totalVirtualCores)
check "YARN memory ${TOTAL_MB} MB is within ${YARN_MB_MIN}..${YARN_MB_MAX}" test "$TOTAL_MB" -ge $YARN_MB_MIN -a "$TOTAL_MB" -le $YARN_MB_MAX
check "YARN vcores ${TOTAL_VC} == CPUs $(nproc)" test "$TOTAL_VC" -eq "$(nproc)"

echo "== scheduler limits"
MAX_ALLOC=$(H 'hdfs getconf -confKey yarn.scheduler.maximum-allocation-mb 2>/dev/null')
MIN_ALLOC=$(H 'hdfs getconf -confKey yarn.scheduler.minimum-allocation-mb 2>/dev/null')
check "max allocation ${MAX_ALLOC} MB <= YARN memory" test "$MAX_ALLOC" -le "$TOTAL_MB"
check "min allocation ${MIN_ALLOC} MB <= container size ${CONTAINER_MB}" test "$MIN_ALLOC" -le $CONTAINER_MB

echo "== tez container sizes"
for k in tez.am.resource.memory.mb tez.task.resource.memory.mb; do
  v=$(sudo -u hadoop python3 -c "
import xml.etree.ElementTree as E
print(next((p.findtext('value') for p in E.parse('/home/hadoop/hive/conf/tez-site.xml').getroot().iter('property') if p.findtext('name')=='$k'), 'unset'))")
  check "$k = $v (want $CONTAINER_MB)" test "$v" = "$CONTAINER_MB"
done
HTC=$(sudo -u hadoop python3 -c "
import xml.etree.ElementTree as E
print(next((p.findtext('value') for p in E.parse('/home/hadoop/hive/conf/hive-site.xml').getroot().iter('property') if p.findtext('name')=='hive.tez.container.size'), 'unset'))")
check "hive.tez.container.size = $HTC (want $CONTAINER_MB)" test "$HTC" = "$CONTAINER_MB"

echo "== live Hive-on-Tez query stays inside the budget"
PEAK_FILE=$(mktemp); echo 0 > "$PEAK_FILE"
( while :; do a=$(metric allocatedMB 2>/dev/null || echo 0); [ "$a" -gt "$(cat $PEAK_FILE)" ] && echo "$a" > "$PEAK_FILE"; sleep 1; done ) &
SAMPLER=$!
START=$(date +%s)
RESULT=$(H 'beeline -u jdbc:hive2://localhost:10000 -n hadoop --silent=true --outputformat=csv2 --showHeader=false \
  -e "SELECT severity, COUNT(*) FROM default.system_logs GROUP BY severity;" 2>/dev/null' | grep -cE '^(normal|warning|error|critical|debug),[0-9]+$')
ELAPSED=$(( $(date +%s) - START ))
kill $SAMPLER 2>/dev/null; wait $SAMPLER 2>/dev/null
PEAK=$(cat "$PEAK_FILE"); rm -f "$PEAK_FILE"
check "GROUP BY on Tez returned all 5 severities (${ELAPSED}s)" test "$RESULT" -eq 5
check "peak YARN allocation ${PEAK} MB <= budget ${YARN_BUDGET_MB} MB" test "$PEAK" -le $YARN_BUDGET_MB
LAST_STATE=$(curl -s 'localhost:8088/ws/v1/cluster/apps?applicationTypes=TEZ&limit=1' | python3 -c "
import json,sys; a=(json.load(sys.stdin).get('apps') or {}).get('app',[]); print(sorted(a,key=lambda x:x['startedTime'])[-1]['finalStatus'] if a else 'NONE')")
check "no Tez application FAILED or KILLED (latest: $LAST_STATE)" bash -c "[ '$LAST_STATE' != FAILED ] && [ '$LAST_STATE' != KILLED ]"

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
