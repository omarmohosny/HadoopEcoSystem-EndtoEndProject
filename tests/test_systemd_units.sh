#!/usr/bin/env bash
# Static + behavioural checks on the HDFS-dependent units: a stuck HDFS safe mode must not take HDFS
# down (hadoop-hdfs) and must make the dependents fail clearly (spark, hive) instead of crash-looping.
# Incident 2026-10-04 02:44: 'dfsadmin -safemode wait' hit the 5-min start timeout, systemd failed
# the unit and SIGTERMed NameNode + DataNode. Runs on the repo copies; no install needed.
#   bash /home/hadoop/kafka/tests/test_systemd_units.sh [path/to/hadoop-hdfs.service]
# The dependents (spark-system-logs, hiveserver2) are read from the same directory as the given unit.
set -u
UNIT=${1:-/home/hadoop/kafka/systemd/hadoop-hdfs.service}
PASS=0; FAIL=0
ok()  { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
R() { cat "$1" 2>/dev/null || sudo -n -u hadoop cat "$1" 2>/dev/null; }  # works as hadoop or via sudo
# live (non-comment) lines of a unit, with line numbers
live() { grep -nv '^[[:space:]]*#' <<< "$1"; }

CONTENT=$(R "$UNIT")
[ -n "$CONTENT" ] || { echo "  FAIL  cannot read $UNIT"; exit 2; }

echo "== hadoop-hdfs safe-mode handling ($UNIT)"
POST=$(live "$CONTENT" | sed 's/^[0-9]*://' | grep '^ExecStartPost=')
L1=$(sed -n 1p <<< "$POST"); L2=$(sed -n 2p <<< "$POST")
TS=$(live "$CONTENT" | sed -n 's/^[0-9]*:TimeoutStartSec=//p')

if grep -q '/dev/tcp/localhost/9000' <<< "$L1" && [[ "$L1" != ExecStartPost=-* ]]; then
  ok "NameNode port wait is first and stays fatal (no '-' prefix)"; else bad "NameNode port wait is first and stays fatal (no '-' prefix)"; fi
if grep -Eq '^ExecStartPost=-/usr/bin/timeout [0-9]+ .*dfsadmin -safemode wait' <<< "$L2"; then
  ok "safemode wait is non-fatal ('-') and bounded by timeout"; else bad "safemode wait is non-fatal ('-') and bounded by timeout"; fi
T=$(sed -nE 's/^ExecStartPost=-\/usr\/bin\/timeout ([0-9]+) .*/\1/p' <<< "$L2")
if [ -n "$T" ] && [ -n "$TS" ] && [ "$TS" -gt $((180 + T)) ]; then
  ok "TimeoutStartSec ($TS) > 180s port wait + ${T}s safemode wait"; else bad "TimeoutStartSec (${TS:-unset}) > 180s port wait + safemode timeout (${T:-none})"; fi

echo "== dependents wait for HDFS to accept writes (fatal ExecStartPre, so a stuck safe mode fails clearly)"
DIR=$(dirname "$UNIT")
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT
for SPEC in spark-system-logs:480 hiveserver2:360; do
  U=${SPEC%%:*}; MIN=${SPEC##*:}
  C=$(R "$DIR/$U.service")
  [ -n "$C" ] || { bad "$U: cannot read $DIR/$U.service"; continue; }
  LIVE=$(live "$C")
  GL=$(grep -E '^[0-9]+:ExecStartPre=.*dfsadmin -safemode get' <<< "$LIVE")
  if [ "$(grep -c . <<< "$GL")" = 1 ] && grep -Eq "^[0-9]+:ExecStartPre=/bin/bash -c '" <<< "$GL"; then
    ok "$U: exactly one live guard, fatal (no '-' prefix)"; else bad "$U: exactly one live guard, fatal (no '-' prefix)"; fi
  GN=${GL%%:*}; EN=$(grep -E '^[0-9]+:ExecStart=' <<< "$LIVE" | head -1 | cut -d: -f1)
  KN=$(grep -E '^[0-9]+:ExecStartPre=.*/dev/tcp/localhost/9092' <<< "$LIVE" | head -1 | cut -d: -f1)
  ORDER=1; [ -n "$GN" ] && [ -n "$EN" ] && [ "$GN" -lt "$EN" ] || ORDER=0
  [ "$U" = spark-system-logs ] && { [ -n "$KN" ] && [ -n "$GN" ] && [ "$KN" -lt "$GN" ] || ORDER=0; }
  if [ "$ORDER" = 1 ]; then ok "$U: guard runs before ExecStart$([ "$U" = spark-system-logs ] && echo ' and after the Kafka wait')"; else bad "$U: guard runs before ExecStart$([ "$U" = spark-system-logs ] && echo ' and after the Kafka wait')"; fi
  DTS=$(sed -n 's/^[0-9]*:TimeoutStartSec=//p' <<< "$LIVE")
  if [ -n "$DTS" ] && [ "$DTS" -ge "$MIN" ]; then ok "$U: TimeoutStartSec ($DTS) >= $MIN"; else bad "$U: TimeoutStartSec (${DTS:-unset}) >= $MIN"; fi

  # behaviour: run the guard body against a fake hdfs (loop shortened to 2 x 0s)
  BODY=$(sed -E "s/^[0-9]+:ExecStartPre=\/bin\/bash -c '//; s/'\$//" <<< "$GL" | sed -e 's/\$\$/$/g' -e 's/seq 1 [0-9]*/seq 1 2/' -e 's/sleep [0-9]*/sleep 0/' -e "s#/home/hadoop/hadoop/bin/hdfs#$TMP/hdfs#")
  for MODE in ON OFF; do
    printf '#!/bin/sh\necho "Safe mode is %s"\n' "$MODE" > "$TMP/hdfs"; chmod +x "$TMP/hdfs"
    bash -c "$BODY" >/dev/null 2>&1; RC=$?
    WANT=1; [ "$MODE" = OFF ] && WANT=0
    if [ "$RC" = "$WANT" ]; then ok "$U: guard exits $WANT when safe mode is $MODE"; else bad "$U: guard exits $WANT when safe mode is $MODE (got $RC)"; fi
  done
  printf '#!/bin/sh\nexit 1\n' > "$TMP/hdfs"; chmod +x "$TMP/hdfs"
  bash -c "$BODY" >/dev/null 2>&1; RC=$?
  if [ "$RC" = 1 ]; then ok "$U: guard exits 1 when HDFS is down"; else bad "$U: guard exits 1 when HDFS is down (got $RC)"; fi
done

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
