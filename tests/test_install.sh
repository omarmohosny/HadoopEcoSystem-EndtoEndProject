#!/usr/bin/env bash
# install.sh steps 4-5 must survive a unit that fails to start (e.g. HDFS stuck in safe mode):
# every unit still gets a start attempt, the status block is printed, safe mode is reported,
# and the exit status tells the operator something failed. Runs with stub systemctl/runuser: no root,
# no real services touched.
#   bash /home/hadoop/kafka/tests/test_install.sh [path/to/install.sh]
set -u
INSTALL=${1:-/home/hadoop/kafka/systemd/install.sh}
PASS=0; FAIL=0
ok()  { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
R() { cat "$1" 2>/dev/null || sudo -n -u hadoop cat "$1" 2>/dev/null; }

SRC=$(R "$INSTALL"); [ -n "$SRC" ] || { echo "  FAIL  cannot read $INSTALL"; exit 2; }
TMP=$(mktemp -d); trap 'rm -rf "$TMP"' EXIT; mkdir "$TMP/bin"

# harness = the same preamble install.sh uses + its steps 4-5 verbatim
{ echo 'set -euo pipefail'
  grep '^UNITS_START_ORDER=' <<< "$SRC"
  sed -n '/^as_hadoop()/,/^}/p' <<< "$SRC"
  sed -n '/^echo "\[4\/5\]/,$p' <<< "$SRC"; } > "$TMP/steps.sh"
# NOTE: steps 4-5 must only use variables defined in the extracted preamble (UNITS_START_ORDER, as_hadoop)
grep -q '^UNITS_START_ORDER=.' "$TMP/steps.sh" && grep -q '^as_hadoop()' "$TMP/steps.sh" || { echo "  FAIL  cannot extract UNITS_START_ORDER / as_hadoop from $INSTALL (reformatted?)"; exit 2; }
grep -q '\[4/5\]' "$TMP/steps.sh" || { echo "  FAIL  no step [4/5] found in $INSTALL"; exit 2; }

cat > "$TMP/bin/systemctl" <<'S'
#!/bin/sh
echo "systemctl $*" >> "$STUB_LOG"
case "$1 $2" in
  "start $FAIL_UNIT.service") exit 1;;
  "is-active "*) [ "$2" = "$FAIL_UNIT" ] && echo failed || echo active;;
  "is-enabled "*) echo enabled;;
esac
exit 0
S
printf '#!/bin/sh\nexit 0\n' > "$TMP/bin/sleep"
# runuser -u hadoop -- bash -c '...' _ hdfs dfsadmin -safemode get   -> report the stubbed mode
cat > "$TMP/bin/runuser" <<'S'
#!/bin/sh
echo "runuser $*" >> "$STUB_LOG"
case "$*" in *"-safemode get"*) [ "$SAFEMODE" = DOWN ] && exit 1; echo "Safe mode is $SAFEMODE";; esac
exit 0
S
chmod +x "$TMP/bin/"*

run() { # <failing unit or none> <ON|OFF>  -> sets OUT, RC, LOG
  : > "$TMP/log"
  OUT=$(PATH="$TMP/bin:$PATH" STUB_LOG="$TMP/log" FAIL_UNIT="$1" SAFEMODE="$2" bash "$TMP/steps.sh" 2>&1); RC=$?
  LOG=$(cat "$TMP/log")
}

echo "== install.sh survives a unit that fails to start ($INSTALL)"
run hiveserver2 ON
for u in hadoop-hdfs hadoop-yarn hiveserver2 kafka kafka-connect system-log-classifier spark-system-logs; do
  if grep -q "^systemctl start $u.service" <<< "$LOG"; then ok "start attempted: $u"; else bad "start attempted: $u (aborted before it)"; fi
done
if grep -q '\[5/5\] Status' <<< "$OUT"; then ok "status block still printed after a failed start"; else bad "status block still printed after a failed start"; fi
if grep -q 'hiveserver2.*failed' <<< "$OUT" && grep -qi 'warn.*hiveserver2\|hiveserver2.*fail.*start' <<< "$OUT"; then ok "the failed unit is named in a warning"; else bad "the failed unit is named in a warning"; fi
if [ "$RC" -ne 0 ]; then ok "exit status non-zero when a unit failed to start"; else bad "exit status non-zero when a unit failed to start (got 0)"; fi
if grep -q 'WARNING.*safe mode' <<< "$OUT"; then ok "status warns that HDFS is in safe mode"; else bad "status warns that HDFS is in safe mode"; fi
if grep -qi 'WARNING.*hadoop-hdfs' <<< "$OUT"; then bad "only the failed unit is named in a warning (hadoop-hdfs wrongly named)"; else ok "only the failed unit is named in a warning"; fi
# dependency order of the start attempts
ORD=$(grep -o '^systemctl start [a-z-]*' <<< "$LOG" | sed 's/systemctl start //' | tr '
' ' ')
for pair in "hadoop-hdfs hadoop-yarn" "hadoop-yarn hiveserver2" "kafka kafka-connect" "kafka-connect system-log-classifier" "system-log-classifier spark-system-logs"; do
  set -- $pair; a=$(grep -n "systemctl start $1.service" <<< "$LOG" | head -1 | cut -d: -f1); b=$(grep -n "systemctl start $2.service" <<< "$LOG" | head -1 | cut -d: -f1)
  if [ -n "$a" ] && [ -n "$b" ] && [ "$a" -lt "$b" ]; then ok "start order: $1 before $2"; else bad "start order: $1 before $2"; fi
done
# reset-failed must precede each start, otherwise a unit left 'failed' by a previous run stays blocked
for u in hadoop-hdfs spark-system-logs; do
  r=$(grep -n "^systemctl reset-failed $u" <<< "$LOG" | head -1 | cut -d: -f1); s=$(grep -n "^systemctl start $u.service" <<< "$LOG" | head -1 | cut -d: -f1)
  if [ -n "$r" ] && [ -n "$s" ] && [ "$r" -lt "$s" ]; then ok "reset-failed precedes start: $u"; else bad "reset-failed precedes start: $u"; fi
done

echo "== HDFS unreachable (dfsadmin fails)"
run none DOWN
if [ "$RC" -eq 0 ]; then ok "unreachable HDFS does not abort the script (exit 0, all units started)"; else bad "unreachable HDFS does not abort the script (got $RC)"; fi
if grep -q 'HDFS: unreachable' <<< "$OUT" && grep -q 'WARNING.*safe mode or unreachable' <<< "$OUT"; then ok "status says unreachable and warns"; else bad "status says unreachable and warns"; fi

echo "== healthy run"
run none OFF
if [ "$RC" -eq 0 ]; then ok "exit status 0 when every unit starts"; else bad "exit status 0 when every unit starts (got $RC)"; fi
if grep -qi 'warn' <<< "$OUT"; then bad "no warning when everything is healthy and safe mode is OFF"; else ok "no warning when everything is healthy and safe mode is OFF"; fi

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
