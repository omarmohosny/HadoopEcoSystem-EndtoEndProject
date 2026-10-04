#!/usr/bin/env bash
# Acceptance tests: project is under git, tracks only project files, and the tree is clean.
#   bash /home/hadoop/kafka/tests/test_repo.sh
set -u
PASS=0; FAIL=0
ok()  { echo "  PASS  $1"; PASS=$((PASS+1)); }
bad() { echo "  FAIL  $1"; FAIL=$((FAIL+1)); }
G() { sudo -u hadoop bash -c 'cd /home/hadoop/kafka && ~/.local/bin/git "$@"' _ "$@"; }

echo "== repository"
if G rev-parse --is-inside-work-tree >/dev/null 2>&1; then ok "/home/hadoop/kafka is a git work tree"; else bad "/home/hadoop/kafka is a git work tree"; fi
if G log -1 --format=%H >/dev/null 2>&1; then ok "has at least one commit"; else bad "has at least one commit"; fi
TRACKED=$(G ls-files 2>/dev/null)

echo "== project files are tracked"
for f in scripts/system_log_classifier.py scripts/system_logs_to_hive.py scripts/system_logs_aggregates.py \
         scripts/system_logs_hive.sql scripts/system_logs_aggregates_hive.sql scripts/validate_system_logs.sh \
         tests/conftest.py tests/test_classifier.py tests/test_spark_transform.py tests/test_aggregates.py \
         tests/test_stack.sh tests/test_aggregates.sh tests/test_yarn_memory.sh tests/test_repo.sh tests/test_systemd_units.sh tests/test_install.sh \
         systemd/install.sh systemd/hadoop-stack.env systemd/kafka.service systemd/hadoop-hdfs.service \
         config/server.properties config/connect-standalone.properties config/omarconnector.properties \
         rsyslog/kafka-system-logs.conf conf-snapshots/yarn-site.xml conf-snapshots/tez-site.xml conf-snapshots/hive-site.xml \
         .gitignore; do
  if grep -qx "$f" <<< "$TRACKED"; then ok "tracked: $f"; else bad "tracked: $f"; fi
done

echo "== nothing that must stay out of git"
LEAK=$(grep -E '(^|/)logs\.txt$|\.bak|\.jar$|\.tgz$|\.tar\.gz$|^libs/|^bin/|^logs/|^site-docs/|__pycache__|^validation/.*\.txt$|metastore_db|\.pid$' <<< "$TRACKED")
if [ -z "$LEAK" ]; then ok "no data, logs, binaries, backups or caches tracked"; else bad "forbidden files tracked:"; echo "$LEAK" | head -5 | sed 's/^/          /'; fi
BIG=$(G ls-files -z 2>/dev/null | sudo -u hadoop bash -c 'cd /home/hadoop/kafka && xargs -0 -r du -k' | awk '$1 > 512 {print $2}')
if [ -z "$BIG" ]; then ok "no tracked file larger than 512 KB"; else bad "large files tracked: $BIG"; fi

echo "== working tree"
DIRTY=$(G status --porcelain 2>/dev/null)
if G rev-parse >/dev/null 2>&1 && [ -z "$DIRTY" ]; then ok "working tree clean"; else bad "working tree clean"; echo "$DIRTY" | head -5 | sed 's/^/          /'; fi

echo
echo "RESULT: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
