# Operations runbook

## Start / stop order
- **Start:** `hadoop-hdfs → hadoop-yarn → hiveserver2 → kafka → kafka-connect → system-log-classifier → spark-system-logs`
- **Stop:** the reverse. `install.sh` does this for you.

## Health checks

```bash
systemctl is-active rsyslog kafka kafka-connect system-log-classifier hadoop-hdfs hadoop-yarn hiveserver2 spark-system-logs
free -m                                                   # keep ≥ 1.5 GB available
hdfs dfsadmin -safemode get; hdfs dfsadmin -report | head -20
hdfs fsck / | tail -20                                    # HEALTHY expected
yarn node -list
kafka-get-offsets.sh --bootstrap-server localhost:9092 --topic system-logs-raw
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --describe --group system-log-classifier-v1   # LAG ≈ 0
hdfs dfs -ls /user/hadoop/checkpoints/system_logs/commits | tail -2   # newest file should be < 1 min old
```

Pipeline-level check: each severity topic's end offset ≈ Hive `count(*)` for that severity (a small gap is just
micro-batch lag). `system-logs-raw` offset minus the sum of the five severity offsets = messages the classifier
skipped as unrecognised (see Known issues).

## Runbooks

### HDFS stuck in safe mode
Symptom: `hdfs dfsadmin -safemode get` says ON for minutes; NameNode log says
`reported blocks N needs additional M blocks`.
1. `hdfs fsck / | tail -30` and `hdfs fsck / -list-corruptfileblocks`. After a hard reset, zero-byte block files
   from files that were being written are the usual cause (see the incident report).
2. Compare the DataNode's block files (`find /home/hadoop/hdfs/datanode -name 'blk_*' ! -name '*.meta' -size 0`)
   with the corrupt list.
3. `hdfs dfsadmin -safemode leave` only clears the flag and deletes nothing. Use it once you understand which blocks
   are bad.
4. **Quarantine, don't delete:** move corrupt files to `/user/hadoop/quarantine/<date>/` with `hdfs dfs -mv`.
   Delete them only after you are sure Kafka still holds the data.

### Spark job won't start or fails on checkpoint
- If the newest checkpoint files are corrupt, move **all** of `offsets/N`, `commits/N` and
  `_spark_metadata/N` for that batch aside together. Spark resumes from batch N-1 and re-reads from Kafka, as long as
  topic retention (default 7 days) still covers those offsets.
- Check `/var/log/hadoop-stack/spark_system_logs.log`. The unit waits up to 300 s for HDFS safe mode to be OFF and logs
  `HDFS still in safe mode after 300s` if it never is.

### Querying Hive
Always connect as `hadoop` (`beeline -u jdbc:hive2://localhost:10000 -n hadoop`). As `anonymous` Tez cannot write
its scratch directory under `/user`. Without a TTY add `export TERM=dumb HADOOP_CLIENT_OPTS="-Dorg.jline.terminal.dumb=true -Dorg.jline.terminal.providers=dumb"`.

### Re-process curated and aggregate layers
`spark-submit --master local[2] scripts/system_logs_curated.py --since YYYY-MM-DD`, then
`scripts/system_logs_aggregates.py --since YYYY-MM-DD`. Both overwrite only the affected `event_date` partitions.

### After a hard reset or reboot
1. `systemctl status hadoop-hdfs`; if it failed, check `hdfs dfsadmin -safemode get` and the runbook above.
2. Confirm the classifier lag returns to 0 and the Spark checkpoint advances.
3. Compare Hive counts to Kafka offsets.

## Logs
`/var/log/hadoop-stack/` (classifier, HiveServer2, Spark), `/home/hadoop/hadoop/logs/` (HDFS/YARN),
`journalctl -u <unit>`.

## Known issues
| Issue | Impact | Status |
|---|---|---|
| Hand-started processes do not survive a reboot until `install.sh` is run | pipeline down after reboot | install needs root |
| `dfs.datanode.synconclose` unset | a hard reset can leave zero-byte blocks | recommended: set to `true` |
| Memory ≈ 1.5 GB available with the full stack | risk of OOM or reset | reduce heaps (e.g. `HADOOP_HEAPSIZE_MAX`) |
| Unrecognised log lines are logged `[SKIPPED]` and dropped | raw offset > sum of severity offsets | consider a dead-letter topic |
| Keyword fallback can misclassify (a message containing "Warning" may land in `critical`) | noisy severity labels | heuristic by design |
| `config/server.properties` defines `log.dirs`, `listeners` etc. twice (last value wins, `/tmp/kraft-combined-logs`) | confusing, but working | clean up carefully; never touch the data dir |
| `Type=oneshot` HDFS/YARN units do not restart a daemon that dies later | silent failure | consider `Type=forking` + PID files |
| Raw `system_logs` has no dedup | double counts after a classifier replay | use `system_logs_curated` |
| `systemd/hadoop-yarn` start is not awaited by HiveServer2 | first query right after boot can fail | wait for YARN |
| The classifier producer has no explicit `acks`/`retries` | relies on client defaults | set `acks="all"` |
