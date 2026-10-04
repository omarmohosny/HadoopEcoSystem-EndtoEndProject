# Setup

## 1. Host prerequisites
- CentOS Stream 10 (or similar systemd Linux), ≥ 7 GB RAM, ≥ 20 GB free disk.
- A `hadoop` user that owns the installs; sudo/root for the systemd install.
- OpenJDK 21 at `/usr/lib/jvm/java-21-openjdk`.
- Installed under `/home/hadoop/`: `kafka` (4.2.2, this repo lives in its root), `hadoop` (3.4.3), `spark` (4.2.0),
  `hive` (4.2.1), `tez` (0.10.5).
- Python 3 with `kafka-python` (3.0.11) and, for tests, `pytest` and `pyspark`.
- `rsyslog` installed and running.

> Safety rules from this project: never delete `/tmp/kraft-combined-logs`, never re-format KRaft storage,
> do not reinstall or upgrade Kafka, and do not disable SELinux (use the provided `selinux-fix.sh`).

## 2. Environment file
`systemd/hadoop-stack.env` holds `JAVA_HOME`, `HADOOP_*`, `SPARK_HOME`, `HIVE_HOME`, `TEZ_*`, `KAFKA_HOME` and `PATH`.
systemd does not expand variables, so every value is literal. `install.sh` copies it to `/etc/hadoop-stack.env`.

## 3. Configs that live outside the repo
`conf-snapshots/` keeps copies of `yarn-site.xml`, `tez-site.xml`, `hive-site.xml`; the live locations are in
`conf-snapshots/README.md`. The rsyslog rule goes to `/etc/rsyslog.d/kafka-system-logs.conf`.

## 4. Install and start
```bash
sudo /home/hadoop/kafka/systemd/install.sh
```
It runs five steps: install units and env → stop old units → stop hand-started processes (pipeline first, then
storage) → start services in order with `reset-failed` before each → print status, including HDFS safe-mode state.
If a unit fails to start the script continues, prints a warning naming it, and exits 1 at the end.
With HDFS stuck in safe mode a start can take several minutes (up to 6 min for hdfs/hive, 9 for spark).

## 5. Create the tables
```bash
for f in system_logs_hive system_logs_curated_hive system_logs_aggregates_hive; do
  beeline -u jdbc:hive2://localhost:10000 -n hadoop -f scripts/$f.sql
done
```

## 6. Verify
```bash
bash scripts/validate_system_logs.sh             # per-topic severity purity report
beeline -u jdbc:hive2://localhost:10000 -n hadoop -e "SELECT severity, count(*) FROM default.system_logs GROUP BY severity;"
systemctl is-active rsyslog kafka kafka-connect system-log-classifier hadoop-hdfs hadoop-yarn hiveserver2 spark-system-logs
hdfs dfsadmin -safemode get          # must say OFF
```
Compare each topic's end offset (`kafka-get-offsets.sh`) with the Hive row count; they should match within one
micro-batch.

## 7. Run the batch layers
```bash
spark-submit --master local[2] scripts/system_logs_curated.py       # add --since YYYY-MM-DD to rebuild a range
spark-submit --master local[2] scripts/system_logs_aggregates.py
```
Both are idempotent (dynamic partition overwrite).

## 8. Reboot test
After installing, `sudo reboot`, wait a few minutes, and confirm every unit is `active` and HDFS safe mode is OFF.
