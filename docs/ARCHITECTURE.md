# Architecture

## 1. Data flow, stage by stage

### 1.1 rsyslog → `logs.txt`
`rsyslog/kafka-system-logs.conf` (installed to `/etc/rsyslog.d/`) writes one line per syslog message to
`/home/hadoop/kafka/logs.txt` using this template:

```
<time> severity=<syslog severity> hostname=<host> program=<program> message=<text>
```

Excluded from the file, because they would pollute or endanger the data:
- `kafka-*` and `connect-*` programs: the pipeline's own console output goes to the journal and would be
  re-ingested (a feedback loop that once dominated the data).
- `authpriv` and `su`: sudo/su/PAM records contain full command lines and authentication details.

Only this destination is filtered; `/var/log/messages` and the journal keep everything.

### 1.2 Kafka Connect → `system-logs-raw`
`config/omarconnector.properties` defines a `FileStreamSource` connector that tails `logs.txt` into the topic
`system-logs-raw`. The worker (`config/connect-standalone.properties`) uses `JsonConverter` with schemas enabled,
so each Kafka value is `{"schema": {...}, "payload": "<log line>"}`.

### 1.3 Classifier (`scripts/system_log_classifier.py`)
Consumes `system-logs-raw` (group `system-log-classifier-v1`) and routes each record to a severity topic.
Detection order:

1. **JSON wrapper:** unwrap `payload`. If the payload is itself JSON with a `level` field, trust it.
2. **rsyslog `severity=`:** map the syslog severity name.
3. **Keyword fallback:** `CRITICAL/CRIT/ALERT/EMERG`, `ERROR/ERR`, `WARNING/WARN`, `DEBUG`, `INFO/NOTICE`.
4. **Unknown:** logged as `[SKIPPED]` and not forwarded (its offset is still committed).

Severity mapping: `info, notice → normal` · `warning, warn → warning` · `err, error → error` ·
`crit, critical, alert, emerg → critical` · `debug → debug`.

**Delivery guarantee.** Auto-commit is off. For every record the classifier (a) sends the routed copy,
(b) waits up to 10 s for the broker acknowledgement, and only then (c) commits the raw offset. A failed send raises
and the record is re-processed after restart, so delivery is **at-least-once**. The routed copy is keyed
`raw:<partition>:<offset>`, so a re-emit after a crash carries the same key and can be removed downstream.

### 1.4 Spark Structured Streaming (`scripts/system_logs_to_hive.py`)
Subscribes to the five severity topics, parses each message, and appends Parquet files to
`hdfs://localhost:9000/user/hive/warehouse/system_logs`, partitioned by `severity`, in 30-second micro-batches.
The checkpoint is `hdfs://localhost:9000/user/hadoop/checkpoints/system_logs`. With `--once` it processes
everything available and exits.

Parsing handles two source formats: `rsyslog` (regex over the template above, year inferred from the Kafka
timestamp because syslog times carry none) and `app_json` (fields `level`, `timestamp`, `service`, `message`).
Anything else is kept as `unknown` with the raw payload preserved.

### 1.5 Hive (`scripts/*_hive.sql`)
External tables over the Parquet directories (dropping a table never deletes data). The embedded Derby metastore
allows only one process, so Spark must **not** use `enableHiveSupport()`; it writes files and Hive reads them.

### 1.6 Curated layer (`scripts/system_logs_curated.py`)
Batch Spark job reading the streaming sink **through Spark** (which honours `_spark_metadata`, so files from
uncommitted micro-batches are never read; a plain Hive directory listing would see them). It:
- drops pipeline self-ingestion and auth noise that predates the rsyslog filter,
- **de-duplicates** on `raw_key` (falling back to topic/partition/offset for legacy rows),
- writes few files per `(event_date, severity)` with dynamic partition overwrite, so reruns are idempotent.

### 1.7 Aggregates (`scripts/system_logs_aggregates.py`)
Builds `system_logs_hourly` (hour × severity × source × host counts) and `system_logs_daily_sources`
(per-day per-source totals, error rate and error rank) from the curated data. Idempotent per `event_date`.

## 2. Process supervision
`systemd/` holds one unit per service; `install.sh` installs them and starts them in dependency order:

```
hadoop-hdfs → hadoop-yarn → hiveserver2 → kafka → kafka-connect → system-log-classifier → spark-system-logs
```

Key unit design points (see the [incident report](INCIDENT-2026-10-04.md) for why):
- `hadoop-hdfs` waits for the NameNode port (fatal if absent), then waits up to 120 s for safe mode to end,
  **non-fatally** (`ExecStartPost=-/usr/bin/timeout 120 …`), so a stuck safe mode can never make systemd kill HDFS.
- `spark-system-logs` and `hiveserver2` have a **fatal** `ExecStartPre` that waits up to 300 s for HDFS to leave safe
  mode and fails with a clear message instead of crash-looping.
- Kafka-dependent units wait for port 9092 before starting.
- The classifier stops on `SIGINT` so it can flush its producer and close the consumer cleanly.

## 3. Design decisions

| Decision | Why |
|---|---|
| Classifier in Python rather than Kafka Streams | simple, easy to test, enough throughput for one host |
| Commit offset after acknowledged send | trade a possible duplicate for zero silent loss |
| Key = `raw:<partition>:<offset>` | stable identity for de-duplication after replays |
| Spark writes plain Parquet, Hive reads external tables | embedded Derby allows one metastore process |
| Curated layer reads via Spark | respects `_spark_metadata`, avoids uncommitted files |
| External Hive tables | dropping a table cannot destroy data |
| Fixed five-value severity partitions | partitions registered once; new files appear automatically |

## 4. Resource budget (single VM, 7.6 GB)
Approximate resident memory with the stack running: Kafka ≈ 0.7 GB, Connect ≈ 0.55 GB, NameNode 0.3, DataNode 0.25,
SecondaryNameNode 0.2, ResourceManager 0.3, NodeManager 0.35, HiveServer2 0.6, Spark driver ≈ 0.8 GB. YARN is
configured with NodeManager 2560 MB, max container 1024 MB, Tez 512 MB.
