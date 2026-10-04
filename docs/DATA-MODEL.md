# Data model

## Kafka topics

| Topic | Producer | Content | Key |
|---|---|---|---|
| `system-logs-raw` | Kafka Connect | every log line, wrapped as Connect JSON | none |
| `system-logs-normal` | classifier | syslog `info`/`notice`, JSON level INFO | `raw:<p>:<offset>` |
| `system-logs-warning` | classifier | `warning`/`warn` | same |
| `system-logs-error` | classifier | `err`/`error` | same |
| `system-logs-critical` | classifier | `crit`/`critical`/`alert`/`emerg` | same |
| `system-logs-debug` | classifier | `debug` | same |

Replication factor 1 (single node). The key is the coordinate of the message in `system-logs-raw`.

## Message formats

**Raw value** (Kafka Connect, schemas enabled):
```json
{"schema": {"type": "string", "optional": false}, "payload": "Oct  4 03:17:50 severity=info hostname=hadoop-master program=systemd message=Started session-c120.scope"}
```

**Application JSON log** (also supported, carried in `payload`):
```json
{"timestamp": "2026-10-04T03:17:50Z", "level": "ERROR", "service": "api", "message": "..."}
```

## Hive tables (database `default`)

### `system_logs`: raw landing table (EXTERNAL, partitioned by `severity`)
Not de-duplicated. Location `hdfs://localhost:9000/user/hive/warehouse/system_logs`.

| Column | Type | Meaning |
|---|---|---|
| `event_time` | timestamp | time parsed from the log line (year assumed from Kafka time) |
| `raw_severity` | string | severity as written in the source |
| `hostname`, `program`, `service` | string | origin of the message |
| `message` | string | message text |
| `source_format` | string | `rsyslog`, `app_json` or `unknown` |
| `raw_payload` | string | the unparsed line |
| `kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp` | | where the severity-topic copy lives |
| `ingest_time` | timestamp | when Spark processed it |
| `raw_key` | string | `raw:<partition>:<offset>` of the raw-topic message; NULL on files written before the column existed |
| `severity` (partition) | string | `normal`, `warning`, `error`, `critical`, `debug` |

### `system_logs_curated`: de-duplicated and noise-free (partitioned by `event_date`, `severity`)
Columns: `event_ts`, `raw_severity`, `hostname`, `program`, `service`, `source`, `message`, `source_format`,
`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp`, `raw_key`, `dedup_key`.
`source` = `program`, else `service`, else `unknown`. `dedup_key` = `raw_key`, else
`kafka:<topic>:<partition>:<offset>`.

### `system_logs_hourly` (partitioned by `event_date`)
`event_hour`, `severity`, `source`, `hostname`, `event_count`, `first_seen`, `last_seen`.

### `system_logs_daily_sources` (partitioned by `event_date`)
`source`, `total_events`, `error_events` (error + critical), `critical_events`, `warning_events`,
`error_rate` (error_events / total_events), `error_rank` (1 = most errors that day).

## Example queries

```sql
-- volume by severity (raw table)
SELECT severity, count(*) FROM default.system_logs GROUP BY severity;

-- worst sources today (aggregates)
SELECT source, error_events, total_events, error_rate
FROM default.system_logs_daily_sources
WHERE event_date = current_date() AND error_rank <= 10
ORDER BY error_rank;

-- trace a row back to Kafka
SELECT raw_key, kafka_topic, kafka_partition, kafka_offset FROM default.system_logs
WHERE raw_key IS NOT NULL LIMIT 5;
```

Register partitions of the curated and aggregate tables after Spark creates new ones:
`MSCK REPAIR TABLE default.system_logs_curated;` (the `.sql` files do this).
