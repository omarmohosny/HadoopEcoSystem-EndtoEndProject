-- Hive external table over the Parquet files written by system_logs_to_hive.py
-- EXTERNAL: dropping the table never deletes the HDFS data.
CREATE EXTERNAL TABLE IF NOT EXISTS default.system_logs (
  event_time       TIMESTAMP,
  raw_severity     STRING,
  hostname         STRING,
  program          STRING,
  service          STRING,
  message          STRING,
  source_format    STRING,
  raw_payload      STRING,
  kafka_topic      STRING,
  kafka_partition  INT,
  kafka_offset     BIGINT,
  kafka_timestamp  TIMESTAMP,
  ingest_time      TIMESTAMP,
  raw_key          STRING
)
PARTITIONED BY (severity STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/system_logs';

-- The five severity values are fixed: register them once, new files show up automatically
ALTER TABLE default.system_logs ADD IF NOT EXISTS
  PARTITION (severity='normal')
  PARTITION (severity='warning')
  PARTITION (severity='error')
  PARTITION (severity='critical')
  PARTITION (severity='debug');

-- Upgrade for tables created before raw_key existed (Parquet columns map by name; old files read NULL).
-- Existing partitions keep their own column list, so add the column to each of them too
-- (ALTER TABLE ... ADD COLUMNS without CASCADE only changes the table-level schema):
-- ALTER TABLE default.system_logs ADD COLUMNS (raw_key STRING);
-- ALTER TABLE default.system_logs PARTITION (severity='normal')   ADD COLUMNS (raw_key STRING);
-- ALTER TABLE default.system_logs PARTITION (severity='warning')  ADD COLUMNS (raw_key STRING);
-- ALTER TABLE default.system_logs PARTITION (severity='error')    ADD COLUMNS (raw_key STRING);
-- ALTER TABLE default.system_logs PARTITION (severity='critical') ADD COLUMNS (raw_key STRING);
-- ALTER TABLE default.system_logs PARTITION (severity='debug')    ADD COLUMNS (raw_key STRING);
