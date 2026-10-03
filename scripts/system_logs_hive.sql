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
  ingest_time      TIMESTAMP
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
