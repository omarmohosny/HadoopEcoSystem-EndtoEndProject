-- Hive external table over the Parquet output of system_logs_curated.py
-- EXTERNAL: dropping the table never deletes HDFS data.
CREATE EXTERNAL TABLE IF NOT EXISTS default.system_logs_curated (
  event_ts         TIMESTAMP,
  raw_severity     STRING,
  hostname         STRING,
  program          STRING,
  service          STRING,
  source           STRING,
  message          STRING,
  source_format    STRING,
  kafka_topic      STRING,
  kafka_partition  INT,
  kafka_offset     BIGINT,
  kafka_timestamp  TIMESTAMP,
  raw_key          STRING,
  dedup_key        STRING
)
PARTITIONED BY (event_date STRING, severity STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/system_logs_curated';

-- event_date/severity partitions are created by Spark: register any new ones
MSCK REPAIR TABLE default.system_logs_curated;
