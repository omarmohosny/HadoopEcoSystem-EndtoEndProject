-- Hive external tables over the Parquet output of system_logs_aggregates.py
-- EXTERNAL: dropping a table never deletes HDFS data.
CREATE EXTERNAL TABLE IF NOT EXISTS default.system_logs_hourly (
  event_hour   TIMESTAMP,
  severity     STRING,
  source       STRING,
  hostname     STRING,
  event_count  BIGINT,
  first_seen   TIMESTAMP,
  last_seen    TIMESTAMP
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/system_logs_hourly';

CREATE EXTERNAL TABLE IF NOT EXISTS default.system_logs_daily_sources (
  source           STRING,
  total_events     BIGINT,
  error_events     BIGINT,
  critical_events  BIGINT,
  warning_events   BIGINT,
  error_rate       DOUBLE,
  error_rank       INT
)
PARTITIONED BY (event_date STRING)
STORED AS PARQUET
LOCATION 'hdfs://localhost:9000/user/hive/warehouse/system_logs_daily_sources';

-- event_date partitions are created by Spark: register any new ones
MSCK REPAIR TABLE default.system_logs_hourly;
MSCK REPAIR TABLE default.system_logs_daily_sources;
