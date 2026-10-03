"""
Batch aggregation layer over the Hive `system_logs` Parquet data.

Builds (Parquet on HDFS, Hive external tables partitioned by event_date):
  system_logs_hourly         hour x severity x source x host event counts
  system_logs_daily_sources  per-day per-source totals, error rate and error rank

Idempotent: each run rewrites only the event_date partitions it computes
(dynamic partition overwrite), so re-running after late data is safe.

Usage:
  spark-submit ... system_logs_aggregates.py                    # all dates
  spark-submit ... system_logs_aggregates.py --since 2026-10-04 # from that date on
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


WAREHOUSE = "hdfs://localhost:9000/user/hive/warehouse"
SOURCE_PATH = f"{WAREHOUSE}/system_logs"
HOURLY_PATH = f"{WAREHOUSE}/system_logs_hourly"
DAILY_SOURCES_PATH = f"{WAREHOUSE}/system_logs_daily_sources"

ERROR_SEVERITIES = ("error", "critical")


def normalize(df):
    """Add event_ts (with Kafka-time fallback), event_hour, event_date and source."""
    event_ts = F.coalesce(F.col("event_time"), F.col("kafka_timestamp"))
    return (
        df.withColumn("event_ts", event_ts)
        .withColumn("event_hour", F.date_trunc("hour", F.col("event_ts")))
        .withColumn("event_date", F.date_format(F.col("event_ts"), "yyyy-MM-dd"))
        .withColumn(
            "source",
            F.coalesce(
                F.nullif(F.col("program"), F.lit("")),
                F.nullif(F.col("service"), F.lit("")),
                F.lit("unknown"),
            ),
        )
    )


def hourly_counts(df):
    return (
        df.groupBy("event_date", "event_hour", "severity", "source", "hostname")
        .agg(
            F.count(F.lit(1)).alias("event_count"),
            F.min("event_ts").alias("first_seen"),
            F.max("event_ts").alias("last_seen"),
        )
        .select("event_hour", "severity", "source", "hostname",
                "event_count", "first_seen", "last_seen", "event_date")
    )


def daily_source_stats(df):
    def count_if(condition):
        return F.sum(F.when(condition, 1).otherwise(0)).cast("long")

    stats = df.groupBy("event_date", "source").agg(
        F.count(F.lit(1)).alias("total_events"),
        count_if(F.col("severity").isin(*ERROR_SEVERITIES)).alias("error_events"),
        count_if(F.col("severity") == "critical").alias("critical_events"),
        count_if(F.col("severity") == "warning").alias("warning_events"),
    )
    rank_window = Window.partitionBy("event_date").orderBy(
        F.col("error_events").desc(), F.col("total_events").desc(), F.col("source").asc()
    )
    return (
        stats.withColumn("error_rate", F.round(F.col("error_events") / F.col("total_events"), 4))
        .withColumn("error_rank", F.row_number().over(rank_window))
        .select("source", "total_events", "error_events", "critical_events",
                "warning_events", "error_rate", "error_rank", "event_date")
    )


def write_partitions(df, path):
    (
        df.repartition("event_date")
        .write.mode("overwrite")
        .partitionBy("event_date")
        .parquet(path)
    )


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="only rebuild event_date >= YYYY-MM-DD")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("system-logs-aggregates")
        # overwrite only the event_date partitions present in this run's output
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    logs = normalize(spark.read.parquet(SOURCE_PATH))
    if args.since:
        logs = logs.where(F.col("event_date") >= args.since)
    logs = logs.cache()

    hourly = hourly_counts(logs)
    daily = daily_source_stats(logs)
    write_partitions(hourly, HOURLY_PATH)
    write_partitions(daily, DAILY_SOURCES_PATH)

    dates = sorted(r["event_date"] for r in logs.select("event_date").distinct().collect())
    print(f"AGGREGATES OK: source_rows={logs.count()} hourly_rows={hourly.count()} "
          f"daily_rows={daily.count()} dates={','.join(dates)}")
    spark.stop()


if __name__ == "__main__":
    main()
