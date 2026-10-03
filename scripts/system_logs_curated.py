"""
Curated layer: system_logs -> system_logs_curated (de-duplicated, noise-free, date partitioned).

- Reads the streaming sink through Spark, which honours _spark_metadata, so files from
  uncommitted micro-batches are never read (Hive lists the directory and would see them).
- Drops pipeline self-ingestion and auth noise that predates the rsyslog filter.
- De-duplicates: classifier key raw:<partition>:<offset> when present (re-emits after a
  crash), otherwise the severity-topic coordinates (legacy rows, partial-file duplicates).
- Writes few files per (event_date, severity) partition; dynamic overwrite = idempotent.

Usage:
  spark-submit ... system_logs_curated.py                    # all dates
  spark-submit ... system_logs_curated.py --since 2026-10-04 # rebuild from that date on
"""

import argparse

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F

from system_logs_aggregates import normalize

WAREHOUSE = "hdfs://localhost:9000/user/hive/warehouse"
SOURCE_PATH = f"{WAREHOUSE}/system_logs"
CURATED_PATH = f"{WAREHOUSE}/system_logs_curated"

# Mirrors the rsyslog filter (rsyslog/kafka-system-logs.conf) for rows ingested before it existed
NOISE_PREFIXES = ("kafka-", "connect-")
NOISE_PROGRAMS = ("sudo", "su", "unix_chkpwd")

CURATED_COLUMNS = [
    "event_ts", "raw_severity", "hostname", "program", "service", "source", "message", "source_format",
    "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "raw_key", "dedup_key",
    "event_date", "severity",
]


def drop_noise(df):
    program = F.coalesce(F.col("program"), F.lit(""))
    noise = program.isin(*NOISE_PROGRAMS)
    for prefix in NOISE_PREFIXES:
        noise = noise | program.startswith(prefix)
    return df.where(~noise)


def dedup(df):
    dedup_key = F.coalesce(
        F.col("raw_key"),
        F.concat_ws(":", F.lit("kafka"), F.col("kafka_topic"),
                    F.col("kafka_partition").cast("string"), F.col("kafka_offset").cast("string")),
    )
    first_copy = Window.partitionBy("dedup_key").orderBy(F.col("kafka_timestamp").asc(), F.col("kafka_offset").asc())
    return (
        df.withColumn("dedup_key", dedup_key)
        .withColumn("_copy", F.row_number().over(first_copy))
        .where(F.col("_copy") == 1)
        .drop("_copy")
    )


def curate(df):
    return dedup(normalize(drop_noise(df))).select(*CURATED_COLUMNS)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--since", help="only rebuild event_date >= YYYY-MM-DD")
    args = parser.parse_args()

    spark = (
        SparkSession.builder.appName("system-logs-curated")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    curated = curate(spark.read.parquet(SOURCE_PATH))
    if args.since:
        curated = curated.where(F.col("event_date") >= args.since)
    curated = curated.cache()

    (
        curated.repartition("event_date", "severity")
        .write.mode("overwrite")
        .partitionBy("event_date", "severity")
        .parquet(CURATED_PATH)
    )
    print(f"CURATED OK: rows={curated.count()} dates="
          + ",".join(sorted(r[0] for r in curated.select("event_date").distinct().collect())))
    spark.stop()


if __name__ == "__main__":
    main()
