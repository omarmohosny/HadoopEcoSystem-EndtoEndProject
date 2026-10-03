"""
Spark Structured Streaming: classified Kafka system-log topics -> Parquet on HDFS
for the Hive external table `system_logs` (partitioned by severity).

Usage:
  spark-submit ... system_logs_to_hive.py           # continuous, 30s micro-batches
  spark-submit ... system_logs_to_hive.py --once    # process everything available, then exit
"""

import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


BOOTSTRAP_SERVERS = "localhost:9092"
TOPICS = ",".join(
    f"system-logs-{s}" for s in ("normal", "warning", "error", "critical", "debug")
)
OUTPUT_PATH = "hdfs://localhost:9000/user/hive/warehouse/system_logs"
CHECKPOINT_PATH = "hdfs://localhost:9000/user/hadoop/checkpoints/system_logs"

# rsyslog template: "%timegenerated% severity=.. hostname=.. program=.. message=.."
RSYSLOG_RE = (
    r"^(\w{3}\s+\d{1,2}\s+\d{2}:\d{2}:\d{2}) severity=(\S+) "
    r"hostname=(\S+) program=(\S*) message=(.*)$"
)


def transform(kafka_df):
    value = F.col("value").cast("string")

    # Kafka Connect JsonConverter wrapper: {"schema": ..., "payload": "<line>"}
    payload = F.coalesce(F.get_json_object(value, "$.payload"), value)

    is_rsyslog = payload.rlike(RSYSLOG_RE)
    is_app_json = F.get_json_object(payload, "$.level").isNotNull()

    def rs(group):
        return F.regexp_extract(payload, RSYSLOG_RE, group)

    def app(field):
        return F.get_json_object(payload, f"$.{field}")

    # rsyslog timestamps carry no year: assume the year the record reached Kafka
    rsyslog_time = F.to_timestamp(
        F.concat_ws(
            " ",
            F.year(F.col("timestamp")).cast("string"),
            F.regexp_replace(rs(1), r"\s+", " "),
        ),
        "yyyy MMM d HH:mm:ss",
    )

    return kafka_df.select(
        F.when(is_rsyslog, rsyslog_time)
        .when(is_app_json, F.to_timestamp(app("timestamp")))
        .alias("event_time"),
        F.when(is_rsyslog, rs(2)).when(is_app_json, app("level")).alias("raw_severity"),
        F.when(is_rsyslog, rs(3)).alias("hostname"),
        F.when(is_rsyslog, rs(4)).alias("program"),
        F.when(is_app_json, app("service")).alias("service"),
        F.when(is_rsyslog, rs(5))
        .when(is_app_json, app("message"))
        .otherwise(payload)
        .alias("message"),
        F.when(is_rsyslog, F.lit("rsyslog"))
        .when(is_app_json, F.lit("app_json"))
        .otherwise(F.lit("unknown"))
        .alias("source_format"),
        payload.alias("raw_payload"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.current_timestamp().alias("ingest_time"),
        # Severity category assigned by the Python classifier (topic suffix)
        F.regexp_extract(F.col("topic"), r"^system-logs-(\w+)$", 1).alias("severity"),
    )


def main():
    once = "--once" in sys.argv

    spark = SparkSession.builder.appName("system-logs-to-hive").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPICS)
        .option("startingOffsets", "earliest")
        .load()
    )

    writer = (
        transform(kafka_df)
        .writeStream.format("parquet")
        .option("path", OUTPUT_PATH)
        .option("checkpointLocation", CHECKPOINT_PATH)
        .partitionBy("severity")
        .outputMode("append")
    )

    if once:
        writer = writer.trigger(availableNow=True)
    else:
        writer = writer.trigger(processingTime="30 seconds")

    query = writer.start()
    query.awaitTermination()


if __name__ == "__main__":
    main()
