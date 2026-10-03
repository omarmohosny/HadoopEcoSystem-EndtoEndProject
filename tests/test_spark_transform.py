import json
from datetime import datetime

import pytest

from system_logs_to_hive import transform


def connect_wrap(line):
    return json.dumps({"schema": {"type": "string", "optional": False}, "payload": line})


def kafka_rows(spark, records):
    """records: list of (topic, value_str, offset) -> DataFrame shaped like the Kafka source."""
    rows = [
        (bytearray(value.encode()), topic, 0, offset, datetime(2026, 10, 4, 0, 2, 30))
        for topic, value, offset in records
    ]
    return spark.createDataFrame(rows, "value binary, topic string, partition int, offset long, timestamp timestamp")


def one(spark, topic, value):
    return transform(kafka_rows(spark, [(topic, value, 7)])).collect()[0].asDict()


def test_rsyslog_record_is_parsed_into_columns(spark):
    line = "Oct  4 00:02:01 severity=crit hostname=hadoop-master program=omarmohosny message=HIVE E2E test"
    r = one(spark, "system-logs-critical", connect_wrap(line))
    assert r["source_format"] == "rsyslog"
    assert r["severity"] == "critical"
    assert r["raw_severity"] == "crit"
    assert r["hostname"] == "hadoop-master"
    assert r["program"] == "omarmohosny"
    assert r["message"] == "HIVE E2E test"
    assert r["event_time"] == datetime(2026, 10, 4, 0, 2, 1)
    assert r["service"] is None


def test_rsyslog_single_digit_day_with_double_space(spark):
    line = "Oct  3 22:31:24 severity=info hostname=h program=p message=m"
    assert one(spark, "system-logs-normal", connect_wrap(line))["event_time"] == datetime(2026, 10, 3, 22, 31, 24)


def test_rsyslog_empty_program_and_message_with_equals(spark):
    line = "Oct 14 09:00:00 severity=notice hostname=h program= message=key=value a=b"
    r = one(spark, "system-logs-normal", connect_wrap(line))
    assert r["program"] == ""
    assert r["message"] == "key=value a=b"


def test_app_json_record_is_parsed_into_columns(spark):
    line = json.dumps({"timestamp": "2026-10-03T15:50:20+03:00", "level": "ERROR",
                       "service": "payment-service", "message": "Payment failed"})
    r = one(spark, "system-logs-error", connect_wrap(line))
    assert r["source_format"] == "app_json"
    assert r["severity"] == "error"
    assert r["raw_severity"] == "ERROR"
    assert r["service"] == "payment-service"
    assert r["message"] == "Payment failed"
    assert r["hostname"] is None
    assert r["event_time"] is not None


def test_unknown_format_keeps_raw_payload_as_message(spark):
    r = one(spark, "system-logs-normal", connect_wrap("DIRECT_WRITE_TEST_12345"))
    assert r["source_format"] == "unknown"
    assert r["message"] == "DIRECT_WRITE_TEST_12345"
    assert r["raw_payload"] == "DIRECT_WRITE_TEST_12345"


def test_kafka_metadata_is_preserved(spark):
    r = one(spark, "system-logs-warning", connect_wrap("Oct  4 00:00:00 severity=warning hostname=h program=p message=m"))
    assert (r["kafka_topic"], r["kafka_partition"], r["kafka_offset"]) == ("system-logs-warning", 0, 7)
    assert r["ingest_time"] is not None


@pytest.mark.parametrize("sev", ["normal", "warning", "error", "critical", "debug"])
def test_severity_partition_comes_from_topic(spark, sev):
    line = "Oct  4 00:00:00 severity=info hostname=h program=p message=m"
    assert one(spark, f"system-logs-{sev}", connect_wrap(line))["severity"] == sev


def test_output_columns_match_hive_table(spark):
    cols = transform(kafka_rows(spark, [("system-logs-normal", connect_wrap("x"), 0)])).columns
    assert cols == ["event_time", "raw_severity", "hostname", "program", "service", "message",
                    "source_format", "raw_payload", "kafka_topic", "kafka_partition",
                    "kafka_offset", "kafka_timestamp", "ingest_time", "severity"]
