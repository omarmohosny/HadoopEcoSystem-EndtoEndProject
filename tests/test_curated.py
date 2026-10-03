from datetime import datetime

import pytest

from system_logs_curated import curate, dedup, drop_noise

SCHEMA = (
    "event_time timestamp, raw_severity string, hostname string, program string, service string, "
    "message string, source_format string, raw_payload string, kafka_topic string, kafka_partition int, "
    "kafka_offset long, kafka_timestamp timestamp, ingest_time timestamp, raw_key string, severity string"
)
T = datetime(2026, 10, 4, 10, 0, 0)
KTS = datetime(2026, 10, 4, 10, 0, 5)


def row(offset, program="sshd", raw_key=None, severity="normal", topic=None, t=T, kts=KTS, ingest=KTS):
    topic = topic or f"system-logs-{severity}"
    return (t, "info", "hadoop-master", program, None, f"m{offset}", "rsyslog", f"p{offset}",
            topic, 0, offset, kts, ingest, raw_key, severity)


def df(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


# ------------------------------------------------------------------ noise

@pytest.mark.parametrize("program", ["kafka-server-start.sh", "connect-standalone.sh", "sudo", "su", "unix_chkpwd"])
def test_drop_noise_removes_self_ingested_and_auth_programs(spark, program):
    assert drop_noise(df(spark, [row(1, program=program)])).count() == 0


@pytest.mark.parametrize("program", ["sshd", "kernel", "systemd", "kafkaesque", "suricata", None])
def test_drop_noise_keeps_ordinary_programs(spark, program):
    assert drop_noise(df(spark, [row(1, program=program)])).count() == 1


# ------------------------------------------------------------------ dedup

def test_dedup_same_kafka_coordinates_keeps_one(spark):
    # Hive may list a partial file from a crashed micro-batch: identical topic/partition/offset
    out = dedup(df(spark, [row(5), row(5), row(6)]))
    assert sorted(r["kafka_offset"] for r in out.collect()) == [5, 6]


def test_dedup_same_raw_key_keeps_earliest_copy(spark):
    # classifier re-emit after a crash: same raw key, new offset in the severity topic
    out = dedup(df(spark, [row(10, raw_key="raw:0:42", kts=datetime(2026, 10, 4, 10, 0, 9)),
                           row(11, raw_key="raw:0:42", kts=datetime(2026, 10, 4, 10, 0, 7)),
                           row(12, raw_key="raw:0:43")])).collect()
    assert len(out) == 2
    assert {r["raw_key"]: r["kafka_offset"] for r in out}["raw:0:42"] == 11


def test_dedup_legacy_rows_without_key_use_coordinates(spark):
    out = dedup(df(spark, [row(1), row(2), row(3)]))
    assert out.count() == 3


def test_dedup_keyed_and_unkeyed_rows_do_not_collide(spark):
    out = dedup(df(spark, [row(1, raw_key="raw:0:1"), row(1, topic="system-logs-warning")]))
    assert out.count() == 2


def test_dedup_is_idempotent(spark):
    once = dedup(df(spark, [row(5), row(5), row(6, raw_key="raw:0:9"), row(7, raw_key="raw:0:9")]))
    twice = dedup(once)
    assert sorted(map(tuple, once.collect())) == sorted(map(tuple, twice.collect()))


# ------------------------------------------------------------------ curate

def test_curate_adds_event_date_and_source_and_drops_noise_and_dups(spark):
    out = curate(df(spark, [row(1), row(1), row(2, program="kafka-server-start.sh"),
                            row(3, program=None, t=None, kts=datetime(2026, 10, 3, 23, 59, 0))]))
    rows = {r["kafka_offset"]: r for r in out.collect()}
    assert set(rows) == {1, 3}
    assert rows[1]["event_date"] == "2026-10-04" and rows[1]["source"] == "sshd"
    assert rows[3]["event_date"] == "2026-10-03" and rows[3]["source"] == "unknown"


def test_curate_columns(spark):
    assert curate(df(spark, [row(1)])).columns == [
        "event_ts", "raw_severity", "hostname", "program", "service", "source", "message", "source_format",
        "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp", "raw_key", "dedup_key",
        "event_date", "severity"]
