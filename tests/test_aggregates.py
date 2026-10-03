from datetime import datetime

import pytest

from system_logs_aggregates import daily_source_stats, from_curated, hourly_counts, normalize

SCHEMA = (
    "event_time timestamp, severity string, hostname string, program string, "
    "service string, kafka_timestamp timestamp"
)
KTS = datetime(2026, 10, 4, 9, 59, 0)


def logs(spark, rows):
    return spark.createDataFrame(rows, SCHEMA)


def r(t, sev, program="sshd", service=None, host="hadoop-master", kts=KTS):
    return (t, sev, host, program, service, kts)


# ---------------------------------------------------------------- normalize

def test_normalize_derives_date_hour_and_source(spark):
    row = normalize(logs(spark, [r(datetime(2026, 10, 4, 10, 37, 12), "normal")])).collect()[0]
    assert row["event_ts"] == datetime(2026, 10, 4, 10, 37, 12)
    assert row["event_hour"] == datetime(2026, 10, 4, 10, 0, 0)
    assert row["event_date"] == "2026-10-04"
    assert row["source"] == "sshd"


def test_normalize_falls_back_to_kafka_timestamp(spark):
    row = normalize(logs(spark, [r(None, "normal")])).collect()[0]
    assert row["event_ts"] == KTS
    assert row["event_hour"] == datetime(2026, 10, 4, 9, 0, 0)


@pytest.mark.parametrize(
    "program, service, expected",
    [("sshd", None, "sshd"), (None, "payment-service", "payment-service"),
     ("", "payment-service", "payment-service"), (None, None, "unknown"), ("", None, "unknown")],
)
def test_normalize_source_precedence(spark, program, service, expected):
    row = normalize(logs(spark, [r(KTS, "normal", program=program, service=service)])).collect()[0]
    assert row["source"] == expected


# ---------------------------------------------------------------- hourly

def test_hourly_counts_group_by_hour_severity_source_host(spark):
    df = logs(spark, [
        r(datetime(2026, 10, 4, 10, 5), "error"),
        r(datetime(2026, 10, 4, 10, 55), "error"),
        r(datetime(2026, 10, 4, 11, 1), "error"),
        r(datetime(2026, 10, 4, 10, 20), "warning"),
        r(datetime(2026, 10, 4, 10, 30), "error", program="kernel"),
    ])
    out = {(x["event_hour"].hour, x["severity"], x["source"]): x
           for x in hourly_counts(normalize(df)).collect()}
    assert out[(10, "error", "sshd")]["event_count"] == 2
    assert out[(10, "error", "sshd")]["first_seen"] == datetime(2026, 10, 4, 10, 5)
    assert out[(10, "error", "sshd")]["last_seen"] == datetime(2026, 10, 4, 10, 55)
    assert out[(11, "error", "sshd")]["event_count"] == 1
    assert out[(10, "warning", "sshd")]["event_count"] == 1
    assert out[(10, "error", "kernel")]["event_count"] == 1
    assert len(out) == 4


def test_hourly_counts_preserve_total(spark):
    rows = [r(datetime(2026, 10, 4, h, m), s) for h in (8, 9) for m in (0, 30) for s in ("normal", "debug")]
    agg = hourly_counts(normalize(logs(spark, rows)))
    assert agg.agg({"event_count": "sum"}).collect()[0][0] == len(rows)


def test_hourly_columns_match_hive_table(spark):
    agg = hourly_counts(normalize(logs(spark, [r(KTS, "normal")])))
    assert agg.columns == ["event_hour", "severity", "source", "hostname",
                           "event_count", "first_seen", "last_seen", "event_date"]


# ---------------------------------------------------------------- daily sources

def test_daily_source_stats_counts_and_error_rate(spark):
    t = datetime(2026, 10, 4, 12, 0)
    df = logs(spark, [r(t, "error"), r(t, "critical"), r(t, "warning"), r(t, "normal"), r(t, "normal"),
                      r(t, "error", program="kernel"), r(t, "normal", program="kernel")])
    out = {x["source"]: x for x in daily_source_stats(normalize(df)).collect()}
    s = out["sshd"]
    assert (s["total_events"], s["error_events"], s["critical_events"], s["warning_events"]) == (5, 2, 1, 1)
    assert s["error_rate"] == pytest.approx(0.4)
    k = out["kernel"]
    assert (k["total_events"], k["error_events"]) == (2, 1)
    assert k["error_rate"] == pytest.approx(0.5)


def test_daily_source_stats_ranks_by_error_events_per_day(spark):
    d1, d2 = datetime(2026, 10, 3, 8), datetime(2026, 10, 4, 8)
    df = logs(spark, [
        r(d1, "error", program="a"), r(d1, "error", program="a"), r(d1, "error", program="b"),
        r(d1, "normal", program="c"),
        r(d2, "error", program="b"), r(d2, "critical", program="b"), r(d2, "error", program="a"),
    ])
    out = {(x["event_date"], x["source"]): x["error_rank"] for x in daily_source_stats(normalize(df)).collect()}
    assert out[("2026-10-03", "a")] == 1
    assert out[("2026-10-03", "b")] == 2
    assert out[("2026-10-03", "c")] == 3
    assert out[("2026-10-04", "b")] == 1
    assert out[("2026-10-04", "a")] == 2


def test_daily_source_stats_ties_rank_by_total_then_name(spark):
    t = datetime(2026, 10, 4, 8)
    df = logs(spark, [r(t, "error", program="zeta"), r(t, "normal", program="zeta"),
                      r(t, "error", program="alpha"), r(t, "error", program="beta")])
    out = {x["source"]: x["error_rank"] for x in daily_source_stats(normalize(df)).collect()}
    assert out == {"zeta": 1, "alpha": 2, "beta": 3}


def test_daily_columns_match_hive_table(spark):
    agg = daily_source_stats(normalize(logs(spark, [r(KTS, "normal")])))
    assert agg.columns == ["source", "total_events", "error_events", "critical_events",
                           "warning_events", "error_rate", "error_rank", "event_date"]


# ---------------------------------------------------------------- curated input

CURATED_SCHEMA = "event_ts timestamp, severity string, source string, hostname string, event_date string"


def test_from_curated_adds_event_hour_and_feeds_hourly_counts(spark):
    rows = [(datetime(2026, 10, 4, 10, 5), "error", "sshd", "h", "2026-10-04"),
            (datetime(2026, 10, 4, 10, 55), "error", "sshd", "h", "2026-10-04")]
    cur = from_curated(spark.createDataFrame(rows, CURATED_SCHEMA))
    assert cur.collect()[0]["event_hour"] == datetime(2026, 10, 4, 10, 0)
    out = hourly_counts(cur).collect()
    assert len(out) == 1 and out[0]["event_count"] == 2 and out[0]["source"] == "sshd"


def test_from_curated_normalises_inferred_date_partition_to_string(spark):
    # Spark reads event_date=2026-10-04 directories back as DATE (partition type inference)
    from datetime import date
    rows = [(datetime(2026, 10, 4, 10, 5), "error", "sshd", "h", date(2026, 10, 4))]
    cur = from_curated(spark.createDataFrame(
        rows, "event_ts timestamp, severity string, source string, hostname string, event_date date"))
    assert cur.collect()[0]["event_date"] == "2026-10-04"
    assert dict(daily_source_stats(cur).dtypes)["event_date"] == "string"
