import json

import pytest


def connect_wrap(line):
    """Wrap a line the way Kafka Connect JsonConverter does."""
    return json.dumps({"schema": {"type": "string", "optional": False}, "payload": line})


def rsyslog(sev, msg="x"):
    return f"Oct  3 23:42:05 severity={sev} hostname=hadoop-master program=test message={msg}"


@pytest.mark.parametrize(
    "sev, expected",
    [
        ("info", "normal"), ("notice", "normal"),
        ("warning", "warning"), ("warn", "warning"),
        ("err", "error"), ("error", "error"),
        ("crit", "critical"), ("critical", "critical"), ("alert", "critical"), ("emerg", "critical"),
        ("debug", "debug"),
    ],
)
def test_rsyslog_severity_mapping(classifier, sev, expected):
    assert classifier.classify_message(connect_wrap(rsyslog(sev)))[0] == expected


def test_rsyslog_severity_field_beats_keywords_in_message(classifier):
    msg = connect_wrap(rsyslog("info", "an ERROR word and CRITICAL too"))
    assert classifier.classify_message(msg)[0] == "normal"


@pytest.mark.parametrize(
    "level, text, expected",
    [
        ("INFO", "retry after error succeeded", "normal"),
        ("WARN", "disk CRITICAL threshold soon", "warning"),
        ("INFO", "debug mode disabled", "normal"),
        ("ERROR", "Payment failed", "error"),
        ("debug", "trace", "debug"),
    ],
)
def test_wrapped_app_json_uses_level_not_keywords(classifier, level, text, expected):
    line = json.dumps({"level": level, "service": "svc", "message": text})
    assert classifier.classify_message(connect_wrap(line))[0] == expected


def test_unwrapped_app_json_uses_level(classifier):
    line = json.dumps({"level": "ERROR", "message": "unwrapped"})
    assert classifier.classify_message(line)[0] == "error"


def test_unknown_record_is_skipped(classifier):
    assert classifier.classify_message(connect_wrap("DIRECT_WRITE_TEST_12345"))[0] is None


def test_output_is_original_record_unchanged(classifier):
    msg = connect_wrap(rsyslog("info"))
    assert classifier.classify_message(msg)[1] == msg
