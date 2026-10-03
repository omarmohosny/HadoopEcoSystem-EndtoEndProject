"""Delivery semantics of the classifier: keyed sends, acked before commit, no silent loss."""
import json

import pytest
from kafka.consumer.fetcher import ConsumerRecord
from kafka.errors import KafkaTimeoutError
from kafka.structs import OffsetAndMetadata, TopicPartition


class FakeFuture:
    def __init__(self, events, error=None):
        self.events, self.error = events, error

    def get(self, timeout=None):
        self.events.append(("ack", timeout))
        if self.error:
            raise self.error
        return "metadata"


class FakeProducer:
    def __init__(self, events, error=None):
        self.events, self.error = events, error

    def send(self, topic, value=None, key=None, **_):
        self.events.append(("send", topic, key, value))
        return FakeFuture(self.events, self.error)


class FakeConsumer:
    def __init__(self, events):
        self.events = events

    def commit(self, offsets=None, **_):
        self.events.append(("commit", offsets))


def raw_record(value, partition=0, offset=42):
    return ConsumerRecord("system-logs-raw", partition, -1, offset, 0, 0, None, value, [], None, -1, len(value), -1)


def wrapped(line):
    return json.dumps({"schema": {"type": "string", "optional": False}, "payload": line})


RSYSLOG_ERR = wrapped("Oct  4 01:00:00 severity=err hostname=h program=p message=boom")


def committed_offsets(offset):
    return {TopicPartition("system-logs-raw", 0): OffsetAndMetadata(offset, None, -1)}


def test_routed_record_is_sent_keyed_by_raw_coordinates(classifier):
    events = []
    classifier.process_record(raw_record(RSYSLOG_ERR, partition=0, offset=42), FakeProducer(events), FakeConsumer(events))
    sends = [e for e in events if e[0] == "send"]
    assert sends == [("send", "system-logs-error", "raw:0:42", RSYSLOG_ERR)]


def test_commit_follows_ack_and_points_past_the_record(classifier):
    events = []
    classifier.process_record(raw_record(RSYSLOG_ERR, offset=42), FakeProducer(events), FakeConsumer(events))
    assert [e[0] for e in events] == ["send", "ack", "commit"]
    assert events[-1] == ("commit", committed_offsets(43))


def test_send_waits_for_broker_ack_with_timeout(classifier):
    events = []
    classifier.process_record(raw_record(RSYSLOG_ERR), FakeProducer(events), FakeConsumer(events))
    acks = [e for e in events if e[0] == "ack"]
    assert acks and acks[0][1] is not None and acks[0][1] > 0


def test_failed_send_raises_and_is_not_committed(classifier):
    events = []
    producer = FakeProducer(events, error=KafkaTimeoutError("broker down"))
    with pytest.raises(KafkaTimeoutError):
        classifier.process_record(raw_record(RSYSLOG_ERR), producer, FakeConsumer(events))
    assert not [e for e in events if e[0] == "commit"]


def test_unknown_record_is_committed_without_sending(classifier):
    events = []
    result = classifier.process_record(raw_record(wrapped("DIRECT_WRITE_TEST_12345"), offset=7),
                                       FakeProducer(events), FakeConsumer(events))
    assert result is None
    assert not [e for e in events if e[0] == "send"]
    assert events == [("commit", committed_offsets(8))]


def test_process_record_returns_destination_topic(classifier):
    events = []
    topic = classifier.process_record(raw_record(RSYSLOG_ERR), FakeProducer(events), FakeConsumer(events))
    assert topic == "system-logs-error"


def test_consumer_auto_commit_is_disabled(classifier):
    assert classifier.CONSUMER_CONFIG["enable_auto_commit"] is False
    assert classifier.CONSUMER_CONFIG["group_id"] == "system-log-classifier-v1"
