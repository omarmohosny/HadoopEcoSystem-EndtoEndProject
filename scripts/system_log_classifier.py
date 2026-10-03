import json
import re

from kafka import KafkaConsumer, KafkaProducer
from kafka.structs import OffsetAndMetadata, TopicPartition


BOOTSTRAP_SERVERS = "localhost:9092"

RAW_TOPIC = "system-logs-raw"

TOPICS = {
    "normal": "system-logs-normal",
    "warning": "system-logs-warning",
    "error": "system-logs-error",
    "critical": "system-logs-critical",
    "debug": "system-logs-debug",
}

CONSUMER_CONFIG = {
    "bootstrap_servers": BOOTSTRAP_SERVERS,
    "group_id": "system-log-classifier-v1",
    "auto_offset_reset": "earliest",
    # commit only after the routed copy is acknowledged (no silent loss)
    "enable_auto_commit": False,
}

SEND_TIMEOUT_S = 10


def normalize_severity(value):
    """Convert different severity names to our five Kafka categories."""

    if not value:
        return None

    value = value.lower().strip()

    if value in ("info", "notice"):
        return "normal"

    if value in ("warning", "warn"):
        return "warning"

    if value in ("err", "error"):
        return "error"

    if value in ("crit", "critical", "alert", "emerg"):
        return "critical"

    if value == "debug":
        return "debug"

    return None


def classify_message(raw_message):
    """
    Classify a Kafka Connect FileStreamSource message.

    Kafka Connect JsonConverter (schemas.enable=true) produces messages like:

    {
        "schema": {...},
        "payload": "Oct 3 ... severity=info ..."
    }
    """

    message = raw_message

    # ---------------------------------------------------------
    # 1. Try to parse the Kafka Connect JSON wrapper
    # ---------------------------------------------------------
    try:
        data = json.loads(raw_message)

        if isinstance(data, dict):
            # Kafka Connect StringConverter format
            if "payload" in data:
                message = str(data["payload"])

                # Application JSON log wrapped by Kafka Connect:
                # trust its "level" field instead of keyword scanning
                try:
                    inner = json.loads(message)
                except json.JSONDecodeError:
                    inner = None

                if isinstance(inner, dict) and "level" in inner:
                    severity = normalize_severity(str(inner["level"]))
                    if severity:
                        return severity, raw_message

            # Also support direct JSON application logs
            elif "level" in data:
                severity = normalize_severity(data["level"])
                return severity, raw_message

    except json.JSONDecodeError:
        pass

    # ---------------------------------------------------------
    # 2. Look for rsyslog severity=
    # ---------------------------------------------------------
    match = re.search(
        r"\bseverity\s*=\s*([A-Za-z]+)",
        message,
        re.IGNORECASE,
    )

    if match:
        severity = normalize_severity(match.group(1))

        if severity:
            return severity, raw_message

    # ---------------------------------------------------------
    # 3. Look for common severity fields
    # ---------------------------------------------------------
    patterns = [
        (r"\bCRITICAL\b", "critical"),
        (r"\bCRIT\b", "critical"),
        (r"\bALERT\b", "critical"),
        (r"\bEMERG\b", "critical"),
        (r"\bERROR\b", "error"),
        (r"\bERR\b", "error"),
        (r"\bWARNING\b", "warning"),
        (r"\bWARN\b", "warning"),
        (r"\bDEBUG\b", "debug"),
        (r"\bINFO\b", "normal"),
        (r"\bNOTICE\b", "normal"),
    ]

    for pattern, severity in patterns:
        if re.search(pattern, message, re.IGNORECASE):
            return severity, raw_message

    # ---------------------------------------------------------
    # 4. Unknown severity
    # ---------------------------------------------------------
    return None, raw_message


def process_record(record, producer, consumer):
    """
    Classify one raw record, deliver it, then commit its offset.

    The copy is keyed by the raw-topic coordinates so a re-emit after a crash
    carries the same key and can be de-duplicated downstream. The offset is
    committed only after the broker acknowledges the send; a failed send
    raises and the record is re-processed after restart.
    Returns the destination topic, or None for unrecognised records.
    """
    severity, output_message = classify_message(record.value)
    destination_topic = TOPICS[severity] if severity else None

    if destination_topic:
        producer.send(
            destination_topic,
            value=output_message,
            key=f"raw:{record.partition}:{record.offset}",
        ).get(timeout=SEND_TIMEOUT_S)

    consumer.commit({
        TopicPartition(record.topic, record.partition): OffsetAndMetadata(record.offset + 1, None, -1)
    })
    return destination_topic


def main():

    print("=" * 70)
    print("System Log Severity Classifier")
    print("=" * 70)
    print(f"Input topic : {RAW_TOPIC}")
    print("Output topics:")
    for severity, topic in TOPICS.items():
        print(f"  {severity:10} -> {topic}")
    print("=" * 70)

    consumer = KafkaConsumer(
        RAW_TOPIC,
        value_deserializer=lambda value: value.decode("utf-8"),
        **CONSUMER_CONFIG,
    )

    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda key: key.encode("utf-8"),
        value_serializer=lambda value: value.encode("utf-8"),
    )

    print("Classifier started.")
    print("Waiting for messages...")
    print()

    try:
        for record in consumer:
            destination_topic = process_record(record, producer, consumer)

            if destination_topic is None:
                print(
                    f"[SKIPPED] partition={record.partition} "
                    f"offset={record.offset} "
                    f"message={record.value}"
                )
                continue

            severity = next(k for k, v in TOPICS.items() if v == destination_topic)
            print(
                f"[{severity.upper():8}] "
                f"partition={record.partition} "
                f"offset={record.offset} "
                f"-> {destination_topic}"
            )

    except KeyboardInterrupt:
        print("\nStopping classifier...")

    finally:
        producer.flush()
        producer.close()
        consumer.close()
        print("Classifier stopped.")


if __name__ == "__main__":
    main()
