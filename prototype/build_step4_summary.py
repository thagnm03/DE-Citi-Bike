from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, TopicPartition


INPUT_TOPIC = "citibike.station-status.v1"
INVALID_TOPIC = "citibike.station-status.invalid.v1"
MAIN_ALERT_TOPIC = "citibike.station-alerts.v1"
REPLAY_ALERT_TOPIC = "citibike.station-alerts.replay.v1"


def load_json_lines(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line]


def topic_count(consumer: Consumer, topic: str) -> int:
    metadata = consumer.list_topics(topic=topic, timeout=10)
    topic_metadata = metadata.topics.get(topic)
    if topic_metadata is None or topic_metadata.error is not None:
        raise RuntimeError(f"Cannot read metadata for {topic}")
    total = 0
    for partition in topic_metadata.partitions:
        low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
        total += high - low
    return total


def consume_topic(bootstrap_servers: str, topic: str, expected_count: int) -> list[dict]:
    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap_servers,
            "group.id": f"citibike-step4-audit-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        topic_metadata = metadata.topics.get(topic)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Cannot read metadata for {topic}")
        assignments: list[TopicPartition] = []
        for partition in sorted(topic_metadata.partitions):
            low, _ = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            assignments.append(TopicPartition(topic, partition, low))
        consumer.assign(assignments)

        records: list[dict] = []
        deadline = time.monotonic() + 20
        while len(records) < expected_count and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            value = json.loads(message.value().decode("utf-8"))
            records.append(
                {
                    "key": message.key().decode("utf-8") if message.key() else None,
                    "value": value,
                    "kafka": {
                        "topic": message.topic(),
                        "partition": message.partition(),
                        "offset": message.offset(),
                    },
                }
            )
        if len(records) != expected_count:
            raise RuntimeError(
                f"Expected {expected_count} records from {topic}, consumed {len(records)}"
            )
        return records
    finally:
        consumer.close()


def summarize_progress(path: Path) -> dict:
    rows = load_json_lines(path)
    dropped = 0
    input_rows = 0
    watermarks: list[str] = []
    single_input_late_batches = 0
    for row in rows:
        input_rows += int(row.get("numInputRows", 0))
        watermark = row.get("eventTime", {}).get("watermark")
        if watermark and watermark not in watermarks:
            watermarks.append(watermark)
        row_dropped = sum(
            int(operator.get("numRowsDroppedByWatermark", 0))
            for operator in row.get("stateOperators", [])
        )
        dropped += row_dropped
        if int(row.get("numInputRows", 0)) == 1 and row_dropped == 1:
            single_input_late_batches += 1
    return {
        "progress_events": len(rows),
        "input_rows_reported": input_rows,
        "rows_dropped_by_watermark": dropped,
        "single_input_batches_fully_dropped_by_watermark": single_input_late_batches,
        "observed_watermarks": watermarks,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the machine-readable Step 4 Docker run summary")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--main-alerts", type=Path, required=True)
    parser.add_argument("--replay-alerts", type=Path, required=True)
    parser.add_argument("--main-progress", type=Path, required=True)
    parser.add_argument("--audit-output", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()

    main_alerts = load_json_lines(args.main_alerts)
    replay_alerts = load_json_lines(args.replay_alerts)
    main_ids = [alert["alert_event_id"] for alert in main_alerts]
    replay_ids = [alert["alert_event_id"] for alert in replay_alerts]

    metadata_consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"citibike-step4-count-{uuid.uuid4()}",
            "enable.auto.commit": False,
        }
    )
    try:
        counts = {
            topic: topic_count(metadata_consumer, topic)
            for topic in (INPUT_TOPIC, INVALID_TOPIC, MAIN_ALERT_TOPIC, REPLAY_ALERT_TOPIC)
        }
    finally:
        metadata_consumer.close()

    input_records = consume_topic(args.bootstrap_servers, INPUT_TOPIC, counts[INPUT_TOPIC])
    invalid_records = consume_topic(args.bootstrap_servers, INVALID_TOPIC, counts[INVALID_TOPIC])
    station_partitions: dict[str, set[int]] = {}
    key_matches_payload = True
    for record in input_records:
        station_id = record["value"]["station"]["station_id"]
        key_matches_payload = key_matches_payload and record["key"] == station_id
        station_partitions.setdefault(station_id, set()).add(record["kafka"]["partition"])
    partitions_used = sorted(
        {partition for partitions in station_partitions.values() for partition in partitions}
    )
    progress = summarize_progress(args.main_progress)
    invalid_envelopes_ok = all(
        record["value"].get("error_code") == "CONTRACT_INVALID"
        and bool(record["value"].get("raw_sha256"))
        and bool(record["value"].get("raw_fragment"))
        for record in invalid_records
    )
    late_record_present = any(
        record["value"].get("time", {}).get("snapshot_updated_at_utc")
        == "2026-09-21T01:55:00Z"
        for record in input_records
    )

    checks = {
        "twenty_seven_input_messages_including_replay_and_late_record": counts[INPUT_TOPIC] == 27,
        "seven_station_keys_exercised": len(station_partitions) == 7,
        "multiple_input_partitions_used": len(partitions_used) >= 2,
        "each_station_key_stays_on_one_partition": all(
            len(partitions) == 1 for partitions in station_partitions.values()
        ),
        "kafka_key_matches_station_id": key_matches_payload,
        "late_record_was_sent": late_record_present,
        "isolated_late_record_was_dropped_by_watermark": progress[
            "single_input_batches_fully_dropped_by_watermark"
        ]
        >= 1,
        "structural_invalid_records_routed_to_error_topic": counts[INVALID_TOPIC] == 2,
        "invalid_records_keep_auditable_error_envelope": invalid_envelopes_ok,
        "main_emitted_exactly_two_transitions": counts[MAIN_ALERT_TOPIC] == 2,
        "restart_and_duplicate_replay_emitted_no_extra_alerts": len(main_alerts) == 2,
        "recovery_replay_emitted_exactly_two_transitions": counts[REPLAY_ALERT_TOPIC] == 2,
        "recovery_replay_business_ids_match": main_ids == replay_ids,
        "one_open_then_one_resolved": [item["event_type"] for item in main_alerts]
        == ["ALERT_OPENED", "ALERT_RESOLVED"],
    }
    audit = {
        "input_topic": {
            "record_count": len(input_records),
            "partitions_used": partitions_used,
            "station_partitions": {
                station: sorted(partitions) for station, partitions in sorted(station_partitions.items())
            },
            "records": [
                {
                    "key": record["key"],
                    "event_id": record["value"].get("event_id"),
                    "event_time": record["value"].get("time", {}).get("snapshot_updated_at_utc"),
                    **record["kafka"],
                }
                for record in input_records
            ],
        },
        "invalid_topic": {
            "record_count": len(invalid_records),
            "records": [
                {
                    "key": record["key"],
                    "error_code": record["value"].get("error_code"),
                    "raw_sha256": record["value"].get("raw_sha256"),
                    "raw_fragment": record["value"].get("raw_fragment"),
                    **record["kafka"],
                }
                for record in invalid_records
            ],
        },
        "spark_progress": progress,
    }
    args.audit_output.parent.mkdir(parents=True, exist_ok=True)
    args.audit_output.write_text(json.dumps(audit, indent=2, ensure_ascii=False), encoding="utf-8")

    if not all(checks.values()):
        raise RuntimeError(f"Step 4 gate failed: checks={checks}, counts={counts}, audit={audit}")
    summary = {
        "gate": "PASS",
        "runtime": {
            "kafka": "apache/kafka:4.3.1",
            "spark": "apache/spark:4.2.0-python3",
            "spark_state_store": "RocksDB",
            "kafka_partitions_per_topic": 3,
            "watermark_delay": "10 minutes",
            "dedup_state_ttl_seconds": 86_400,
        },
        "topic_counts": counts,
        "partition_evidence": {
            "unique_station_keys": len(station_partitions),
            "partitions_used": partitions_used,
        },
        "watermark_evidence": progress,
        "checks": checks,
        "alert_id": main_alerts[0]["alert_id"],
        "alert_event_ids": main_ids,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
