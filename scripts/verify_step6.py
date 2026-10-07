from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Consumer, TopicPartition

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from prototype.contracts import load_validator, validate_or_raise


TOPIC = "citibike.station-status.v1"


def main() -> int:
    parser = argparse.ArgumentParser(description="Verify the live Step 6 Kafka ingestion gate")
    parser.add_argument("--bootstrap-servers", default="127.0.0.1:9092")
    parser.add_argument("--metrics", required=True)
    parser.add_argument("--output", default="artifacts/step6/live-review-summary.json")
    args = parser.parse_args()

    metrics_path = (ROOT / args.metrics).resolve()
    metrics = json.loads(metrics_path.read_text(encoding="utf-8"))
    if metrics["poll_success_total"] < 1 or metrics["events_published_total"] < 1:
        raise RuntimeError("Collector metrics do not contain a successful published snapshot")
    if metrics["invalid_events_total"] != 0:
        raise RuntimeError("Live collector emitted invalid records")

    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": "step6-verifier",
            "enable.auto.commit": False,
            "auto.offset.reset": "earliest",
        }
    )
    try:
        metadata = consumer.list_topics(TOPIC, timeout=10)
        topic_metadata = metadata.topics.get(TOPIC)
        if topic_metadata is None or topic_metadata.error is not None:
            raise RuntimeError(f"Kafka topic unavailable: {topic_metadata}")
        partition_ids = sorted(topic_metadata.partitions)
        offsets: dict[int, dict[str, int]] = {}
        latest_positions: list[TopicPartition] = []
        for partition_id in partition_ids:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(TOPIC, partition_id), timeout=10
            )
            offsets[partition_id] = {"low": low, "high": high, "retained_records": high - low}
            if high > low:
                latest_positions.append(TopicPartition(TOPIC, partition_id, high - 1))
        if not latest_positions:
            raise RuntimeError("Kafka observation topic contains no records")

        consumer.assign(latest_positions)
        samples: list[tuple[str, dict]] = []
        deadline = datetime.now(timezone.utc).timestamp() + 15
        while len(samples) < len(latest_positions) and datetime.now(timezone.utc).timestamp() < deadline:
            message = consumer.poll(1)
            if message is None:
                continue
            if message.error() is not None:
                raise RuntimeError(str(message.error()))
            samples.append((message.key().decode("utf-8"), json.loads(message.value())))
    finally:
        consumer.close()

    if not samples:
        raise RuntimeError("Could not read a live event from Kafka")
    validator = load_validator(ROOT / "contracts" / "station-status-v1.json")
    for kafka_key, sample in samples:
        validate_or_raise(sample, validator)
        if kafka_key != sample["station"]["station_id"]:
            raise RuntimeError("Kafka key is not the canonical station_id")
        if sample["source"]["snapshot_sha256"] != metrics["last_snapshot_sha256"]:
            raise RuntimeError("Latest Kafka event does not match the collector snapshot hash")
        archive_path = ROOT / sample["source"]["raw_archive_path"]
        if not archive_path.exists():
            raise RuntimeError(f"Raw archive is missing: {archive_path}")

    summary = {
        "gate": "PASS",
        "verified_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "collector": {
            "poll_success_total": metrics["poll_success_total"],
            "events_published_total": metrics["events_published_total"],
            "invalid_events_total": metrics["invalid_events_total"],
            "last_snapshot_sha256": metrics["last_snapshot_sha256"],
        },
        "kafka": {
            "topic": TOPIC,
            "partition_count": len(partition_ids),
            "offsets": offsets,
            "samples_validated": len(samples),
        },
        "source": {
            "feed_version": samples[0][1]["source"]["feed_version"],
            "raw_archive_path": samples[0][1]["source"]["raw_archive_path"],
        },
    }
    output_path = (ROOT / args.output).resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    print(json.dumps(summary, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
