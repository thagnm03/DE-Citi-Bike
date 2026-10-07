from __future__ import annotations

import argparse
import json
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer, KafkaError

from .contracts import load_validator, validate_or_raise


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Read and validate Step 4 alert events from Kafka")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="citibike.station-alerts.v1")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--expected-count", type=int, default=2)
    parser.add_argument("--timeout-seconds", type=int, default=20)
    args = parser.parse_args()

    validator = load_validator(root / "contracts" / "station-alert-v1.json")
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": f"citibike-step4-inspector-{uuid.uuid4()}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([args.topic])
    records: list[dict] = []
    deadline = time.monotonic() + args.timeout_seconds
    try:
        while time.monotonic() < deadline and len(records) < args.expected_count:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(message.error())
            alert = json.loads(message.value().decode("utf-8"))
            validate_or_raise(alert, validator)
            alert["_kafka"] = {
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            }
            records.append(alert)
    finally:
        consumer.close()

    if len(records) != args.expected_count:
        raise RuntimeError(f"Expected {args.expected_count} alerts from {args.topic}, got {len(records)}")
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        "".join(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n" for record in records),
        encoding="utf-8",
    )
    print(f"CONTRACT_VALID=TRUE TOPIC={args.topic} COUNT={len(records)}")
    for record in records:
        print(
            f"{record['event_type']} alert_id={record['alert_id']} "
            f"partition={record['_kafka']['partition']} offset={record['_kafka']['offset']}"
        )


if __name__ == "__main__":
    main()
