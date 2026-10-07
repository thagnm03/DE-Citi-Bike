from __future__ import annotations

import argparse
import json
from pathlib import Path

from confluent_kafka import Producer


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    args = parser.parse_args()

    producer = Producer({"bootstrap.servers": args.bootstrap_servers, "enable.idempotence": True, "acks": "all"})
    count = 0
    for line in args.input.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        payload = json.loads(line)
        producer.produce(
            args.topic,
            key=payload["station"]["station_id"].encode("utf-8"),
            value=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        )
        producer.poll(0)
        count += 1
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"Kafka did not acknowledge {remaining} fixture records")
    print(json.dumps({"topic": args.topic, "published": count}))


if __name__ == "__main__":
    main()
