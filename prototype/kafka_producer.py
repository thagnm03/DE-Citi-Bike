from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from confluent_kafka import Producer

from .contracts import load_validator, validate_or_raise


def delivery_report(error, message) -> None:
    if error is not None:
        raise RuntimeError(f"Kafka delivery failed: {error}")
    print(f"DELIVERED {message.topic()}[{message.partition()}]@{message.offset()}")


def main() -> None:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Replay station observations into Kafka")
    parser.add_argument(
        "--input",
        type=Path,
        default=root / "artifacts/step4/local-demo/input-observations.ndjson",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--topic", default="citibike.station-status.v1")
    parser.add_argument("--invalid-topic", default="citibike.station-status.invalid.v1")
    parser.add_argument("--invalid-input", type=Path)
    parser.add_argument("--additional-input", type=Path, action="append", default=[])
    args = parser.parse_args()

    delivery_errors: list[str] = []
    delivered = 0

    def on_delivery(error, message) -> None:
        nonlocal delivered
        if error is not None:
            delivery_errors.append(str(error))
            return
        delivered += 1
        print(f"DELIVERED {message.topic()}[{message.partition()}]@{message.offset()}")

    producer = Producer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,
            "acks": "all",
            "client.id": "citibike-step4-replay",
            "message.timeout.ms": 30000,
        }
    )
    validator = load_validator(root / "contracts" / "station-status-v1.json")
    valid_count = 0
    invalid_count = 0
    inputs = [args.input, *args.additional_input] + ([args.invalid_input] if args.invalid_input else [])
    for input_path in inputs:
        for line_number, line in enumerate(input_path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            observation = None
            try:
                observation = json.loads(line)
                validate_or_raise(observation, validator)
            except (json.JSONDecodeError, ValueError) as error:
                invalid_count += 1
                station_id = (
                    observation.get("station", {}).get("station_id", "unknown")
                    if isinstance(observation, dict)
                    else "unknown"
                )
                invalid_envelope = {
                    "error_code": "CONTRACT_INVALID",
                    "error_message": str(error),
                    "source_file": str(input_path),
                    "source_line": line_number,
                    "raw_sha256": hashlib.sha256(line.encode("utf-8")).hexdigest(),
                    "raw_fragment": line[:1000],
                }
                producer.produce(
                    args.invalid_topic,
                    key=str(station_id).encode("utf-8"),
                    value=json.dumps(invalid_envelope, separators=(",", ":"), sort_keys=True).encode("utf-8"),
                    on_delivery=on_delivery,
                )
            else:
                valid_count += 1
                producer.produce(
                    args.topic,
                    key=observation["station"]["station_id"].encode("utf-8"),
                    value=json.dumps(observation, separators=(",", ":"), sort_keys=True).encode("utf-8"),
                    on_delivery=on_delivery,
                )
            producer.poll(0)
    remaining = producer.flush(30)
    if remaining:
        raise RuntimeError(f"{remaining} Kafka messages were not delivered")
    if delivery_errors:
        raise RuntimeError("Kafka delivery errors: " + "; ".join(delivery_errors))
    print(f"PUBLISHED={delivered} VALID={valid_count} INVALID={invalid_count}")


if __name__ == "__main__":
    main()
