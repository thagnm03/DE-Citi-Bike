from __future__ import annotations

import argparse
import json
import logging
import os
import time
from pathlib import Path
from typing import Any, Iterable

from confluent_kafka import Consumer, KafkaError
from jsonschema import Draft7Validator, FormatChecker

from serving.repository import ServingRepository


ROOT = Path(__file__).resolve().parents[1]
LOGGER = logging.getLogger("citibike-serving-materializer")


def configure_logging() -> None:
    logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"), format="%(message)s")


def log_event(level: int, event: str, message: str, **fields: Any) -> None:
    payload = {
        "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "level": logging.getLevelName(level),
        "service": "serving-materializer",
        "event": event,
        "message": message,
        **fields,
    }
    LOGGER.log(level, json.dumps(payload, separators=(",", ":"), default=str))


def alert_validator() -> Draft7Validator:
    schema = json.loads((ROOT / "contracts" / "station-alert-v1.json").read_text(encoding="utf-8"))
    return Draft7Validator(schema, format_checker=FormatChecker())


def validate_current_state(payload: dict[str, Any]) -> None:
    required = {"schema_version", "station_id", "event_id", "event_time_utc", "station", "availability", "service"}
    missing = sorted(required - payload.keys())
    if missing:
        raise ValueError(f"Current state missing fields: {', '.join(missing)}")
    if payload["station_id"] != payload["station"].get("station_id"):
        raise ValueError("Current state station_id does not match station.station_id")


def unwrap_record(record: dict[str, Any], kind: str) -> dict[str, Any]:
    if kind == "alert" and "alert_json" in record:
        return json.loads(record["alert_json"])
    if kind == "current" and "current_state_json" in record:
        return json.loads(record["current_state_json"])
    return record


def data_files(path: Path) -> list[Path]:
    if path.is_file():
        return [path]
    return sorted(item for item in path.rglob("part-*") if item.is_file() and not item.name.startswith("."))


def read_json_lines(path: Path, kind: str) -> Iterable[dict[str, Any]]:
    for file_path in data_files(path):
        for line_number, line in enumerate(file_path.read_text(encoding="utf-8").splitlines(), start=1):
            if not line.strip():
                continue
            try:
                raw = json.loads(line)
                yield unwrap_record(raw, kind)
            except Exception as exc:
                raise ValueError(f"Invalid JSON at {file_path}:{line_number}: {exc}") from exc


def validate_payload(payload: dict[str, Any], kind: str, validator: Draft7Validator) -> None:
    if kind == "alert":
        errors = sorted(validator.iter_errors(payload), key=lambda item: list(item.path))
        if errors:
            raise ValueError(errors[0].message)
    else:
        validate_current_state(payload)


def materialize_files(
    repository: ServingRepository,
    *,
    alerts_path: Path | None,
    current_path: Path | None,
) -> dict[str, int]:
    validator = alert_validator()
    result = {"alert_read": 0, "alert_inserted": 0, "current_read": 0, "current_updated": 0}
    for kind, path in (("current", current_path), ("alert", alerts_path)):
        if path is None:
            continue
        for payload in read_json_lines(path, kind):
            validate_payload(payload, kind, validator)
            result[f"{kind}_read"] += 1
            changed = (
                repository.store_alert_event(payload)
                if kind == "alert"
                else repository.store_current_state(payload)
            )
            result[f"{kind}_inserted" if kind == "alert" else "current_updated"] += int(changed)
    log_event(logging.INFO, "file_materialization_complete", "File serving projection completed", **result)
    return result


def run_kafka(repository: ServingRepository, args: argparse.Namespace) -> None:
    validator = alert_validator()
    consumer = Consumer(
        {
            "bootstrap.servers": args.bootstrap_servers,
            "group.id": args.group_id,
            "auto.offset.reset": args.auto_offset_reset,
            "enable.auto.commit": False,
        }
    )
    topics = [args.alert_topic] if args.no_current else [args.current_topic, args.alert_topic]
    consumer.subscribe(topics)
    idle_since = time.monotonic()
    log_event(logging.INFO, "consumer_started", "Serving materializer subscribed", topics=topics)
    try:
        while True:
            message = consumer.poll(1.0)
            if message is None:
                if args.max_idle_seconds and time.monotonic() - idle_since >= args.max_idle_seconds:
                    break
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise RuntimeError(str(message.error()))
            idle_since = time.monotonic()
            topic = message.topic()
            kind = "alert" if topic == args.alert_topic else "current"
            payload = json.loads(message.value().decode("utf-8"))
            validate_payload(payload, kind, validator)
            repository.store_consumer_record(
                topic=topic,
                partition=message.partition(),
                offset=message.offset(),
                payload=payload,
                kind=kind,
            )
            consumer.commit(message=message, asynchronous=False)
    finally:
        consumer.close()
        log_event(logging.INFO, "consumer_stopped", "Serving materializer stopped")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Materialize Citi Bike state and alerts into PostgreSQL")
    parser.add_argument("--source", choices=("file", "kafka"), default="kafka")
    parser.add_argument("--database-url", default=os.environ.get("DATABASE_URL", "postgresql://citibike:citibike_local_only@postgres:5432/citibike"))
    parser.add_argument("--alerts-path", type=Path)
    parser.add_argument("--current-path", type=Path)
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092"))
    parser.add_argument("--group-id", default="citibike-serving-v1")
    parser.add_argument("--auto-offset-reset", choices=("earliest", "latest"), default="earliest")
    parser.add_argument("--current-topic", default="citibike.station-current.v1")
    parser.add_argument("--alert-topic", default="citibike.station-alerts.v1")
    parser.add_argument("--no-current", action="store_true", help="Subscribe only to the alert topic")
    parser.add_argument("--max-idle-seconds", type=float, default=0)
    return parser.parse_args()


def main() -> None:
    configure_logging()
    args = parse_args()
    repository = ServingRepository(args.database_url)
    if args.source == "file":
        if not args.alerts_path and not args.current_path:
            raise SystemExit("File source requires --alerts-path and/or --current-path")
        result = materialize_files(
            repository,
            alerts_path=args.alerts_path,
            current_path=args.current_path,
        )
        print(json.dumps(result, indent=2))
    else:
        run_kafka(repository, args)


if __name__ == "__main__":
    main()
