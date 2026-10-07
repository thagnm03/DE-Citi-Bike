from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from prototype.contracts import load_validator, validate_or_raise
from tests.alerts.fixtures import scenario


def write_event(path: Path, value: object, modified_epoch: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    values = value if isinstance(value, list) else [value]
    path.write_text(
        "".join(
            (item if isinstance(item, str) else json.dumps(item, separators=(",", ":"), sort_keys=True))
            + "\n"
            for item in values
        ),
        encoding="utf-8",
    )
    os.utime(path, (modified_epoch, modified_epoch))


def prepare(runtime: Path) -> dict[str, Any]:
    fixture = scenario()
    epoch = 1_810_000_000
    phase1_batches = [
        fixture["phase1"][0:6],
        fixture["phase1"][6:12],
        fixture["phase1"][12:19],
        fixture["phase1"][19:20],
        fixture["phase1"][20:21],
    ]
    for index, batch in enumerate(phase1_batches):
        write_event(runtime / "input" / f"event-{index:03d}.json", batch, epoch + index)
    write_event(
        runtime / "staged_phase2" / f"event-{len(phase1_batches):03d}.json",
        fixture["phase2"],
        epoch + len(phase1_batches),
    )
    ack_file = runtime / "acknowledgements.jsonl"
    ack_file.parent.mkdir(parents=True, exist_ok=True)
    ack_file.write_text(
        "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in fixture["acknowledgements"]),
        encoding="utf-8",
    )

    latest: dict[str, dict[str, Any]] = {}
    for item in [*fixture["phase1"], *fixture["phase2"]]:
        if not isinstance(item, dict) or "station_id" not in item or "event_time_utc" not in item:
            continue
        existing = latest.get(item["station_id"])
        if existing is None or item["event_time_utc"] > existing["event_time_utc"]:
            latest[item["station_id"]] = item
    current_path = runtime / "current-state.jsonl"
    current_path.write_text(
        "".join(
            json.dumps(item, separators=(",", ":"), sort_keys=True) + "\n"
            for item in sorted(latest.values(), key=lambda value: value["station_id"])
        ),
        encoding="utf-8",
    )
    manifest = {
        "phase1_files": len(phase1_batches),
        "phase2_files": 1,
        "current_station_records": len(latest),
        **fixture["expected"],
    }
    (runtime / "scenario.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return manifest


def read_rows(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for file in sorted(path.glob("part-*.json")):
        rows.extend(
            json.loads(line)
            for line in file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return rows


def package(runtime: Path) -> dict[str, Any]:
    wrappers = read_rows(runtime / "spark-output" / "alert_events")
    alerts = [json.loads(row["alert_json"]) for row in wrappers]
    validator = load_validator(ROOT / "contracts" / "station-alert-v1.json")
    for alert in alerts:
        validate_or_raise(alert, validator)
    event_ids = [alert["alert_event_id"] for alert in alerts]
    if len(event_ids) != len(set(event_ids)):
        raise AssertionError("Spark demo output contains duplicate lifecycle event IDs")
    alerts.sort(key=lambda item: (item["time"]["status_changed_at_utc"], item["alert_event_id"]))
    publish_path = runtime / "alerts-to-publish.jsonl"
    publish_path.write_text(
        "".join(json.dumps(item, separators=(",", ":"), sort_keys=True) + "\n" for item in alerts),
        encoding="utf-8",
    )
    queue = read_rows(runtime / "spark-output" / "priority_queue")
    dead_letter = read_rows(runtime / "spark-output" / "dead_letter")
    result = {
        "spark_alert_events": len(alerts),
        "unique_alert_event_ids": len(set(event_ids)),
        "active_priority_tasks": len(queue),
        "dead_letter_rows": len(dead_letter),
        "recommended_actions": sorted({item["recommended_action"] for item in queue}),
        "top_priority_station": next(
            item["station_id"] for item in queue if int(item["queue_rank"]) == 1
        ),
    }
    (runtime / "spark-package-summary.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description="Prepare or package the deterministic final demo")
    parser.add_argument("command", choices=("prepare", "package"))
    parser.add_argument("--runtime", type=Path, required=True)
    args = parser.parse_args()
    result = prepare(args.runtime) if args.command == "prepare" else package(args.runtime)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
