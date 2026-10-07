from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from prototype.contracts import load_validator, validate_or_raise


RUNTIME = ROOT / "artifacts" / "step9" / "runtime"


def read_rows(path: Path) -> list[dict]:
    rows: list[dict] = []
    for file in sorted(path.glob("part-*.json")):
        rows.extend(
            json.loads(line)
            for line in file.read_text(encoding="utf-8").splitlines()
            if line.strip()
        )
    return rows


def main() -> int:
    scenario = json.loads((RUNTIME / "scenario.json").read_text(encoding="utf-8"))
    output = RUNTIME / "output"
    wrappers = read_rows(output / "alert_events")
    alerts = [json.loads(row["alert_json"]) for row in wrappers]
    dlq = read_rows(output / "dead_letter")
    queue = read_rows(output / "priority_queue")

    if len(dlq) != 1 or dlq[0]["validation_errors"] != ["MALFORMED_CURRENT_STATE"]:
        raise RuntimeError(f"Expected one malformed current-state DLQ row, got {dlq}")

    counts = Counter(event["event_type"] for event in alerts)
    if dict(counts) != scenario["event_type_counts"]:
        raise RuntimeError(f"Lifecycle counts differ: {dict(counts)}")
    event_ids = [event["alert_event_id"] for event in alerts]
    if len(event_ids) != len(set(event_ids)):
        raise RuntimeError("Duplicate alert lifecycle event was emitted")

    validator = load_validator(ROOT / "contracts" / "station-alert-v1.json")
    for alert in alerts:
        validate_or_raise(alert, validator)

    high_events = [event for event in alerts if event["alert_id"] == scenario["high_alert_id"]]
    if [event["event_type"] for event in sorted(high_events, key=lambda item: item["time"]["status_changed_at_utc"])] != [
        "ALERT_OPENED",
        "ALERT_ACKNOWLEDGED",
        "ALERT_RESOLVED",
    ]:
        raise RuntimeError(f"High-demand lifecycle is wrong: {high_events}")

    opened_by_station = {
        event["station"]["station_id"]: event
        for event in alerts
        if event["event_type"] == "ALERT_OPENED"
    }
    high = opened_by_station["alert-high"]
    low = opened_by_station["alert-low"]
    if high["priority"]["score"] <= low["priority"]["score"]:
        raise RuntimeError("Historical high-demand EMPTY alert did not outrank low demand")
    if not {
        "EMPTY_PERSISTED",
        "HIGH_HISTORICAL_PICKUP_DEMAND",
        "RAPID_OUTFLOW",
    }.issubset(high["evidence"]["reason_codes"]):
        raise RuntimeError(f"High-demand reasons are incomplete: {high['evidence']['reason_codes']}")

    if len(queue) != int(scenario["active_alerts"]):
        raise RuntimeError(f"Expected {scenario['active_alerts']} active tasks, got {len(queue)}")
    ranks = sorted(int(row["queue_rank"]) for row in queue)
    if ranks != list(range(1, len(queue) + 1)):
        raise RuntimeError(f"Queue ranks are not contiguous: {ranks}")
    top = next(row for row in queue if int(row["queue_rank"]) == 1)
    if top["station_id"] != scenario["top_station_id"]:
        raise RuntimeError(f"Wrong top priority station: {top['station_id']}")
    actions = sorted({row["recommended_action"] for row in queue})
    if actions != sorted(scenario["actions"]):
        raise RuntimeError(f"Queue actions differ: {actions}")
    if any(row["alert_id"] == scenario["high_alert_id"] for row in queue):
        raise RuntimeError("Resolved alert remains in the priority queue")

    replay_alerts = [
        json.loads(row["alert_json"])
        for row in read_rows(RUNTIME / "replay_output" / "alert_events")
    ]
    replay_ids = sorted(event["alert_event_id"] for event in replay_alerts)
    if replay_ids != sorted(event_ids):
        raise RuntimeError("Clean replay did not reproduce identical business event IDs")

    result = {
        "gate": "PASS",
        "dead_letter_rows": len(dlq),
        "lifecycle_event_counts": dict(sorted(counts.items())),
        "unique_alert_events": len(event_ids),
        "active_priority_tasks": len(queue),
        "top_priority_station": top["station_id"],
        "top_priority_score": top["priority_score"],
        "recommended_actions": actions,
        "high_demand_score": high["priority"]["score"],
        "low_demand_score": low["priority"]["score"],
        "clean_replay_same_business_ids": True,
        "checkpoint_directories": sorted(
            path.name for path in (RUNTIME / "checkpoints").iterdir() if path.is_dir()
        ),
    }
    destination = ROOT / "artifacts" / "step9" / "verification-summary.json"
    destination.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
