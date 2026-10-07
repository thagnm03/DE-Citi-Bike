from __future__ import annotations

import argparse
import json
from pathlib import Path

from spark.common.alert_rules import BusinessRuleConfig, build_alert_event, sha256_parts, utc_text
from tests.alerts.fixtures import BASE_TIME, current_state


ROOT = Path(__file__).resolve().parents[2]


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text(
        "".join(json.dumps(row, separators=(",", ":")) + "\n" for row in rows),
        encoding="utf-8",
    )


def build_fixture(output: Path) -> dict:
    output.mkdir(parents=True, exist_ok=True)
    config = BusinessRuleConfig.from_json(ROOT / "config" / "business-rules-v1.json")
    base_ms = int(BASE_TIME.timestamp() * 1000)

    high_open_state = current_state("HIGH", 5, bikes=0, docks=20, p95_pickups=8)
    high_resolved_state = current_state("HIGH", 12, bikes=10, docks=10, p95_pickups=8)
    full_open_state = current_state("FULL", 5, bikes=20, docks=0, p95_dropoffs=8)
    full_updated_state = current_state("FULL", 10, bikes=20, docks=0, p95_dropoffs=8)
    offline_open_state = current_state("OFFLINE", 5, bikes=0, docks=20, renting=False)

    def alert_id(state: dict, alert_type: str) -> str:
        return sha256_parts("citibike_nyc", state["station_id"], alert_type, utc_text(base_ms))

    high_id = alert_id(high_open_state, "EMPTY")
    full_id = alert_id(full_open_state, "FULL")
    offline_id = alert_id(offline_open_state, "OFFLINE")

    def event(state: dict, aid: str, alert_type: str, minute: int, event_type: str, trend: float, offset: int) -> dict:
        return build_alert_event(
            payload=state,
            alert_id=aid,
            alert_type=alert_type,
            episode_started_ms=base_ms,
            detected_ms=base_ms + 5 * 60_000,
            event_time_ms=base_ms + minute * 60_000,
            event_type=event_type,
            trend_score=trend,
            config=config,
            input_partition=offset % 3,
            input_offset=offset,
        )

    phase1 = [
        event(high_open_state, high_id, "EMPTY", 5, "ALERT_OPENED", 100.0, 1),
        event(full_open_state, full_id, "FULL", 5, "ALERT_OPENED", 100.0, 2),
        event(offline_open_state, offline_id, "OFFLINE", 5, "ALERT_OPENED", 0.0, 3),
        event(high_resolved_state, high_id, "EMPTY", 12, "ALERT_RESOLVED", 0.0, 4),
    ]
    phase2 = [event(full_updated_state, full_id, "FULL", 10, "ALERT_UPDATED", 100.0, 5)]
    current = [high_resolved_state, full_updated_state, offline_open_state]

    write_jsonl(output / "alerts-phase1.jsonl", phase1)
    write_jsonl(output / "alerts-phase2.jsonl", phase2)
    write_jsonl(output / "current-state.jsonl", current)
    expected = {
        "resolved_alert_id": high_id,
        "acknowledge_alert_id": full_id,
        "offline_alert_id": offline_id,
        "phase1_alert_events": 4,
        "final_alert_events": 6,
        "active_alerts": 2,
        "stations": 3,
    }
    (output / "expected.json").write_text(json.dumps(expected, indent=2), encoding="utf-8")
    return expected


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build_fixture(args.output), indent=2))


if __name__ == "__main__":
    main()
