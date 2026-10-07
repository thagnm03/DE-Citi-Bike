from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
RUNTIME = ROOT / "artifacts" / "step8" / "runtime"


def read_json_rows(path: Path) -> list[dict]:
    rows: list[dict] = []
    for file in sorted(path.glob("part-*.json")):
        rows.extend(json.loads(line) for line in file.read_text(encoding="utf-8").splitlines() if line.strip())
    return rows


def main() -> int:
    scenario = json.loads((RUNTIME / "scenario.json").read_text(encoding="utf-8"))
    output = RUNTIME / "output"
    dlq = read_json_rows(output / "dead_letter")
    updates = read_json_rows(output / "state_updates")
    snapshot = read_json_rows(output / "current_state_snapshot")
    windows = read_json_rows(output / "window_metrics")

    if len(dlq) != 2:
        raise RuntimeError(f"Expected 2 DLQ rows, found {len(dlq)}")
    error_codes = {code for row in dlq for code in row["error_codes"]}
    if "MALFORMED_JSON" not in error_codes or "AVAILABILITY_INVALID" not in error_codes:
        raise RuntimeError(f"DLQ error codes are incomplete: {sorted(error_codes)}")

    accepted = scenario["accepted_event_ids"]
    update_ids = [row["event_id"] for row in updates]
    if len(updates) != 6 or set(update_ids) != set(accepted):
        raise RuntimeError(f"State updates differ from accepted events: {update_ids}")
    if len(update_ids) != len(set(update_ids)):
        raise RuntimeError("Duplicate state update was emitted")
    for rejected in (
        scenario["rejected_out_of_order_event_id"],
        scenario["rejected_late_event_id"],
    ):
        if rejected in update_ids:
            raise RuntimeError(f"Rejected event changed current state: {rejected}")

    if len(snapshot) != 3:
        raise RuntimeError(f"Expected 3 current stations, found {len(snapshot)}")
    latest = {row["station_id"]: row["event_id"] for row in snapshot}
    if latest != scenario["latest_by_station"]:
        raise RuntimeError(f"Current snapshot is wrong: {latest}")
    baseline_found = []
    for row in snapshot:
        payload = json.loads(row["current_state_json"])
        baseline_found.append(payload["historical_demand"]["found"])
    if not all(baseline_found):
        raise RuntimeError("Historical baseline join failed for a controlled station")

    observation_sum = sum(int(row["observation_count"]) for row in windows)
    expected_window_count = int(scenario["window_observation_count"])
    if observation_sum != expected_window_count:
        raise RuntimeError(
            "Window metrics should include in-watermark out-of-order data and exclude "
            f"duplicates/late data: expected {expected_window_count}, got {observation_sum}"
        )
    first_a_window = [
        row
        for row in windows
        if row["station_id"] == "gbfs-station-a"
        and row["window_start"] == "2026-04-06T12:00:00.000Z"
    ]
    if len(first_a_window) != 1 or int(first_a_window[0]["observation_count"]) != int(
        scenario["a_first_window_observation_count"]
    ):
        raise RuntimeError(
            "The first A window must contain A0, A1, and allowed out-of-order A3 only; "
            f"got {first_a_window}"
        )

    progress_rows: list[dict] = []
    for path in (RUNTIME / "progress").glob("*.ndjson"):
        progress_rows.extend(
            json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()
        )
    watermark_drops = sum(
        int(operator.get("numRowsDroppedByWatermark", 0))
        for progress in progress_rows
        for operator in progress.get("stateOperators", [])
    )
    result = {
        "gate": "PASS",
        "dead_letter_rows": len(dlq),
        "dead_letter_error_codes": sorted(error_codes),
        "state_updates": len(updates),
        "current_stations": len(snapshot),
        "baseline_joined_stations": sum(baseline_found),
        "window_rows": len(windows),
        "window_observation_sum": observation_sum,
        "late_record_excluded_from_closed_windows": True,
        "reported_watermark_rows_dropped": watermark_drops,
        "checkpoint_directories": sorted(
            path.name for path in (RUNTIME / "checkpoints").iterdir() if path.is_dir()
        ),
    }
    destination = ROOT / "artifacts" / "step8" / "verification-summary.json"
    destination.write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
