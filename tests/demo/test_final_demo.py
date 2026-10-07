from __future__ import annotations

import json

from demo.final_demo import prepare


def test_prepare_builds_reproducible_demo_inputs(tmp_path) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first_manifest = prepare(first)
    second_manifest = prepare(second)
    assert first_manifest == second_manifest
    assert first_manifest["current_station_records"] == 6
    assert len(list((first / "input").glob("event-*.json"))) == 5
    assert len(list((first / "staged_phase2").glob("event-*.json"))) == 1
    assert (first / "acknowledgements.jsonl").read_bytes() == (
        second / "acknowledgements.jsonl"
    ).read_bytes()


def test_prepare_selects_latest_current_state_per_station(tmp_path) -> None:
    runtime = tmp_path / "runtime"
    prepare(runtime)
    rows = [
        json.loads(line)
        for line in (runtime / "current-state.jsonl").read_text(encoding="utf-8").splitlines()
    ]
    assert len(rows) == len({row["station_id"] for row in rows}) == 6
    by_station = {row["station_id"]: row for row in rows}
    assert by_station["alert-update"]["event_time_utc"] == "2026-04-06T12:10:00Z"
    assert by_station["alert-high"]["availability"]["bikes_available"] == 10
    assert all(isinstance(row, dict) for row in rows)
