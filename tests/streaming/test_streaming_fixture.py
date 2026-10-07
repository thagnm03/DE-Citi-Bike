from __future__ import annotations

from prototype.contracts import load_validator, validate_or_raise
from tests.streaming.fixtures import scenario


def test_controlled_stream_has_expected_duplicate_and_ordering() -> None:
    fixture = scenario()
    phase1 = fixture["phase1"]
    valid = [item for item in phase1 if isinstance(item, dict) and item["availability"]["bikes_available"] >= 0]
    event_ids = [item["event_id"] for item in valid]
    assert event_ids.count(fixture["expected"]["duplicate_event_id"]) == 2
    assert fixture["expected"]["rejected_out_of_order_event_id"] in event_ids
    assert fixture["expected"]["rejected_late_event_id"] in event_ids


def test_all_intentionally_valid_fixture_records_match_contract() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[2]
    validator = load_validator(root / "contracts" / "station-status-v1.json")
    fixture = scenario()
    for item in [*fixture["phase1"], *fixture["phase2"]]:
        if not isinstance(item, dict) or item["availability"]["bikes_available"] < 0:
            continue
        validate_or_raise(item, validator)


def test_fixture_uses_historical_short_names_for_baseline_join() -> None:
    fixture = scenario()
    short_names = {
        item["station"]["short_name"]
        for item in [*fixture["phase1"], *fixture["phase2"]]
        if isinstance(item, dict)
    }
    assert short_names == {"7293.10", "7456.03", "7014.12"}
