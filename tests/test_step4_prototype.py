from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path

import pytest

from prototype.contracts import load_validator, validate_or_raise
from prototype.demo import run_demo
from prototype.fixtures import (
    late_observation,
    make_observation,
    partition_probe_observations,
    persistent_empty_sequence,
)
from prototype.gbfs_adapter import normalize_snapshot
from prototype.rules import EpisodeEngine, RuleConfig, classify


ROOT = Path(__file__).resolve().parents[1]
CONFIG = RuleConfig.from_json(ROOT / "config" / "prototype-rules.json")


def test_fixture_and_alerts_satisfy_contracts() -> None:
    status_validator = load_validator(ROOT / "contracts" / "station-status-v1.json")
    alert_validator = load_validator(ROOT / "contracts" / "station-alert-v1.json")
    engine = EpisodeEngine(CONFIG)

    alerts = []
    for observation in persistent_empty_sequence():
        validate_or_raise(observation, status_validator)
        alerts.extend(engine.process(observation))

    assert [alert["event_type"] for alert in alerts] == ["ALERT_OPENED", "ALERT_RESOLVED"]
    assert alerts[0]["alert_id"] == alerts[1]["alert_id"]
    for alert in alerts:
        validate_or_raise(alert, alert_validator)


def test_duplicate_does_not_open_second_alert() -> None:
    engine = EpisodeEngine(CONFIG)
    alerts = engine.process_many(persistent_empty_sequence())

    assert len([alert for alert in alerts if alert["event_type"] == "ALERT_OPENED"]) == 1
    assert engine.metrics.duplicates == 1


def test_restart_and_clean_replay_are_deterministic(tmp_path: Path) -> None:
    observations = persistent_empty_sequence()
    checkpoint = tmp_path / "checkpoint.json"
    engine = EpisodeEngine(CONFIG)
    before_restart = engine.process_many(observations[:4])
    engine.save_checkpoint(checkpoint)

    restarted = EpisodeEngine.load_checkpoint(checkpoint, CONFIG)
    after_restart = restarted.process_many(observations[4:])
    restarted.save_checkpoint(checkpoint)
    original_ids = [item["alert_event_id"] for item in before_restart + after_restart]

    same_checkpoint = EpisodeEngine.load_checkpoint(checkpoint, CONFIG)
    assert same_checkpoint.process_many(observations) == []

    clean_replay = EpisodeEngine(CONFIG).process_many(observations)
    assert [item["alert_event_id"] for item in clean_replay] == original_ids


def test_health_precedence_prevents_false_empty_alert() -> None:
    now = datetime(2026, 9, 21, 3, 0, tzinfo=timezone.utc)
    offline = make_observation(now, bikes=0, docks=10, renting=False)
    stale = make_observation(now, bikes=0, docks=10, source_age_seconds=600, issue_codes=["SOURCE_STALE"])

    assert classify(offline, CONFIG) == "OFFLINE"
    assert classify(stale, CONFIG) == "STALE"


def test_capacity_mismatch_is_warning_not_rejection() -> None:
    observation = make_observation(datetime(2026, 9, 21, 3, 0, tzinfo=timezone.utc), bikes=5, docks=5)
    observation["station"]["capacity"] = 25
    observation["quality"]["capacity_consistency"] = "MISMATCH"
    observation["quality"]["issue_codes"] = ["CAPACITY_MISMATCH"]

    validate_or_raise(observation, load_validator(ROOT / "contracts" / "station-status-v1.json"))
    assert classify(observation, CONFIG) == "BALANCED"


def test_out_of_order_record_cannot_rewind_station_state() -> None:
    base = datetime(2026, 9, 21, 3, 0, tzinfo=timezone.utc)
    engine = EpisodeEngine(CONFIG)
    latest = make_observation(base + timedelta(minutes=2), bikes=5, docks=5)
    older = make_observation(base + timedelta(minutes=1), bikes=0, docks=10)

    assert engine.process(latest) == []
    assert engine.process(older) == []
    assert engine.metrics.out_of_order == 1
    assert engine.states["demo-station-001"].current_risk is None


def test_invalid_negative_count_is_rejected() -> None:
    observation = make_observation(datetime(2026, 9, 21, 3, 0, tzinfo=timezone.utc), bikes=5, docks=5)
    observation["availability"]["bikes_available"] = -1

    with pytest.raises(ValueError):
        validate_or_raise(observation, load_validator(ROOT / "contracts" / "station-status-v1.json"))


def test_demo_gate(tmp_path: Path) -> None:
    result = run_demo(ROOT, tmp_path / "demo")
    assert result["gate"] == "PASS"
    assert result["same_checkpoint_replay_outputs"] == 0
    assert result["clean_replay_same_business_ids"] is True
    assert result["structural_invalid_fixtures"] == 1


def test_real_gbfs_snapshot_normalizes_and_preserves_raw_hash() -> None:
    status_path = ROOT / "data" / "samples" / "gbfs" / "station_status_01.json"
    information_path = ROOT / "data" / "samples" / "gbfs" / "station_information.json"
    status_bytes = status_path.read_bytes()
    status_document = json.loads(status_bytes)
    observations = normalize_snapshot(
        status_bytes,
        json.loads(information_path.read_bytes()),
        observed_at=datetime.fromtimestamp(status_document["last_updated"] + 2, tz=timezone.utc),
        raw_archive_path="data/samples/gbfs/station_status_01.json",
        stale_after_seconds=180,
    )

    validator = load_validator(ROOT / "contracts" / "station-status-v1.json")
    assert len(observations) == len(status_document["data"]["stations"])
    assert observations[0]["source"]["snapshot_sha256"] == hashlib.sha256(status_bytes).hexdigest()
    for observation in observations:
        validate_or_raise(observation, validator)


def test_gbfs_sentinel_86400_is_kept_but_not_converted_to_1970() -> None:
    status_path = ROOT / "data" / "samples" / "gbfs" / "station_status_01.json"
    information_path = ROOT / "data" / "samples" / "gbfs" / "station_information.json"
    status_bytes = status_path.read_bytes()
    status_document = json.loads(status_bytes)
    observations = normalize_snapshot(
        status_bytes,
        json.loads(information_path.read_bytes()),
        observed_at=datetime.fromtimestamp(status_document["last_updated"] + 2, tz=timezone.utc),
        raw_archive_path="data/samples/gbfs/station_status_01.json",
    )
    sentinel = next(item for item in observations if item["time"]["station_reported_at_raw"] == 86400)

    assert sentinel["time"]["station_reported_at_utc"] is None
    assert sentinel["time"]["source_age_seconds"] is None
    assert sentinel["quality"]["timestamp_status"] == "SENTINEL"
    assert "SOURCE_TIMESTAMP_SENTINEL" in sentinel["quality"]["issue_codes"]


def test_two_station_keys_can_map_to_different_partitions() -> None:
    first = "demo-station-001"
    first_partition = int(hashlib.sha256(first.encode("utf-8")).hexdigest()[:8], 16) % 3
    second = next(
        f"demo-station-{index:03d}"
        for index in range(2, 100)
        if int(hashlib.sha256(f"demo-station-{index:03d}".encode("utf-8")).hexdigest()[:8], 16) % 3
        != first_partition
    )
    second_partition = int(hashlib.sha256(second.encode("utf-8")).hexdigest()[:8], 16) % 3

    assert first_partition != second_partition


def test_partition_probes_are_valid_balanced_records() -> None:
    validator = load_validator(ROOT / "contracts" / "station-status-v1.json")
    probes = partition_probe_observations()
    assert len({item["station"]["station_id"] for item in probes}) == 6
    for probe in probes:
        validate_or_raise(probe, validator)
        assert classify(probe, CONFIG) == "BALANCED"


def test_late_fixture_is_older_and_unique() -> None:
    late = late_observation()
    sequence = persistent_empty_sequence()
    assert late["event_id"] not in {item["event_id"] for item in sequence}
    assert late["time"]["snapshot_updated_at_utc"] < sequence[0]["time"]["snapshot_updated_at_utc"]
