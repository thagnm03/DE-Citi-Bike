from __future__ import annotations

from pathlib import Path

from prototype.contracts import load_validator, validate_or_raise
from spark.common.alert_rules import (
    BusinessRuleConfig,
    build_acknowledged_event,
    build_alert_event,
    classify_current_state,
    onset_trend_score,
    priority_for,
    sha256_parts,
)
from tests.alerts.fixtures import BASE_TIME, current_state


ROOT = Path(__file__).resolve().parents[2]
CONFIG = BusinessRuleConfig.from_json(ROOT / "config" / "business-rules-v1.json")


def test_health_precedence_over_empty() -> None:
    assert classify_current_state(
        current_state("OFFLINE", 0, bikes=0, docks=20, renting=False), CONFIG
    ) == "OFFLINE"
    assert classify_current_state(
        current_state("STALE", 0, bikes=0, docks=20, source_age=600), CONFIG
    ) == "STALE"


def test_historical_demand_and_trend_raise_explainable_priority() -> None:
    high = current_state("HIGH", 5, bikes=0, docks=20, p95_pickups=8)
    severity, action, reasons, components, score = priority_for(
        "EMPTY", 300, high["historical_demand"], 100.0, CONFIG
    )
    low_score = priority_for("EMPTY", 300, {"found": True, "p95_pickups": 0}, 0.0, CONFIG)[4]
    assert severity == "CRITICAL"
    assert action == "DELIVER_BIKES"
    assert "HIGH_HISTORICAL_PICKUP_DEMAND" in reasons
    assert "RAPID_OUTFLOW" in reasons
    assert components["historical_demand"] == 100.0
    assert score > low_score


def test_open_acknowledged_and_resolved_events_match_contract() -> None:
    payload = current_state("HIGH", 5, bikes=0, docks=20, p95_pickups=8)
    start_ms = int(BASE_TIME.timestamp() * 1000)
    event_ms = start_ms + 300_000
    alert_id = sha256_parts("citibike_nyc", payload["station_id"], "EMPTY", "2026-04-06T12:00:00Z")
    opened = build_alert_event(
        payload=payload,
        alert_id=alert_id,
        alert_type="EMPTY",
        episode_started_ms=start_ms,
        detected_ms=event_ms,
        event_time_ms=event_ms,
        event_type="ALERT_OPENED",
        trend_score=onset_trend_score("EMPTY", 10, 10, 0, 20),
        config=CONFIG,
        input_partition=0,
        input_offset=7,
    )
    acknowledged = build_acknowledged_event(opened, "2026-04-06T12:06:00Z")
    resolved = build_alert_event(
        payload=current_state("HIGH", 12, bikes=10, docks=10, p95_pickups=8),
        alert_id=alert_id,
        alert_type="EMPTY",
        episode_started_ms=start_ms,
        detected_ms=event_ms,
        event_time_ms=start_ms + 720_000,
        event_type="ALERT_RESOLVED",
        trend_score=100.0,
        config=CONFIG,
        input_partition=0,
        input_offset=8,
    )
    validator = load_validator(ROOT / "contracts" / "station-alert-v1.json")
    for event in (opened, acknowledged, resolved):
        validate_or_raise(event, validator)
    assert len({event["alert_event_id"] for event in (opened, acknowledged, resolved)}) == 3
    assert resolved["alert_id"] == opened["alert_id"]
    assert resolved["priority"]["score"] == 0.0
