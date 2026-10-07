from __future__ import annotations

import json

import pytest

from serving.materializer import unwrap_record, validate_current_state
from serving.repository import command_id, effective_lifecycle
from serving.api import AcknowledgeRequest
from tests.alerts.fixtures import current_state


@pytest.mark.parametrize(
    ("current", "incoming", "expected"),
    [
        (None, "OPEN", "OPEN"),
        ("OPEN", "ACKNOWLEDGED", "ACKNOWLEDGED"),
        ("ACKNOWLEDGED", "OPEN", "ACKNOWLEDGED"),
        ("ACKNOWLEDGED", "RESOLVED", "RESOLVED"),
        ("RESOLVED", "OPEN", "RESOLVED"),
    ],
)
def test_effective_lifecycle_never_loses_ack_or_resolution(current, incoming, expected) -> None:
    assert effective_lifecycle(current, incoming) == expected


def test_command_id_is_deterministic_and_alert_scoped() -> None:
    first = command_id("retry-key-1", "alert-a")
    assert first == command_id("retry-key-1", "alert-a")
    assert first != command_id("retry-key-1", "alert-b")


def test_unwrap_spark_alert_record() -> None:
    payload = {"alert_id": "a"}
    assert unwrap_record({"alert_json": json.dumps(payload)}, "alert") == payload


def test_unwrap_spark_current_record() -> None:
    payload = current_state("HIGH", 1, bikes=5, docks=15)
    assert unwrap_record({"current_state_json": json.dumps(payload)}, "current") == payload


def test_current_state_validation_accepts_contract_shape() -> None:
    validate_current_state(current_state("HIGH", 1, bikes=5, docks=15))


def test_current_state_validation_rejects_station_mismatch() -> None:
    payload = current_state("HIGH", 1, bikes=5, docks=15)
    payload["station"]["station_id"] = "wrong"
    with pytest.raises(ValueError, match="does not match"):
        validate_current_state(payload)


def test_acknowledge_timestamp_requires_timezone() -> None:
    with pytest.raises(ValueError, match="timezone"):
        AcknowledgeRequest(
            idempotency_key="valid-key",
            requested_by="operator",
            acknowledged_at_utc="2026-04-06T12:06:00",
        )
