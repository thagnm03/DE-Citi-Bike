from __future__ import annotations

import pytest

from spark.batch.historical_pipeline import normalized_header, resolve_columns


MODERN_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "start_station_name",
    "start_station_id",
    "end_station_name",
    "end_station_id",
    "start_lat",
    "start_lng",
    "end_lat",
    "end_lng",
    "member_casual",
]


def test_modern_schema_allows_additive_columns() -> None:
    mapping, variant = resolve_columns(MODERN_COLUMNS + ["future_optional_column"])
    assert variant == "modern"
    assert mapping["ride_id"] == "ride_id"
    assert mapping["started_at"] == "started_at"


def test_legacy_aliases_are_resolved_and_missing_ride_id_is_allowed() -> None:
    mapping, variant = resolve_columns(
        [
            "Start Time",
            "Stop Time",
            "Start Station ID",
            "End Station ID",
            "Start Station Name",
            "End Station Name",
            "Start Station Latitude",
            "Start Station Longitude",
            "End Station Latitude",
            "End Station Longitude",
            "User Type",
        ]
    )
    assert variant == "legacy_aliases"
    assert mapping["ride_id"] is None
    assert mapping["started_at"] == "Start Time"
    assert mapping["member_casual"] == "User Type"


def test_missing_required_timestamp_column_fails_fast() -> None:
    with pytest.raises(ValueError, match="ended_at"):
        resolve_columns(["ride_id", "started_at", "start_station_id"])


def test_header_normalization_is_stable() -> None:
    assert normalized_header(" Start Station ID ") == "start_station_id"
    assert normalized_header("start-station.id") == "start_station_id"
