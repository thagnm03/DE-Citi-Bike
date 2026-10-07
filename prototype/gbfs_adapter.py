from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

from .rules import sha256_parts, utc_text


MIN_PLAUSIBLE_STATION_EPOCH = 1_450_155_600
FUTURE_TOLERANCE_SECONDS = 300
SENTINEL_STATION_EPOCH = 86_400


def _timestamp_fields(raw_value: Any, observed_at: datetime) -> tuple[str, str | None, int | None, list[str]]:
    if raw_value is None:
        return "MISSING", None, None, ["SOURCE_TIMESTAMP_MISSING"]
    if not isinstance(raw_value, int):
        return "OUT_OF_RANGE", None, None, ["SOURCE_TIMESTAMP_OUT_OF_RANGE"]
    if raw_value == SENTINEL_STATION_EPOCH:
        return "SENTINEL", None, None, ["SOURCE_TIMESTAMP_SENTINEL"]
    if raw_value < MIN_PLAUSIBLE_STATION_EPOCH:
        return "OUT_OF_RANGE", None, None, ["SOURCE_TIMESTAMP_OUT_OF_RANGE"]
    if raw_value > int(observed_at.timestamp()) + FUTURE_TOLERANCE_SECONDS:
        return "FUTURE", None, None, ["SOURCE_TIMESTAMP_FUTURE"]
    reported_at = datetime.fromtimestamp(raw_value, tz=timezone.utc)
    age_seconds = max(0, int((observed_at - reported_at).total_seconds()))
    return "VALID", utc_text(reported_at), age_seconds, []


def normalize_snapshot(
    status_bytes: bytes,
    station_information: dict[str, Any],
    *,
    observed_at: datetime,
    raw_archive_path: str,
    stale_after_seconds: int = 180,
) -> list[dict[str, Any]]:
    """Normalize one immutable GBFS status response into the v1 observation contract."""
    observed_at = observed_at.astimezone(timezone.utc)
    normalized_at_text = utc_text(observed_at)
    status_document = json.loads(status_bytes)
    snapshot_epoch = int(status_document["last_updated"])
    snapshot_time = utc_text(datetime.fromtimestamp(snapshot_epoch, tz=timezone.utc))
    snapshot_sha256 = hashlib.sha256(status_bytes).hexdigest()
    feed_version = str(status_document["version"])
    feed_ttl = int(status_document["ttl"])
    metadata_by_id = {
        str(item["station_id"]): item for item in station_information["data"]["stations"]
    }

    output: list[dict[str, Any]] = []
    for source in status_document["data"]["stations"]:
        station_id = str(source["station_id"])
        metadata = metadata_by_id.get(station_id)
        timestamp_status, reported_at, source_age, issue_codes = _timestamp_fields(
            source.get("last_reported"), observed_at
        )
        if source_age is not None and source_age > stale_after_seconds:
            issue_codes.append("SOURCE_STALE")
        if metadata is None:
            issue_codes.append("METADATA_UNMATCHED")

        bikes = int(source["num_bikes_available"])
        bikes_disabled = int(source["num_bikes_disabled"])
        docks = int(source["num_docks_available"])
        docks_disabled = int(source["num_docks_disabled"])
        capacity = int(metadata["capacity"]) if metadata is not None else None
        observed_capacity = bikes + bikes_disabled + docks + docks_disabled
        if capacity is None:
            capacity_consistency = "NOT_EVALUATED"
        elif observed_capacity == capacity:
            capacity_consistency = "EXACT"
        else:
            capacity_consistency = "MISMATCH"
            issue_codes.append("CAPACITY_MISMATCH")

        installed = bool(source["is_installed"])
        renting = bool(source["is_renting"])
        returning = bool(source["is_returning"])
        event_id = sha256_parts(
            "citibike_nyc",
            station_id,
            snapshot_time,
            bikes,
            bikes_disabled,
            docks,
            docks_disabled,
            int(installed),
            int(renting),
            int(returning),
            source.get("last_reported"),
        )
        output.append(
            {
                "schema_version": "1.0",
                "event_id": event_id,
                "event_type": "STATION_STATUS_OBSERVED",
                "source_system": "citibike_nyc",
                "station": {
                    "station_id": station_id,
                    "short_name": metadata.get("short_name") if metadata else None,
                    "name": metadata.get("name") if metadata else None,
                    "latitude": metadata.get("lat") if metadata else None,
                    "longitude": metadata.get("lon") if metadata else None,
                    "capacity": capacity,
                    "metadata_match_status": "MATCHED_BY_STATION_ID" if metadata else "UNMATCHED",
                },
                "availability": {
                    "bikes_available": bikes,
                    "bikes_disabled": bikes_disabled,
                    "docks_available": docks,
                    "docks_disabled": docks_disabled,
                    "ebikes_available": source.get("num_ebikes_available"),
                },
                "service": {
                    "is_installed": installed,
                    "is_renting": renting,
                    "is_returning": returning,
                },
                "time": {
                    "snapshot_updated_at_utc": snapshot_time,
                    "station_reported_at_raw": source.get("last_reported"),
                    "station_reported_at_utc": reported_at,
                    "observed_at_utc": normalized_at_text,
                    "normalized_at_utc": normalized_at_text,
                    "source_age_seconds": source_age,
                },
                "source": {
                    "feed_name": "station_status",
                    "feed_version": feed_version,
                    "feed_ttl_seconds": feed_ttl,
                    "snapshot_sha256": snapshot_sha256,
                    "raw_archive_path": raw_archive_path,
                },
                "quality": {
                    "timestamp_status": timestamp_status,
                    "capacity_consistency": capacity_consistency,
                    "issue_codes": list(dict.fromkeys(issue_codes)),
                },
            }
        )
    return output
