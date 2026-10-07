from __future__ import annotations

import hashlib
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from .rules import sha256_parts, utc_text


def make_observation(
    event_time: datetime,
    *,
    bikes: int,
    docks: int,
    station_id: str = "demo-station-001",
    installed: bool = True,
    renting: bool = True,
    returning: bool = True,
    source_age_seconds: int | None = 10,
    timestamp_status: str = "VALID",
    issue_codes: list[str] | None = None,
) -> dict[str, Any]:
    event_time = event_time.astimezone(timezone.utc)
    event_time_text = utc_text(event_time)
    reported_raw = int(event_time.timestamp()) - source_age_seconds if source_age_seconds is not None else 86400
    reported_text = (
        utc_text(datetime.fromtimestamp(reported_raw, tz=timezone.utc)) if timestamp_status == "VALID" else None
    )
    canonical = (
        "citibike_nyc",
        station_id,
        event_time_text,
        bikes,
        0,
        docks,
        0,
        int(installed),
        int(renting),
        int(returning),
        reported_raw,
    )
    event_id = sha256_parts(*canonical)
    snapshot_hash = hashlib.sha256(f"snapshot|{event_time_text}".encode("utf-8")).hexdigest()

    return {
        "schema_version": "1.0",
        "event_id": event_id,
        "event_type": "STATION_STATUS_OBSERVED",
        "source_system": "citibike_nyc",
        "station": {
            "station_id": station_id,
            "short_name": "DEMO.01",
            "name": "Prototype station",
            "latitude": 40.72,
            "longitude": -73.94,
            "capacity": bikes + docks,
            "metadata_match_status": "MATCHED_BY_STATION_ID",
        },
        "availability": {
            "bikes_available": bikes,
            "bikes_disabled": 0,
            "docks_available": docks,
            "docks_disabled": 0,
            "ebikes_available": 0,
        },
        "service": {
            "is_installed": installed,
            "is_renting": renting,
            "is_returning": returning,
        },
        "time": {
            "snapshot_updated_at_utc": event_time_text,
            "station_reported_at_raw": reported_raw,
            "station_reported_at_utc": reported_text,
            "observed_at_utc": utc_text(event_time + timedelta(seconds=2)),
            "normalized_at_utc": utc_text(event_time + timedelta(seconds=2)),
            "source_age_seconds": source_age_seconds,
        },
        "source": {
            "feed_name": "station_status",
            "feed_version": "1.1",
            "feed_ttl_seconds": 60,
            "snapshot_sha256": snapshot_hash,
            "raw_archive_path": f"raw/prototype/{event_time.strftime('%Y%m%dT%H%M%SZ')}.json",
        },
        "quality": {
            "timestamp_status": timestamp_status,
            "capacity_consistency": "EXACT",
            "issue_codes": issue_codes or [],
        },
    }


def persistent_empty_sequence() -> list[dict[str, Any]]:
    base = datetime(2026, 9, 21, 2, 0, tzinfo=timezone.utc)
    observations = [
        make_observation(base, bikes=5, docks=5),
        make_observation(base + timedelta(minutes=1), bikes=0, docks=10),
        make_observation(base + timedelta(minutes=3), bikes=0, docks=10),
        make_observation(base + timedelta(minutes=6), bikes=0, docks=10),
    ]
    observations.append(deepcopy(observations[-1]))
    observations.extend(
        [
            make_observation(base + timedelta(minutes=9), bikes=0, docks=10),
            make_observation(base + timedelta(minutes=12), bikes=5, docks=5),
        ]
    )
    return observations


def partition_probe_observations() -> list[dict[str, Any]]:
    """Balanced observations for multiple keys; they must never create alerts."""
    event_time = datetime(2026, 9, 21, 2, 0, tzinfo=timezone.utc)
    return [
        make_observation(
            event_time,
            bikes=5,
            docks=5,
            station_id=f"partition-probe-{index:02d}",
        )
        for index in range(1, 7)
    ]


def late_observation() -> dict[str, Any]:
    """A unique event older than the watermark established by the main sequence."""
    return make_observation(
        datetime(2026, 9, 21, 1, 55, tzinfo=timezone.utc),
        bikes=4,
        docks=6,
        station_id="demo-station-001",
    )
