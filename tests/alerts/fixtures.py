from __future__ import annotations

import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any

from spark.common.alert_rules import sha256_parts, utc_text


BASE_TIME = datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc)
STATIONS = {
    "HIGH": ("alert-high", "7293.10", "High pickup demand"),
    "LOW": ("alert-low", "7456.03", "Low pickup demand"),
    "UPDATE": ("alert-update", "7014.12", "Persistent empty update"),
    "FULL": ("alert-full", "7293.10", "High dropoff demand"),
    "OFFLINE": ("alert-offline", "7456.03", "Offline inspection"),
    "STALE": ("alert-stale", "7014.12", "Stale inspection"),
}


def current_state(
    station: str,
    minute: int,
    *,
    bikes: int,
    docks: int,
    p95_pickups: int = 0,
    p95_dropoffs: int = 0,
    renting: bool = True,
    source_age: int | None = 10,
) -> dict[str, Any]:
    station_id, short_name, name = STATIONS[station]
    event_time = BASE_TIME + timedelta(minutes=minute)
    event_text = event_time.isoformat(timespec="seconds").replace("+00:00", "Z")
    event_id = hashlib.sha256(
        f"{station_id}|{event_text}|{bikes}|{docks}|{renting}|{source_age}".encode()
    ).hexdigest()
    return {
        "schema_version": "1.0",
        "station_id": station_id,
        "event_id": event_id,
        "event_time_utc": event_text,
        "station": {
            "station_id": station_id,
            "short_name": short_name,
            "name": name,
            "latitude": 40.77,
            "longitude": -73.95,
            "capacity": 20,
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
            "is_installed": True,
            "is_renting": renting,
            "is_returning": renting,
        },
        "quality": {
            "timestamp_status": "VALID",
            "capacity_consistency": "EXACT",
            "issue_codes": [],
        },
        "source_age_seconds": source_age,
        "historical_demand": {
            "found": True,
            "iso_weekday": 1,
            "local_hour": 8,
            "calendar_days": 4,
            "avg_pickups": float(p95_pickups) / 2,
            "p50_pickups": p95_pickups // 2,
            "p95_pickups": p95_pickups,
            "avg_dropoffs": float(p95_dropoffs) / 2,
            "p50_dropoffs": p95_dropoffs // 2,
            "p95_dropoffs": p95_dropoffs,
        },
        "lineage": {"input_partition": 0, "input_offset": 0},
    }


def scenario() -> dict[str, Any]:
    high_balanced = current_state("HIGH", -1, bikes=10, docks=10, p95_pickups=8)
    low_balanced = current_state("LOW", -1, bikes=10, docks=10)
    update_balanced = current_state("UPDATE", -1, bikes=10, docks=10, p95_pickups=8)
    full_balanced = current_state("FULL", -1, bikes=10, docks=10, p95_dropoffs=8)
    offline_balanced = current_state("OFFLINE", -1, bikes=10, docks=10)
    stale_balanced = current_state("STALE", -1, bikes=10, docks=10)

    high_empty = current_state("HIGH", 0, bikes=0, docks=20, p95_pickups=8)
    low_empty = current_state("LOW", 0, bikes=0, docks=20)
    update_empty = current_state("UPDATE", 0, bikes=0, docks=20, p95_pickups=8)
    full = current_state("FULL", 0, bikes=20, docks=0, p95_dropoffs=8)
    offline = current_state("OFFLINE", 0, bikes=0, docks=20, renting=False)
    stale = current_state("STALE", 0, bikes=0, docks=20, source_age=600)

    high_open = current_state("HIGH", 5, bikes=0, docks=20, p95_pickups=8)
    low_open = current_state("LOW", 5, bikes=0, docks=20)
    update_open = current_state("UPDATE", 5, bikes=0, docks=20, p95_pickups=8)
    full_open = current_state("FULL", 5, bikes=20, docks=0, p95_dropoffs=8)
    offline_open = current_state("OFFLINE", 5, bikes=0, docks=20, renting=False)
    stale_open = current_state("STALE", 5, bikes=0, docks=20, source_age=600)
    low_out_of_order = current_state("LOW", 3, bikes=2, docks=18)

    high_alert_id = sha256_parts(
        "citibike_nyc",
        STATIONS["HIGH"][0],
        "EMPTY",
        utc_text(int(BASE_TIME.timestamp() * 1000)),
    )
    return {
        "phase1": [
            high_balanced,
            low_balanced,
            update_balanced,
            full_balanced,
            offline_balanced,
            stale_balanced,
            high_empty,
            low_empty,
            update_empty,
            full,
            offline,
            stale,
            high_open,
            low_open,
            low_open,
            update_open,
            full_open,
            offline_open,
            stale_open,
            low_out_of_order,
            "{broken-current-state",
        ],
        "phase2": [
            current_state("UPDATE", 10, bikes=0, docks=20, p95_pickups=8),
            current_state("HIGH", 12, bikes=10, docks=10, p95_pickups=8),
        ],
        "acknowledgements": [
            {
                "alert_id": high_alert_id,
                "acknowledged_at_utc": "2026-04-06T12:06:00Z",
            }
        ],
        "expected": {
            "high_alert_id": high_alert_id,
            "event_type_counts": {
                "ALERT_OPENED": 6,
                "ALERT_UPDATED": 1,
                "ALERT_ACKNOWLEDGED": 1,
                "ALERT_RESOLVED": 1,
            },
            "active_alerts": 5,
            "top_station_id": STATIONS["UPDATE"][0],
            "actions": ["DELIVER_BIKES", "INSPECT_STATION", "REMOVE_BIKES"],
        },
    }
