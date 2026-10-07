from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RISK_ACTION = {
    "EMPTY": "DELIVER_BIKES",
    "LOW_BIKES": "DELIVER_BIKES",
    "LOW_DOCKS": "REMOVE_BIKES",
    "FULL": "REMOVE_BIKES",
    "STALE": "INSPECT_STATION",
    "OFFLINE": "INSPECT_STATION",
    "DATA_QUALITY": "INSPECT_STATION",
}

RISK_REASON = {
    "EMPTY": "EMPTY_PERSISTED",
    "LOW_BIKES": "LOW_BIKES_PERSISTED",
    "LOW_DOCKS": "LOW_DOCKS_PERSISTED",
    "FULL": "FULL_PERSISTED",
    "STALE": "SOURCE_STALE",
    "OFFLINE": "STATION_OFFLINE",
    "DATA_QUALITY": "DATA_QUALITY_RISK",
}

RISK_SEVERITY = {
    "EMPTY": ("CRITICAL", 100.0),
    "FULL": ("CRITICAL", 100.0),
    "OFFLINE": ("CRITICAL", 90.0),
    "STALE": ("WARNING", 80.0),
    "LOW_BIKES": ("WARNING", 60.0),
    "LOW_DOCKS": ("WARNING", 60.0),
    "DATA_QUALITY": ("WARNING", 50.0),
}


@dataclass(frozen=True)
class BusinessRuleConfig:
    rule_version: str
    persistence_seconds: int
    stale_after_seconds: int
    low_bikes_threshold: int
    low_docks_threshold: int
    dedup_ttl_seconds: int
    watermark_delay: str
    priority_update_delta: float
    priority_update_interval_seconds: int
    high_demand_p95_reference: float
    rapid_trend_threshold: float
    weights: dict[str, float]

    @classmethod
    def from_json(cls, path: Path) -> "BusinessRuleConfig":
        config = cls(**json.loads(path.read_text(encoding="utf-8")))
        required = {
            "severity",
            "persistence",
            "historical_demand",
            "recent_trend",
            "service_impact",
        }
        if set(config.weights) != required:
            raise ValueError(f"Priority weights must be exactly {sorted(required)}")
        if abs(sum(config.weights.values()) - 1.0) > 1e-9:
            raise ValueError("Priority weights must sum to 1.0")
        if config.persistence_seconds <= 0 or config.high_demand_p95_reference <= 0:
            raise ValueError("Persistence and demand reference must be positive")
        return config


def sha256_parts(*parts: object) -> str:
    value = "|".join("null" if part is None else str(part) for part in parts)
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def utc_text(epoch_ms: int) -> str:
    return (
        datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )


def classify_current_state(payload: dict[str, Any], config: BusinessRuleConfig) -> str:
    quality = payload["quality"]
    availability = payload["availability"]
    service = payload["service"]
    source_age = payload.get("source_age_seconds")

    if "SOURCE_STALE" in quality["issue_codes"] or (
        source_age is not None and source_age > config.stale_after_seconds
    ):
        return "STALE"
    if not service["is_installed"] or not service["is_renting"] or not service["is_returning"]:
        return "OFFLINE"
    if (
        quality["timestamp_status"] != "VALID"
        or payload["station"]["metadata_match_status"] == "UNMATCHED"
    ):
        return "DATA_QUALITY"

    bikes = int(availability["bikes_available"])
    docks = int(availability["docks_available"])
    if bikes == 0 and docks == 0:
        return "DATA_QUALITY"
    if bikes == 0:
        return "EMPTY"
    if docks == 0:
        return "FULL"
    if bikes <= config.low_bikes_threshold:
        return "LOW_BIKES"
    if docks <= config.low_docks_threshold:
        return "LOW_DOCKS"
    return "BALANCED"


def onset_trend_score(
    risk: str,
    previous_bikes: int | None,
    previous_docks: int | None,
    current_bikes: int,
    current_docks: int,
) -> float:
    if risk in {"EMPTY", "LOW_BIKES"} and previous_bikes is not None:
        return round(100.0 * max(0, previous_bikes - current_bikes) / max(previous_bikes, 1), 2)
    if risk in {"FULL", "LOW_DOCKS"} and previous_docks is not None:
        return round(100.0 * max(0, previous_docks - current_docks) / max(previous_docks, 1), 2)
    return 0.0


def priority_for(
    risk: str,
    duration_seconds: int,
    historical: dict[str, Any],
    trend_score: float,
    config: BusinessRuleConfig,
) -> tuple[str, str, list[str], dict[str, float], float]:
    severity, severity_score = RISK_SEVERITY[risk]
    action = RISK_ACTION[risk]
    reasons = [RISK_REASON[risk]]

    persistence = min(
        100.0,
        60.0 + 40.0 * max(0, duration_seconds - config.persistence_seconds) / config.persistence_seconds,
    )
    demand_value = 0.0
    demand_reason = None
    if historical.get("found"):
        if risk in {"EMPTY", "LOW_BIKES"}:
            demand_value = float(historical.get("p95_pickups") or 0)
            demand_reason = "HIGH_HISTORICAL_PICKUP_DEMAND"
        elif risk in {"FULL", "LOW_DOCKS"}:
            demand_value = float(historical.get("p95_dropoffs") or 0)
            demand_reason = "HIGH_HISTORICAL_DROPOFF_DEMAND"
    demand_score = min(100.0, 100.0 * demand_value / config.high_demand_p95_reference)
    if demand_reason and demand_score >= 80.0:
        reasons.append(demand_reason)

    trend_score = min(100.0, max(0.0, float(trend_score)))
    if trend_score >= config.rapid_trend_threshold:
        if risk in {"EMPTY", "LOW_BIKES"}:
            reasons.append("RAPID_OUTFLOW")
        elif risk in {"FULL", "LOW_DOCKS"}:
            reasons.append("RAPID_INFLOW")

    service_impact = 100.0 if risk in {"EMPTY", "FULL", "OFFLINE"} else 60.0
    components = {
        "severity": round(severity_score, 2),
        "persistence": round(persistence, 2),
        "historical_demand": round(demand_score, 2),
        "recent_trend": round(trend_score, 2),
        "service_impact": round(service_impact, 2),
    }
    score = round(sum(config.weights[name] * value for name, value in components.items()), 2)
    return severity, action, reasons, components, score


def build_alert_event(
    *,
    payload: dict[str, Any],
    alert_id: str,
    alert_type: str,
    episode_started_ms: int,
    detected_ms: int,
    event_time_ms: int,
    event_type: str,
    trend_score: float,
    config: BusinessRuleConfig,
    input_partition: int,
    input_offset: int,
) -> dict[str, Any]:
    resolved = event_type == "ALERT_RESOLVED"
    duration = max(0, (event_time_ms - episode_started_ms) // 1000)
    if resolved:
        severity = "INFO"
        action = "NONE"
        reasons = ["RISK_CLEARED"]
        components = {name: 0.0 for name in config.weights}
        score = 0.0
    else:
        severity, action, reasons, components, score = priority_for(
            alert_type,
            duration,
            payload.get("historical_demand") or {},
            trend_score,
            config,
        )

    event_time = utc_text(event_time_ms)
    return {
        "schema_version": "1.0",
        "alert_event_id": sha256_parts(alert_id, event_type, event_time),
        "alert_id": alert_id,
        "event_type": event_type,
        "lifecycle_status": "RESOLVED" if resolved else "OPEN",
        "alert_type": alert_type,
        "recommended_action": action,
        "severity": severity,
        "station": {
            "station_id": payload["station_id"],
            "short_name": payload["station"].get("short_name"),
            "name": payload["station"].get("name"),
        },
        "time": {
            "episode_started_at_utc": utc_text(episode_started_ms),
            "detected_at_utc": utc_text(detected_ms),
            "status_changed_at_utc": event_time,
            "last_observed_at_utc": event_time,
            "resolved_at_utc": event_time if resolved else None,
        },
        "evidence": {
            "risk_duration_seconds": int(duration),
            "bikes_available": int(payload["availability"]["bikes_available"]),
            "docks_available": int(payload["availability"]["docks_available"]),
            "is_renting": bool(payload["service"]["is_renting"]),
            "is_returning": bool(payload["service"]["is_returning"]),
            "source_age_seconds": payload.get("source_age_seconds"),
            "reason_codes": reasons,
        },
        "priority": {
            "score": score,
            "rule_version": config.rule_version,
            "components": components,
        },
        "lineage": {
            "triggering_event_id": payload["event_id"],
            "input_topic": "citibike.station-status.v1",
            "input_partition": int(input_partition),
            "input_offset": int(input_offset),
            "generated_at_utc": event_time,
        },
    }


def build_acknowledged_event(open_event: dict[str, Any], acknowledged_at_utc: str) -> dict[str, Any]:
    if open_event["lifecycle_status"] not in {"OPEN", "ACKNOWLEDGED"}:
        raise ValueError("Only an active alert can be acknowledged")
    acknowledged = json.loads(json.dumps(open_event))
    acknowledged["event_type"] = "ALERT_ACKNOWLEDGED"
    acknowledged["lifecycle_status"] = "ACKNOWLEDGED"
    acknowledged["time"]["status_changed_at_utc"] = acknowledged_at_utc
    acknowledged["time"]["last_observed_at_utc"] = acknowledged_at_utc
    acknowledged["lineage"]["generated_at_utc"] = acknowledged_at_utc
    acknowledged["alert_event_id"] = sha256_parts(
        acknowledged["alert_id"], "ALERT_ACKNOWLEDGED", acknowledged_at_utc
    )
    return acknowledged
