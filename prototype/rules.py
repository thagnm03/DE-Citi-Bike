from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


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


def parse_utc(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError(f"Timestamp must include a timezone: {value}")
    return parsed.astimezone(timezone.utc)


def utc_text(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def sha256_parts(*parts: object) -> str:
    text = "|".join("null" if part is None else str(part) for part in parts)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class RuleConfig:
    persistence_seconds: int = 300
    stale_after_seconds: int = 180
    low_bikes_threshold: int = 2
    low_docks_threshold: int = 2
    dedup_ttl_seconds: int = 86_400
    watermark_delay: str = "10 minutes"
    rule_version: str = "prototype-rules-1.0"

    @classmethod
    def from_json(cls, path: Path) -> "RuleConfig":
        return cls(**json.loads(path.read_text(encoding="utf-8")))


@dataclass
class StationState:
    current_risk: str | None = None
    episode_started_at_utc: str | None = None
    alert_id: str | None = None
    alert_opened: bool = False
    detected_at_utc: str | None = None
    last_event_time_utc: str | None = None


@dataclass
class Metrics:
    received: int = 0
    processed: int = 0
    duplicates: int = 0
    out_of_order: int = 0
    alerts_opened: int = 0
    alerts_resolved: int = 0
    quality_warnings: int = 0


def classify(observation: dict[str, Any], config: RuleConfig) -> str:
    """Classify one observation; health/freshness always precede availability."""
    quality = observation["quality"]
    availability = observation["availability"]
    service = observation["service"]
    source_age = observation["time"]["source_age_seconds"]

    if "SOURCE_STALE" in quality["issue_codes"] or (
        source_age is not None and source_age > config.stale_after_seconds
    ):
        return "STALE"

    if not service["is_installed"] or not service["is_renting"] or not service["is_returning"]:
        return "OFFLINE"

    if quality["timestamp_status"] != "VALID" or observation["station"]["metadata_match_status"] == "UNMATCHED":
        return "DATA_QUALITY"

    bikes = availability["bikes_available"]
    docks = availability["docks_available"]

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


class EpisodeEngine:
    """Deterministic reference implementation of the stateful station rule."""

    def __init__(self, config: RuleConfig | None = None) -> None:
        self.config = config or RuleConfig()
        self.states: dict[str, StationState] = {}
        self.seen_event_ids: set[str] = set()
        self.metrics = Metrics()

    def process(
        self,
        observation: dict[str, Any],
        *,
        input_partition: int = 0,
        input_offset: int = 0,
    ) -> list[dict[str, Any]]:
        self.metrics.received += 1
        event_id = observation["event_id"]
        if event_id in self.seen_event_ids:
            self.metrics.duplicates += 1
            return []

        station_id = observation["station"]["station_id"]
        event_time_text = observation["time"]["snapshot_updated_at_utc"]
        event_time = parse_utc(event_time_text)
        state = self.states.setdefault(station_id, StationState())

        if state.last_event_time_utc and event_time <= parse_utc(state.last_event_time_utc):
            self.seen_event_ids.add(event_id)
            self.metrics.out_of_order += 1
            return []

        self.seen_event_ids.add(event_id)
        self.metrics.processed += 1
        if observation["quality"]["issue_codes"]:
            self.metrics.quality_warnings += 1

        risk = classify(observation, self.config)
        output: list[dict[str, Any]] = []

        if risk != state.current_risk:
            if state.alert_opened and state.current_risk:
                output.append(
                    self._make_alert(
                        observation,
                        state,
                        event_type="ALERT_RESOLVED",
                        input_partition=input_partition,
                        input_offset=input_offset,
                    )
                )
                self.metrics.alerts_resolved += 1

            state.current_risk = None if risk == "BALANCED" else risk
            state.episode_started_at_utc = None if risk == "BALANCED" else event_time_text
            state.alert_id = None
            state.alert_opened = False
            state.detected_at_utc = None

        if state.current_risk and state.episode_started_at_utc:
            duration = int((event_time - parse_utc(state.episode_started_at_utc)).total_seconds())
            if not state.alert_opened and duration >= self.config.persistence_seconds:
                state.alert_id = sha256_parts(
                    observation["source_system"],
                    station_id,
                    state.current_risk,
                    state.episode_started_at_utc,
                )
                state.alert_opened = True
                state.detected_at_utc = event_time_text
                output.append(
                    self._make_alert(
                        observation,
                        state,
                        event_type="ALERT_OPENED",
                        input_partition=input_partition,
                        input_offset=input_offset,
                    )
                )
                self.metrics.alerts_opened += 1

        state.last_event_time_utc = event_time_text
        return output

    def process_many(self, observations: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
        alerts: list[dict[str, Any]] = []
        for offset, observation in enumerate(observations):
            station_id = observation["station"]["station_id"]
            partition = int(hashlib.sha256(station_id.encode("utf-8")).hexdigest()[:8], 16) % 3
            alerts.extend(self.process(observation, input_partition=partition, input_offset=offset))
        return alerts

    def _make_alert(
        self,
        observation: dict[str, Any],
        state: StationState,
        *,
        event_type: str,
        input_partition: int,
        input_offset: int,
    ) -> dict[str, Any]:
        assert state.current_risk
        assert state.episode_started_at_utc
        assert state.alert_id
        event_time_text = observation["time"]["snapshot_updated_at_utc"]
        duration = int((parse_utc(event_time_text) - parse_utc(state.episode_started_at_utc)).total_seconds())
        resolved = event_type == "ALERT_RESOLVED"

        if resolved:
            severity = "INFO"
            action = "NONE"
            reason_codes = ["RISK_CLEARED"]
            components = {
                "severity": 0.0,
                "persistence": 0.0,
                "historical_demand": 0.0,
                "recent_trend": 0.0,
                "service_impact": 0.0,
            }
            score = 0.0
        else:
            severity, severity_score = RISK_SEVERITY[state.current_risk]
            action = RISK_ACTION[state.current_risk]
            reason_codes = [RISK_REASON[state.current_risk]]
            persistence_score = min(100.0, 60.0 * duration / self.config.persistence_seconds)
            service_score = 100.0 if state.current_risk in {"EMPTY", "FULL", "OFFLINE"} else 60.0
            components = {
                "severity": severity_score,
                "persistence": round(persistence_score, 2),
                "historical_demand": 0.0,
                "recent_trend": 0.0,
                "service_impact": service_score,
            }
            score = round(
                0.4 * components["severity"]
                + 0.2 * components["persistence"]
                + 0.2 * components["historical_demand"]
                + 0.1 * components["recent_trend"]
                + 0.1 * components["service_impact"],
                2,
            )

        alert_event_id = sha256_parts(state.alert_id, event_type, event_time_text)
        availability = observation["availability"]
        service = observation["service"]
        station = observation["station"]

        return {
            "schema_version": "1.0",
            "alert_event_id": alert_event_id,
            "alert_id": state.alert_id,
            "event_type": event_type,
            "lifecycle_status": "RESOLVED" if resolved else "OPEN",
            "alert_type": state.current_risk,
            "recommended_action": action,
            "severity": severity,
            "station": {
                "station_id": station["station_id"],
                "short_name": station["short_name"],
                "name": station["name"],
            },
            "time": {
                "episode_started_at_utc": state.episode_started_at_utc,
                "detected_at_utc": state.detected_at_utc or event_time_text,
                "status_changed_at_utc": event_time_text,
                "last_observed_at_utc": event_time_text,
                "resolved_at_utc": event_time_text if resolved else None,
            },
            "evidence": {
                "risk_duration_seconds": duration,
                "bikes_available": availability["bikes_available"],
                "docks_available": availability["docks_available"],
                "is_renting": service["is_renting"],
                "is_returning": service["is_returning"],
                "source_age_seconds": observation["time"]["source_age_seconds"],
                "reason_codes": reason_codes,
            },
            "priority": {
                "score": score,
                "rule_version": self.config.rule_version,
                "components": components,
            },
            "lineage": {
                "triggering_event_id": observation["event_id"],
                "input_topic": "citibike.station-status.v1",
                "input_partition": input_partition,
                "input_offset": input_offset,
                "generated_at_utc": event_time_text,
            },
        }

    def save_checkpoint(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "rule_version": self.config.rule_version,
            "states": {key: asdict(value) for key, value in self.states.items()},
            "seen_event_ids": sorted(self.seen_event_ids),
            "metrics": asdict(self.metrics),
        }
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
        os.replace(temporary, path)

    @classmethod
    def load_checkpoint(cls, path: Path, config: RuleConfig | None = None) -> "EpisodeEngine":
        engine = cls(config)
        if not path.exists():
            return engine
        payload = json.loads(path.read_text(encoding="utf-8"))
        if payload["rule_version"] != engine.config.rule_version:
            raise ValueError("Checkpoint rule version does not match current configuration")
        engine.states = {key: StationState(**value) for key, value in payload["states"].items()}
        engine.seen_event_ids = set(payload["seen_event_ids"])
        engine.metrics = Metrics(**payload["metrics"])
        return engine
