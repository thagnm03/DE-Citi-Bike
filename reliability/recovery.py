from __future__ import annotations

import hashlib
import json
import re
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


METRIC_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+([-+0-9.eE]+)$")


def request(url: str, *, timeout: float = 15.0) -> tuple[int, bytes]:
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read()


def request_json(url: str, *, timeout: float = 15.0) -> tuple[int, dict[str, Any]]:
    status, body = request(url, timeout=timeout)
    return status, json.loads(body.decode("utf-8"))


def metric_value(text: str, name: str) -> float | None:
    for line in text.splitlines():
        match = METRIC_RE.match(line)
        if match and match.group(1) == name:
            return float(match.group(2))
    return None


def canonical_hash(payload: Any) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode()
    return hashlib.sha256(encoded).hexdigest()


def capture_state(base_url: str, database_url: str) -> dict[str, Any]:
    base = base_url.rstrip("/")
    alert_status, alerts = request_json(base + "/api/v1/alerts?active_only=false&limit=500")
    station_status, stations = request_json(base + "/api/v1/stations?limit=500")
    if alert_status != 200 or station_status != 200:
        raise AssertionError(
            f"Serving snapshot failed: alerts={alert_status}, stations={station_status}"
        )

    with psycopg.connect(database_url, row_factory=dict_row) as connection:
        counts = connection.execute(
            """
            SELECT
              (SELECT count(*) FROM serving.alert_events) AS alert_events,
              (SELECT count(*) FROM serving.alert_state) AS alert_states,
              (SELECT count(*) FROM serving.station_current) AS stations,
              (SELECT count(*) FROM serving.alert_commands) AS commands
            """
        ).fetchone()
        event_ids = [
            row["alert_event_id"]
            for row in connection.execute(
                "SELECT alert_event_id FROM serving.alert_events ORDER BY alert_event_id"
            ).fetchall()
        ]
        state_versions = [
            [row["alert_id"], row["last_event_id"], row["lifecycle_status"]]
            for row in connection.execute(
                """SELECT alert_id, last_event_id, lifecycle_status
                   FROM serving.alert_state ORDER BY alert_id"""
            ).fetchall()
        ]
        station_versions = [
            [row["station_id"], row["event_id"]]
            for row in connection.execute(
                "SELECT station_id, event_id FROM serving.station_current ORDER BY station_id"
            ).fetchall()
        ]
        command_ids = [
            row["command_id"]
            for row in connection.execute(
                "SELECT command_id FROM serving.alert_commands ORDER BY command_id"
            ).fetchall()
        ]

    business_identity = {
        "event_ids": event_ids,
        "state_versions": state_versions,
        "station_versions": station_versions,
        "command_ids": command_ids,
    }
    return {
        "api": {
            "alert_count": alerts["count"],
            "station_count": stations["count"],
            "priority_scores": [item["priority"]["score"] for item in alerts["items"]],
        },
        "database_counts": {key: int(value) for key, value in counts.items()},
        "business_fingerprint": canonical_hash(business_identity),
    }


def observe_database_outage(base_url: str) -> dict[str, Any]:
    base = base_url.rstrip("/")
    ready_status, ready = request_json(base + "/health/ready")
    metrics_status, metrics_body = request(base + "/metrics")
    text = metrics_body.decode("utf-8")
    database_up = metric_value(text, "citibike_serving_database_up")
    return {
        "readiness_http_status": ready_status,
        "readiness_detail": ready.get("detail"),
        "metrics_http_status": metrics_status,
        "database_up_metric": database_up,
        "pass": ready_status == 503 and metrics_status == 200 and database_up == 0,
    }


def wait_for_alert(
    prometheus_url: str,
    alert_name: str,
    *,
    state: str = "firing",
    timeout_seconds: float = 70.0,
) -> dict[str, Any]:
    expression = urllib.parse.quote(
        f'ALERTS{{alertname="{alert_name}",alertstate="{state}"}}'
    )
    url = prometheus_url.rstrip("/") + "/api/v1/query?query=" + expression
    started = time.monotonic()
    last_result: list[dict[str, Any]] = []
    while time.monotonic() - started < timeout_seconds:
        try:
            status, payload = request_json(url)
            last_result = payload.get("data", {}).get("result", [])
            if status == 200 and last_result:
                return {
                    "alert_name": alert_name,
                    "alert_state": state,
                    "observed": True,
                    "time_to_observe_seconds": round(time.monotonic() - started, 3),
                }
        except Exception:
            pass
        time.sleep(2)
    return {
        "alert_name": alert_name,
        "alert_state": state,
        "observed": False,
        "time_to_observe_seconds": round(time.monotonic() - started, 3),
        "last_result": last_result,
    }


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
