from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request
from pathlib import Path
from types import SimpleNamespace

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from serving.materializer import materialize_files, run_kafka
from serving.repository import ServingRepository


def request_json(base_url: str, path: str, *, method: str = "GET", body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        data=data,
        method=method,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def verify(args: argparse.Namespace) -> dict:
    expected = json.loads((args.fixture_root / "expected.json").read_text(encoding="utf-8"))
    repository = ServingRepository(args.database_url)

    status, ready = request_json(args.base_url, "/health/ready")
    require(status == 200 and ready.get("database") == "healthy", "Readiness endpoint failed")

    status, queue_before = request_json(args.base_url, "/api/v1/alerts")
    require(status == 200, "Alert queue endpoint failed")
    require(queue_before["count"] == expected["active_alerts"], "Unexpected active alert count")
    scores = [item["priority"]["score"] for item in queue_before["items"]]
    require(scores == sorted(scores, reverse=True), "Priority queue is not sorted descending")

    status, stations = request_json(args.base_url, "/api/v1/stations")
    require(status == 200 and stations["count"] == expected["stations"], "Station serving projection mismatch")

    resolved_id = expected["resolved_alert_id"]
    status, resolved = request_json(args.base_url, f"/api/v1/alerts/{resolved_id}")
    require(status == 200 and resolved["lifecycle_status"] == "RESOLVED", "Resolved alert projection mismatch")
    status, resolved_history = request_json(args.base_url, f"/api/v1/alerts/{resolved_id}/history")
    require(status == 200 and resolved_history["count"] == 2, "Resolved alert history mismatch")

    acknowledge_id = expected["acknowledge_alert_id"]
    command = {
        "idempotency_key": "step10-ack-command-0001",
        "requested_by": "step10-gate",
        "acknowledged_at_utc": "2026-04-06T12:06:00Z",
    }
    status, first_ack = request_json(
        args.base_url,
        f"/api/v1/alerts/{acknowledge_id}/acknowledge",
        method="POST",
        body=command,
    )
    require(status == 200, "First acknowledge command failed")
    status, retry_ack = request_json(
        args.base_url,
        f"/api/v1/alerts/{acknowledge_id}/acknowledge",
        method="POST",
        body=command,
    )
    require(status == 200, "Idempotent acknowledge retry failed")
    first_event_id = first_ack["alert_event"]["alert_event_id"]
    require(first_event_id == retry_ack["alert_event"]["alert_event_id"], "ACK retry returned a different event")

    status, conflict = request_json(
        args.base_url,
        f"/api/v1/alerts/{resolved_id}/acknowledge",
        method="POST",
        body={**command, "idempotency_key": "step10-resolved-command"},
    )
    require(status == 409 and "Resolved" in conflict["detail"], "Resolved ACK did not return 409")

    if args.kafka_alert_topic:
        run_kafka(
            repository,
            SimpleNamespace(
                bootstrap_servers=args.bootstrap_servers,
                group_id=args.kafka_group_id,
                auto_offset_reset="earliest",
                current_topic="",
                alert_topic=args.kafka_alert_topic,
                no_current=True,
                max_idle_seconds=5.0,
            ),
        )
    else:
        phase2 = materialize_files(
            repository,
            alerts_path=args.fixture_root / "alerts-phase2.jsonl",
            current_path=None,
        )
        require(phase2["alert_inserted"] == 1, "Late engine update was not inserted")
    status, acknowledged = request_json(args.base_url, f"/api/v1/alerts/{acknowledge_id}")
    require(status == 200, "Acknowledged alert endpoint failed")
    require(
        acknowledged["lifecycle_status"] == "ACKNOWLEDGED",
        "A later Spark update regressed ACKNOWLEDGED to OPEN",
    )
    require(acknowledged["priority"]["score"] == 100.0, "Later priority update was not materialized")

    replay_phase1 = materialize_files(
        repository,
        alerts_path=args.fixture_root / "alerts-phase1.jsonl",
        current_path=args.fixture_root / "current-state.jsonl",
    )
    replay_phase2 = materialize_files(
        repository,
        alerts_path=args.fixture_root / "alerts-phase2.jsonl",
        current_path=None,
    )
    require(replay_phase1["alert_inserted"] == 0, "Alert replay inserted duplicates")
    require(replay_phase1["current_updated"] == 0, "Current-state replay overwrote same-version rows")
    require(replay_phase2["alert_inserted"] == 0, "Update replay inserted duplicates")

    status, missing = request_json(args.base_url, "/api/v1/alerts/not-found")
    require(status == 404 and "not found" in missing["detail"].lower(), "Missing alert did not return 404")

    with repository.connect() as connection:
        counts = connection.execute(
            """
            SELECT
                (SELECT count(*) FROM serving.alert_events) AS alert_events,
                (SELECT count(*) FROM serving.alert_state) AS alert_states,
                (SELECT count(*) FROM serving.alert_state WHERE lifecycle_status <> 'RESOLVED') AS active_alerts,
                (SELECT count(*) FROM serving.station_current) AS stations,
                (SELECT count(*) FROM serving.alert_commands) AS commands
            """
        ).fetchone()
    require(counts["alert_events"] == expected["final_alert_events"], "Alert event table count mismatch")
    require(counts["alert_states"] == 3, "Alert state table count mismatch")
    require(counts["active_alerts"] == expected["active_alerts"], "Active alert table count mismatch")
    require(counts["stations"] == expected["stations"], "Station table count mismatch")
    require(counts["commands"] == 1, "ACK retry created a duplicate command")

    status, history = request_json(args.base_url, f"/api/v1/alerts/{acknowledge_id}/history")
    require(status == 200 and history["count"] == 3, "ACK alert audit history mismatch")

    summary = {
        "gate": "PASS",
        "api_readiness": "ready",
        "active_priority_tasks": counts["active_alerts"],
        "station_current_rows": counts["stations"],
        "alert_episode_rows": counts["alert_states"],
        "append_only_alert_events": counts["alert_events"],
        "ack_commands": counts["commands"],
        "ack_retry_same_event_id": True,
        "ack_survives_later_engine_update": True,
        "resolved_ack_http_status": 409,
        "missing_alert_http_status": 404,
        "replay_inserted_alert_events": 0,
        "priority_queue_sorted": True,
        "api_paths_verified": [
            "/health/ready",
            "/api/v1/alerts",
            "/api/v1/alerts/{alert_id}",
            "/api/v1/alerts/{alert_id}/history",
            "/api/v1/alerts/{alert_id}/acknowledge",
            "/api/v1/stations",
        ],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://serving-api:8000")
    parser.add_argument("--database-url", default="postgresql://citibike:citibike_local_only@postgres:5432/citibike")
    parser.add_argument("--fixture-root", type=Path, default=Path("/workspace/artifacts/step10/fixture"))
    parser.add_argument("--output", type=Path, default=Path("/workspace/artifacts/step10/verification-summary.json"))
    parser.add_argument("--bootstrap-servers", default="kafka:29092")
    parser.add_argument("--kafka-alert-topic")
    parser.add_argument("--kafka-group-id", default="citibike-serving-step10-gate")
    args = parser.parse_args()
    print(json.dumps(verify(args), indent=2))


if __name__ == "__main__":
    main()
