from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import dict_row


ROOT = Path(__file__).resolve().parents[1]


def request_json(
    base_url: str,
    path: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        base_url.rstrip("/") + path,
        method=method,
        data=data,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


def request_text(base_url: str, path: str) -> tuple[int, str]:
    try:
        with urllib.request.urlopen(base_url.rstrip("/") + path, timeout=15) as response:
            return response.status, response.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read().decode("utf-8")


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def read_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def wait_prometheus(prometheus_url: str, timeout_seconds: float = 45.0) -> None:
    query = urllib.parse.quote('up{job="serving-api"}')
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            status, result = request_json(
                prometheus_url, f"/api/v1/query?query={query}"
            )
            values = result.get("data", {}).get("result", [])
            if status == 200 and values and float(values[0]["value"][1]) == 1:
                return
        except Exception:
            pass
        time.sleep(2)
    raise AssertionError("Prometheus did not report serving-api up")


def verify(args: argparse.Namespace) -> dict[str, Any]:
    package = read_json(args.runtime / "spark-package-summary.json")
    timings = read_json(args.timings)

    dashboard_status, dashboard = request_text(args.base_url, "/dashboard")
    require(dashboard_status == 200 and 'id="alert-rows"' in dashboard, "Dashboard unavailable")
    metrics_status, metrics = request_text(args.base_url, "/metrics")
    require(
        metrics_status == 200 and "citibike_station_source_age_seconds" in metrics,
        "Metrics contract unavailable",
    )

    status, alerts_before = request_json(args.base_url, "/api/v1/alerts?limit=500")
    require(status == 200 and alerts_before["count"] == 5, "Expected five active tasks")
    scores = [item["priority"]["score"] for item in alerts_before["items"]]
    require(scores == sorted(scores, reverse=True), "Serving queue lost priority ordering")
    actions = sorted({item["recommended_action"] for item in alerts_before["items"]})
    require(
        actions == ["DELIVER_BIKES", "INSPECT_STATION", "REMOVE_BIKES"],
        f"Unexpected action coverage: {actions}",
    )
    top = alerts_before["items"][0]
    require(top["station"]["station_id"] == "alert-update", "Wrong top priority station")

    status, stations = request_json(args.base_url, "/api/v1/stations?limit=500")
    require(status == 200 and stations["count"] == 6, "Expected six station projections")

    ack_body = {
        "idempotency_key": "step14-final-demo-ack-0001",
        "requested_by": "final-demo-operator",
        "acknowledged_at_utc": "2026-04-06T12:11:00Z",
    }
    ack_path = f"/api/v1/alerts/{top['alert_id']}/acknowledge"
    first_status, first_ack = request_json(args.base_url, ack_path, method="POST", body=ack_body)
    retry_status, retry_ack = request_json(args.base_url, ack_path, method="POST", body=ack_body)
    require(first_status == retry_status == 200, "Operator ACK command failed")
    ack_event_id = first_ack["alert_event"]["alert_event_id"]
    require(
        ack_event_id == retry_ack["alert_event"]["alert_event_id"],
        "ACK retry created another event",
    )
    history_status, history = request_json(
        args.base_url, f"/api/v1/alerts/{top['alert_id']}/history"
    )
    require(history_status == 200 and history["count"] >= 3, "Lifecycle history is incomplete")
    require(
        history["items"][-1]["lifecycle_status"] == "ACKNOWLEDGED",
        "Lifecycle history did not end in ACKNOWLEDGED",
    )

    wait_prometheus(args.prometheus_url)
    target_status, targets = request_json(args.prometheus_url, "/api/v1/targets")
    active_targets = targets.get("data", {}).get("activeTargets", [])
    require(
        target_status == 200
        and any(
            item.get("labels", {}).get("job") == "serving-api" and item.get("health") == "up"
            for item in active_targets
        ),
        "Central monitoring target is not up",
    )

    with psycopg.connect(args.database_url, row_factory=dict_row) as connection:
        counts = connection.execute(
            """
            SELECT
              (SELECT count(*) FROM serving.alert_events) AS alert_events,
              (SELECT count(*) FROM serving.alert_state) AS alert_states,
              (SELECT count(*) FROM serving.station_current) AS stations,
              (SELECT count(*) FROM serving.alert_commands) AS commands
            """
        ).fetchone()
        offsets = connection.execute(
            """SELECT topic, sum(next_offset) AS next_offset
               FROM serving.consumer_offsets GROUP BY topic ORDER BY topic"""
        ).fetchall()
    offset_topics = {row["topic"]: int(row["next_offset"]) for row in offsets}
    require(args.alert_topic in offset_topics and offset_topics[args.alert_topic] > 0, "Alert Kafka lineage missing")
    require(args.current_topic in offset_topics and offset_topics[args.current_topic] > 0, "Current-state Kafka lineage missing")
    require(int(counts["alert_events"]) == package["spark_alert_events"] + 1, "Alert audit count mismatch")
    require(int(counts["commands"]) == 1, "ACK retry was not idempotent")

    evidence_paths = {
        "step4": ROOT / "artifacts" / "step4" / "docker-latest" / "run-summary.json",
        "step5": ROOT / "artifacts" / "step5" / "bootstrap-summary-local.json",
        "step6": ROOT / "artifacts" / "step6" / "live-review-summary.json",
        **{
            f"step{step}": ROOT / "artifacts" / f"step{step}" / "verification-summary.json"
            for step in range(7, 14)
        },
    }
    evidence = {
        name: path.relative_to(ROOT).as_posix()
        for name, path in evidence_paths.items()
        if path.exists() and read_json(path).get("gate") == "PASS"
    }
    require(len(evidence) == 10, f"Prior PASS evidence incomplete: {sorted(evidence)}")
    total_seconds = round(time.time() - (args.started_at_epoch_ms / 1000), 3)
    timings["total_seconds"] = total_seconds
    args.timings.write_text(json.dumps(timings, indent=2), encoding="utf-8")
    require(total_seconds <= 420, "Demo exceeded the seven-minute target")

    acceptance = {
        "source_event_to_operational_action": True,
        "explainable_priority_and_lineage": True,
        "duplicate_and_replay_idempotency": "step9" in evidence,
        "late_and_out_of_order_policy": "step8" in evidence,
        "malformed_record_isolation": package["dead_letter_rows"] == 1,
        "checkpoint_recovery": "step8" in evidence,
        "kafka_backlog_recovery": "step13" in evidence,
        "distributed_historical_baseline": "step7" in evidence,
        "dashboard_api_and_ack": True,
        "observability_slo_and_recovery": "step12" in evidence and "step13" in evidence,
        "demo_within_seven_minutes": True,
    }
    require(all(acceptance.values()), f"Final acceptance failed: {acceptance}")

    summary = {
        "gate": "PASS",
        "demo_total_seconds": total_seconds,
        "demo_total_minutes": round(total_seconds / 60, 2),
        "phase_seconds": timings["phases"],
        "spark_alert_events": package["spark_alert_events"],
        "spark_dead_letter_rows": package["dead_letter_rows"],
        "kafka_records_materialized": {
            "alerts": offset_topics[args.alert_topic],
            "current_state": offset_topics[args.current_topic],
        },
        "serving": {
            "alert_events": int(counts["alert_events"]),
            "alert_states": int(counts["alert_states"]),
            "stations": int(counts["stations"]),
            "commands": int(counts["commands"]),
            "active_tasks": alerts_before["count"],
            "top_priority_station": top["station"]["station_id"],
            "recommended_actions": actions,
        },
        "ack_retry_same_event_id": True,
        "dashboard_http_status": dashboard_status,
        "metrics_http_status": metrics_status,
        "prometheus_target": "up",
        "prior_step_evidence": evidence,
        "final_acceptance": acceptance,
        "acceptance_checks_passed": len(acceptance),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify the final end-to-end demo")
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--prometheus-url", required=True)
    parser.add_argument("--database-url", required=True)
    parser.add_argument("--alert-topic", required=True)
    parser.add_argument("--current-topic", required=True)
    parser.add_argument("--runtime", type=Path, required=True)
    parser.add_argument("--timings", type=Path, required=True)
    parser.add_argument("--started-at-epoch-ms", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(verify(args), indent=2))


if __name__ == "__main__":
    main()
