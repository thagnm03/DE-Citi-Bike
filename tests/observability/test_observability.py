from __future__ import annotations

import json
from pathlib import Path

from observability.benchmark import percentile
from observability.metrics import ApiMetrics
from observability.slo import evaluate_slos


ROOT = Path(__file__).resolve().parents[2]


def test_metrics_expose_counter_histogram_and_operational_gauges() -> None:
    registry = ApiMetrics(buckets=(0.1, 0.5))
    registry.observe("GET", "/api/v1/alerts", 200, 0.08)
    registry.observe("GET", "/api/v1/alerts", 500, 0.2)
    text = registry.render(
        {
            "database_up": 1,
            "station_rows": 3,
            "active_alerts": 2,
            "critical_unacknowledged_alerts": 1,
            "reported_source_age_seconds": 10,
            "projection_age_seconds": 2,
        }
    )
    assert 'route="/api/v1/alerts",status_code="200"} 1' in text
    assert 'route="/api/v1/alerts",status_code="500"} 1' in text
    assert 'status_code="200",le="0.1"} 1' in text
    assert 'status_code="500",le="0.1"} 0' in text
    assert "citibike_station_source_age_seconds 12" in text
    assert "citibike_station_projection_rows 3" in text


def test_metric_labels_are_escaped() -> None:
    registry = ApiMetrics(buckets=(1.0,))
    registry.observe("GET", '/route/"quoted"', 200, 0.1)
    assert 'route="/route/\\"quoted\\""' in registry.render({"database_up": 0})


def test_nearest_rank_percentile() -> None:
    assert percentile([10, 20, 30, 40], 0.50) == 20
    assert percentile([10, 20, 30, 40], 0.95) == 40
    assert percentile([], 0.95) == 0


def test_slo_evaluator_reports_pass_and_failure() -> None:
    targets = {
        "availability_percent": 99.0,
        "p95_latency_ms": 500.0,
        "minimum_throughput_requests_per_second": 15.0,
        "maximum_source_age_seconds": 180.0,
    }
    benchmark = {
        "success_rate_percent": 100.0,
        "throughput_requests_per_second": 50.0,
        "latency_ms": {"p95": 100.0},
    }
    metrics = {
        "citibike_station_source_age_seconds": 20.0,
        "citibike_serving_database_up": 1.0,
    }
    assert evaluate_slos(targets, benchmark, metrics)["gate"] == "PASS"
    benchmark["latency_ms"]["p95"] = 501.0
    assert evaluate_slos(targets, benchmark, metrics)["gate"] == "FAIL"


def test_prometheus_config_and_alert_rules_match_slo_contract() -> None:
    prometheus = (ROOT / "monitoring" / "prometheus.yml").read_text(encoding="utf-8")
    rules = (ROOT / "monitoring" / "alert-rules.yml").read_text(encoding="utf-8")
    targets = json.loads((ROOT / "config" / "slo-targets.json").read_text(encoding="utf-8"))
    assert 'targets: ["serving-api:8000"]' in prometheus
    assert "ServingAPIDown" in rules
    assert "ServingAPIErrorBudgetBurn" in rules
    assert "StationDataStale" in rules
    assert targets["availability_percent"] == 99.0
    assert targets["p95_latency_ms"] == 500.0
