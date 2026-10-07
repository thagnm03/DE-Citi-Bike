from __future__ import annotations

from typing import Any


def evaluate_slos(targets: dict[str, Any], benchmark: dict[str, Any], metrics: dict[str, float]) -> dict:
    checks = {
        "availability": {
            "actual": benchmark["success_rate_percent"],
            "target": targets["availability_percent"],
            "unit": "percent",
            "pass": benchmark["success_rate_percent"] >= targets["availability_percent"],
        },
        "p95_latency": {
            "actual": benchmark["latency_ms"]["p95"],
            "target": targets["p95_latency_ms"],
            "unit": "ms",
            "pass": benchmark["latency_ms"]["p95"] <= targets["p95_latency_ms"],
        },
        "minimum_throughput": {
            "actual": benchmark["throughput_requests_per_second"],
            "target": targets["minimum_throughput_requests_per_second"],
            "unit": "requests/second",
            "pass": benchmark["throughput_requests_per_second"]
            >= targets["minimum_throughput_requests_per_second"],
        },
        "source_freshness": {
            "actual": metrics["citibike_station_source_age_seconds"],
            "target": targets["maximum_source_age_seconds"],
            "unit": "seconds",
            "pass": metrics["citibike_station_source_age_seconds"]
            <= targets["maximum_source_age_seconds"],
        },
        "database_dependency": {
            "actual": metrics["citibike_serving_database_up"],
            "target": 1,
            "unit": "boolean",
            "pass": metrics["citibike_serving_database_up"] == 1,
        },
    }
    return {
        "gate": "PASS" if all(check["pass"] for check in checks.values()) else "FAIL",
        "measurement_scope": "controlled local gate window; not a production 30-day SLO claim",
        "checks": checks,
    }
