from __future__ import annotations

import argparse
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from observability.slo import evaluate_slos


SAMPLE_RE = re.compile(r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{[^}]*\})?\s+([-+0-9.eE]+)$")


def fetch_text(url: str, timeout: float = 10.0) -> tuple[int, str]:
    with urllib.request.urlopen(url, timeout=timeout) as response:
        return response.status, response.read().decode("utf-8")


def fetch_json(url: str, timeout: float = 10.0) -> dict:
    status, body = fetch_text(url, timeout)
    if status != 200:
        raise AssertionError(f"{url} returned HTTP {status}")
    return json.loads(body)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def samples(metrics_text: str, metric_name: str) -> list[float]:
    values: list[float] = []
    for line in metrics_text.splitlines():
        match = SAMPLE_RE.match(line)
        if match and match.group(1) == metric_name:
            values.append(float(match.group(2)))
    return values


def wait_for_prometheus_target(prometheus_url: str, timeout_seconds: float = 45.0) -> dict:
    deadline = time.monotonic() + timeout_seconds
    query = urllib.parse.quote('up{job="serving-api"}')
    last: dict = {}
    while time.monotonic() < deadline:
        try:
            last = fetch_json(f"{prometheus_url.rstrip('/')}/api/v1/query?query={query}")
            result = last.get("data", {}).get("result", [])
            if result and float(result[0]["value"][1]) == 1.0:
                return last
        except Exception:
            pass
        time.sleep(2)
    raise AssertionError(f"Prometheus did not report serving-api up: {last}")


def verify(args: argparse.Namespace) -> dict:
    targets = json.loads(args.slo_targets.read_text(encoding="utf-8"))
    benchmark = json.loads(args.benchmark.read_text(encoding="utf-8"))

    metrics_status, metrics_text = fetch_text(args.base_url.rstrip("/") + "/metrics")
    require(metrics_status == 200, "Metrics endpoint did not return HTTP 200")
    required_metrics = {
        "citibike_api_http_requests_total",
        "citibike_api_http_request_duration_seconds_bucket",
        "citibike_serving_database_up",
        "citibike_station_projection_rows",
        "citibike_station_source_age_seconds",
        "citibike_active_alerts",
    }
    missing_metrics = sorted(name for name in required_metrics if not samples(metrics_text, name))
    require(not missing_metrics, f"Metrics endpoint is missing series: {missing_metrics}")
    request_count = sum(samples(metrics_text, "citibike_api_http_requests_total"))
    require(request_count >= benchmark["requests"], "HTTP counter did not observe the benchmark load")

    wait_for_prometheus_target(args.prometheus_url)
    prometheus_ready, ready_text = fetch_text(args.prometheus_url.rstrip("/") + "/-/ready")
    require(prometheus_ready == 200 and "ready" in ready_text.lower(), "Prometheus is not ready")

    target_payload = fetch_json(args.prometheus_url.rstrip("/") + "/api/v1/targets")
    active_targets = target_payload.get("data", {}).get("activeTargets", [])
    serving_targets = [
        item for item in active_targets if item.get("labels", {}).get("job") == "serving-api"
    ]
    require(len(serving_targets) == 1, "Expected exactly one centralized serving-api scrape target")
    require(serving_targets[0].get("health") == "up", "Prometheus serving-api target is not up")

    rules_payload = fetch_json(args.prometheus_url.rstrip("/") + "/api/v1/rules")
    rules = [
        rule
        for group in rules_payload.get("data", {}).get("groups", [])
        for rule in group.get("rules", [])
    ]
    rule_names = {rule.get("name") for rule in rules}
    expected_alerts = {
        "ServingAPIDown",
        "ServingDatabaseUnavailable",
        "ServingAPIErrorBudgetBurn",
        "ServingAPILatencyHigh",
        "StationProjectionMissing",
        "StationDataStale",
    }
    require(expected_alerts <= rule_names, "Prometheus did not load every required alert rule")

    metric_values = {
        "citibike_serving_database_up": samples(metrics_text, "citibike_serving_database_up")[0],
        "citibike_station_source_age_seconds": samples(
            metrics_text, "citibike_station_source_age_seconds"
        )[0],
    }
    slo_report = evaluate_slos(targets, benchmark, metric_values)
    require(slo_report["gate"] == "PASS", f"SLO gate failed: {slo_report['checks']}")
    args.slo_output.parent.mkdir(parents=True, exist_ok=True)
    args.slo_output.write_text(json.dumps(slo_report, indent=2), encoding="utf-8")

    summary = {
        "gate": "PASS",
        "metrics_endpoint_http_status": metrics_status,
        "required_metric_families": sorted(required_metrics),
        "observed_http_requests": int(request_count),
        "prometheus_ready": True,
        "prometheus_serving_target": "up",
        "prometheus_alert_rules_loaded": len(expected_alerts),
        "benchmark_requests": benchmark["requests"],
        "benchmark_concurrency": benchmark["concurrency"],
        "benchmark_success_rate_percent": benchmark["success_rate_percent"],
        "benchmark_throughput_requests_per_second": benchmark[
            "throughput_requests_per_second"
        ],
        "benchmark_p95_latency_ms": benchmark["latency_ms"]["p95"],
        "effective_source_age_seconds": metric_values["citibike_station_source_age_seconds"],
        "slo_checks_passed": len(slo_report["checks"]),
        "measurement_scope": slo_report["measurement_scope"],
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Verify Step 12 observability and SLO gate")
    parser.add_argument("--base-url", default="http://localhost:18000")
    parser.add_argument("--prometheus-url", default="http://localhost:19090")
    parser.add_argument("--benchmark", type=Path, required=True)
    parser.add_argument("--slo-targets", type=Path, default=ROOT / "config" / "slo-targets.json")
    parser.add_argument("--output", type=Path, default=ROOT / "artifacts" / "step12" / "verification-summary.json")
    parser.add_argument("--slo-output", type=Path, default=ROOT / "artifacts" / "step12" / "slo-report.json")
    args = parser.parse_args()
    print(json.dumps(verify(args), indent=2))


if __name__ == "__main__":
    main()
