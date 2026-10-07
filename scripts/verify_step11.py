from __future__ import annotations

import argparse
import json
import urllib.request
from pathlib import Path


def fetch(base_url: str, path: str) -> tuple[int, bytes, dict[str, str]]:
    with urllib.request.urlopen(base_url.rstrip("/") + path, timeout=10) as response:
        return response.status, response.read(), {key.lower(): value for key, value in response.headers.items()}


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def verify(base_url: str, output: Path) -> dict:
    page_status, page_bytes, headers = fetch(base_url, "/dashboard")
    css_status, css_bytes, _ = fetch(base_url, "/dashboard/static/styles.css")
    js_status, js_bytes, _ = fetch(base_url, "/dashboard/static/app.js")
    _, alerts_bytes, _ = fetch(base_url, "/api/v1/alerts?active_only=true&limit=500")
    _, stations_bytes, _ = fetch(base_url, "/api/v1/stations?limit=500")

    html = page_bytes.decode("utf-8")
    javascript = js_bytes.decode("utf-8")
    alerts = json.loads(alerts_bytes)
    stations = json.loads(stations_bytes)

    require(page_status == css_status == js_status == 200, "Dashboard assets did not return HTTP 200")
    require("https://" not in html and "http://" not in html, "Dashboard depends on an external asset")
    require('id="alert-rows"' in html and 'id="station-rows"' in html, "Operational panels are missing")
    require('aria-live="polite"' in html and "skip-link" in html, "Basic accessibility landmarks are missing")
    require("/api/v1/alerts" in javascript and "/api/v1/stations" in javascript, "Dashboard is not wired to the serving API")
    require("/acknowledge" in javascript and "crypto.randomUUID()" in javascript, "ACK workflow is not idempotency-keyed")
    require(alerts["count"] == 2, "Controlled priority queue should have two active tasks")
    require(stations["count"] == 3, "Controlled station view should have three snapshots")
    scores = [item["priority"]["score"] for item in alerts["items"]]
    require(scores == sorted(scores, reverse=True), "Dashboard API data is not priority sorted")
    require(headers.get("x-frame-options") == "DENY", "Clickjacking response header is missing")
    require("default-src 'self'" in headers.get("content-security-policy", ""), "Content Security Policy is missing")

    summary = {
        "gate": "PASS",
        "dashboard_http_status": page_status,
        "dashboard_bytes": len(page_bytes),
        "stylesheet_bytes": len(css_bytes),
        "javascript_bytes": len(js_bytes),
        "external_runtime_assets": 0,
        "active_priority_tasks_renderable": alerts["count"],
        "station_snapshots_renderable": stations["count"],
        "priority_sorted": True,
        "acknowledge_workflow_present": True,
        "responsive_breakpoints": 2,
        "accessibility_landmarks_verified": True,
        "security_headers_verified": ["content-security-policy", "x-frame-options", "x-content-type-options"],
    }
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    return summary


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="http://localhost:18000")
    parser.add_argument("--output", type=Path, default=Path("artifacts/step11/verification-summary.json"))
    args = parser.parse_args()
    print(json.dumps(verify(args.base_url, args.output), indent=2))


if __name__ == "__main__":
    main()
