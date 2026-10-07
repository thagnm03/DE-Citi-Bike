from __future__ import annotations

import math
import threading
import time
from collections import Counter
from dataclasses import dataclass
from typing import Any, Iterable


DEFAULT_BUCKETS = (0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)


def _escape_label(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def _labels(values: dict[str, str], *, extra: tuple[str, str] | None = None) -> str:
    pairs = list(values.items())
    if extra is not None:
        pairs.append(extra)
    return "{" + ",".join(f'{key}="{_escape_label(value)}"' for key, value in pairs) + "}"


def _number(value: float | int) -> str:
    if isinstance(value, int):
        return str(value)
    if math.isnan(value):
        return "NaN"
    if math.isinf(value):
        return "+Inf" if value > 0 else "-Inf"
    return f"{value:.6f}".rstrip("0").rstrip(".")


@dataclass(frozen=True)
class RequestKey:
    method: str
    route: str
    status_code: str


class ApiMetrics:
    """Small in-process Prometheus registry with bounded HTTP labels."""

    def __init__(self, buckets: Iterable[float] = DEFAULT_BUCKETS) -> None:
        self.buckets = tuple(sorted(float(value) for value in buckets))
        self.started_at = time.time()
        self._lock = threading.Lock()
        self._requests: Counter[RequestKey] = Counter()
        self._duration_count: Counter[RequestKey] = Counter()
        self._duration_sum: Counter[RequestKey] = Counter()
        self._duration_buckets: Counter[tuple[RequestKey, float]] = Counter()

    def observe(self, method: str, route: str, status_code: int, duration_seconds: float) -> None:
        key = RequestKey(method.upper(), route, str(status_code))
        duration = max(0.0, float(duration_seconds))
        with self._lock:
            self._requests[key] += 1
            self._duration_count[key] += 1
            self._duration_sum[key] += duration
            for bucket in self.buckets:
                if duration <= bucket:
                    self._duration_buckets[(key, bucket)] += 1

    def render(self, operational: dict[str, Any]) -> str:
        with self._lock:
            requests = dict(self._requests)
            counts = dict(self._duration_count)
            sums = dict(self._duration_sum)
            buckets = dict(self._duration_buckets)

        lines = [
            "# HELP citibike_api_http_requests_total Completed HTTP requests.",
            "# TYPE citibike_api_http_requests_total counter",
        ]
        for key in sorted(requests, key=lambda item: (item.route, item.method, item.status_code)):
            label_values = {"method": key.method, "route": key.route, "status_code": key.status_code}
            lines.append(f"citibike_api_http_requests_total{_labels(label_values)} {requests[key]}")

        lines.extend(
            [
                "# HELP citibike_api_http_request_duration_seconds HTTP request latency.",
                "# TYPE citibike_api_http_request_duration_seconds histogram",
            ]
        )
        for key in sorted(counts, key=lambda item: (item.route, item.method, item.status_code)):
            label_values = {"method": key.method, "route": key.route, "status_code": key.status_code}
            for bucket in self.buckets:
                lines.append(
                    "citibike_api_http_request_duration_seconds_bucket"
                    f"{_labels(label_values, extra=('le', _number(bucket)))} "
                    f"{buckets.get((key, bucket), 0)}"
                )
            lines.append(
                "citibike_api_http_request_duration_seconds_bucket"
                f"{_labels(label_values, extra=('le', '+Inf'))} {counts[key]}"
            )
            lines.append(
                f"citibike_api_http_request_duration_seconds_sum{_labels(label_values)} "
                f"{_number(float(sums[key]))}"
            )
            lines.append(
                f"citibike_api_http_request_duration_seconds_count{_labels(label_values)} {counts[key]}"
            )

        projection_age = float(operational.get("projection_age_seconds") or 0.0)
        reported_source_age = float(operational.get("reported_source_age_seconds") or 0.0)
        effective_source_age = projection_age + reported_source_age
        gauges = (
            (
                "citibike_api_process_uptime_seconds",
                "Seconds since the serving API process started.",
                max(0.0, time.time() - self.started_at),
            ),
            (
                "citibike_serving_database_up",
                "Whether the serving database query succeeded (1 or 0).",
                float(operational.get("database_up", 0)),
            ),
            (
                "citibike_active_alerts",
                "Current unresolved operational alert episodes.",
                float(operational.get("active_alerts", 0)),
            ),
            (
                "citibike_station_projection_rows",
                "Current station projection row count.",
                float(operational.get("station_rows", 0)),
            ),
            (
                "citibike_critical_unacknowledged_alerts",
                "Current critical alerts that have not been acknowledged.",
                float(operational.get("critical_unacknowledged_alerts", 0)),
            ),
            (
                "citibike_station_projection_age_seconds",
                "Age of the newest station projection database write.",
                projection_age,
            ),
            (
                "citibike_station_source_age_seconds",
                "Effective source age: feed-reported age plus projection age.",
                effective_source_age,
            ),
        )
        for name, help_text, value in gauges:
            lines.extend((f"# HELP {name} {help_text}", f"# TYPE {name} gauge", f"{name} {_number(value)}"))
        return "\n".join(lines) + "\n"


api_metrics = ApiMetrics()
