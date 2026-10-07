from __future__ import annotations

import argparse
import http.client
import json
import math
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlsplit


DEFAULT_PATHS = (
    "/api/v1/alerts?active_only=true&limit=100",
    "/api/v1/stations?limit=100",
    "/health/ready",
)


def percentile(values: list[float], quantile: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = max(0, math.ceil(quantile * len(ordered)) - 1)
    return ordered[rank]


def benchmark_worker(
    base_url: str,
    request_paths: list[str],
    timeout: float,
) -> list[tuple[int, float]]:
    """Send one worker's requests over a persistent HTTP connection."""
    parsed = urlsplit(base_url)
    connection_type = (
        http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    )
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    connection = connection_type(parsed.hostname, port, timeout=timeout)
    prefix = parsed.path.rstrip("/")
    results: list[tuple[int, float]] = []
    try:
        for path in request_paths:
            started = time.perf_counter()
            try:
                connection.request(
                    "GET",
                    prefix + path,
                    headers={"Accept": "application/json", "Connection": "keep-alive"},
                )
                response = connection.getresponse()
                response.read()
                status = response.status
            except Exception:
                status = 0
                connection.close()
                connection = connection_type(parsed.hostname, port, timeout=timeout)
            results.append((status, (time.perf_counter() - started) * 1000))
    finally:
        connection.close()
    return results


def run_benchmark(
    base_url: str,
    *,
    requests: int,
    concurrency: int,
    timeout: float,
    paths: tuple[str, ...] = DEFAULT_PATHS,
) -> dict:
    if requests < 1 or concurrency < 1:
        raise ValueError("requests and concurrency must be positive")
    worker_count = min(requests, concurrency)
    worker_paths: list[list[str]] = [[] for _ in range(worker_count)]
    for index in range(requests):
        worker_paths[index % worker_count].append(paths[index % len(paths)])
    started = time.perf_counter()
    results: list[tuple[int, float]] = []
    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [
            executor.submit(benchmark_worker, base_url, assigned_paths, timeout)
            for assigned_paths in worker_paths
        ]
        for future in as_completed(futures):
            results.extend(future.result())
    elapsed = time.perf_counter() - started
    status_counts = Counter(str(status) for status, _ in results)
    durations = [duration for _, duration in results]
    successes = sum(1 for status, _ in results if 200 <= status < 400)
    return {
        "requests": requests,
        "concurrency": concurrency,
        "elapsed_seconds": round(elapsed, 3),
        "throughput_requests_per_second": round(requests / elapsed, 2),
        "success_count": successes,
        "success_rate_percent": round(successes * 100 / requests, 3),
        "status_counts": dict(sorted(status_counts.items())),
        "latency_ms": {
            "min": round(min(durations), 2),
            "p50": round(percentile(durations, 0.50), 2),
            "p95": round(percentile(durations, 0.95), 2),
            "p99": round(percentile(durations, 0.99), 2),
            "max": round(max(durations), 2),
        },
        "paths": list(paths),
        "connection_strategy": "one persistent HTTP connection per concurrent worker",
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Bounded concurrent serving API benchmark")
    parser.add_argument("--base-url", default="http://localhost:18000")
    parser.add_argument("--requests", type=int, default=300)
    parser.add_argument("--concurrency", type=int, default=12)
    parser.add_argument("--timeout", type=float, default=5.0)
    parser.add_argument("--output", type=Path, default=Path("artifacts/step12/benchmark-report.json"))
    args = parser.parse_args()
    report = run_benchmark(
        args.base_url,
        requests=args.requests,
        concurrency=args.concurrency,
        timeout=args.timeout,
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2), encoding="utf-8")
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    main()
