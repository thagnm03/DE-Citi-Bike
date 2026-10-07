from __future__ import annotations

import json
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

import pytest

from producers.gbfs_collector.collector import (
    CollectorConfig,
    GBFSCollector,
    InMemoryPublisher,
    SourceUnavailable,
)


ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 9, 23, 8, 0, tzinfo=timezone.utc)


class FeedServer:
    def __init__(self) -> None:
        self.requests: dict[str, int] = {}
        self.failures_before_success: dict[str, int] = {}
        self.documents: dict[str, dict[str, Any]] = {}
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                outer.requests[self.path] = outer.requests.get(self.path, 0) + 1
                if outer.requests[self.path] <= outer.failures_before_success.get(self.path, 0):
                    self.send_response(503)
                    self.end_headers()
                    return
                document = outer.documents.get(self.path)
                if document is None:
                    self.send_response(404)
                    self.end_headers()
                    return
                content = json.dumps(document, separators=(",", ":")).encode("utf-8")
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(content)))
                self.end_headers()
                self.wfile.write(content)

            def log_message(self, _format: str, *_args: object) -> None:
                return None

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        host, port = self.server.server_address
        self.base_url = f"http://{host}:{port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def start(self) -> None:
        self.thread.start()

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


@contextmanager
def gbfs_feed(*, snapshot_age_seconds: int = 30) -> Iterator[FeedServer]:
    feed = FeedServer()
    epoch = int((NOW - timedelta(seconds=snapshot_age_seconds)).timestamp())
    feed.documents = {
        "/gbfs.json": {
            "last_updated": epoch,
            "ttl": 60,
            "version": "2.3",
            "data": {
                "en": {
                    "feeds": [
                        {"name": "station_information", "url": f"{feed.base_url}/station_information.json"},
                        {"name": "station_status", "url": f"{feed.base_url}/station_status.json"},
                    ]
                }
            },
        },
        "/station_information.json": {
            "last_updated": epoch,
            "ttl": 60,
            "version": "2.3",
            "data": {
                "stations": [
                    {
                        "station_id": "station-a",
                        "short_name": "1001",
                        "name": "First Station",
                        "lat": 40.7,
                        "lon": -73.9,
                        "capacity": 10,
                    },
                    {
                        "station_id": "station-b",
                        "short_name": "1002",
                        "name": "Second Station",
                        "lat": 40.8,
                        "lon": -73.8,
                        "capacity": 12,
                    },
                ]
            },
        },
        "/station_status.json": {
            "last_updated": epoch,
            "ttl": 60,
            "version": "2.3",
            "data": {
                "stations": [
                    {
                        "station_id": "station-a",
                        "num_bikes_available": 4,
                        "num_bikes_disabled": 1,
                        "num_docks_available": 5,
                        "num_docks_disabled": 0,
                        "num_ebikes_available": 2,
                        "is_installed": 1,
                        "is_renting": 1,
                        "is_returning": 1,
                        "last_reported": epoch,
                    },
                    {
                        "station_id": "station-b",
                        "num_bikes_available": 0,
                        "num_bikes_disabled": 1,
                        "num_docks_available": 10,
                        "num_docks_disabled": 1,
                        "num_ebikes_available": 0,
                        "is_installed": 1,
                        "is_renting": 1,
                        "is_returning": 1,
                        "last_reported": epoch,
                    },
                ]
            },
        },
    }
    feed.start()
    try:
        yield feed
    finally:
        feed.stop()


def build_collector(
    tmp_path: Path,
    feed: FeedServer,
    publisher: InMemoryPublisher,
    *,
    sleeps: list[float] | None = None,
    retry_attempts: int = 3,
) -> GBFSCollector:
    config = CollectorConfig(
        discovery_url=f"{feed.base_url}/gbfs.json",
        bootstrap_servers="unused:9092",
        min_poll_seconds=10,
        stale_after_seconds=180,
        retry_attempts=retry_attempts,
        retry_base_seconds=0.01,
        retry_max_seconds=0.02,
        request_timeout_seconds=1,
    )
    return GBFSCollector(
        config,
        publisher,
        workspace_root=tmp_path,
        archive_root=tmp_path / "data" / "raw" / "gbfs",
        state_file=tmp_path / "data" / "state" / "collector.json",
        metrics_file=tmp_path / "artifacts" / "step6" / "metrics.json",
        contract_path=ROOT / "contracts" / "station-status-v1.json",
        sleeper=(sleeps.append if sleeps is not None else lambda _seconds: None),
        now=lambda: NOW,
    )


def test_collect_once_archives_splits_validates_and_skips_exact_duplicate(tmp_path: Path) -> None:
    with gbfs_feed() as feed:
        publisher = InMemoryPublisher()
        collector = build_collector(tmp_path, feed, publisher)

        first = collector.collect_once()
        second = collector.collect_once()

    assert first.events_published == 2
    assert first.invalid_events == 0
    assert first.next_poll_seconds == 60
    assert second.duplicate_snapshot is True
    assert second.events_published == 0
    assert len(publisher.observations) == 2
    assert {item["station"]["station_id"] for item in publisher.observations} == {
        "station-a",
        "station-b",
    }
    assert all(item["source"]["feed_version"] == "2.3" for item in publisher.observations)
    assert len({item["event_id"] for item in publisher.observations}) == 2
    assert (tmp_path / first.raw_archive_path).read_bytes()
    assert collector.metrics.snapshots_published_total == 1
    assert collector.metrics.duplicate_snapshots_total == 1
    assert feed.requests["/station_information.json"] == 1


def test_transient_source_failure_retries_with_bounded_exponential_backoff(tmp_path: Path) -> None:
    with gbfs_feed() as feed:
        feed.failures_before_success["/station_status.json"] = 2
        sleeps: list[float] = []
        collector = build_collector(tmp_path, feed, InMemoryPublisher(), sleeps=sleeps)
        result = collector.collect_once()

    assert result.events_published == 2
    assert feed.requests["/station_status.json"] == 3
    assert sleeps == [0.01, 0.02]
    assert collector.metrics.retry_attempts_total == 2
    assert collector.metrics.poll_failures_total == 0


def test_permanent_source_failure_is_recorded_and_publishes_nothing(tmp_path: Path) -> None:
    with gbfs_feed() as feed:
        feed.failures_before_success["/station_status.json"] = 99
        publisher = InMemoryPublisher()
        collector = build_collector(tmp_path, feed, publisher, retry_attempts=3)
        with pytest.raises(SourceUnavailable):
            collector.collect_once()

    assert publisher.observations == []
    assert collector.metrics.poll_failures_total == 1
    assert collector.metrics.retry_attempts_total == 2
    assert "SourceUnavailable" in (collector.metrics.last_error or "")


def test_stale_feed_is_detected_without_losing_the_snapshot(tmp_path: Path) -> None:
    with gbfs_feed(snapshot_age_seconds=600) as feed:
        publisher = InMemoryPublisher()
        collector = build_collector(tmp_path, feed, publisher)
        result = collector.collect_once()

    assert result.stale_snapshot is True
    assert result.events_published == 2
    assert collector.metrics.stale_snapshots_total == 1
    assert all("SOURCE_STALE" in item["quality"]["issue_codes"] for item in publisher.observations)


def test_event_id_is_deterministic_across_independent_runs(tmp_path: Path) -> None:
    with gbfs_feed() as feed:
        first_publisher = InMemoryPublisher()
        second_publisher = InMemoryPublisher()
        first = build_collector(tmp_path / "first", feed, first_publisher)
        second = build_collector(tmp_path / "second", feed, second_publisher)
        first.collect_once()
        second.collect_once()

    assert [item["event_id"] for item in first_publisher.observations] == [
        item["event_id"] for item in second_publisher.observations
    ]
