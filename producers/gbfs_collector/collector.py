from __future__ import annotations

import hashlib
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Protocol

from confluent_kafka import Producer

from prototype.contracts import load_validator
from prototype.gbfs_adapter import normalize_snapshot
from prototype.rules import utc_text


DEFAULT_DISCOVERY_URL = "https://gbfs.citibikenyc.com/gbfs/2.3/gbfs.json"
OBSERVATION_TOPIC = "citibike.station-status.v1"
INVALID_TOPIC = "citibike.station-status.invalid.v1"


class CollectorError(RuntimeError):
    """Base error for one collection cycle."""


class SourceUnavailable(CollectorError):
    """Raised after bounded source retries are exhausted."""


class PublishError(CollectorError):
    """Raised when Kafka cannot acknowledge all events."""


@dataclass(frozen=True)
class CollectorConfig:
    discovery_url: str = DEFAULT_DISCOVERY_URL
    language: str = "en"
    bootstrap_servers: str = "localhost:9092"
    observation_topic: str = OBSERVATION_TOPIC
    invalid_topic: str = INVALID_TOPIC
    min_poll_seconds: int = 60
    stale_after_seconds: int = 180
    metadata_refresh_seconds: int = 3600
    request_timeout_seconds: float = 20.0
    retry_attempts: int = 4
    retry_base_seconds: float = 1.0
    retry_max_seconds: float = 15.0
    publish_timeout_seconds: float = 30.0


@dataclass
class CollectorMetrics:
    poll_attempts_total: int = 0
    poll_success_total: int = 0
    poll_failures_total: int = 0
    retry_attempts_total: int = 0
    snapshots_received_total: int = 0
    snapshots_published_total: int = 0
    duplicate_snapshots_total: int = 0
    stale_snapshots_total: int = 0
    metadata_refresh_total: int = 0
    events_normalized_total: int = 0
    events_published_total: int = 0
    invalid_events_total: int = 0
    last_success_at_utc: str | None = None
    last_snapshot_updated_at_utc: str | None = None
    last_snapshot_sha256: str | None = None
    last_poll_duration_ms: int | None = None
    last_error: str | None = None


@dataclass(frozen=True)
class FeedEndpoints:
    station_information: str
    station_status: str
    discovery_version: str
    discovery_ttl_seconds: int


@dataclass(frozen=True)
class PollResult:
    events_published: int
    invalid_events: int
    duplicate_snapshot: bool
    stale_snapshot: bool
    source_ttl_seconds: int
    next_poll_seconds: int
    snapshot_sha256: str
    raw_archive_path: str


class Publisher(Protocol):
    def publish_observations(self, observations: list[dict[str, Any]]) -> None: ...

    def publish_invalid(self, invalid_records: list[dict[str, Any]]) -> None: ...

    def close(self) -> None: ...


class JsonLogger:
    def __init__(self, run_id: str) -> None:
        self.run_id = run_id

    def emit(self, level: str, event: str, message: str, **fields: Any) -> None:
        payload = {
            "timestamp_utc": utc_text(datetime.now(timezone.utc)),
            "level": level,
            "service": "gbfs-collector",
            "event": event,
            "message": message,
            "run_id": self.run_id,
            **fields,
        }
        print(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), flush=True)


class JsonFileStore:
    def __init__(self, path: Path) -> None:
        self.path = path

    def read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {}
        return json.loads(self.path.read_text(encoding="utf-8"))

    def write(self, payload: dict[str, Any]) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        temporary = self.path.with_suffix(self.path.suffix + ".tmp")
        temporary.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        os.replace(temporary, self.path)


class RetryingHttpClient:
    def __init__(
        self,
        config: CollectorConfig,
        *,
        sleeper: Callable[[float], None] = time.sleep,
        opener: Callable[..., Any] = urllib.request.urlopen,
        on_retry: Callable[[int, str, Exception], None] | None = None,
    ) -> None:
        self.config = config
        self.sleeper = sleeper
        self.opener = opener
        self.on_retry = on_retry

    def get(self, url: str) -> bytes:
        last_error: Exception | None = None
        for attempt in range(1, self.config.retry_attempts + 1):
            try:
                request = urllib.request.Request(
                    url,
                    headers={"User-Agent": "ie212-citibike-gbfs-collector/1.0"},
                )
                with self.opener(request, timeout=self.config.request_timeout_seconds) as response:
                    return response.read()
            except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError, OSError) as error:
                last_error = error
                if attempt >= self.config.retry_attempts:
                    break
                if self.on_retry:
                    self.on_retry(attempt, url, error)
                delay = min(
                    self.config.retry_max_seconds,
                    self.config.retry_base_seconds * (2 ** (attempt - 1)),
                )
                self.sleeper(delay)
        raise SourceUnavailable(
            f"Source request failed after {self.config.retry_attempts} attempts: {url}: {last_error}"
        ) from last_error


class RawArchiver:
    def __init__(self, workspace_root: Path, archive_root: Path) -> None:
        self.workspace_root = workspace_root.resolve()
        self.archive_root = archive_root.resolve()

    def archive(
        self,
        feed_name: str,
        content: bytes,
        observed_at: datetime,
        source_epoch: int | None,
    ) -> str:
        digest = hashlib.sha256(content).hexdigest()
        day = observed_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
        identity = source_epoch or int(observed_at.timestamp())
        destination = self.archive_root / feed_name / f"date={day}" / f"{identity}-{digest[:12]}.json"
        destination.parent.mkdir(parents=True, exist_ok=True)
        if not destination.exists():
            temporary = destination.with_suffix(".json.tmp")
            temporary.write_bytes(content)
            os.replace(temporary, destination)
        try:
            return destination.relative_to(self.workspace_root).as_posix()
        except ValueError:
            return destination.as_posix()


class KafkaPublisher:
    def __init__(self, config: CollectorConfig) -> None:
        self.config = config
        self.producer = Producer(
            {
                "bootstrap.servers": config.bootstrap_servers,
                "client.id": "gbfs-collector",
                "enable.idempotence": True,
                "acks": "all",
                "compression.type": "lz4",
            }
        )

    def _publish(
        self,
        topic: str,
        records: list[dict[str, Any]],
        key_for: Callable[[dict[str, Any]], str],
    ) -> None:
        delivery_errors: list[str] = []

        def delivered(error: Any, _message: Any) -> None:
            if error is not None:
                delivery_errors.append(str(error))

        for record in records:
            key = key_for(record).encode("utf-8")
            value = json.dumps(record, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
            while True:
                try:
                    self.producer.produce(topic, key=key, value=value, on_delivery=delivered)
                    self.producer.poll(0)
                    break
                except BufferError:
                    self.producer.poll(1)
        remaining = self.producer.flush(self.config.publish_timeout_seconds)
        if remaining or delivery_errors:
            raise PublishError(
                f"Kafka publish incomplete: remaining={remaining}, errors={delivery_errors[:3]}"
            )

    def publish_observations(self, observations: list[dict[str, Any]]) -> None:
        if observations:
            self._publish(
                self.config.observation_topic,
                observations,
                lambda record: str(record["station"]["station_id"]),
            )

    def publish_invalid(self, invalid_records: list[dict[str, Any]]) -> None:
        if invalid_records:
            self._publish(
                self.config.invalid_topic,
                invalid_records,
                lambda record: str(record["record_key"]),
            )

    def close(self) -> None:
        self.producer.flush(self.config.publish_timeout_seconds)


class InMemoryPublisher:
    def __init__(self) -> None:
        self.observations: list[dict[str, Any]] = []
        self.invalid_records: list[dict[str, Any]] = []

    def publish_observations(self, observations: list[dict[str, Any]]) -> None:
        self.observations.extend(observations)

    def publish_invalid(self, invalid_records: list[dict[str, Any]]) -> None:
        self.invalid_records.extend(invalid_records)

    def close(self) -> None:
        return None


def parse_json_document(content: bytes, feed_name: str) -> dict[str, Any]:
    try:
        document = json.loads(content)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise CollectorError(f"{feed_name} returned invalid JSON: {error}") from error
    if not isinstance(document, dict):
        raise CollectorError(f"{feed_name} root must be an object")
    return document


def discover_endpoints(document: dict[str, Any], language: str) -> FeedEndpoints:
    languages = document.get("data")
    if not isinstance(languages, dict) or not languages:
        raise CollectorError("GBFS discovery document has no language feeds")
    selected = languages.get(language)
    if selected is None:
        selected = next(iter(languages.values()))
    feeds = selected.get("feeds") if isinstance(selected, dict) else None
    if not isinstance(feeds, list):
        raise CollectorError("GBFS discovery document has no feeds list")
    by_name = {
        item.get("name"): item.get("url")
        for item in feeds
        if isinstance(item, dict) and isinstance(item.get("url"), str)
    }
    missing = {"station_information", "station_status"} - set(by_name)
    if missing:
        raise CollectorError(f"GBFS discovery is missing feeds: {sorted(missing)}")
    return FeedEndpoints(
        station_information=by_name["station_information"],
        station_status=by_name["station_status"],
        discovery_version=str(document.get("version", "unknown")),
        discovery_ttl_seconds=int(document.get("ttl", 0)),
    )


class GBFSCollector:
    def __init__(
        self,
        config: CollectorConfig,
        publisher: Publisher,
        *,
        workspace_root: Path,
        archive_root: Path,
        state_file: Path,
        metrics_file: Path,
        contract_path: Path,
        logger: JsonLogger | None = None,
        sleeper: Callable[[float], None] = time.sleep,
        now: Callable[[], datetime] | None = None,
        opener: Callable[..., Any] = urllib.request.urlopen,
    ) -> None:
        self.config = config
        self.publisher = publisher
        self.workspace_root = workspace_root.resolve()
        self.state_store = JsonFileStore(state_file)
        self.metrics_store = JsonFileStore(metrics_file)
        metric_values = self.metrics_store.read()
        allowed_metrics = set(CollectorMetrics.__dataclass_fields__)
        self.metrics = CollectorMetrics(
            **{key: value for key, value in metric_values.items() if key in allowed_metrics}
        )
        self.archiver = RawArchiver(self.workspace_root, archive_root)
        self.validator = load_validator(contract_path)
        self.sleeper = sleeper
        self.now = now or (lambda: datetime.now(timezone.utc))
        run_id = "collector-" + self.now().astimezone(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.logger = logger or JsonLogger(run_id)
        self.http = RetryingHttpClient(
            config,
            sleeper=sleeper,
            opener=opener,
            on_retry=self._on_retry,
        )
        self._endpoints: FeedEndpoints | None = None
        self._station_information: dict[str, Any] | None = None
        self._metadata_refreshed_at: datetime | None = None

    def _on_retry(self, attempt: int, url: str, error: Exception) -> None:
        self.metrics.retry_attempts_total += 1
        self.logger.emit(
            "WARN",
            "source_retry",
            "GBFS request failed; retry scheduled",
            attempt=attempt,
            source_url=url,
            error_type=type(error).__name__,
        )

    def _save_metrics(self) -> None:
        self.metrics_store.write(asdict(self.metrics))

    def _fetch_endpoints(self) -> FeedEndpoints:
        content = self.http.get(self.config.discovery_url)
        endpoints = discover_endpoints(
            parse_json_document(content, "gbfs_discovery"), self.config.language
        )
        self._endpoints = endpoints
        return endpoints

    def _metadata_due(self, observed_at: datetime) -> bool:
        if self._station_information is None or self._metadata_refreshed_at is None:
            return True
        elapsed = (observed_at - self._metadata_refreshed_at).total_seconds()
        return elapsed >= self.config.metadata_refresh_seconds

    def _refresh_metadata(self, endpoints: FeedEndpoints, observed_at: datetime) -> None:
        content = self.http.get(endpoints.station_information)
        document = parse_json_document(content, "station_information")
        source_epoch = int(document["last_updated"])
        self.archiver.archive("station_information", content, observed_at, source_epoch)
        self._station_information = document
        self._metadata_refreshed_at = observed_at
        self.metrics.metadata_refresh_total += 1

    def collect_once(self) -> PollResult:
        started = time.perf_counter()
        self.metrics.poll_attempts_total += 1
        observed_at = self.now().astimezone(timezone.utc)
        try:
            endpoints = self._endpoints or self._fetch_endpoints()
            status_bytes = self.http.get(endpoints.station_status)
            status_document = parse_json_document(status_bytes, "station_status")
            source_epoch = int(status_document["last_updated"])
            source_ttl = int(status_document.get("ttl", endpoints.discovery_ttl_seconds))
            snapshot_sha = hashlib.sha256(status_bytes).hexdigest()
            snapshot_time = datetime.fromtimestamp(source_epoch, tz=timezone.utc)
            snapshot_age = max(0, int((observed_at - snapshot_time).total_seconds()))
            stale_snapshot = snapshot_age > self.config.stale_after_seconds
            raw_path = self.archiver.archive(
                "station_status", status_bytes, observed_at, source_epoch
            )
            self.metrics.snapshots_received_total += 1
            self.metrics.last_snapshot_updated_at_utc = utc_text(snapshot_time)
            if stale_snapshot:
                self.metrics.stale_snapshots_total += 1
                self.logger.emit(
                    "WARN",
                    "stale_feed_detected",
                    "GBFS station_status snapshot is stale",
                    snapshot_age_seconds=snapshot_age,
                    stale_after_seconds=self.config.stale_after_seconds,
                    snapshot_sha256=snapshot_sha,
                )

            state = self.state_store.read()
            if state.get("last_published_snapshot_sha256") == snapshot_sha:
                self.metrics.duplicate_snapshots_total += 1
                self.metrics.poll_success_total += 1
                self.metrics.last_success_at_utc = utc_text(observed_at)
                self.metrics.last_snapshot_sha256 = snapshot_sha
                self.metrics.last_error = None
                self.metrics.last_poll_duration_ms = int((time.perf_counter() - started) * 1000)
                self._save_metrics()
                self.logger.emit(
                    "INFO",
                    "duplicate_snapshot_skipped",
                    "Exact duplicate GBFS snapshot was not republished",
                    snapshot_sha256=snapshot_sha,
                    duration_ms=self.metrics.last_poll_duration_ms,
                )
                return PollResult(
                    events_published=0,
                    invalid_events=0,
                    duplicate_snapshot=True,
                    stale_snapshot=stale_snapshot,
                    source_ttl_seconds=source_ttl,
                    next_poll_seconds=max(self.config.min_poll_seconds, source_ttl),
                    snapshot_sha256=snapshot_sha,
                    raw_archive_path=raw_path,
                )

            if self._metadata_due(observed_at):
                self._refresh_metadata(endpoints, observed_at)
            assert self._station_information is not None
            observations = normalize_snapshot(
                status_bytes,
                self._station_information,
                observed_at=observed_at,
                raw_archive_path=raw_path,
                stale_after_seconds=self.config.stale_after_seconds,
            )
            self.metrics.events_normalized_total += len(observations)

            valid: list[dict[str, Any]] = []
            invalid: list[dict[str, Any]] = []
            for observation in observations:
                errors = sorted(
                    self.validator.iter_errors(observation),
                    key=lambda error: list(error.absolute_path),
                )
                if not errors:
                    valid.append(observation)
                    continue
                invalid.append(
                    {
                        "record_key": observation.get("station", {}).get("station_id")
                        or observation.get("event_id")
                        or snapshot_sha,
                        "source_snapshot_sha256": snapshot_sha,
                        "observed_at_utc": utc_text(observed_at),
                        "error_type": "CONTRACT_VALIDATION_FAILED",
                        "errors": [
                            {
                                "path": ".".join(str(part) for part in error.absolute_path),
                                "message": error.message,
                            }
                            for error in errors
                        ],
                        "record": observation,
                    }
                )

            self.publisher.publish_observations(valid)
            self.publisher.publish_invalid(invalid)
            self.state_store.write(
                {
                    "last_published_snapshot_sha256": snapshot_sha,
                    "last_published_at_utc": utc_text(observed_at),
                    "last_raw_archive_path": raw_path,
                }
            )
            self.metrics.snapshots_published_total += 1
            self.metrics.events_published_total += len(valid)
            self.metrics.invalid_events_total += len(invalid)
            self.metrics.poll_success_total += 1
            self.metrics.last_success_at_utc = utc_text(observed_at)
            self.metrics.last_snapshot_sha256 = snapshot_sha
            self.metrics.last_error = None
            self.metrics.last_poll_duration_ms = int((time.perf_counter() - started) * 1000)
            self._save_metrics()
            self.logger.emit(
                "INFO",
                "snapshot_published",
                "Published normalized station snapshot",
                snapshot_sha256=snapshot_sha,
                station_count=len(observations),
                events_published=len(valid),
                invalid_events=len(invalid),
                source_version=str(status_document.get("version")),
                source_ttl_seconds=source_ttl,
                duration_ms=self.metrics.last_poll_duration_ms,
            )
            return PollResult(
                events_published=len(valid),
                invalid_events=len(invalid),
                duplicate_snapshot=False,
                stale_snapshot=stale_snapshot,
                source_ttl_seconds=source_ttl,
                next_poll_seconds=max(self.config.min_poll_seconds, source_ttl),
                snapshot_sha256=snapshot_sha,
                raw_archive_path=raw_path,
            )
        except Exception as error:
            self.metrics.poll_failures_total += 1
            self.metrics.last_error = f"{type(error).__name__}: {error}"
            self.metrics.last_poll_duration_ms = int((time.perf_counter() - started) * 1000)
            self._save_metrics()
            self.logger.emit(
                "ERROR",
                "poll_failed",
                "GBFS collection cycle failed",
                error_type=type(error).__name__,
                duration_ms=self.metrics.last_poll_duration_ms,
            )
            raise

    def run(self, max_polls: int | None = None) -> None:
        completed = 0
        consecutive_failures = 0
        while max_polls is None or completed < max_polls:
            try:
                result = self.collect_once()
                consecutive_failures = 0
                delay = result.next_poll_seconds
            except Exception:
                consecutive_failures += 1
                delay = min(
                    self.config.retry_max_seconds,
                    self.config.retry_base_seconds * (2 ** (consecutive_failures - 1)),
                )
                self._endpoints = None
            completed += 1
            if max_polls is None or completed < max_polls:
                self.sleeper(delay)

    def close(self) -> None:
        self.publisher.close()
