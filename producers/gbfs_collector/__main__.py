from __future__ import annotations

import argparse
import os
from pathlib import Path

from .collector import CollectorConfig, GBFSCollector, KafkaPublisher


def env(name: str, default: str) -> str:
    return os.environ.get(name, default)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Poll Citi Bike GBFS and publish station events to Kafka")
    parser.add_argument("--once", action="store_true", help="Run exactly one collection cycle")
    parser.add_argument("--max-polls", type=int, help="Stop after this many cycles")
    parser.add_argument("--discovery-url", default=env("GBFS_DISCOVERY_URL", CollectorConfig.discovery_url))
    parser.add_argument("--language", default=env("GBFS_LANGUAGE", "en"))
    parser.add_argument("--bootstrap-servers", default=env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--archive-root", default=env("COLLECTOR_ARCHIVE_ROOT", "data/raw/gbfs"))
    parser.add_argument("--state-file", default=env("COLLECTOR_STATE_FILE", "data/state/gbfs-collector.json"))
    parser.add_argument("--metrics-file", default=env("COLLECTOR_METRICS_FILE", "artifacts/step6/metrics.json"))
    parser.add_argument("--min-poll-seconds", type=int, default=int(env("GBFS_MIN_POLL_SECONDS", "60")))
    parser.add_argument("--stale-after-seconds", type=int, default=int(env("GBFS_STALE_AFTER_SECONDS", "180")))
    parser.add_argument("--metadata-refresh-seconds", type=int, default=int(env("GBFS_METADATA_REFRESH_SECONDS", "3600")))
    parser.add_argument("--request-timeout-seconds", type=float, default=float(env("GBFS_REQUEST_TIMEOUT_SECONDS", "20")))
    parser.add_argument("--retry-attempts", type=int, default=int(env("GBFS_RETRY_ATTEMPTS", "4")))
    parser.add_argument("--retry-base-seconds", type=float, default=float(env("GBFS_RETRY_BASE_SECONDS", "1")))
    parser.add_argument("--retry-max-seconds", type=float, default=float(env("GBFS_RETRY_MAX_SECONDS", "15")))
    return parser


def main() -> int:
    args = build_parser().parse_args()
    if args.retry_attempts < 1:
        raise SystemExit("--retry-attempts must be at least 1")
    if args.min_poll_seconds < 0 or args.stale_after_seconds < 1:
        raise SystemExit("poll/stale thresholds are invalid")

    root = Path.cwd().resolve()
    config = CollectorConfig(
        discovery_url=args.discovery_url,
        language=args.language,
        bootstrap_servers=args.bootstrap_servers,
        min_poll_seconds=args.min_poll_seconds,
        stale_after_seconds=args.stale_after_seconds,
        metadata_refresh_seconds=args.metadata_refresh_seconds,
        request_timeout_seconds=args.request_timeout_seconds,
        retry_attempts=args.retry_attempts,
        retry_base_seconds=args.retry_base_seconds,
        retry_max_seconds=args.retry_max_seconds,
    )
    publisher = KafkaPublisher(config)
    collector = GBFSCollector(
        config,
        publisher,
        workspace_root=root,
        archive_root=root / args.archive_root,
        state_file=root / args.state_file,
        metrics_file=root / args.metrics_file,
        contract_path=root / "contracts" / "station-status-v1.json",
    )
    try:
        if args.once:
            collector.collect_once()
        else:
            collector.run(max_polls=args.max_polls)
    finally:
        collector.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
