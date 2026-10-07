from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from reliability.recovery import (
    capture_state,
    observe_database_outage,
    wait_for_alert,
    write_json,
)


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def read(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def finalize(args: argparse.Namespace) -> dict:
    targets = read(args.targets)
    baseline = read(args.baseline)
    after_api = read(args.after_api)
    outage = read(args.outage)
    database_alert = read(args.database_alert)
    after_database = read(args.after_database)
    backlog = read(args.backlog)
    after_backlog = read(args.after_backlog)
    backup = read(args.backup)
    after_restore = read(args.after_restore)
    soak = read(args.soak)
    timings = read(args.timings)

    fingerprint = baseline["business_fingerprint"]
    fingerprint_checks = {
        "api_restart": after_api["business_fingerprint"] == fingerprint,
        "database_restart": after_database["business_fingerprint"] == fingerprint,
        "kafka_replay": after_backlog["business_fingerprint"] == fingerprint,
        "backup_restore": after_restore["business_fingerprint"] == fingerprint,
    }
    checks = {
        "api_restart_rto": timings["api_restart_rto_seconds"]
        <= targets["api_restart_rto_seconds"],
        "database_outage_detected": outage["pass"],
        "database_alert_fired": database_alert["observed"],
        "database_restart_rto": timings["database_restart_rto_seconds"]
        <= targets["database_restart_rto_seconds"],
        "kafka_backlog_buffered": backlog["lag_while_consumer_stopped"] > 0,
        "kafka_backlog_drained": backlog["lag_after_recovery"] == 0,
        "backlog_catchup_rto": backlog["catchup_seconds"]
        <= targets["backlog_catchup_seconds"],
        "backup_created": backup["bytes"] > 0 and len(backup["sha256"]) == 64,
        "business_state_unchanged": all(fingerprint_checks.values()),
        "soak_availability": soak["success_rate_percent"]
        >= targets["minimum_soak_success_rate_percent"],
        "soak_latency": soak["latency_ms"]["p95"]
        <= targets["maximum_soak_p95_latency_ms"],
    }
    require(all(checks.values()), f"Step 13 recovery checks failed: {checks}")

    before_counts = baseline["database_counts"]
    after_counts = after_restore["database_counts"]
    lost_records = sum(
        max(0, before_counts[key] - after_counts[key])
        for key in ("alert_events", "alert_states", "stations", "commands")
    )
    require(
        lost_records == targets["recovery_point_objective_lost_business_records"],
        f"Recovery lost {lost_records} business records",
    )

    summary = {
        "gate": "PASS",
        "checks_passed": len(checks),
        "api_restart_rto_seconds": timings["api_restart_rto_seconds"],
        "database_outage_readiness_http_status": outage["readiness_http_status"],
        "database_outage_metric": outage["database_up_metric"],
        "database_alert_fired": database_alert["observed"],
        "database_alert_time_to_observe_seconds": database_alert[
            "time_to_observe_seconds"
        ],
        "database_restart_rto_seconds": timings["database_restart_rto_seconds"],
        "kafka_lag_while_consumer_stopped": backlog["lag_while_consumer_stopped"],
        "kafka_lag_after_recovery": backlog["lag_after_recovery"],
        "backlog_catchup_seconds": backlog["catchup_seconds"],
        "business_fingerprint_preserved_across": fingerprint_checks,
        "backup_bytes": backup["bytes"],
        "backup_sha256": backup["sha256"],
        "lost_business_records": lost_records,
        "soak_requests": soak["requests"],
        "soak_success_rate_percent": soak["success_rate_percent"],
        "soak_throughput_requests_per_second": soak[
            "throughput_requests_per_second"
        ],
        "soak_p95_latency_ms": soak["latency_ms"]["p95"],
        "scope": "controlled single-host failure injection; not a high-availability claim",
    }
    write_json(args.output, summary)
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Step 13 recovery evidence helper")
    subparsers = parser.add_subparsers(dest="command", required=True)

    capture = subparsers.add_parser("capture")
    capture.add_argument("--base-url", required=True)
    capture.add_argument("--database-url", required=True)
    capture.add_argument("--output", type=Path, required=True)

    outage = subparsers.add_parser("outage")
    outage.add_argument("--base-url", required=True)
    outage.add_argument("--output", type=Path, required=True)

    alert = subparsers.add_parser("wait-alert")
    alert.add_argument("--prometheus-url", required=True)
    alert.add_argument("--alert-name", required=True)
    alert.add_argument("--timeout-seconds", type=float, default=70.0)
    alert.add_argument("--output", type=Path, required=True)

    final = subparsers.add_parser("finalize")
    final.add_argument("--targets", type=Path, required=True)
    final.add_argument("--baseline", type=Path, required=True)
    final.add_argument("--after-api", type=Path, required=True)
    final.add_argument("--outage", type=Path, required=True)
    final.add_argument("--database-alert", type=Path, required=True)
    final.add_argument("--after-database", type=Path, required=True)
    final.add_argument("--backlog", type=Path, required=True)
    final.add_argument("--after-backlog", type=Path, required=True)
    final.add_argument("--backup", type=Path, required=True)
    final.add_argument("--after-restore", type=Path, required=True)
    final.add_argument("--soak", type=Path, required=True)
    final.add_argument("--timings", type=Path, required=True)
    final.add_argument("--output", type=Path, required=True)

    args = parser.parse_args()
    if args.command == "capture":
        result = capture_state(args.base_url, args.database_url)
        write_json(args.output, result)
    elif args.command == "outage":
        result = observe_database_outage(args.base_url)
        write_json(args.output, result)
    elif args.command == "wait-alert":
        result = wait_for_alert(
            args.prometheus_url,
            args.alert_name,
            timeout_seconds=args.timeout_seconds,
        )
        write_json(args.output, result)
    else:
        result = finalize(args)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
