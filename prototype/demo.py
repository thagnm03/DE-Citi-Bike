from __future__ import annotations

import argparse
import json
from copy import deepcopy
from dataclasses import asdict
from pathlib import Path

from .contracts import load_validator, validate_or_raise
from .fixtures import late_observation, partition_probe_observations, persistent_empty_sequence
from .rules import EpisodeEngine, RuleConfig


def write_json_lines(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = "".join(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n" for row in rows)
    path.write_text(text, encoding="utf-8")


def run_demo(project_root: Path, output_dir: Path) -> dict:
    output_dir.mkdir(parents=True, exist_ok=True)
    config = RuleConfig.from_json(project_root / "config" / "prototype-rules.json")
    status_validator = load_validator(project_root / "contracts" / "station-status-v1.json")
    alert_validator = load_validator(project_root / "contracts" / "station-alert-v1.json")

    observations = persistent_empty_sequence()
    for observation in observations:
        validate_or_raise(observation, status_validator)
    write_json_lines(output_dir / "input-observations.ndjson", observations)
    write_json_lines(output_dir / "partition-probes.ndjson", partition_probe_observations())
    write_json_lines(output_dir / "late-observation.ndjson", [late_observation()])
    invalid_observation = deepcopy(observations[0])
    invalid_observation["availability"]["bikes_available"] = -1
    write_json_lines(output_dir / "invalid-observations.ndjson", [invalid_observation])

    checkpoint_path = output_dir / "checkpoint.json"
    first_engine = EpisodeEngine(config)
    first_alerts = first_engine.process_many(observations[:4])
    first_engine.save_checkpoint(checkpoint_path)

    restarted_engine = EpisodeEngine.load_checkpoint(checkpoint_path, config)
    remaining_alerts = restarted_engine.process_many(observations[4:])
    alerts = first_alerts + remaining_alerts
    restarted_engine.save_checkpoint(checkpoint_path)

    for alert in alerts:
        validate_or_raise(alert, alert_validator)

    if [alert["event_type"] for alert in alerts] != ["ALERT_OPENED", "ALERT_RESOLVED"]:
        raise AssertionError("Expected exactly one OPENED and one RESOLVED transition")
    if alerts[0]["alert_id"] != alerts[1]["alert_id"]:
        raise AssertionError("OPENED and RESOLVED must belong to the same episode")
    if restarted_engine.metrics.duplicates != 1:
        raise AssertionError("The retry fixture must be counted as one duplicate")

    write_json_lines(output_dir / "alerts.ndjson", alerts)

    idempotency_engine = EpisodeEngine.load_checkpoint(checkpoint_path, config)
    duplicate_outputs = idempotency_engine.process_many(observations)
    if duplicate_outputs:
        raise AssertionError("Reprocessing the same input against the checkpoint produced duplicate alerts")

    clean_replay_engine = EpisodeEngine(config)
    clean_replay_alerts = clean_replay_engine.process_many(observations)
    original_ids = [alert["alert_event_id"] for alert in alerts]
    replay_ids = [alert["alert_event_id"] for alert in clean_replay_alerts]
    if original_ids != replay_ids:
        raise AssertionError("Clean replay did not reproduce deterministic business event IDs")

    summary = {
        "gate": "PASS",
        "rule_version": config.rule_version,
        "input_records": len(observations),
        "unique_records_processed": restarted_engine.metrics.processed,
        "duplicate_records": restarted_engine.metrics.duplicates,
        "structural_invalid_fixtures": 1,
        "restart_simulated_after_record": 4,
        "alert_transitions": [alert["event_type"] for alert in alerts],
        "alert_id": alerts[0]["alert_id"],
        "alert_event_ids": original_ids,
        "same_checkpoint_replay_outputs": len(duplicate_outputs),
        "clean_replay_same_business_ids": original_ids == replay_ids,
        "metrics_after_restart": asdict(restarted_engine.metrics),
    }
    (output_dir / "run-summary.json").write_text(
        json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    return summary


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the deterministic Step 4 vertical prototype")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("artifacts/step4/local-demo"),
        help="Directory for observations, checkpoint, alerts and summary",
    )
    args = parser.parse_args()
    project_root = Path(__file__).resolve().parents[1]
    output_dir = args.output_dir if args.output_dir.is_absolute() else project_root / args.output_dir
    summary = run_demo(project_root, output_dir)
    print(json.dumps(summary, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
