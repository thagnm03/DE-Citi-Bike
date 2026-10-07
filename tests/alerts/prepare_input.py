from __future__ import annotations

import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.alerts.fixtures import scenario


def write_event(path: Path, value: object, modified_epoch: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    values = value if isinstance(value, list) else [value]
    content = "".join(
        (item if isinstance(item, str) else json.dumps(item, separators=(",", ":"), sort_keys=True))
        + "\n"
        for item in values
    )
    path.write_text(content, encoding="utf-8")
    os.utime(path, (modified_epoch, modified_epoch))


def main() -> int:
    runtime = ROOT / "artifacts" / "step9" / "runtime"
    fixture = scenario()
    epoch = 1_810_000_000
    phase1_batches = [
        fixture["phase1"][0:6],
        fixture["phase1"][6:12],
        fixture["phase1"][12:19],
        fixture["phase1"][19:20],
        fixture["phase1"][20:21],
    ]
    for index, batch in enumerate(phase1_batches):
        write_event(runtime / "input" / f"event-{index:03d}.json", batch, epoch + index)
    write_event(
        runtime / "staged_phase2" / f"event-{len(phase1_batches):03d}.json",
        fixture["phase2"],
        epoch + len(phase1_batches),
    )
    ack_file = runtime / "acknowledgements.jsonl"
    ack_file.parent.mkdir(parents=True, exist_ok=True)
    ack_file.write_text(
        "".join(json.dumps(item, separators=(",", ":")) + "\n" for item in fixture["acknowledgements"]),
        encoding="utf-8",
    )
    manifest = {
        "phase1_files": len(phase1_batches),
        "phase2_files": 1,
        **fixture["expected"],
    }
    (runtime / "scenario.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
