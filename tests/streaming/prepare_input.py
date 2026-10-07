from __future__ import annotations

import json
import os
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from tests.streaming.fixtures import scenario


def write_batch(path: Path, value: object, modified_epoch: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = value if isinstance(value, str) else json.dumps(value, separators=(",", ":"), sort_keys=True)
    path.write_text(text + "\n", encoding="utf-8")
    os.utime(path, (modified_epoch, modified_epoch))


def main() -> int:
    runtime = ROOT / "artifacts" / "step8" / "runtime"
    input_dir = runtime / "input"
    staged = runtime / "staged_phase2"
    fixture = scenario()
    base_epoch = 1_800_000_000
    for index, item in enumerate(fixture["phase1"]):
        write_batch(input_dir / f"batch-{index:03d}.json", item, base_epoch + index)
    for offset, item in enumerate(fixture["phase2"], start=len(fixture["phase1"])):
        write_batch(staged / f"batch-{offset:03d}.json", item, base_epoch + offset)
    manifest = {
        "phase1_files": len(fixture["phase1"]),
        "phase2_files": len(fixture["phase2"]),
        **fixture["expected"],
    }
    (runtime / "scenario.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
