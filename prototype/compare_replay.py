from __future__ import annotations

import argparse
import json
from pathlib import Path


def load_ids(path: Path) -> list[str]:
    return [json.loads(line)["alert_event_id"] for line in path.read_text(encoding="utf-8").splitlines() if line]


def main() -> None:
    parser = argparse.ArgumentParser(description="Compare main and recovery-replay business event IDs")
    parser.add_argument("main", type=Path)
    parser.add_argument("replay", type=Path)
    args = parser.parse_args()
    main_ids = load_ids(args.main)
    replay_ids = load_ids(args.replay)
    if main_ids != replay_ids:
        raise RuntimeError(f"Replay IDs differ: main={main_ids}, replay={replay_ids}")
    print(f"REPLAY_EQUIVALENT=TRUE ALERT_EVENT_IDS={','.join(main_ids)}")


if __name__ == "__main__":
    main()
