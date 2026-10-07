from __future__ import annotations

import hashlib
import json
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.compute as pc
import pyarrow.dataset as ds


ROOT = Path(__file__).resolve().parents[1]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def directory_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def main() -> int:
    summary_path = ROOT / "artifacts" / "step7" / "batch-summary.json"
    manifest_path = ROOT / "data" / "raw" / "trips" / "manifest.json"
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if summary["gate"] != "PASS":
        raise RuntimeError("Batch summary did not pass")
    if manifest["total_rows"] != summary["input"]["rows"]:
        raise RuntimeError("Manifest and batch input counts differ")
    if sum(item["row_count"] for item in manifest["files"]) != manifest["total_rows"]:
        raise RuntimeError("Per-file manifest row counts do not add up")
    for item in manifest["files"]:
        path = (ROOT / item["path"]).resolve()
        if not path.exists() or sha256_file(path) != item["sha256"]:
            raise RuntimeError(f"Raw file checksum mismatch: {path}")

    silver_path = ROOT / "data" / "silver" / "trips"
    quality_path = ROOT / "data" / "silver" / "trip_quality_issues"
    hourly_path = ROOT / "data" / "gold" / "station_hourly_demand"
    baseline_path = ROOT / "data" / "gold" / "station_demand_baseline"
    silver = ds.dataset(silver_path, format="parquet", partitioning="hive")
    quality = ds.dataset(quality_path, format="parquet")
    hourly = ds.dataset(hourly_path, format="parquet", partitioning="hive")
    baseline = ds.dataset(baseline_path, format="parquet", partitioning="hive")

    counts = {
        "silver": silver.count_rows(),
        "quality": quality.count_rows(),
        "hourly": hourly.count_rows(),
        "baseline": baseline.count_rows(),
    }
    expected = {
        "silver": summary["silver"]["rows"],
        "quality": summary["silver"]["quarantine_rows"],
        "hourly": summary["gold"]["station_hourly_rows"],
        "baseline": summary["gold"]["baseline_rows"],
    }
    if counts != expected:
        raise RuntimeError(f"Parquet row counts differ from summary: {counts} != {expected}")

    pickup_total = 0
    dropoff_total = 0
    for batch in hourly.scanner(columns=["pickup_count", "dropoff_count"]).to_batches():
        pickup_total += int(pc.sum(batch.column("pickup_count")).as_py() or 0)
        dropoff_total += int(pc.sum(batch.column("dropoff_count")).as_py() or 0)
    if pickup_total != summary["input"]["rows"] - summary["silver"]["quality_flag_counts"]["START_STATION_MISSING"]:
        raise RuntimeError("Gold pickup total does not match usable start endpoints")
    if dropoff_total != summary["input"]["rows"] - summary["silver"]["quality_flag_counts"]["END_STATION_MISSING"]:
        raise RuntimeError("Gold dropoff total does not match usable end endpoints")

    bucket_counts: dict[str, int] = defaultdict(int)
    seen_keys: set[tuple[str, int, int]] = set()
    for batch in baseline.scanner(columns=["station_id", "iso_weekday", "local_hour"]).to_batches():
        for row in batch.to_pylist():
            key = (row["station_id"], int(row["iso_weekday"]), int(row["local_hour"]))
            if key in seen_keys:
                raise RuntimeError(f"Duplicate baseline key: {key}")
            seen_keys.add(key)
            bucket_counts[row["station_id"]] += 1
    if set(bucket_counts.values()) != {168}:
        raise RuntimeError("Every station must have exactly 7 weekdays x 24 hours")

    sample = silver.to_table(
        columns=["ride_id", "started_at_local", "started_at_utc", "duration_seconds"],
        filter=ds.field("ride_id") == "C88217D5C42993C6",
    ).to_pylist()
    if len(sample) != 1:
        raise RuntimeError("Known timezone verification ride was not found exactly once")
    known = sample[0]
    if known["started_at_local"] != "2026-04-06 18:55:38.804":
        raise RuntimeError("Local source timestamp was not preserved")
    if known["started_at_utc"].replace(tzinfo=timezone.utc) != datetime(
        2026, 4, 6, 22, 55, 38, 804000, tzinfo=timezone.utc
    ):
        raise RuntimeError("America/New_York timestamp was not normalized to UTC correctly")

    verification = {
        "gate": "PASS",
        "verified_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "manifest": {
            "files": len(manifest["files"]),
            "rows": manifest["total_rows"],
            "checksums_verified": len(manifest["files"]),
        },
        "parquet_rows": counts,
        "demand_totals": {"pickups": pickup_total, "dropoffs": dropoff_total},
        "baseline": {
            "stations": len(bucket_counts),
            "buckets_per_station": 168,
            "unique_keys": len(seen_keys),
        },
        "timezone_probe": {
            "ride_id": known["ride_id"],
            "local": known["started_at_local"],
            "utc": known["started_at_utc"].isoformat(),
        },
        "output_bytes": {
            "silver": directory_bytes(silver_path),
            "quality": directory_bytes(quality_path),
            "hourly": directory_bytes(hourly_path),
            "baseline": directory_bytes(baseline_path),
        },
    }
    output = ROOT / "artifacts" / "step7" / "verification-summary.json"
    output.write_text(json.dumps(verification, indent=2), encoding="utf-8")
    print(json.dumps(verification, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
