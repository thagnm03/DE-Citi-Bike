from __future__ import annotations

import json
import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from spark.batch.historical_pipeline import ALIASES, deduplicate, demand_events, normalize_rows


def row(
    ride_id: str | None,
    started: str,
    ended: str,
    start_id: str | None = "1001",
    end_id: str | None = "1002",
) -> dict[str, str | None]:
    return {
        "ride_id": ride_id,
        "rideable_type": "classic_bike",
        "started_at": started,
        "ended_at": ended,
        "start_station_name": "Start" if start_id else None,
        "start_station_id": start_id,
        "end_station_name": "End" if end_id else None,
        "end_station_id": end_id,
        "start_lat": "40.7" if start_id else None,
        "start_lng": "-73.9" if start_id else None,
        "end_lat": "40.8" if end_id else None,
        "end_lng": "-73.8" if end_id else None,
        "member_casual": "member",
        "_source_file": "quality-fixture.csv",
        "_source_schema_variant": "modern",
    }


def main() -> int:
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("step7-quality-fixture")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        records = [
            row("DUPLICATE", "2026-04-01 08:00:00.000", "2026-04-01 08:10:00.000"),
            row("DUPLICATE", "2026-04-01 08:00:00.000", "2026-04-01 08:10:00.000"),
            row("NEGATIVE", "2026-04-01 09:00:00.000", "2026-04-01 08:59:00.000"),
            row("LONG", "2026-04-01 10:00:00.000", "2026-04-02 11:00:01.000"),
            row("MISSING_START", "2026-04-01 11:00:00.000", "2026-04-01 11:05:00.000", None, "1002"),
            row("MISSING_BOTH", "2026-04-01 12:00:00.000", "2026-04-01 12:05:00.000", None, None),
            row(None, "2026-03-08 01:30:00.000", "2026-03-08 03:30:00.000"),
        ]
        schema = StructType(
            [StructField(column, StringType(), True) for column in list(ALIASES) + ["_source_file", "_source_schema_variant"]]
        )
        raw = spark.createDataFrame(records, schema=schema)
        normalized = normalize_rows(
            raw, source_timezone="America/New_York", long_duration_seconds=86_400
        )
        silver = deduplicate(normalized).cache()
        silver_rows = silver.count()
        if normalized.count() - silver_rows != 1:
            raise RuntimeError("Duplicate ride_id policy failed")
        if silver_rows != 6:
            raise RuntimeError("Unexpected Silver fixture row count")

        flags = {
            item["quality_flag"]: item["count"]
            for item in (
                silver.select(F.explode("quality_flags").alias("quality_flag"))
                .groupBy("quality_flag")
                .count()
                .collect()
            )
        }
        expected_flags = {
            "DURATION_NONPOSITIVE": 1,
            "DURATION_OVER_24H": 1,
            "START_STATION_MISSING": 2,
            "END_STATION_MISSING": 1,
            "COORDINATE_MISSING_OR_INVALID": 2,
        }
        if flags != expected_flags:
            raise RuntimeError(f"Quality flags differ: {flags} != {expected_flags}")
        quarantine_rows = silver.filter(~F.col("valid_for_demand")).count()
        if quarantine_rows != 2:
            raise RuntimeError("Negative-duration and both-endpoint-missing rows must be quarantined")
        if demand_events(silver).count() != 7:
            raise RuntimeError("Demand events did not preserve every usable endpoint")

        generated = silver.filter(F.col("ride_id_generated")).select(
            "ride_id", "duration_seconds", "started_at_utc", "ended_at_utc"
        ).first()
        if generated is None or len(generated["ride_id"]) != 64:
            raise RuntimeError("Legacy missing ride ID was not replaced deterministically")
        if generated["duration_seconds"] != 3600.0:
            raise RuntimeError("DST transition must represent one elapsed hour, not two wall-clock hours")

        result = {
            "gate": "PASS",
            "raw_rows": len(records),
            "silver_rows": silver_rows,
            "duplicate_rows_removed": 1,
            "quarantine_rows": quarantine_rows,
            "demand_event_rows": 7,
            "quality_flag_counts": flags,
            "generated_ride_id": generated["ride_id"],
            "dst_elapsed_seconds": generated["duration_seconds"],
        }
        output = ROOT / "artifacts" / "step7" / "quality-fixture-summary.json"
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2))
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    raise SystemExit(main())
