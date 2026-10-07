from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from pyspark import StorageLevel
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


ROOT = Path(__file__).resolve().parents[2]

ALIASES: dict[str, tuple[str, ...]] = {
    "ride_id": ("ride_id", "ride id"),
    "rideable_type": ("rideable_type", "rideable type", "bike type"),
    "started_at": ("started_at", "starttime", "start time"),
    "ended_at": ("ended_at", "stoptime", "stop time"),
    "start_station_name": ("start_station_name", "start station name"),
    "start_station_id": ("start_station_id", "start station id"),
    "end_station_name": ("end_station_name", "end station name"),
    "end_station_id": ("end_station_id", "end station id"),
    "start_lat": ("start_lat", "start station latitude"),
    "start_lng": ("start_lng", "start station longitude"),
    "end_lat": ("end_lat", "end station latitude"),
    "end_lng": ("end_lng", "end station longitude"),
    "member_casual": ("member_casual", "usertype", "user type"),
}

REQUIRED_CANONICAL_COLUMNS = {"started_at", "ended_at"}


def normalized_header(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def resolve_columns(columns: Iterable[str]) -> tuple[dict[str, str | None], str]:
    normalized_to_source: dict[str, str] = {}
    for column in columns:
        key = normalized_header(column)
        if key in normalized_to_source:
            raise ValueError(f"Header collision after normalization: {column}")
        normalized_to_source[key] = column

    mapping: dict[str, str | None] = {}
    used_alias = False
    for canonical, aliases in ALIASES.items():
        source = next(
            (
                normalized_to_source[normalized_header(alias)]
                for alias in aliases
                if normalized_header(alias) in normalized_to_source
            ),
            None,
        )
        mapping[canonical] = source
        if source is not None and normalized_header(source) != canonical:
            used_alias = True

    missing = sorted(column for column in REQUIRED_CANONICAL_COLUMNS if mapping[column] is None)
    if missing:
        raise ValueError(f"Required trip columns are missing: {missing}")
    variant = "legacy_aliases" if used_alias or mapping["ride_id"] is None else "modern"
    return mapping, variant


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def source_files(input_path: Path) -> list[Path]:
    if input_path.is_file() and input_path.suffix.lower() == ".csv":
        return [input_path.resolve()]
    if input_path.is_dir():
        files = sorted(path.resolve() for path in input_path.glob("*.csv") if path.is_file())
        if files:
            return files
    raise FileNotFoundError(f"No CSV source files found at {input_path}")


def source_manifest(files: list[Path], source_archive: Path | None) -> dict[str, Any]:
    def portable_path(path: Path) -> str:
        resolved = path.resolve()
        try:
            return resolved.relative_to(ROOT.resolve()).as_posix()
        except ValueError:
            return resolved.as_posix()

    entries: list[dict[str, Any]] = []
    for path in files:
        with path.open("r", encoding="utf-8-sig", newline="") as handle:
            header = next(csv.reader(handle))
        mapping, variant = resolve_columns(header)
        entries.append(
            {
                "path": portable_path(path),
                "bytes": path.stat().st_size,
                "sha256": sha256_file(path),
                "header": header,
                "schema_variant": variant,
                "canonical_mapping": mapping,
            }
        )
    archive = None
    if source_archive is not None:
        archive = {
            "path": portable_path(source_archive),
            "bytes": source_archive.stat().st_size,
            "sha256": sha256_file(source_archive),
        }
    return {
        "manifest_version": "1.0",
        "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "source_archive": archive,
        "files": entries,
    }


def optional_source(mapping: dict[str, str | None], canonical: str) -> Any:
    source = mapping[canonical]
    return F.col(source).cast("string") if source is not None else F.lit(None).cast("string")


def read_sources(spark: SparkSession, files: list[Path]) -> DataFrame:
    frames: list[DataFrame] = []
    for path in files:
        raw = spark.read.option("header", True).option("mode", "PERMISSIVE").csv(path.as_posix())
        mapping, variant = resolve_columns(raw.columns)
        selected = raw.select(
            *[optional_source(mapping, canonical).alias(canonical) for canonical in ALIASES],
            F.lit(path.name).alias("_source_file"),
            F.lit(variant).alias("_source_schema_variant"),
        )
        frames.append(selected)
    combined = frames[0]
    for frame in frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def clean_string(column: Any) -> Any:
    trimmed = F.trim(column)
    return F.when(trimmed == "", F.lit(None)).otherwise(trimmed)


def parse_local_timestamp(column: Any) -> Any:
    return F.coalesce(
        F.try_to_timestamp(column, F.lit("yyyy-MM-dd HH:mm:ss.SSS")),
        F.try_to_timestamp(column, F.lit("yyyy-MM-dd HH:mm:ss")),
        F.try_to_timestamp(column),
    )


def normalize_rows(raw: DataFrame, *, source_timezone: str, long_duration_seconds: int) -> DataFrame:
    cleaned = raw
    for column in ALIASES:
        cleaned = cleaned.withColumn(column, clean_string(F.col(column)))

    started_local = parse_local_timestamp(F.col("started_at"))
    ended_local = parse_local_timestamp(F.col("ended_at"))
    started_utc = F.to_utc_timestamp(started_local, source_timezone)
    ended_utc = F.to_utc_timestamp(ended_local, source_timezone)
    duration = (
        F.unix_micros(ended_utc).cast("double") - F.unix_micros(started_utc).cast("double")
    ) / F.lit(1_000_000.0)
    generated_id = F.sha2(
        F.concat_ws(
            "|",
            *[
                F.coalesce(F.col(column), F.lit("<null>"))
                for column in (
                    "started_at",
                    "ended_at",
                    "start_station_id",
                    "end_station_id",
                    "start_station_name",
                    "end_station_name",
                )
            ],
        ),
        256,
    )
    start_lat = F.col("start_lat").cast("double")
    start_lng = F.col("start_lng").cast("double")
    end_lat = F.col("end_lat").cast("double")
    end_lng = F.col("end_lng").cast("double")
    coordinate_issue = (
        start_lat.isNull()
        | start_lng.isNull()
        | end_lat.isNull()
        | end_lng.isNull()
        | (~start_lat.between(-90.0, 90.0))
        | (~end_lat.between(-90.0, 90.0))
        | (~start_lng.between(-180.0, 180.0))
        | (~end_lng.between(-180.0, 180.0))
    )

    normalized = (
        cleaned.withColumn("ride_id_generated", F.col("ride_id").isNull())
        .withColumn("ride_id", F.coalesce(F.col("ride_id"), generated_id))
        .withColumn("started_at_local", F.col("started_at"))
        .withColumn("ended_at_local", F.col("ended_at"))
        .withColumn("_started_local_ts", started_local)
        .withColumn("_ended_local_ts", ended_local)
        .withColumn("started_at_utc", started_utc)
        .withColumn("ended_at_utc", ended_utc)
        .withColumn("duration_seconds", duration)
        .withColumn("start_lat", start_lat)
        .withColumn("start_lng", start_lng)
        .withColumn("end_lat", end_lat)
        .withColumn("end_lng", end_lng)
        .withColumn("started_local_date", F.to_date(started_local))
        .withColumn("ended_local_date", F.to_date(ended_local))
        .withColumn("started_local_hour", F.hour(started_local))
        .withColumn("ended_local_hour", F.hour(ended_local))
        .withColumn("started_iso_weekday", ((F.dayofweek(started_local) + 5) % 7 + 1).cast("int"))
        .withColumn("ended_iso_weekday", ((F.dayofweek(ended_local) + 5) % 7 + 1).cast("int"))
        .withColumn("started_year", F.year(started_local))
        .withColumn("started_month", F.month(started_local))
    )

    timestamp_invalid = F.col("started_at_utc").isNull() | F.col("ended_at_utc").isNull()
    nonpositive_duration = F.col("duration_seconds").isNull() | (F.col("duration_seconds") <= 0)
    both_endpoints_missing = F.col("start_station_id").isNull() & F.col("end_station_id").isNull()
    flags = F.filter(
        F.array(
            F.when(timestamp_invalid, F.lit("TIMESTAMP_INVALID")),
            F.when(nonpositive_duration, F.lit("DURATION_NONPOSITIVE")),
            F.when(F.col("duration_seconds") > long_duration_seconds, F.lit("DURATION_OVER_24H")),
            F.when(F.col("start_station_id").isNull(), F.lit("START_STATION_MISSING")),
            F.when(F.col("end_station_id").isNull(), F.lit("END_STATION_MISSING")),
            F.when(coordinate_issue, F.lit("COORDINATE_MISSING_OR_INVALID")),
        ),
        lambda value: value.isNotNull(),
    )
    return (
        normalized.withColumn("quality_flags", flags)
        .withColumn(
            "valid_for_demand",
            (~timestamp_invalid) & (~nonpositive_duration) & (~both_endpoints_missing),
        )
        .drop("started_at", "ended_at", "_started_local_ts", "_ended_local_ts")
    )


def deduplicate(rows: DataFrame) -> DataFrame:
    window = Window.partitionBy("ride_id").orderBy(F.col("_source_file").asc())
    return rows.withColumn("_dedup_rank", F.row_number().over(window)).filter(
        F.col("_dedup_rank") == 1
    ).drop("_dedup_rank")


def demand_events(silver: DataFrame) -> DataFrame:
    eligible = silver.filter(F.col("valid_for_demand"))
    pickups = eligible.filter(F.col("start_station_id").isNotNull()).select(
        "ride_id",
        F.col("start_station_id").alias("station_id"),
        F.col("start_station_name").alias("station_name"),
        F.col("started_local_date").alias("local_date"),
        F.col("started_iso_weekday").alias("iso_weekday"),
        F.col("started_local_hour").alias("local_hour"),
        F.lit(1).alias("pickups"),
        F.lit(0).alias("dropoffs"),
    )
    dropoffs = eligible.filter(F.col("end_station_id").isNotNull()).select(
        "ride_id",
        F.col("end_station_id").alias("station_id"),
        F.col("end_station_name").alias("station_name"),
        F.col("ended_local_date").alias("local_date"),
        F.col("ended_iso_weekday").alias("iso_weekday"),
        F.col("ended_local_hour").alias("local_hour"),
        F.lit(0).alias("pickups"),
        F.lit(1).alias("dropoffs"),
    )
    return pickups.unionByName(dropoffs)


def build_hourly_and_baseline(
    events: DataFrame, silver: DataFrame, spark: SparkSession
) -> tuple[DataFrame, DataFrame]:
    hourly = events.groupBy("station_id", "local_date", "iso_weekday", "local_hour").agg(
        F.first("station_name", ignorenulls=True).alias("station_name"),
        F.sum("pickups").cast("long").alias("pickup_count"),
        F.sum("dropoffs").cast("long").alias("dropoff_count"),
    )
    station_dimension = events.groupBy("station_id").agg(
        F.first("station_name", ignorenulls=True).alias("station_name")
    )
    calendar_bounds = silver.filter(F.col("valid_for_demand")).agg(
        F.min("started_local_date").alias("first_date"),
        F.max("started_local_date").alias("last_date"),
    )
    calendar = (
        calendar_bounds.select(
            F.explode(F.sequence("first_date", "last_date")).alias("local_date")
        )
        .withColumn("iso_weekday", ((F.dayofweek("local_date") + 5) % 7 + 1).cast("int"))
        .crossJoin(spark.range(24).select(F.col("id").cast("int").alias("local_hour")))
    )
    complete = (
        station_dimension.crossJoin(calendar)
        .join(
            hourly.select(
                "station_id", "local_date", "local_hour", "pickup_count", "dropoff_count"
            ),
            ["station_id", "local_date", "local_hour"],
            "left",
        )
        .fillna(0, subset=["pickup_count", "dropoff_count"])
    )
    baseline = complete.groupBy("station_id", "iso_weekday", "local_hour").agg(
        F.first("station_name", ignorenulls=True).alias("station_name"),
        F.countDistinct("local_date").cast("int").alias("calendar_days"),
        F.round(F.avg("pickup_count"), 3).alias("avg_pickups"),
        F.expr("percentile_approx(pickup_count, 0.5)").cast("long").alias("p50_pickups"),
        F.expr("percentile_approx(pickup_count, 0.95)").cast("long").alias("p95_pickups"),
        F.round(F.avg("dropoff_count"), 3).alias("avg_dropoffs"),
        F.expr("percentile_approx(dropoff_count, 0.5)").cast("long").alias("p50_dropoffs"),
        F.expr("percentile_approx(dropoff_count, 0.95)").cast("long").alias("p95_dropoffs"),
    )
    return hourly, baseline


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, indent=2, ensure_ascii=False), encoding="utf-8")
    os.replace(temporary, path)


def assert_safe_output(path: Path) -> None:
    resolved = path.resolve()
    allowed = [(ROOT / "data").resolve(), (ROOT / "artifacts").resolve()]
    if not any(resolved == base or base in resolved.parents for base in allowed):
        raise ValueError(f"Output must stay under data/ or artifacts/: {resolved}")


def run_pipeline(args: argparse.Namespace) -> dict[str, Any]:
    started = time.perf_counter()
    input_path = Path(args.input).resolve()
    files = source_files(input_path)
    source_archive = Path(args.source_archive).resolve() if args.source_archive else None
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    manifest_path = Path(args.manifest).resolve()
    summary_path = Path(args.summary).resolve()
    silver_path = Path(args.silver).resolve()
    quality_path = Path(args.quality).resolve()
    hourly_path = Path(args.hourly).resolve()
    baseline_path = Path(args.baseline).resolve()
    for output in (manifest_path, summary_path, silver_path, quality_path, hourly_path, baseline_path):
        assert_safe_output(output)

    manifest = source_manifest(files, source_archive)
    spark = (
        SparkSession.builder.appName("citibike-historical-batch")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        raw = read_sources(spark, files)
        normalized = normalize_rows(
            raw,
            source_timezone=config["source_timezone"],
            long_duration_seconds=int(config["long_duration_seconds"]),
        ).persist(StorageLevel.MEMORY_AND_DISK)
        raw_rows = normalized.count()
        silver = deduplicate(normalized).persist(StorageLevel.MEMORY_AND_DISK)
        silver_rows = silver.count()
        duplicate_rows = raw_rows - silver_rows

        counts_by_file = {
            row["_source_file"]: row["count"]
            for row in normalized.groupBy("_source_file").count().collect()
        }
        for entry in manifest["files"]:
            entry["row_count"] = counts_by_file.get(Path(entry["path"]).name, 0)
        manifest["total_rows"] = raw_rows
        manifest["duplicate_ride_id_rows"] = duplicate_rows
        write_json_atomic(manifest_path, manifest)

        quality_counts = {
            row["quality_flag"]: row["count"]
            for row in (
                silver.select(F.explode("quality_flags").alias("quality_flag"))
                .groupBy("quality_flag")
                .count()
                .collect()
            )
        }
        quarantine = silver.filter(~F.col("valid_for_demand"))
        quarantine_rows = quarantine.count()
        demand = demand_events(silver).persist(StorageLevel.MEMORY_AND_DISK)
        demand_event_rows = demand.count()
        hourly, baseline = build_hourly_and_baseline(demand, silver, spark)

        date_bounds = silver.agg(
            F.min("started_local_date").alias("first_start_date"),
            F.max("started_local_date").alias("last_start_date"),
            F.min("ended_local_date").alias("first_end_date"),
            F.max("ended_local_date").alias("last_end_date"),
        ).first()

        (
            silver.repartition(int(config["silver_output_partitions"]), "started_year", "started_month")
            .write.mode("overwrite")
            .partitionBy("started_year", "started_month")
            .parquet(silver_path.as_posix())
        )
        quarantine.write.mode("overwrite").parquet(quality_path.as_posix())
        hourly.repartition(int(config["gold_output_partitions"]), "iso_weekday").write.mode(
            "overwrite"
        ).partitionBy("iso_weekday").parquet(hourly_path.as_posix())
        baseline.repartition(int(config["gold_output_partitions"]), "iso_weekday").write.mode(
            "overwrite"
        ).partitionBy("iso_weekday").parquet(baseline_path.as_posix())

        hourly_rows = spark.read.parquet(hourly_path.as_posix()).count()
        baseline_rows = spark.read.parquet(baseline_path.as_posix()).count()
        summary = {
            "gate": "PASS",
            "generated_at_utc": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
            "source_timezone": config["source_timezone"],
            "analysis_window": {
                "first_start_date": str(date_bounds["first_start_date"]),
                "last_start_date": str(date_bounds["last_start_date"]),
                "first_end_date": str(date_bounds["first_end_date"]),
                "last_end_date": str(date_bounds["last_end_date"]),
                "baseline_calendar": "first_start_date..last_start_date",
            },
            "input": {
                "files": len(files),
                "rows": raw_rows,
                "duplicate_ride_id_rows_removed": duplicate_rows,
            },
            "silver": {
                "rows": silver_rows,
                "quarantine_rows": quarantine_rows,
                "quality_flag_counts": quality_counts,
                "path": silver_path.as_posix(),
            },
            "gold": {
                "demand_event_rows": demand_event_rows,
                "station_hourly_rows": hourly_rows,
                "baseline_rows": baseline_rows,
                "hourly_path": hourly_path.as_posix(),
                "baseline_path": baseline_path.as_posix(),
            },
            "duration_seconds": round(time.perf_counter() - started, 3),
        }
        write_json_atomic(summary_path, summary)
        print(json.dumps(summary, indent=2))
        return summary
    finally:
        spark.stop()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build Citi Bike historical Silver and Gold datasets")
    parser.add_argument("--input", required=True)
    parser.add_argument("--source-archive")
    parser.add_argument("--config", default=str(ROOT / "config" / "batch-rules.json"))
    parser.add_argument("--manifest", default=str(ROOT / "data" / "raw" / "trips" / "manifest.json"))
    parser.add_argument("--silver", default=str(ROOT / "data" / "silver" / "trips"))
    parser.add_argument("--quality", default=str(ROOT / "data" / "silver" / "trip_quality_issues"))
    parser.add_argument("--hourly", default=str(ROOT / "data" / "gold" / "station_hourly_demand"))
    parser.add_argument("--baseline", default=str(ROOT / "data" / "gold" / "station_demand_baseline"))
    parser.add_argument("--summary", default=str(ROOT / "artifacts" / "step7" / "batch-summary.json"))
    return parser


if __name__ == "__main__":
    run_pipeline(build_parser().parse_args())
