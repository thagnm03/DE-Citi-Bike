from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from spark.common.schemas import STATION_STATUS_SCHEMA


EPISODE_KEY_SCHEMA = StructType([StructField("event_id", StringType(), False)])
EPISODE_VALUE_SCHEMA = StructType([StructField("seen_at_epoch_ms", LongType(), False)])
LATEST_STATE_SCHEMA = StructType(
    [
        StructField("last_event_time_epoch_ms", LongType(), False),
        StructField("last_event_id", StringType(), False),
    ]
)
CURRENT_STATE_OUTPUT_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), False),
        StructField("event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("current_state_json", StringType(), False),
        StructField("input_partition", LongType(), False),
        StructField("input_offset", LongType(), False),
    ]
)


def validation_errors() -> Any:
    observation = F.col("observation")
    availability = observation["availability"]
    service = observation["service"]
    source = observation["source"]
    return F.filter(
        F.array(
            F.when(~F.col("json_is_parseable"), F.lit("MALFORMED_JSON")),
            F.when(
                observation["schema_version"].isNull()
                | (observation["schema_version"] != "1.0"),
                F.lit("SCHEMA_VERSION_INVALID"),
            ),
            F.when(
                observation["event_id"].isNull()
                | (~observation["event_id"].rlike("^[a-f0-9]{64}$")),
                F.lit("EVENT_ID_INVALID"),
            ),
            F.when(
                observation["event_type"].isNull()
                | (observation["event_type"] != "STATION_STATUS_OBSERVED"),
                F.lit("EVENT_TYPE_INVALID"),
            ),
            F.when(
                observation["source_system"].isNull()
                | (observation["source_system"] != "citibike_nyc"),
                F.lit("SOURCE_SYSTEM_INVALID"),
            ),
            F.when(
                observation["station"].isNull()
                | observation["station"]["station_id"].isNull()
                | (F.length(observation["station"]["station_id"]) == 0),
                F.lit("STATION_ID_MISSING"),
            ),
            F.when(
                availability.isNull()
                | availability["bikes_available"].isNull()
                | availability["bikes_disabled"].isNull()
                | availability["docks_available"].isNull()
                | availability["docks_disabled"].isNull()
                | (availability["bikes_available"] < 0)
                | (availability["bikes_disabled"] < 0)
                | (availability["docks_available"] < 0)
                | (availability["docks_disabled"] < 0)
                | (availability["ebikes_available"] < 0),
                F.lit("AVAILABILITY_INVALID"),
            ),
            F.when(
                service.isNull()
                | service["is_installed"].isNull()
                | service["is_renting"].isNull()
                | service["is_returning"].isNull(),
                F.lit("SERVICE_FLAGS_INVALID"),
            ),
            F.when(F.col("event_time").isNull(), F.lit("EVENT_TIME_INVALID")),
            F.when(
                source.isNull()
                | source["feed_version"].isNull()
                | (~source["feed_version"].isin("1.1", "2.3"))
                | source["snapshot_sha256"].isNull()
                | (~source["snapshot_sha256"].rlike("^[a-f0-9]{64}$")),
                F.lit("SOURCE_LINEAGE_INVALID"),
            ),
            F.when(
                observation["quality"].isNull()
                | observation["quality"]["timestamp_status"].isNull()
                | observation["quality"]["capacity_consistency"].isNull()
                | observation["quality"]["issue_codes"].isNull(),
                F.lit("QUALITY_INVALID"),
            ),
            F.when(
                F.col("input_key").isNotNull()
                & observation["station"]["station_id"].isNotNull()
                & (F.col("input_key") != observation["station"]["station_id"]),
                F.lit("KAFKA_KEY_STATION_MISMATCH"),
            ),
        ),
        lambda code: code.isNotNull(),
    )


def parse_and_validate(raw: DataFrame) -> DataFrame:
    parsed = (
        raw.withColumn(
            "json_is_parseable", F.get_json_object("raw_value", "$").isNotNull()
        )
        .withColumn("observation", F.from_json("raw_value", STATION_STATUS_SCHEMA))
        .withColumn(
            "event_time",
            F.to_timestamp(F.col("observation.time.snapshot_updated_at_utc")),
        )
        .withColumn("validation_errors", validation_errors())
        .withColumn(
            "validation_errors",
            F.when(
                ~F.col("json_is_parseable"), F.array(F.lit("MALFORMED_JSON"))
            ).otherwise(F.col("validation_errors")),
        )
    )
    return parsed.withColumn("is_valid", F.size("validation_errors") == 0)


def build_dlq(validated: DataFrame) -> DataFrame:
    return validated.filter(~F.col("is_valid")).select(
        F.coalesce(F.col("input_key"), F.sha2("raw_value", 256)).alias("record_key"),
        F.col("validation_errors").alias("error_codes"),
        F.sha2("raw_value", 256).alias("raw_sha256"),
        F.substring("raw_value", 1, 1000).alias("raw_fragment"),
        F.col("input_topic"),
        F.col("input_partition"),
        F.col("input_offset"),
        F.current_timestamp().alias("detected_at_utc"),
    )


def valid_events(validated: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        validated.filter(F.col("is_valid"))
        .select(
            F.col("observation.event_id").alias("event_id"),
            F.col("observation.station.station_id").alias("station_id"),
            F.col("observation.station.short_name").alias("short_name"),
            F.col("observation.station.name").alias("station_name"),
            F.col("observation.availability.bikes_available").alias("bikes_available"),
            F.col("observation.availability.docks_available").alias("docks_available"),
            F.col("observation.time.source_age_seconds").alias("source_age_seconds"),
            F.col("raw_value").alias("observation_json"),
            F.col("event_time"),
            F.col("input_partition"),
            F.col("input_offset"),
        )
        .withWatermark("event_time", watermark_delay)
    )


def add_historical_baseline(events: DataFrame, baseline: DataFrame) -> DataFrame:
    local_time = F.from_utc_timestamp("event_time", "America/New_York")
    enriched = (
        events.withColumn("local_hour", F.hour(local_time).cast("int"))
        .withColumn("iso_weekday", ((F.dayofweek(local_time) + 5) % 7 + 1).cast("int"))
    )
    reference = baseline.select(
        F.col("station_id").alias("baseline_short_name"),
        F.col("iso_weekday").alias("baseline_iso_weekday"),
        F.col("local_hour").alias("baseline_local_hour"),
        "calendar_days",
        "avg_pickups",
        "p50_pickups",
        "p95_pickups",
        "avg_dropoffs",
        "p50_dropoffs",
        "p95_dropoffs",
    )
    return enriched.join(
        F.broadcast(reference),
        (enriched.short_name == reference.baseline_short_name)
        & (enriched.iso_weekday == reference.baseline_iso_weekday)
        & (enriched.local_hour == reference.baseline_local_hour),
        "left",
    ).drop("baseline_short_name", "baseline_iso_weekday", "baseline_local_hour")


class LatestStationProcessor(StatefulProcessor):
    def __init__(self, dedup_ttl_seconds: int) -> None:
        self.dedup_ttl_seconds = dedup_ttl_seconds

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.latest = handle.getValueState("latest", LATEST_STATE_SCHEMA)
        self.seen_events = handle.getMapState(
            "seen_events", EPISODE_KEY_SCHEMA, EPISODE_VALUE_SCHEMA
        )

    def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
        station_id = key[0]
        ordered = sorted(rows, key=lambda row: (row.event_time, row.input_offset))
        if not ordered:
            return iter(())

        newest_ms = int(ordered[-1].event_time.timestamp() * 1000)
        cutoff_ms = newest_ms - self.dedup_ttl_seconds * 1000
        expired = [
            seen_key
            for seen_key, seen_value in self.seen_events.iterator()
            if int(seen_value[0]) < cutoff_ms
        ]
        for seen_key in expired:
            self.seen_events.removeKey(seen_key)

        last_epoch_ms = None
        last_id = None
        if self.latest.exists():
            state = self.latest.get()
            last_epoch_ms, last_id = int(state[0]), state[1]

        output: list[Row] = []
        for row in ordered:
            row_epoch_ms = int(row.event_time.timestamp() * 1000)
            seen_key = (row.event_id,)
            if self.seen_events.containsKey(seen_key):
                continue
            self.seen_events.updateValue(
                seen_key, (row_epoch_ms,)
            )
            if last_epoch_ms is not None and row_epoch_ms <= last_epoch_ms:
                continue

            observation = json.loads(row.observation_json)
            baseline_found = row.calendar_days is not None
            current_state = {
                "schema_version": "1.0",
                "station_id": station_id,
                "event_id": row.event_id,
                "event_time_utc": row.event_time.replace(tzinfo=timezone.utc).isoformat().replace(
                    "+00:00", "Z"
                ),
                "station": observation["station"],
                "availability": observation["availability"],
                "service": observation["service"],
                "quality": observation["quality"],
                "source_age_seconds": observation["time"]["source_age_seconds"],
                "historical_demand": {
                    "found": baseline_found,
                    "iso_weekday": int(row.iso_weekday),
                    "local_hour": int(row.local_hour),
                    "calendar_days": int(row.calendar_days) if baseline_found else None,
                    "avg_pickups": float(row.avg_pickups) if baseline_found else None,
                    "p50_pickups": int(row.p50_pickups) if baseline_found else None,
                    "p95_pickups": int(row.p95_pickups) if baseline_found else None,
                    "avg_dropoffs": float(row.avg_dropoffs) if baseline_found else None,
                    "p50_dropoffs": int(row.p50_dropoffs) if baseline_found else None,
                    "p95_dropoffs": int(row.p95_dropoffs) if baseline_found else None,
                },
                "lineage": {
                    "input_partition": int(row.input_partition),
                    "input_offset": int(row.input_offset),
                },
            }
            output.append(
                Row(
                    station_id=station_id,
                    event_id=row.event_id,
                    event_time=row.event_time,
                    current_state_json=json.dumps(
                        current_state, ensure_ascii=False, separators=(",", ":"), sort_keys=True
                    ),
                    input_partition=int(row.input_partition),
                    input_offset=int(row.input_offset),
                )
            )
            last_epoch_ms = row_epoch_ms
            last_id = row.event_id

        if last_epoch_ms is not None and last_id is not None:
            self.latest.update((last_epoch_ms, last_id))
        return iter(output)

    def close(self) -> None:
        return None


def build_source(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.source == "file":
        raw = (
            spark.readStream.format("text")
            .option("maxFilesPerTrigger", 1)
            .load(str(args.input_dir))
        )
        return raw.select(
            F.get_json_object("value", "$.station.station_id").alias("input_key"),
            F.col("value").alias("raw_value"),
            F.lit("file.station-status").alias("input_topic"),
            F.pmod(
                F.xxhash64(F.get_json_object("value", "$.station.station_id")), F.lit(3)
            ).cast("long").alias("input_partition"),
            F.abs(F.xxhash64("value")).cast("long").alias("input_offset"),
        )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )
    return raw.select(
        F.col("key").cast("string").alias("input_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("input_topic"),
        F.col("partition").cast("long").alias("input_partition"),
        F.col("offset").cast("long").alias("input_offset"),
    )


def window_metrics(events: DataFrame, duration: str) -> DataFrame:
    deduplicated = events.dropDuplicatesWithinWatermark(["event_id"])
    return deduplicated.groupBy(F.window("event_time", duration), "station_id").agg(
        F.count("*").cast("long").alias("observation_count"),
        F.round(F.avg("bikes_available"), 3).alias("avg_bikes_available"),
        F.round(F.avg("docks_available"), 3).alias("avg_docks_available"),
        F.max("source_age_seconds").cast("long").alias("max_source_age_seconds"),
    )


def append_json_batch(frame: DataFrame, _batch_id: int, output_path: str) -> None:
    if frame.rdd.isEmpty():
        return
    frame.write.mode("append").json(output_path)


def progress_to_file(query: Any, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for progress in query.recentProgress:
            handle.write(json.dumps(dict(progress), ensure_ascii=False, default=str) + "\n")


def materialize_current_snapshot(
    spark: SparkSession, updates_path: Path, snapshot_path: Path
) -> None:
    if not updates_path.exists() or not list(updates_path.glob("part-*.json")):
        return
    updates = spark.read.json(str(updates_path)).withColumn(
        "event_time", F.to_timestamp("event_time")
    )
    latest = (
        updates.withColumn(
            "_rank",
            F.row_number().over(
                Window.partitionBy("station_id").orderBy(
                    F.col("event_time").desc(), F.col("input_offset").desc()
                )
            ),
        )
        .filter(F.col("_rank") == 1)
        .drop("_rank")
    )
    latest.write.mode("overwrite").json(str(snapshot_path))


def start_file_queries(
    dlq: DataFrame,
    state_updates: DataFrame,
    windows: DataFrame,
    args: argparse.Namespace,
) -> list[Any]:
    output = args.output_root
    checkpoint = args.checkpoint_root
    invalid_query = (
        dlq.writeStream.format("json")
        .option("path", str(output / "dead_letter"))
        .option("checkpointLocation", str(checkpoint / "dead_letter"))
        .outputMode("append")
        .queryName("station-status-dead-letter")
        .trigger(availableNow=True)
        .start()
    )
    state_query = (
        state_updates.writeStream.foreachBatch(
            lambda frame, batch_id: append_json_batch(
                frame, batch_id, str(output / "state_updates")
            )
        )
        .option("checkpointLocation", str(checkpoint / "station_state"))
        .outputMode("update")
        .queryName("station-current-state")
        .trigger(availableNow=True)
        .start()
    )
    window_query = (
        windows.select(
            "station_id",
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            "observation_count",
            "avg_bikes_available",
            "avg_docks_available",
            "max_source_age_seconds",
        )
        .writeStream.format("json")
        .option("path", str(output / "window_metrics"))
        .option("checkpointLocation", str(checkpoint / "window_metrics"))
        .outputMode("append")
        .queryName("station-window-metrics")
        .trigger(availableNow=True)
        .start()
    )
    return [invalid_query, state_query, window_query]


def start_kafka_queries(
    dlq: DataFrame,
    state_updates: DataFrame,
    windows: DataFrame,
    args: argparse.Namespace,
) -> list[Any]:
    checkpoint = args.checkpoint_root
    dlq_kafka = dlq.select(
        F.col("record_key").cast("string").alias("key"),
        F.to_json(F.struct("*")).alias("value"),
    )
    current_kafka = state_updates.select(
        F.col("station_id").cast("string").alias("key"),
        F.col("current_state_json").alias("value"),
    )
    window_values = windows.select(
        F.concat_ws(
            "|", F.col("station_id"), F.col("window.end").cast("string")
        ).alias("key"),
        F.to_json(F.struct("station_id", "window", "observation_count", "avg_bikes_available", "avg_docks_available", "max_source_age_seconds")).alias("value"),
    )
    definitions = [
        (dlq_kafka, args.invalid_topic, "dead_letter"),
        (current_kafka, args.current_topic, "station_state"),
        (window_values, args.window_topic, "window_metrics"),
    ]
    queries = []
    for frame, topic, name in definitions:
        writer = (
            frame.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", topic)
            .option("checkpointLocation", str(checkpoint / name))
            .outputMode("update" if name == "station_state" else "append")
            .queryName(f"kafka-{name}")
        )
        queries.append(writer.trigger(availableNow=True).start() if args.available_now else writer.start())
    return queries


def run(args: argparse.Namespace) -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    builder = (
        SparkSession.builder.appName(args.app_name)
        .master(args.master)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        baseline = spark.read.parquet(str(args.baseline_path))
        validated = parse_and_validate(build_source(spark, args))
        dlq = build_dlq(validated)
        events = valid_events(validated, args.watermark_delay)
        enriched = add_historical_baseline(events, baseline)
        state_updates = enriched.groupBy("station_id").transformWithState(
            statefulProcessor=LatestStationProcessor(args.dedup_ttl_seconds),
            outputStructType=CURRENT_STATE_OUTPUT_SCHEMA,
            outputMode="Update",
            timeMode="EventTime",
            eventTimeColumnName="event_time",
        )
        windows = window_metrics(events, args.window_duration)

        queries = (
            start_file_queries(dlq, state_updates, windows, args)
            if args.sink == "file"
            else start_kafka_queries(dlq, state_updates, windows, args)
        )
        for query in queries:
            query.awaitTermination()
            if args.progress_root:
                progress_to_file(query, args.progress_root / f"{query.name}.ndjson")
        if args.sink == "file":
            materialize_current_snapshot(
                spark,
                args.output_root / "state_updates",
                args.output_root / "current_state_snapshot",
            )
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Citi Bike complete station-state streaming job")
    parser.add_argument("--source", choices=("file", "kafka"), default="file")
    parser.add_argument("--sink", choices=("file", "kafka"), default="file")
    parser.add_argument("--input-dir", type=Path, default=ROOT / "artifacts/step8/runtime/input")
    parser.add_argument("--output-root", type=Path, default=ROOT / "artifacts/step8/runtime/output")
    parser.add_argument("--checkpoint-root", type=Path, default=ROOT / "artifacts/step8/runtime/checkpoints")
    parser.add_argument("--progress-root", type=Path, default=ROOT / "artifacts/step8/runtime/progress")
    parser.add_argument("--baseline-path", type=Path, default=ROOT / "data/gold/station_demand_baseline")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="citibike.station-status.v1")
    parser.add_argument("--invalid-topic", default="citibike.station-status.invalid.v1")
    parser.add_argument("--current-topic", default="citibike.station-current.v1")
    parser.add_argument("--window-topic", default="citibike.station-windows.v1")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--watermark-delay", default="10 minutes")
    parser.add_argument("--window-duration", default="5 minutes")
    parser.add_argument("--dedup-ttl-seconds", type=int, default=86_400)
    parser.add_argument("--shuffle-partitions", type=int, default=3)
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--app-name", default="citibike-station-state-v1")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
