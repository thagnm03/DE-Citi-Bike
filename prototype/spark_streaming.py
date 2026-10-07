from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Iterator

from pyspark.sql import Row, SparkSession, functions as F
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

from .rules import EpisodeEngine, RuleConfig, StationState


EPISODE_STATE_SCHEMA = StructType(
    [
        StructField("current_risk", StringType(), True),
        StructField("episode_started_at_utc", StringType(), True),
        StructField("alert_id", StringType(), True),
        StructField("alert_opened", BooleanType(), False),
        StructField("detected_at_utc", StringType(), True),
        StructField("last_event_time_utc", StringType(), True),
    ]
)

SEEN_KEY_SCHEMA = StructType([StructField("event_id", StringType(), False)])
SEEN_VALUE_SCHEMA = StructType([StructField("seen_at_epoch_ms", LongType(), False)])
OUTPUT_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("alert_event_id", StringType(), False),
        StructField("alert_json", StringType(), False),
        StructField("event_time", TimestampType(), False),
    ]
)


class StationEpisodeProcessor(StatefulProcessor):
    """Spark-managed per-station episode state and event-id dedup map."""

    def __init__(self, config: RuleConfig) -> None:
        self.config = config

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.episode_state = handle.getValueState("episode", EPISODE_STATE_SCHEMA)
        self.seen_events = handle.getMapState(
            "seen_events",
            SEEN_KEY_SCHEMA,
            SEEN_VALUE_SCHEMA,
        )

    def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
        station_id = key[0]
        ordered_rows = sorted(rows, key=lambda row: row.event_time)
        engine = EpisodeEngine(self.config)

        if ordered_rows:
            newest_epoch_ms = int(ordered_rows[-1].event_time.timestamp() * 1000)
            cutoff_epoch_ms = newest_epoch_ms - self.config.dedup_ttl_seconds * 1000
            expired_keys = [
                seen_key
                for seen_key, seen_value in self.seen_events.iterator()
                if int(seen_value[0]) < cutoff_epoch_ms
            ]
            for expired_key in expired_keys:
                self.seen_events.removeKey(expired_key)

        if self.episode_state.exists():
            state_tuple = self.episode_state.get()
            engine.states[station_id] = StationState(*state_tuple)

        output: list[Row] = []
        for row in ordered_rows:
            seen_key = (row.event_id,)
            if self.seen_events.containsKey(seen_key):
                continue
            self.seen_events.updateValue(seen_key, (int(row.event_time.timestamp() * 1000),))
            observation = json.loads(row.observation_json)
            alerts = engine.process(
                observation,
                input_partition=int(row.input_partition),
                input_offset=int(row.input_offset),
            )
            for alert in alerts:
                output.append(
                    Row(
                        station_id=station_id,
                        event_type=alert["event_type"],
                        alert_event_id=alert["alert_event_id"],
                        alert_json=json.dumps(alert, ensure_ascii=False, sort_keys=True),
                        event_time=row.event_time,
                    )
                )

        state = engine.states.get(station_id)
        if state:
            self.episode_state.update(
                (
                    state.current_risk,
                    state.episode_started_at_utc,
                    state.alert_id,
                    state.alert_opened,
                    state.detected_at_utc,
                    state.last_event_time_utc,
                )
            )
        return iter(output)

    def close(self) -> None:
        pass


def prepare_file_input(source_ndjson: Path, input_dir: Path) -> None:
    input_dir.mkdir(parents=True, exist_ok=True)
    existing = list(input_dir.glob("batch-*.json"))
    if existing:
        raise FileExistsError(f"Input directory is not empty: {input_dir}")
    lines = [line for line in source_ndjson.read_text(encoding="utf-8").splitlines() if line.strip()]
    for index, line in enumerate(lines):
        (input_dir / f"batch-{index:04d}.json").write_text(line + "\n", encoding="utf-8")


def build_source(spark: SparkSession, args: argparse.Namespace):
    if args.source == "file":
        raw = spark.readStream.format("text").option("maxFilesPerTrigger", 1).load(str(args.input_dir))
        partition_expr = F.pmod(F.xxhash64(F.get_json_object("value", "$.station.station_id")), F.lit(3))
        offset_expr = F.abs(F.xxhash64(F.get_json_object("value", "$.event_id")))
        return raw.select(
            F.col("value").alias("observation_json"),
            F.get_json_object("value", "$.event_id").alias("event_id"),
            F.get_json_object("value", "$.station.station_id").alias("station_id"),
            F.to_timestamp(F.get_json_object("value", "$.time.snapshot_updated_at_utc")).alias("event_time"),
            partition_expr.cast("int").alias("input_partition"),
            offset_expr.cast("long").alias("input_offset"),
        )

    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )
    return raw.select(
        F.col("value").cast("string").alias("observation_json"),
        F.get_json_object(F.col("value").cast("string"), "$.event_id").alias("event_id"),
        F.col("key").cast("string").alias("station_id"),
        F.to_timestamp(
            F.get_json_object(F.col("value").cast("string"), "$.time.snapshot_updated_at_utc")
        ).alias("event_time"),
        F.col("partition").cast("int").alias("input_partition"),
        F.col("offset").cast("long").alias("input_offset"),
    )


def append_file_batch(batch_df, _batch_id: int, output_file: str) -> None:
    rows = batch_df.orderBy("station_id", "alert_event_id").select("alert_json").collect()
    if not rows:
        return
    output_path = Path(output_file)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(row.alert_json + "\n")


def append_progress(query, progress_file: Path | None) -> None:
    if progress_file is None:
        return
    progress_file.parent.mkdir(parents=True, exist_ok=True)
    with progress_file.open("a", encoding="utf-8") as handle:
        for progress in query.recentProgress:
            handle.write(json.dumps(dict(progress), ensure_ascii=False, default=str) + "\n")


def run(args: argparse.Namespace) -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    config = RuleConfig.from_json(args.rules)
    builder = (
        SparkSession.builder.appName(args.app_name)
        .master(args.master)
        .config("spark.sql.shuffle.partitions", "3")
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
    )
    if args.source == "kafka" or args.sink == "kafka":
        builder = builder.config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:4.2.0",
        ).config("spark.jars.ivy", args.ivy_dir)
    if os.name == "nt":
        builder = builder.config(
            "spark.sql.streaming.checkpointFileManagerClass",
            "org.apache.spark.sql.execution.streaming.checkpointing.FileSystemBasedCheckpointFileManager",
        )
    spark = builder.getOrCreate()
    if os.name == "nt":
        spark.sparkContext._jsc.hadoopConfiguration().set(
            "fs.file.impl", "com.globalmentor.apache.hadoop.fs.BareLocalFileSystem"
        )
    spark.sparkContext.setLogLevel("WARN")

    source = build_source(spark, args).filter(
        F.col("station_id").isNotNull() & F.col("event_id").isNotNull() & F.col("event_time").isNotNull()
    ).withWatermark("event_time", config.watermark_delay)
    alerts = source.groupBy("station_id").transformWithState(
        statefulProcessor=StationEpisodeProcessor(config),
        outputStructType=OUTPUT_SCHEMA,
        outputMode="Update",
        timeMode="EventTime",
        eventTimeColumnName="event_time",
    )

    if args.sink == "kafka":
        writer = (
            alerts.select(F.col("station_id").cast("string").alias("key"), F.col("alert_json").alias("value"))
            .writeStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", args.output_topic)
            .option("checkpointLocation", str(args.checkpoint_dir))
            .outputMode("update")
        )
        query = writer.trigger(availableNow=True).start() if args.available_now else writer.start()
    else:
        writer = (
            alerts.writeStream.foreachBatch(
                lambda frame, batch_id: append_file_batch(frame, batch_id, str(args.output_file))
            )
            .option("checkpointLocation", str(args.checkpoint_dir))
            .outputMode("update")
        )
        query = writer.trigger(availableNow=True).start() if args.available_now or args.source == "file" else writer.start()

    query.awaitTermination()
    append_progress(query, args.progress_file)
    spark.stop()


def parse_args() -> argparse.Namespace:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description="Citi Bike Step 4 Spark Structured Streaming prototype")
    parser.add_argument("--source", choices=("file", "kafka"), default="file")
    parser.add_argument("--sink", choices=("file", "kafka"), default="file")
    parser.add_argument("--input-dir", type=Path, default=root / "artifacts/step4/spark-input")
    parser.add_argument("--output-file", type=Path, default=root / "artifacts/step4/spark-alerts.ndjson")
    parser.add_argument("--checkpoint-dir", type=Path, default=root / "artifacts/step4/spark-checkpoint")
    parser.add_argument("--rules", type=Path, default=root / "config/prototype-rules.json")
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="citibike.station-status.v1")
    parser.add_argument("--output-topic", default="citibike.station-alerts.v1")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--ivy-dir", default="/tmp/.ivy2")
    parser.add_argument("--app-name", default="citibike-step4-station-alerts")
    parser.add_argument("--progress-file", type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
