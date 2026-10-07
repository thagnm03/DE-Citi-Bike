from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Iterator

from pyspark.sql import DataFrame, Row, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))

from spark.common.alert_rules import (
    BusinessRuleConfig,
    build_acknowledged_event,
    build_alert_event,
    classify_current_state,
    onset_trend_score,
    priority_for,
    sha256_parts,
    utc_text,
)
from spark.common.schemas import CURRENT_STATION_STATE_SCHEMA


SEEN_KEY_SCHEMA = StructType([StructField("event_id", StringType(), False)])
SEEN_VALUE_SCHEMA = StructType([StructField("seen_at_epoch_ms", LongType(), False)])
EPISODE_STATE_SCHEMA = StructType(
    [
        StructField("current_risk", StringType(), True),
        StructField("episode_started_ms", LongType(), True),
        StructField("alert_id", StringType(), True),
        StructField("alert_opened", BooleanType(), False),
        StructField("detected_ms", LongType(), True),
        StructField("last_event_ms", LongType(), False),
        StructField("last_event_id", StringType(), False),
        StructField("last_bikes", LongType(), False),
        StructField("last_docks", LongType(), False),
        StructField("episode_trend_score", DoubleType(), False),
        StructField("last_emitted_score", DoubleType(), True),
        StructField("last_emitted_ms", LongType(), True),
    ]
)
ALERT_OUTPUT_SCHEMA = StructType(
    [
        StructField("station_id", StringType(), False),
        StructField("alert_id", StringType(), False),
        StructField("alert_event_id", StringType(), False),
        StructField("event_time", TimestampType(), False),
        StructField("event_type", StringType(), False),
        StructField("lifecycle_status", StringType(), False),
        StructField("recommended_action", StringType(), False),
        StructField("priority_score", DoubleType(), False),
        StructField("alert_json", StringType(), False),
        StructField("input_partition", LongType(), False),
        StructField("input_offset", LongType(), False),
    ]
)


def parse_and_validate(raw: DataFrame) -> DataFrame:
    parsed = (
        raw.withColumn("json_is_parseable", F.get_json_object("raw_value", "$").isNotNull())
        .withColumn("current", F.from_json("raw_value", CURRENT_STATION_STATE_SCHEMA))
        .withColumn("event_time", F.to_timestamp("current.event_time_utc"))
    )
    errors = F.filter(
        F.array(
            F.when(~F.col("json_is_parseable"), F.lit("MALFORMED_CURRENT_STATE")),
            F.when(
                F.col("current.schema_version").isNull()
                | (F.col("current.schema_version") != "1.0"),
                F.lit("SCHEMA_VERSION_INVALID"),
            ),
            F.when(
                F.col("current.station_id").isNull()
                | (F.col("current.station_id") != F.col("current.station.station_id")),
                F.lit("STATION_ID_INVALID"),
            ),
            F.when(
                F.col("input_key").isNotNull()
                & (F.col("input_key") != F.col("current.station_id")),
                F.lit("KAFKA_KEY_STATION_MISMATCH"),
            ),
            F.when(
                F.col("current.event_id").isNull()
                | (~F.col("current.event_id").rlike("^[a-f0-9]{64}$")),
                F.lit("EVENT_ID_INVALID"),
            ),
            F.when(F.col("event_time").isNull(), F.lit("EVENT_TIME_INVALID")),
            F.when(
                F.col("current.availability").isNull()
                | F.col("current.availability.bikes_available").isNull()
                | F.col("current.availability.docks_available").isNull(),
                F.lit("AVAILABILITY_INVALID"),
            ),
            F.when(
                F.col("current.service").isNull()
                | F.col("current.service.is_installed").isNull()
                | F.col("current.service.is_renting").isNull()
                | F.col("current.service.is_returning").isNull(),
                F.lit("SERVICE_INVALID"),
            ),
            F.when(
                F.col("current.quality").isNull()
                | F.col("current.quality.timestamp_status").isNull()
                | F.col("current.quality.issue_codes").isNull(),
                F.lit("QUALITY_INVALID"),
            ),
            F.when(
                F.col("current.historical_demand").isNull()
                | F.col("current.historical_demand.found").isNull(),
                F.lit("BASELINE_CONTEXT_MISSING"),
            ),
        ),
        lambda code: code.isNotNull(),
    )
    parsed = parsed.withColumn("validation_errors", errors).withColumn(
        "validation_errors",
        F.when(
            ~F.col("json_is_parseable"), F.array(F.lit("MALFORMED_CURRENT_STATE"))
        ).otherwise(F.col("validation_errors")),
    )
    return parsed.withColumn("is_valid", F.size("validation_errors") == 0)


def build_dlq(validated: DataFrame) -> DataFrame:
    return validated.filter(~F.col("is_valid")).select(
        F.coalesce(F.col("input_key"), F.sha2("raw_value", 256)).alias("record_key"),
        "validation_errors",
        F.sha2("raw_value", 256).alias("raw_sha256"),
        F.substring("raw_value", 1, 1000).alias("raw_fragment"),
        "input_topic",
        "input_partition",
        "input_offset",
        F.current_timestamp().alias("detected_at_utc"),
    )


def valid_events(validated: DataFrame, watermark_delay: str) -> DataFrame:
    return (
        validated.filter("is_valid")
        .select(
            F.col("current.station_id").alias("station_id"),
            F.col("current.event_id").alias("event_id"),
            F.col("raw_value").alias("current_state_json"),
            "event_time",
            "input_partition",
            "input_offset",
        )
        .withWatermark("event_time", watermark_delay)
    )


class AlertEpisodeProcessor(StatefulProcessor):
    def __init__(self, config: BusinessRuleConfig) -> None:
        self.config = config

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.episode = handle.getValueState("episode", EPISODE_STATE_SCHEMA)
        self.seen = handle.getMapState("seen_events", SEEN_KEY_SCHEMA, SEEN_VALUE_SCHEMA)

    def _output(self, alert: dict[str, Any], row: Row) -> Row:
        return Row(
            station_id=alert["station"]["station_id"],
            alert_id=alert["alert_id"],
            alert_event_id=alert["alert_event_id"],
            event_time=row.event_time,
            event_type=alert["event_type"],
            lifecycle_status=alert["lifecycle_status"],
            recommended_action=alert["recommended_action"],
            priority_score=float(alert["priority"]["score"]),
            alert_json=json.dumps(alert, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
            input_partition=int(row.input_partition),
            input_offset=int(row.input_offset),
        )

    def handleInputRows(self, key, rows: Iterator[Row], timerValues) -> Iterator[Row]:
        station_id = key[0]
        ordered = sorted(rows, key=lambda row: (row.event_time, row.input_offset))
        if not ordered:
            return iter(())

        newest_ms = int(ordered[-1].event_time.timestamp() * 1000)
        cutoff_ms = newest_ms - self.config.dedup_ttl_seconds * 1000
        for seen_key, seen_value in list(self.seen.iterator()):
            if int(seen_value[0]) < cutoff_ms:
                self.seen.removeKey(seen_key)

        state = {
            "current_risk": None,
            "episode_started_ms": None,
            "alert_id": None,
            "alert_opened": False,
            "detected_ms": None,
            "last_event_ms": -1,
            "last_event_id": "",
            "last_bikes": 0,
            "last_docks": 0,
            "episode_trend_score": 0.0,
            "last_emitted_score": None,
            "last_emitted_ms": None,
        }
        if self.episode.exists():
            values = self.episode.get()
            state = dict(zip(state, values))

        output: list[Row] = []
        for row in ordered:
            event_ms = int(row.event_time.timestamp() * 1000)
            seen_key = (row.event_id,)
            if self.seen.containsKey(seen_key):
                continue
            self.seen.updateValue(seen_key, (event_ms,))
            if event_ms <= int(state["last_event_ms"]):
                continue

            payload = json.loads(row.current_state_json)
            risk = classify_current_state(payload, self.config)
            bikes = int(payload["availability"]["bikes_available"])
            docks = int(payload["availability"]["docks_available"])
            opened_now = False

            if risk != state["current_risk"]:
                if state["alert_opened"] and state["current_risk"] and state["alert_id"]:
                    resolved = build_alert_event(
                        payload=payload,
                        alert_id=state["alert_id"],
                        alert_type=state["current_risk"],
                        episode_started_ms=int(state["episode_started_ms"]),
                        detected_ms=int(state["detected_ms"]),
                        event_time_ms=event_ms,
                        event_type="ALERT_RESOLVED",
                        trend_score=float(state["episode_trend_score"]),
                        config=self.config,
                        input_partition=row.input_partition,
                        input_offset=row.input_offset,
                    )
                    output.append(self._output(resolved, row))

                if risk == "BALANCED":
                    state.update(
                        current_risk=None,
                        episode_started_ms=None,
                        alert_id=None,
                        alert_opened=False,
                        detected_ms=None,
                        episode_trend_score=0.0,
                        last_emitted_score=None,
                        last_emitted_ms=None,
                    )
                else:
                    state.update(
                        current_risk=risk,
                        episode_started_ms=event_ms,
                        alert_id=None,
                        alert_opened=False,
                        detected_ms=None,
                        episode_trend_score=onset_trend_score(
                            risk,
                            int(state["last_bikes"]) if state["last_event_ms"] >= 0 else None,
                            int(state["last_docks"]) if state["last_event_ms"] >= 0 else None,
                            bikes,
                            docks,
                        ),
                        last_emitted_score=None,
                        last_emitted_ms=None,
                    )

            if state["current_risk"] and state["episode_started_ms"] is not None:
                duration = (event_ms - int(state["episode_started_ms"])) // 1000
                if not state["alert_opened"] and duration >= self.config.persistence_seconds:
                    state["alert_id"] = sha256_parts(
                        "citibike_nyc", station_id, state["current_risk"], utc_text(int(state["episode_started_ms"]))
                    )
                    state["alert_opened"] = True
                    state["detected_ms"] = event_ms
                    opened = build_alert_event(
                        payload=payload,
                        alert_id=state["alert_id"],
                        alert_type=state["current_risk"],
                        episode_started_ms=int(state["episode_started_ms"]),
                        detected_ms=event_ms,
                        event_time_ms=event_ms,
                        event_type="ALERT_OPENED",
                        trend_score=float(state["episode_trend_score"]),
                        config=self.config,
                        input_partition=row.input_partition,
                        input_offset=row.input_offset,
                    )
                    output.append(self._output(opened, row))
                    state["last_emitted_score"] = float(opened["priority"]["score"])
                    state["last_emitted_ms"] = event_ms
                    opened_now = True

                if state["alert_opened"] and not opened_now:
                    _, _, _, _, score = priority_for(
                        state["current_risk"],
                        int(duration),
                        payload.get("historical_demand") or {},
                        float(state["episode_trend_score"]),
                        self.config,
                    )
                    score_changed = abs(score - float(state["last_emitted_score"])) >= self.config.priority_update_delta
                    update_due = (
                        event_ms - int(state["last_emitted_ms"])
                        >= self.config.priority_update_interval_seconds * 1000
                    )
                    if score_changed or update_due:
                        updated = build_alert_event(
                            payload=payload,
                            alert_id=state["alert_id"],
                            alert_type=state["current_risk"],
                            episode_started_ms=int(state["episode_started_ms"]),
                            detected_ms=int(state["detected_ms"]),
                            event_time_ms=event_ms,
                            event_type="ALERT_UPDATED",
                            trend_score=float(state["episode_trend_score"]),
                            config=self.config,
                            input_partition=row.input_partition,
                            input_offset=row.input_offset,
                        )
                        output.append(self._output(updated, row))
                        state["last_emitted_score"] = score
                        state["last_emitted_ms"] = event_ms

            state["last_event_ms"] = event_ms
            state["last_event_id"] = row.event_id
            state["last_bikes"] = bikes
            state["last_docks"] = docks

        self.episode.update(tuple(state.values()))
        return iter(output)

    def close(self) -> None:
        return None


def build_source(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    if args.source == "file":
        source = (
            spark.readStream.format("text")
            .option("maxFilesPerTrigger", 1)
            .load(str(args.input_dir))
        )
        return source.select(
            F.get_json_object("value", "$.station_id").alias("input_key"),
            F.col("value").alias("raw_value"),
            F.lit("citibike.station-current.v1").alias("input_topic"),
            F.pmod(F.xxhash64(F.get_json_object("value", "$.station_id")), F.lit(3)).cast("long").alias("input_partition"),
            F.abs(F.xxhash64("value")).cast("long").alias("input_offset"),
        )
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap_servers)
        .option("subscribe", args.input_topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
    )
    return source.select(
        F.col("key").cast("string").alias("input_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("topic").alias("input_topic"),
        F.col("partition").cast("long").alias("input_partition"),
        F.col("offset").cast("long").alias("input_offset"),
    )


def append_json_batch(frame: DataFrame, _batch_id: int, output_path: str) -> None:
    if not frame.rdd.isEmpty():
        frame.write.mode("append").json(output_path)


def start_queries(dlq: DataFrame, alerts: DataFrame, args: argparse.Namespace) -> list[Any]:
    if args.sink == "file":
        dlq_query = (
            dlq.writeStream.format("json")
            .option("path", str(args.output_root / "dead_letter"))
            .option("checkpointLocation", str(args.checkpoint_root / "dead_letter"))
            .outputMode("append")
            .queryName("alert-input-dead-letter")
            .trigger(availableNow=True)
            .start()
        )
        alert_query = (
            alerts.writeStream.foreachBatch(
                lambda frame, batch_id: append_json_batch(
                    frame, batch_id, str(args.output_root / "alert_events")
                )
            )
            .option("checkpointLocation", str(args.checkpoint_root / "alert_episodes"))
            .outputMode("update")
            .queryName("station-alert-lifecycle")
            .trigger(availableNow=True)
            .start()
        )
        return [dlq_query, alert_query]

    definitions = [
        (
            dlq.select(
                F.col("record_key").cast("string").alias("key"),
                F.to_json(F.struct("*")).alias("value"),
            ),
            args.invalid_topic,
            "alert_input_dead_letter",
            "append",
        ),
        (
            alerts.select(
                F.col("station_id").cast("string").alias("key"),
                F.col("alert_json").alias("value"),
            ),
            args.output_topic,
            "alert_episodes",
            "update",
        ),
    ]
    queries = []
    for frame, topic, checkpoint_name, mode in definitions:
        writer = (
            frame.writeStream.format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap_servers)
            .option("topic", topic)
            .option("checkpointLocation", str(args.checkpoint_root / checkpoint_name))
            .outputMode(mode)
            .queryName(f"kafka-{checkpoint_name}")
        )
        queries.append(writer.trigger(availableNow=True).start() if args.available_now else writer.start())
    return queries


def apply_acknowledgements(spark: SparkSession, events_path: Path, ack_file: Path) -> None:
    if not ack_file.exists() or not events_path.exists():
        return
    event_files = list(events_path.glob("part-*.json"))
    if not event_files:
        return
    rows = spark.read.json(str(events_path)).collect()
    existing_ids = {row.alert_event_id for row in rows}
    latest: dict[str, dict[str, Any]] = {}
    for row in sorted(rows, key=lambda item: (item.event_time, item.alert_event_id)):
        event = json.loads(row.alert_json)
        if event["lifecycle_status"] != "RESOLVED":
            latest[event["alert_id"]] = event
        else:
            latest.pop(event["alert_id"], None)

    ack_rows = []
    for line in ack_file.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        command = json.loads(line)
        expected_event_id = sha256_parts(
            command["alert_id"], "ALERT_ACKNOWLEDGED", command["acknowledged_at_utc"]
        )
        if expected_event_id in existing_ids:
            continue
        active = latest.get(command["alert_id"])
        if not active:
            raise ValueError(f"Cannot acknowledge inactive alert {command['alert_id']}")
        event = build_acknowledged_event(active, command["acknowledged_at_utc"])
        if event["alert_event_id"] in existing_ids:
            continue
        timestamp = datetime.fromisoformat(command["acknowledged_at_utc"].replace("Z", "+00:00")).replace(tzinfo=None)
        ack_rows.append(
            Row(
                station_id=event["station"]["station_id"],
                alert_id=event["alert_id"],
                alert_event_id=event["alert_event_id"],
                event_time=timestamp,
                event_type=event["event_type"],
                lifecycle_status=event["lifecycle_status"],
                recommended_action=event["recommended_action"],
                priority_score=float(event["priority"]["score"]),
                alert_json=json.dumps(event, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                input_partition=int(event["lineage"]["input_partition"]),
                input_offset=int(event["lineage"]["input_offset"]),
            )
        )
    if ack_rows:
        spark.createDataFrame(ack_rows, ALERT_OUTPUT_SCHEMA).write.mode("append").json(str(events_path))


def materialize_priority_queue(spark: SparkSession, events_path: Path, queue_path: Path) -> None:
    if not events_path.exists() or not list(events_path.glob("part-*.json")):
        return
    events = spark.read.json(str(events_path)).withColumn("event_time", F.to_timestamp("event_time"))
    latest = (
        events.withColumn(
            "_latest",
            F.row_number().over(
                Window.partitionBy("alert_id").orderBy(
                    F.col("event_time").desc(), F.col("alert_event_id").desc()
                )
            ),
        )
        .filter("_latest = 1 AND lifecycle_status <> 'RESOLVED'")
        .drop("_latest")
        .withColumn(
            "queue_rank",
            F.row_number().over(
                Window.orderBy(F.col("priority_score").desc(), F.col("event_time").asc())
            ),
        )
    )
    latest.write.mode("overwrite").json(str(queue_path))


def run(args: argparse.Namespace) -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    config = BusinessRuleConfig.from_json(args.rules)
    spark = (
        SparkSession.builder.appName(args.app_name)
        .master(args.master)
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.streaming.stateStore.providerClass",
            "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider",
        )
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        validated = parse_and_validate(build_source(spark, args))
        events = valid_events(validated, config.watermark_delay)
        alerts = events.groupBy("station_id").transformWithState(
            statefulProcessor=AlertEpisodeProcessor(config),
            outputStructType=ALERT_OUTPUT_SCHEMA,
            outputMode="Update",
            timeMode="EventTime",
            eventTimeColumnName="event_time",
        )
        queries = start_queries(build_dlq(validated), alerts, args)
        for query in queries:
            query.awaitTermination()
        if args.sink == "file":
            if args.ack_file:
                apply_acknowledgements(spark, args.output_root / "alert_events", args.ack_file)
            materialize_priority_queue(
                spark, args.output_root / "alert_events", args.output_root / "priority_queue"
            )
    finally:
        spark.stop()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Citi Bike alert and priority decision engine")
    parser.add_argument("--source", choices=("file", "kafka"), default="file")
    parser.add_argument("--sink", choices=("file", "kafka"), default="file")
    parser.add_argument("--input-dir", type=Path, default=ROOT / "artifacts/step9/runtime/input")
    parser.add_argument("--output-root", type=Path, default=ROOT / "artifacts/step9/runtime/output")
    parser.add_argument("--checkpoint-root", type=Path, default=ROOT / "artifacts/step9/runtime/checkpoints")
    parser.add_argument("--rules", type=Path, default=ROOT / "config/business-rules-v1.json")
    parser.add_argument("--ack-file", type=Path)
    parser.add_argument("--master", default="local[2]")
    parser.add_argument("--bootstrap-servers", default="localhost:9092")
    parser.add_argument("--input-topic", default="citibike.station-current.v1")
    parser.add_argument("--output-topic", default="citibike.station-alerts.v1")
    parser.add_argument("--invalid-topic", default="citibike.station-alerts.invalid.v1")
    parser.add_argument("--starting-offsets", default="latest")
    parser.add_argument("--available-now", action="store_true")
    parser.add_argument("--shuffle-partitions", type=int, default=3)
    parser.add_argument("--app-name", default="citibike-station-alerts-v1")
    return parser.parse_args()


if __name__ == "__main__":
    run(parse_args())
