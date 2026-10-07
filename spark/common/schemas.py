from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)


STATION_STATUS_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("source_system", StringType(), True),
        StructField(
            "station",
            StructType(
                [
                    StructField("station_id", StringType(), True),
                    StructField("short_name", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                    StructField("capacity", LongType(), True),
                    StructField("metadata_match_status", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "availability",
            StructType(
                [
                    StructField("bikes_available", LongType(), True),
                    StructField("bikes_disabled", LongType(), True),
                    StructField("docks_available", LongType(), True),
                    StructField("docks_disabled", LongType(), True),
                    StructField("ebikes_available", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "service",
            StructType(
                [
                    StructField("is_installed", BooleanType(), True),
                    StructField("is_renting", BooleanType(), True),
                    StructField("is_returning", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "time",
            StructType(
                [
                    StructField("snapshot_updated_at_utc", StringType(), True),
                    StructField("station_reported_at_raw", LongType(), True),
                    StructField("station_reported_at_utc", StringType(), True),
                    StructField("observed_at_utc", StringType(), True),
                    StructField("normalized_at_utc", StringType(), True),
                    StructField("source_age_seconds", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "source",
            StructType(
                [
                    StructField("feed_name", StringType(), True),
                    StructField("feed_version", StringType(), True),
                    StructField("feed_ttl_seconds", LongType(), True),
                    StructField("snapshot_sha256", StringType(), True),
                    StructField("raw_archive_path", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "quality",
            StructType(
                [
                    StructField("timestamp_status", StringType(), True),
                    StructField("capacity_consistency", StringType(), True),
                    StructField("issue_codes", ArrayType(StringType()), True),
                ]
            ),
            True,
        ),
    ]
)


CURRENT_STATION_STATE_SCHEMA = StructType(
    [
        StructField("schema_version", StringType(), True),
        StructField("station_id", StringType(), True),
        StructField("event_id", StringType(), True),
        StructField("event_time_utc", StringType(), True),
        StructField(
            "station",
            StructType(
                [
                    StructField("station_id", StringType(), True),
                    StructField("short_name", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("latitude", DoubleType(), True),
                    StructField("longitude", DoubleType(), True),
                    StructField("capacity", LongType(), True),
                    StructField("metadata_match_status", StringType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "availability",
            StructType(
                [
                    StructField("bikes_available", LongType(), True),
                    StructField("bikes_disabled", LongType(), True),
                    StructField("docks_available", LongType(), True),
                    StructField("docks_disabled", LongType(), True),
                    StructField("ebikes_available", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "service",
            StructType(
                [
                    StructField("is_installed", BooleanType(), True),
                    StructField("is_renting", BooleanType(), True),
                    StructField("is_returning", BooleanType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "quality",
            StructType(
                [
                    StructField("timestamp_status", StringType(), True),
                    StructField("capacity_consistency", StringType(), True),
                    StructField("issue_codes", ArrayType(StringType()), True),
                ]
            ),
            True,
        ),
        StructField("source_age_seconds", LongType(), True),
        StructField(
            "historical_demand",
            StructType(
                [
                    StructField("found", BooleanType(), True),
                    StructField("iso_weekday", LongType(), True),
                    StructField("local_hour", LongType(), True),
                    StructField("calendar_days", LongType(), True),
                    StructField("avg_pickups", DoubleType(), True),
                    StructField("p50_pickups", LongType(), True),
                    StructField("p95_pickups", LongType(), True),
                    StructField("avg_dropoffs", DoubleType(), True),
                    StructField("p50_dropoffs", LongType(), True),
                    StructField("p95_dropoffs", LongType(), True),
                ]
            ),
            True,
        ),
        StructField(
            "lineage",
            StructType(
                [
                    StructField("input_partition", LongType(), True),
                    StructField("input_offset", LongType(), True),
                ]
            ),
            True,
        ),
    ]
)
