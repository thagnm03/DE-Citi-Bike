#!/usr/bin/env python3
"""Profile the Step 2 Citi Bike GBFS snapshots and one monthly trip archive.

The script intentionally uses only the Python standard library so the audit is
reproducible before the project runtime dependencies are selected.
"""

from __future__ import annotations

import csv
import io
import json
import math
import os
import statistics
import sys
import zipfile
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
GBFS_DIR = ROOT / "data" / "samples" / "gbfs"
TRIP_ZIP = ROOT / "data" / "samples" / "trips" / "202604-citibike-tripdata.zip"
TRIP_SAMPLE = ROOT / "data" / "samples" / "trips" / "202604-trip-sample-500.csv"
OUTPUT = ROOT / "data" / "profile-step2.json"


def read_json(path: Path) -> dict[str, Any]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def epoch_iso(value: int | float | None) -> str | None:
    if value is None:
        return None
    return datetime.fromtimestamp(value, tz=timezone.utc).isoformat()


def file_mtime_iso(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()


def quantiles_from_sample(values: list[float]) -> dict[str, float] | None:
    if not values:
        return None
    ordered = sorted(values)

    def percentile(p: float) -> float:
        position = (len(ordered) - 1) * p
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return ordered[lower]
        return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)

    return {
        "p01": round(percentile(0.01), 3),
        "p50": round(percentile(0.50), 3),
        "p95": round(percentile(0.95), 3),
        "p99": round(percentile(0.99), 3),
    }


def profile_gbfs() -> tuple[dict[str, Any], set[str], set[str], set[str]]:
    system = read_json(GBFS_DIR / "system_information.json")
    info = read_json(GBFS_DIR / "station_information.json")
    status_paths = sorted(GBFS_DIR.glob("station_status_*.json"))
    snapshots = [read_json(path) for path in status_paths]

    info_rows = info["data"]["stations"]
    station_ids = [str(row.get("station_id", "")) for row in info_rows]
    short_names = [str(row.get("short_name", "")) for row in info_rows]
    station_names = [str(row.get("name", "")) for row in info_rows]
    capacities = [row.get("capacity") for row in info_rows if isinstance(row.get("capacity"), int)]
    station_id_set = {value for value in station_ids if value}
    short_name_set = {value for value in short_names if value}
    station_name_set = {value for value in station_names if value}

    info_profile = {
        "file": str((GBFS_DIR / "station_information.json").relative_to(ROOT)),
        "retrieved_at_utc_from_file_mtime": file_mtime_iso(GBFS_DIR / "station_information.json"),
        "feed_last_updated": info.get("last_updated"),
        "feed_last_updated_utc": epoch_iso(info.get("last_updated")),
        "ttl_seconds": info.get("ttl"),
        "version": info.get("version"),
        "station_count": len(info_rows),
        "unique_station_ids": len(station_id_set),
        "duplicate_station_id_rows": len(station_ids) - len(station_id_set),
        "missing_station_id_rows": sum(not value for value in station_ids),
        "unique_nonblank_short_names": len(short_name_set),
        "duplicate_nonblank_short_name_rows": sum(short_names.count(value) - 1 for value in short_name_set),
        "missing_short_name_rows": sum(not value for value in short_names),
        "missing_name_rows": sum(not row.get("name") for row in info_rows),
        "unique_nonblank_station_names": len(station_name_set),
        "duplicate_nonblank_station_name_rows": sum(
            station_names.count(value) - 1 for value in station_name_set
        ),
        "missing_coordinate_rows": sum(row.get("lat") is None or row.get("lon") is None for row in info_rows),
        "missing_capacity_rows": sum(row.get("capacity") is None for row in info_rows),
        "nonpositive_capacity_rows": sum(
            isinstance(row.get("capacity"), (int, float)) and row.get("capacity", 0) <= 0
            for row in info_rows
        ),
        "capacity_min": min(capacities) if capacities else None,
        "capacity_median": statistics.median(capacities) if capacities else None,
        "capacity_max": max(capacities) if capacities else None,
        "station_types": dict(Counter(str(row.get("station_type")) for row in info_rows)),
    }

    info_by_id = {str(row.get("station_id")): row for row in info_rows if row.get("station_id")}
    snapshot_profiles: list[dict[str, Any]] = []
    snapshot_maps: list[dict[str, dict[str, Any]]] = []
    for path, snapshot in zip(status_paths, snapshots):
        rows = snapshot["data"]["stations"]
        mapping = {str(row.get("station_id")): row for row in rows if row.get("station_id")}
        snapshot_maps.append(mapping)
        wrapper_time = snapshot.get("last_updated")
        ages = [
            wrapper_time - row["last_reported"]
            for row in rows
            if isinstance(wrapper_time, (int, float)) and isinstance(row.get("last_reported"), (int, float))
        ]
        inconsistent_capacity = 0
        exact_capacity = 0
        capacity_delta = Counter()
        for station_id, row in mapping.items():
            capacity = info_by_id.get(station_id, {}).get("capacity")
            components = [
                row.get("num_bikes_available"),
                row.get("num_bikes_disabled"),
                row.get("num_docks_available"),
                row.get("num_docks_disabled"),
            ]
            if isinstance(capacity, int) and all(isinstance(value, int) for value in components):
                delta = sum(components) - capacity
                capacity_delta[str(delta)] += 1
                if delta == 0:
                    exact_capacity += 1
                else:
                    inconsistent_capacity += 1

        ids = [str(row.get("station_id", "")) for row in rows]
        snapshot_profiles.append(
            {
                "file": str(path.relative_to(ROOT)),
                "retrieved_at_utc_from_file_mtime": file_mtime_iso(path),
                "feed_last_updated": wrapper_time,
                "feed_last_updated_utc": epoch_iso(wrapper_time),
                "ttl_seconds": snapshot.get("ttl"),
                "version": snapshot.get("version"),
                "station_count": len(rows),
                "unique_station_ids": len(set(ids)),
                "duplicate_station_id_rows": len(ids) - len(set(ids)),
                "ids_missing_from_station_information": len(set(ids) - station_id_set),
                "station_information_ids_missing_from_status": len(station_id_set - set(ids)),
                "empty_bike_rows": sum(row.get("num_bikes_available") == 0 for row in rows),
                "full_rows_by_zero_available_docks": sum(row.get("num_docks_available") == 0 for row in rows),
                "offline_rows": sum(
                    row.get("is_installed") != 1
                    or row.get("is_renting") != 1
                    or row.get("is_returning") != 1
                    for row in rows
                ),
                "last_reported_epoch_86400_rows": sum(row.get("last_reported") == 86400 for row in rows),
                "source_age_seconds_min": min(ages) if ages else None,
                "source_age_seconds_median": statistics.median(ages) if ages else None,
                "source_age_seconds_max": max(ages) if ages else None,
                "source_age_over_300_seconds_rows": sum(age > 300 for age in ages),
                "source_age_over_3600_seconds_rows": sum(age > 3600 for age in ages),
                "capacity_component_exact_match_rows": exact_capacity,
                "capacity_component_mismatch_rows": inconsistent_capacity,
                "capacity_component_delta_top": capacity_delta.most_common(10),
            }
        )

    changes: list[dict[str, Any]] = []
    compare_fields = (
        "num_bikes_available",
        "num_bikes_disabled",
        "num_docks_available",
        "num_docks_disabled",
        "is_installed",
        "is_renting",
        "is_returning",
        "last_reported",
    )
    for index in range(1, len(snapshot_maps)):
        previous = snapshot_maps[index - 1]
        current = snapshot_maps[index]
        common = set(previous) & set(current)
        changes.append(
            {
                "from": status_paths[index - 1].name,
                "to": status_paths[index].name,
                "feed_last_updated_delta_seconds": snapshots[index].get("last_updated", 0)
                - snapshots[index - 1].get("last_updated", 0),
                "common_stations": len(common),
                "stations_with_any_compared_field_changed": sum(
                    any(previous[key].get(field) != current[key].get(field) for field in compare_fields)
                    for key in common
                ),
                "stations_with_inventory_changed": sum(
                    previous[key].get("num_bikes_available") != current[key].get("num_bikes_available")
                    or previous[key].get("num_docks_available") != current[key].get("num_docks_available")
                    for key in common
                ),
                "stations_with_same_last_reported": sum(
                    previous[key].get("last_reported") == current[key].get("last_reported") for key in common
                ),
            }
        )

    return (
        {
            "system_information": {
                **system["data"],
                "feed_last_updated": system.get("last_updated"),
                "feed_last_updated_utc": epoch_iso(system.get("last_updated")),
                "ttl_seconds": system.get("ttl"),
                "version": system.get("version"),
            },
            "station_information": info_profile,
            "station_status_snapshots": snapshot_profiles,
            "snapshot_change_summary": changes,
        },
        station_id_set,
        short_name_set,
        station_name_set,
    )


def parse_datetime(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def parse_float(value: str) -> float | None:
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def profile_trips(current_short_names: set[str], current_station_names: set[str]) -> dict[str, Any]:
    if not TRIP_ZIP.exists():
        raise FileNotFoundError(TRIP_ZIP)

    total_rows = 0
    entry_rows: dict[str, int] = {}
    headers_by_entry: dict[str, list[str]] = {}
    null_counts: Counter[str] = Counter()
    rideable_types: Counter[str] = Counter()
    member_types: Counter[str] = Counter()
    start_ids: set[str] = set()
    end_ids: set[str] = set()
    start_names: set[str] = set()
    end_names: set[str] = set()
    ride_id_fingerprints: set[int] = set()
    duplicate_ride_ids = 0
    nonhex_ride_ids = 0
    timestamp_parse_errors = 0
    duration_negative = 0
    duration_zero = 0
    duration_under_60 = 0
    duration_over_24h = 0
    duration_sum = 0.0
    duration_count = 0
    duration_min: float | None = None
    duration_max: float | None = None
    duration_sample: list[float] = []
    start_min: datetime | None = None
    start_max: datetime | None = None
    end_min: datetime | None = None
    end_max: datetime | None = None
    outside_expected_month = 0
    invalid_coordinate_rows = 0
    start_current_short_name_match_rows = 0
    end_current_short_name_match_rows = 0
    start_unmatched_id_but_current_name_match_rows = 0
    end_unmatched_id_but_current_name_match_rows = 0
    unmatched_start_pairs: Counter[tuple[str, str]] = Counter()
    unmatched_end_pairs: Counter[tuple[str, str]] = Counter()
    sample_rows: list[dict[str, str]] = []

    with zipfile.ZipFile(TRIP_ZIP) as archive:
        csv_entries = [item for item in archive.infolist() if item.filename.lower().endswith(".csv")]
        for item in csv_entries:
            count = 0
            with archive.open(item) as binary_handle:
                with io.TextIOWrapper(binary_handle, encoding="utf-8-sig", newline="") as text_handle:
                    reader = csv.DictReader(text_handle)
                    headers = reader.fieldnames or []
                    headers_by_entry[item.filename] = headers
                    for row in reader:
                        total_rows += 1
                        count += 1
                        if len(sample_rows) < 500:
                            sample_rows.append(dict(row))

                        for field in headers:
                            if row.get(field, "") == "":
                                null_counts[field] += 1

                        ride_id = row.get("ride_id", "")
                        if ride_id:
                            try:
                                fingerprint = int(ride_id, 16)
                            except ValueError:
                                nonhex_ride_ids += 1
                                fingerprint = hash(ride_id)
                            if fingerprint in ride_id_fingerprints:
                                duplicate_ride_ids += 1
                            else:
                                ride_id_fingerprints.add(fingerprint)

                        rideable_types[row.get("rideable_type", "")] += 1
                        member_types[row.get("member_casual", "")] += 1

                        start_id = row.get("start_station_id", "")
                        end_id = row.get("end_station_id", "")
                        start_name = row.get("start_station_name", "")
                        end_name = row.get("end_station_name", "")
                        if start_id:
                            start_ids.add(start_id)
                            start_current_short_name_match_rows += start_id in current_short_names
                            if start_id not in current_short_names:
                                unmatched_start_pairs[(start_id, start_name)] += 1
                                start_unmatched_id_but_current_name_match_rows += start_name in current_station_names
                        if end_id:
                            end_ids.add(end_id)
                            end_current_short_name_match_rows += end_id in current_short_names
                            if end_id not in current_short_names:
                                unmatched_end_pairs[(end_id, end_name)] += 1
                                end_unmatched_id_but_current_name_match_rows += end_name in current_station_names
                        if start_name:
                            start_names.add(start_name)
                        if end_name:
                            end_names.add(end_name)

                        started_at = parse_datetime(row.get("started_at", ""))
                        ended_at = parse_datetime(row.get("ended_at", ""))
                        if started_at is None or ended_at is None:
                            timestamp_parse_errors += 1
                        else:
                            start_min = started_at if start_min is None or started_at < start_min else start_min
                            start_max = started_at if start_max is None or started_at > start_max else start_max
                            end_min = ended_at if end_min is None or ended_at < end_min else end_min
                            end_max = ended_at if end_max is None or ended_at > end_max else end_max
                            outside_expected_month += not (started_at.year == 2026 and started_at.month == 4)
                            duration = (ended_at - started_at).total_seconds()
                            duration_count += 1
                            duration_sum += duration
                            duration_min = duration if duration_min is None or duration < duration_min else duration_min
                            duration_max = duration if duration_max is None or duration > duration_max else duration_max
                            duration_negative += duration < 0
                            duration_zero += duration == 0
                            duration_under_60 += 0 <= duration < 60
                            duration_over_24h += duration > 86400
                            if total_rows % 1000 == 0:
                                duration_sample.append(duration)

                        coordinates = [
                            parse_float(row.get("start_lat", "")),
                            parse_float(row.get("start_lng", "")),
                            parse_float(row.get("end_lat", "")),
                            parse_float(row.get("end_lng", "")),
                        ]
                        if (
                            any(value is None for value in coordinates)
                            or not -90 <= coordinates[0] <= 90
                            or not -180 <= coordinates[1] <= 180
                            or not -90 <= coordinates[2] <= 90
                            or not -180 <= coordinates[3] <= 180
                        ):
                            invalid_coordinate_rows += 1
            entry_rows[item.filename] = count

    TRIP_SAMPLE.parent.mkdir(parents=True, exist_ok=True)
    if sample_rows:
        with TRIP_SAMPLE.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(sample_rows[0].keys()))
            writer.writeheader()
            writer.writerows(sample_rows)

    headers_consistent = len({tuple(value) for value in headers_by_entry.values()}) == 1
    all_headers = next(iter(headers_by_entry.values()), [])
    return {
        "archive": str(TRIP_ZIP.relative_to(ROOT)),
        "archive_size_bytes": TRIP_ZIP.stat().st_size,
        "archive_mtime_utc": file_mtime_iso(TRIP_ZIP),
        "csv_entry_count": len(entry_rows),
        "entry_rows": entry_rows,
        "headers_consistent_across_entries": headers_consistent,
        "columns": all_headers,
        "row_count": total_rows,
        "null_counts": dict(null_counts),
        "null_percentages": {
            field: round(null_counts[field] * 100 / total_rows, 6) if total_rows else None
            for field in all_headers
        },
        "unique_ride_id_fingerprints": len(ride_id_fingerprints),
        "duplicate_ride_id_rows": duplicate_ride_ids,
        "nonhex_ride_id_rows": nonhex_ride_ids,
        "rideable_types": dict(rideable_types),
        "member_types": dict(member_types),
        "timestamp_parse_error_rows": timestamp_parse_errors,
        "started_at_min": start_min.isoformat(sep=" ") if start_min else None,
        "started_at_max": start_max.isoformat(sep=" ") if start_max else None,
        "ended_at_min": end_min.isoformat(sep=" ") if end_min else None,
        "ended_at_max": end_max.isoformat(sep=" ") if end_max else None,
        "started_at_outside_2026_04_rows": outside_expected_month,
        "duration_seconds_min": duration_min,
        "duration_seconds_mean": round(duration_sum / duration_count, 3) if duration_count else None,
        "duration_seconds_max": duration_max,
        "duration_seconds_approx_quantiles_from_every_1000th_row": quantiles_from_sample(duration_sample),
        "duration_negative_rows": duration_negative,
        "duration_zero_rows": duration_zero,
        "duration_under_60_nonnegative_rows": duration_under_60,
        "duration_over_24h_rows": duration_over_24h,
        "invalid_or_missing_coordinate_rows": invalid_coordinate_rows,
        "unique_nonblank_start_station_ids": len(start_ids),
        "unique_nonblank_end_station_ids": len(end_ids),
        "unique_nonblank_start_station_names": len(start_names),
        "unique_nonblank_end_station_names": len(end_names),
        "rows_with_nonblank_start_station_id": total_rows - null_counts["start_station_id"],
        "rows_with_nonblank_end_station_id": total_rows - null_counts["end_station_id"],
        "start_station_ids_matching_current_gbfs_short_name": len(start_ids & current_short_names),
        "end_station_ids_matching_current_gbfs_short_name": len(end_ids & current_short_names),
        "start_station_ids_not_matching_current_gbfs_short_name": len(start_ids - current_short_names),
        "end_station_ids_not_matching_current_gbfs_short_name": len(end_ids - current_short_names),
        "rows_with_start_station_id_matching_current_gbfs_short_name": start_current_short_name_match_rows,
        "rows_with_end_station_id_matching_current_gbfs_short_name": end_current_short_name_match_rows,
        "rows_with_unmatched_start_id_but_current_station_name_match": start_unmatched_id_but_current_name_match_rows,
        "rows_with_unmatched_end_id_but_current_station_name_match": end_unmatched_id_but_current_name_match_rows,
        "top_unmatched_start_id_name_pairs": [
            {"station_id": pair[0], "station_name": pair[1], "rows": count}
            for pair, count in unmatched_start_pairs.most_common(20)
        ],
        "top_unmatched_end_id_name_pairs": [
            {"station_id": pair[0], "station_name": pair[1], "rows": count}
            for pair, count in unmatched_end_pairs.most_common(20)
        ],
        "sample_csv": str(TRIP_SAMPLE.relative_to(ROOT)),
        "sample_csv_rows": len(sample_rows),
    }


def main() -> int:
    if not GBFS_DIR.exists():
        print(f"Missing GBFS sample directory: {GBFS_DIR}", file=sys.stderr)
        return 1
    gbfs_profile, _, current_short_names, current_station_names = profile_gbfs()
    trip_profile = profile_trips(current_short_names, current_station_names)
    result = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "gbfs": gbfs_profile,
        "historical_trips": trip_profile,
    }
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    temporary = OUTPUT.with_suffix(".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(result, handle, indent=2, ensure_ascii=False)
        handle.write("\n")
    os.replace(temporary, OUTPUT)
    print(json.dumps(result, indent=2, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
