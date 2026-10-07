from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timedelta, timezone
from typing import Any

from prototype.fixtures import make_observation


BASE_TIME = datetime(2026, 4, 6, 12, 0, tzinfo=timezone.utc)
STATIONS = {
    "A": ("gbfs-station-a", "7293.10", "E 88 St & Park Ave"),
    "B": ("gbfs-station-b", "7456.03", "E 106 St & 1 Ave"),
    "C": ("gbfs-station-c", "7014.12", "Broadway & W 61 St"),
}


def observation(
    station: str,
    minute: int,
    *,
    bikes: int,
    docks: int,
) -> dict[str, Any]:
    station_id, short_name, name = STATIONS[station]
    item = make_observation(
        BASE_TIME + timedelta(minutes=minute),
        bikes=bikes,
        docks=docks,
        station_id=station_id,
    )
    item["station"]["short_name"] = short_name
    item["station"]["name"] = name
    item["source"]["feed_version"] = "2.3"
    return item


def scenario() -> dict[str, Any]:
    a0 = observation("A", 0, bikes=5, docks=5)
    a1 = observation("A", 1, bikes=0, docks=10)
    duplicate_a1 = deepcopy(a1)
    a6 = observation("A", 6, bikes=0, docks=10)
    out_of_order_a3 = observation("A", 3, bikes=4, docks=6)
    b20 = observation("B", 20, bikes=6, docks=4)
    late_a2 = observation("A", 2, bikes=3, docks=7)
    invalid_negative = observation("C", 4, bikes=1, docks=9)
    invalid_negative["availability"]["bikes_available"] = -1
    a21 = observation("A", 21, bikes=6, docks=4)
    c40 = observation("C", 40, bikes=7, docks=3)
    return {
        "phase1": [
            a0,
            a1,
            duplicate_a1,
            a6,
            out_of_order_a3,
            b20,
            # This on-time batch makes the previous batch's watermark frontier
            # effective before the intentionally late event is submitted.
            a21,
            late_a2,
            invalid_negative,
            "{not-json",
        ],
        "phase2": [c40],
        "expected": {
            "accepted_event_ids": [
                a0["event_id"],
                a1["event_id"],
                a6["event_id"],
                b20["event_id"],
                a21["event_id"],
                c40["event_id"],
            ],
            "rejected_out_of_order_event_id": out_of_order_a3["event_id"],
            "rejected_late_event_id": late_a2["event_id"],
            "duplicate_event_id": a1["event_id"],
            # C40 remains in an open 12:40 window in append mode. The six
            # closed-window observations include A3 (out of order but allowed)
            # and exclude the duplicate plus A2 (past the watermark).
            "window_observation_count": 6,
            "a_first_window_observation_count": 3,
            "latest_by_station": {
                STATIONS["A"][0]: a21["event_id"],
                STATIONS["B"][0]: b20["event_id"],
                STATIONS["C"][0]: c40["event_id"],
            },
        },
    }
