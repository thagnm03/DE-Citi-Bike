from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any

import psycopg
from psycopg.rows import dict_row
from psycopg.types.json import Jsonb
from psycopg_pool import ConnectionPool

from spark.common.alert_rules import build_acknowledged_event


class ServingNotFoundError(Exception):
    pass


class ServingConflictError(Exception):
    pass


def effective_lifecycle(current: str | None, incoming: str) -> str:
    """Merge engine lifecycle with an operator acknowledgement overlay."""
    if incoming == "RESOLVED":
        return "RESOLVED"
    if current == "ACKNOWLEDGED" and incoming == "OPEN":
        return "ACKNOWLEDGED"
    if current == "RESOLVED":
        return "RESOLVED"
    return incoming


def utc_now_text() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def command_id(idempotency_key: str, alert_id: str) -> str:
    return hashlib.sha256(f"{idempotency_key}|{alert_id}|ACKNOWLEDGE".encode()).hexdigest()


class ServingRepository:
    def __init__(self, database_url: str, *, connection_pool: ConnectionPool | None = None):
        self.database_url = database_url
        self.connection_pool = connection_pool

    def connect(self, *, timeout_seconds: float | None = None):
        if self.connection_pool is not None:
            if timeout_seconds is not None:
                return self.connection_pool.connection(timeout=timeout_seconds)
            return self.connection_pool.connection()
        return psycopg.connect(self.database_url, row_factory=dict_row)

    def health(self) -> bool:
        with self.connect(timeout_seconds=1.0) as connection:
            return connection.execute("SELECT 1 AS value").fetchone()["value"] == 1

    def observability_snapshot(self) -> dict[str, float | int]:
        """Return bounded, low-cardinality gauges for the serving control plane."""
        with self.connect(timeout_seconds=1.0) as connection:
            row = connection.execute(
                """
                SELECT
                    (SELECT count(*) FROM serving.station_current) AS station_rows,
                    (SELECT count(*) FROM serving.alert_state
                     WHERE lifecycle_status <> 'RESOLVED') AS active_alerts,
                    (SELECT count(*) FROM serving.alert_state
                     WHERE lifecycle_status = 'OPEN' AND severity = 'CRITICAL')
                        AS critical_unacknowledged_alerts,
                    COALESCE((SELECT max(source_age_seconds)
                              FROM serving.station_current), 0) AS reported_source_age_seconds,
                    COALESCE((SELECT EXTRACT(EPOCH FROM (now() - max(updated_at)))
                              FROM serving.station_current), 0) AS projection_age_seconds
                """
            ).fetchone()
        return {
            "database_up": 1,
            "station_rows": int(row["station_rows"]),
            "active_alerts": int(row["active_alerts"]),
            "critical_unacknowledged_alerts": int(row["critical_unacknowledged_alerts"]),
            "reported_source_age_seconds": max(0.0, float(row["reported_source_age_seconds"])),
            "projection_age_seconds": max(0.0, float(row["projection_age_seconds"])),
        }

    @staticmethod
    def _event_time(event: dict[str, Any]) -> str:
        return event["time"]["status_changed_at_utc"]

    def _store_alert_event(
        self,
        connection: psycopg.Connection,
        event: dict[str, Any],
        *,
        source_topic: str | None = None,
        source_partition: int | None = None,
        source_offset: int | None = None,
    ) -> bool:
        inserted = connection.execute(
            """
            INSERT INTO serving.alert_events (
                alert_event_id, alert_id, station_id, event_type, lifecycle_status,
                alert_type, recommended_action, severity, priority_score,
                status_changed_at, source_topic, source_partition, source_offset, payload
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::timestamptz, %s, %s, %s, %s
            )
            ON CONFLICT (alert_event_id) DO NOTHING
            RETURNING alert_event_id
            """,
            (
                event["alert_event_id"],
                event["alert_id"],
                event["station"]["station_id"],
                event["event_type"],
                event["lifecycle_status"],
                event["alert_type"],
                event["recommended_action"],
                event["severity"],
                event["priority"]["score"],
                self._event_time(event),
                source_topic,
                source_partition,
                source_offset,
                Jsonb(event),
            ),
        ).fetchone()
        if not inserted:
            return False

        is_ack = event["lifecycle_status"] == "ACKNOWLEDGED"
        connection.execute(
            """
            INSERT INTO serving.alert_state (
                alert_id, last_event_id, station_id, station_name, short_name,
                alert_type, lifecycle_status, recommended_action, severity,
                priority_score, episode_started_at, detected_at, last_observed_at,
                status_changed_at, resolved_at, acknowledged_at, payload
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                %s::timestamptz, %s::timestamptz, %s::timestamptz,
                %s::timestamptz, %s::timestamptz,
                CASE WHEN %s THEN %s::timestamptz ELSE NULL END,
                %s
            )
            ON CONFLICT (alert_id) DO UPDATE SET
                last_event_id = EXCLUDED.last_event_id,
                station_id = EXCLUDED.station_id,
                station_name = EXCLUDED.station_name,
                short_name = EXCLUDED.short_name,
                alert_type = EXCLUDED.alert_type,
                lifecycle_status = CASE
                    WHEN EXCLUDED.lifecycle_status = 'RESOLVED' THEN 'RESOLVED'
                    WHEN serving.alert_state.lifecycle_status = 'RESOLVED' THEN 'RESOLVED'
                    WHEN serving.alert_state.lifecycle_status = 'ACKNOWLEDGED'
                         AND EXCLUDED.lifecycle_status = 'OPEN' THEN 'ACKNOWLEDGED'
                    ELSE EXCLUDED.lifecycle_status
                END,
                recommended_action = EXCLUDED.recommended_action,
                severity = EXCLUDED.severity,
                priority_score = EXCLUDED.priority_score,
                last_observed_at = EXCLUDED.last_observed_at,
                status_changed_at = EXCLUDED.status_changed_at,
                resolved_at = COALESCE(EXCLUDED.resolved_at, serving.alert_state.resolved_at),
                acknowledged_at = CASE
                    WHEN EXCLUDED.lifecycle_status = 'ACKNOWLEDGED'
                        THEN EXCLUDED.acknowledged_at
                    ELSE serving.alert_state.acknowledged_at
                END,
                payload = EXCLUDED.payload,
                updated_at = now()
            WHERE EXCLUDED.status_changed_at >= serving.alert_state.status_changed_at
            """,
            (
                event["alert_id"],
                event["alert_event_id"],
                event["station"]["station_id"],
                event["station"].get("name"),
                event["station"].get("short_name"),
                event["alert_type"],
                event["lifecycle_status"],
                event["recommended_action"],
                event["severity"],
                event["priority"]["score"],
                event["time"]["episode_started_at_utc"],
                event["time"]["detected_at_utc"],
                event["time"]["last_observed_at_utc"],
                event["time"]["status_changed_at_utc"],
                event["time"].get("resolved_at_utc"),
                is_ack,
                event["time"]["status_changed_at_utc"],
                Jsonb(event),
            ),
        )
        return True

    def store_alert_event(
        self,
        event: dict[str, Any],
        *,
        source_topic: str | None = None,
        source_partition: int | None = None,
        source_offset: int | None = None,
    ) -> bool:
        with self.connect() as connection:
            return self._store_alert_event(
                connection,
                event,
                source_topic=source_topic,
                source_partition=source_partition,
                source_offset=source_offset,
            )

    def _store_current_state(self, connection: psycopg.Connection, state: dict[str, Any]) -> bool:
        station = state["station"]
        availability = state["availability"]
        service = state["service"]
        row = connection.execute(
            """
            INSERT INTO serving.station_current (
                station_id, event_id, event_time, station_name, short_name,
                bikes_available, docks_available, capacity, source_age_seconds,
                is_installed, is_renting, is_returning, payload
            ) VALUES (
                %s, %s, %s::timestamptz, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (station_id) DO UPDATE SET
                event_id = EXCLUDED.event_id,
                event_time = EXCLUDED.event_time,
                station_name = EXCLUDED.station_name,
                short_name = EXCLUDED.short_name,
                bikes_available = EXCLUDED.bikes_available,
                docks_available = EXCLUDED.docks_available,
                capacity = EXCLUDED.capacity,
                source_age_seconds = EXCLUDED.source_age_seconds,
                is_installed = EXCLUDED.is_installed,
                is_renting = EXCLUDED.is_renting,
                is_returning = EXCLUDED.is_returning,
                payload = EXCLUDED.payload,
                updated_at = now()
            WHERE EXCLUDED.event_time > serving.station_current.event_time
            RETURNING station_id
            """,
            (
                state["station_id"],
                state["event_id"],
                state["event_time_utc"],
                station.get("name"),
                station.get("short_name"),
                availability["bikes_available"],
                availability["docks_available"],
                station.get("capacity"),
                state.get("source_age_seconds"),
                service["is_installed"],
                service["is_renting"],
                service["is_returning"],
                Jsonb(state),
            ),
        ).fetchone()
        return row is not None

    def store_current_state(self, state: dict[str, Any]) -> bool:
        with self.connect() as connection:
            return self._store_current_state(connection, state)

    def store_consumer_record(
        self,
        *,
        topic: str,
        partition: int,
        offset: int,
        payload: dict[str, Any],
        kind: str,
    ) -> bool:
        with self.connect() as connection:
            if kind == "alert":
                changed = self._store_alert_event(
                    connection,
                    payload,
                    source_topic=topic,
                    source_partition=partition,
                    source_offset=offset,
                )
            elif kind == "current":
                changed = self._store_current_state(connection, payload)
            else:
                raise ValueError(f"Unsupported materialization kind: {kind}")
            connection.execute(
                """
                INSERT INTO serving.consumer_offsets (topic, partition, next_offset)
                VALUES (%s, %s, %s)
                ON CONFLICT (topic, partition) DO UPDATE SET
                    next_offset = GREATEST(serving.consumer_offsets.next_offset, EXCLUDED.next_offset),
                    updated_at = now()
                """,
                (topic, partition, offset + 1),
            )
            return changed

    @staticmethod
    def _state_row(row: dict[str, Any]) -> dict[str, Any]:
        payload = row["payload"]
        return {
            "alert_id": row["alert_id"],
            "last_event_id": row["last_event_id"],
            "station": {
                "station_id": row["station_id"],
                "name": row["station_name"],
                "short_name": row["short_name"],
            },
            "alert_type": row["alert_type"],
            "lifecycle_status": row["lifecycle_status"],
            "recommended_action": row["recommended_action"],
            "severity": row["severity"],
            "priority": payload["priority"],
            "evidence": payload["evidence"],
            "time": {
                "episode_started_at_utc": row["episode_started_at"].isoformat(),
                "detected_at_utc": row["detected_at"].isoformat(),
                "last_observed_at_utc": row["last_observed_at"].isoformat(),
                "status_changed_at_utc": row["status_changed_at"].isoformat(),
                "resolved_at_utc": row["resolved_at"].isoformat() if row["resolved_at"] else None,
            },
            "acknowledgement": (
                {
                    "acknowledged_at_utc": row["acknowledged_at"].isoformat(),
                    "acknowledged_by": row["acknowledged_by"],
                }
                if row["acknowledged_at"]
                else None
            ),
        }

    def list_alerts(
        self,
        *,
        lifecycle_status: str | None = None,
        recommended_action: str | None = None,
        alert_type: str | None = None,
        station_id: str | None = None,
        active_only: bool = True,
        limit: int = 100,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if active_only:
            clauses.append("lifecycle_status <> 'RESOLVED'")
        for column, value in (
            ("lifecycle_status", lifecycle_status),
            ("recommended_action", recommended_action),
            ("alert_type", alert_type),
            ("station_id", station_id),
        ):
            if value:
                clauses.append(f"{column} = %s")
                values.append(value)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.extend([limit, offset])
        query = (
            "SELECT * FROM serving.alert_state"
            + where
            + " ORDER BY priority_score DESC, detected_at, alert_id LIMIT %s OFFSET %s"
        )
        with self.connect() as connection:
            rows = connection.execute(query, values).fetchall()
        return [self._state_row(row) for row in rows]

    def get_alert(self, alert_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT * FROM serving.alert_state WHERE alert_id = %s", (alert_id,)
            ).fetchone()
        if not row:
            raise ServingNotFoundError(f"Alert {alert_id} was not found")
        return self._state_row(row)

    def alert_history(self, alert_id: str) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT payload FROM serving.alert_events
                WHERE alert_id = %s
                ORDER BY status_changed_at, alert_event_id
                """,
                (alert_id,),
            ).fetchall()
        if not rows:
            raise ServingNotFoundError(f"Alert {alert_id} was not found")
        return [row["payload"] for row in rows]

    def list_stations(self, *, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        with self.connect() as connection:
            rows = connection.execute(
                """
                SELECT payload FROM serving.station_current
                ORDER BY event_time DESC, station_id
                LIMIT %s OFFSET %s
                """,
                (limit, offset),
            ).fetchall()
        return [row["payload"] for row in rows]

    def get_station(self, station_id: str) -> dict[str, Any]:
        with self.connect() as connection:
            row = connection.execute(
                "SELECT payload FROM serving.station_current WHERE station_id = %s",
                (station_id,),
            ).fetchone()
        if not row:
            raise ServingNotFoundError(f"Station {station_id} was not found")
        return row["payload"]

    def acknowledge(
        self,
        *,
        alert_id: str,
        idempotency_key: str,
        requested_by: str,
        acknowledged_at_utc: str | None = None,
    ) -> dict[str, Any]:
        acknowledged_at_utc = acknowledged_at_utc or utc_now_text()
        with self.connect() as connection:
            existing = connection.execute(
                """
                SELECT alert_id, result_event_id FROM serving.alert_commands
                WHERE idempotency_key = %s
                """,
                (idempotency_key,),
            ).fetchone()
            if existing:
                if existing["alert_id"] != alert_id:
                    raise ServingConflictError("Idempotency key already belongs to another alert")
                return connection.execute(
                    "SELECT payload FROM serving.alert_events WHERE alert_event_id = %s",
                    (existing["result_event_id"],),
                ).fetchone()["payload"]

            state = connection.execute(
                "SELECT * FROM serving.alert_state WHERE alert_id = %s FOR UPDATE",
                (alert_id,),
            ).fetchone()
            if not state:
                raise ServingNotFoundError(f"Alert {alert_id} was not found")
            if state["lifecycle_status"] == "RESOLVED":
                raise ServingConflictError("Resolved alerts cannot be acknowledged")

            if state["lifecycle_status"] == "ACKNOWLEDGED":
                latest_ack = connection.execute(
                    """
                    SELECT payload FROM serving.alert_events
                    WHERE alert_id = %s AND event_type = 'ALERT_ACKNOWLEDGED'
                    ORDER BY status_changed_at DESC LIMIT 1
                    """,
                    (alert_id,),
                ).fetchone()
                event = latest_ack["payload"]
            else:
                source = json.loads(json.dumps(state["payload"]))
                source["lifecycle_status"] = "OPEN"
                event = build_acknowledged_event(source, acknowledged_at_utc)
                self._store_alert_event(connection, event)
                connection.execute(
                    """
                    UPDATE serving.alert_state
                    SET acknowledged_by = %s, updated_at = now()
                    WHERE alert_id = %s
                    """,
                    (requested_by, alert_id),
                )

            connection.execute(
                """
                INSERT INTO serving.alert_commands (
                    command_id, idempotency_key, alert_id, command_type,
                    requested_by, requested_at, result_event_id
                ) VALUES (%s, %s, %s, 'ACKNOWLEDGE', %s, %s::timestamptz, %s)
                """,
                (
                    command_id(idempotency_key, alert_id),
                    idempotency_key,
                    alert_id,
                    requested_by,
                    acknowledged_at_utc,
                    event["alert_event_id"],
                ),
            )
            return event
