CREATE SCHEMA IF NOT EXISTS serving;

CREATE TABLE IF NOT EXISTS serving.schema_migrations (
    version integer PRIMARY KEY,
    applied_at timestamptz NOT NULL DEFAULT now()
);

INSERT INTO serving.schema_migrations (version)
VALUES (1)
ON CONFLICT (version) DO NOTHING;

CREATE TABLE IF NOT EXISTS serving.station_current (
    station_id text PRIMARY KEY,
    event_id text NOT NULL UNIQUE,
    event_time timestamptz NOT NULL,
    station_name text,
    short_name text,
    bikes_available integer NOT NULL CHECK (bikes_available >= 0),
    docks_available integer NOT NULL CHECK (docks_available >= 0),
    capacity integer CHECK (capacity IS NULL OR capacity >= 0),
    source_age_seconds integer CHECK (source_age_seconds IS NULL OR source_age_seconds >= 0),
    is_installed boolean NOT NULL,
    is_renting boolean NOT NULL,
    is_returning boolean NOT NULL,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS station_current_event_time_idx
    ON serving.station_current (event_time DESC);

CREATE TABLE IF NOT EXISTS serving.alert_events (
    alert_event_id text PRIMARY KEY,
    alert_id text NOT NULL,
    station_id text NOT NULL,
    event_type text NOT NULL CHECK (
        event_type IN ('ALERT_OPENED', 'ALERT_UPDATED', 'ALERT_ACKNOWLEDGED', 'ALERT_RESOLVED')
    ),
    lifecycle_status text NOT NULL CHECK (
        lifecycle_status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')
    ),
    alert_type text NOT NULL,
    recommended_action text NOT NULL,
    severity text NOT NULL,
    priority_score numeric(6, 2) NOT NULL CHECK (priority_score BETWEEN 0 AND 100),
    status_changed_at timestamptz NOT NULL,
    source_topic text,
    source_partition integer,
    source_offset bigint,
    payload jsonb NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alert_events_alert_history_idx
    ON serving.alert_events (alert_id, status_changed_at, alert_event_id);
CREATE INDEX IF NOT EXISTS alert_events_station_history_idx
    ON serving.alert_events (station_id, status_changed_at DESC);

CREATE TABLE IF NOT EXISTS serving.alert_state (
    alert_id text PRIMARY KEY,
    last_event_id text NOT NULL REFERENCES serving.alert_events(alert_event_id),
    station_id text NOT NULL,
    station_name text,
    short_name text,
    alert_type text NOT NULL,
    lifecycle_status text NOT NULL CHECK (
        lifecycle_status IN ('OPEN', 'ACKNOWLEDGED', 'RESOLVED')
    ),
    recommended_action text NOT NULL,
    severity text NOT NULL,
    priority_score numeric(6, 2) NOT NULL CHECK (priority_score BETWEEN 0 AND 100),
    episode_started_at timestamptz NOT NULL,
    detected_at timestamptz NOT NULL,
    last_observed_at timestamptz NOT NULL,
    status_changed_at timestamptz NOT NULL,
    resolved_at timestamptz,
    acknowledged_at timestamptz,
    acknowledged_by text,
    payload jsonb NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alert_state_priority_queue_idx
    ON serving.alert_state (priority_score DESC, detected_at, alert_id)
    WHERE lifecycle_status <> 'RESOLVED';
CREATE INDEX IF NOT EXISTS alert_state_station_idx
    ON serving.alert_state (station_id, status_changed_at DESC);

CREATE TABLE IF NOT EXISTS serving.alert_commands (
    command_id text PRIMARY KEY,
    idempotency_key text NOT NULL UNIQUE,
    alert_id text NOT NULL,
    command_type text NOT NULL CHECK (command_type = 'ACKNOWLEDGE'),
    requested_by text NOT NULL,
    requested_at timestamptz NOT NULL,
    result_event_id text NOT NULL REFERENCES serving.alert_events(alert_event_id),
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS alert_commands_alert_idx
    ON serving.alert_commands (alert_id, requested_at DESC);

CREATE TABLE IF NOT EXISTS serving.consumer_offsets (
    topic text NOT NULL,
    partition integer NOT NULL,
    next_offset bigint NOT NULL CHECK (next_offset >= 0),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);
