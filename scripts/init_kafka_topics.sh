#!/usr/bin/env bash
set -euo pipefail

KAFKA_TOPICS=/opt/kafka/bin/kafka-topics.sh
KAFKA_CONFIGS=/opt/kafka/bin/kafka-configs.sh
BOOTSTRAP_SERVER=${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}
PARTITIONS=${KAFKA_TOPIC_PARTITIONS:-3}
REPLICATION_FACTOR=${KAFKA_REPLICATION_FACTOR:-1}
OBSERVATION_RETENTION_MS=${KAFKA_OBSERVATION_RETENTION_MS:-259200000}
ALERT_RETENTION_MS=${KAFKA_ALERT_RETENTION_MS:-604800000}
WINDOW_RETENTION_MS=${KAFKA_WINDOW_RETENTION_MS:-604800000}

create_or_update_topic() {
  local topic=$1
  local topic_config=$2

  "$KAFKA_TOPICS" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --create \
    --if-not-exists \
    --topic "$topic" \
    --partitions "$PARTITIONS" \
    --replication-factor "$REPLICATION_FACTOR"

  "$KAFKA_CONFIGS" \
    --bootstrap-server "$BOOTSTRAP_SERVER" \
    --entity-type topics \
    --entity-name "$topic" \
    --alter \
    --add-config "$topic_config"
}

create_or_update_topic citibike.station-status.v1 "cleanup.policy=delete,retention.ms=$OBSERVATION_RETENTION_MS"
create_or_update_topic citibike.station-status.invalid.v1 "cleanup.policy=delete,retention.ms=$OBSERVATION_RETENTION_MS"
create_or_update_topic citibike.station-current.v1 "cleanup.policy=compact,min.cleanable.dirty.ratio=0.01"
create_or_update_topic citibike.station-windows.v1 "cleanup.policy=delete,retention.ms=$WINDOW_RETENTION_MS"
create_or_update_topic citibike.station-alerts.invalid.v1 "cleanup.policy=delete,retention.ms=$ALERT_RETENTION_MS"
create_or_update_topic citibike.station-alerts.v1 "cleanup.policy=delete,retention.ms=$ALERT_RETENTION_MS"
create_or_update_topic citibike.station-alerts.replay.v1 "cleanup.policy=delete,retention.ms=$ALERT_RETENTION_MS"

"$KAFKA_TOPICS" --bootstrap-server "$BOOTSTRAP_SERVER" --describe
