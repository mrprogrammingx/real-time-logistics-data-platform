#!/usr/bin/env bash
# Create / list / describe the FlowFleet Kafka topics via the running broker container.
# The location-generator also auto-creates them on startup; this script is for manual use.
set -euo pipefail

CMD="${1:-list}"
KAFKA="docker compose exec -T kafka kafka-topics --bootstrap-server kafka:29092"

# name:partitions:retention-ms   (RF is 1 on the single-node dev broker)
TOPICS=(
  "flowfleet.driver.locations:6:86400000"
  "flowfleet.driver.geofence-events:3:604800000"
  "flowfleet.delivery.events:3:2592000000"
  "flowfleet.delivery.alerts:3:2592000000"
  "flowfleet.driver.locations.dlq:3:1209600000"
)

case "$CMD" in
  create)
    for spec in "${TOPICS[@]}"; do
      IFS=: read -r name parts retention <<< "$spec"
      $KAFKA --create --if-not-exists --topic "$name" \
        --partitions "$parts" --replication-factor 1 \
        --config retention.ms="$retention" --config cleanup.policy=delete
    done
    $KAFKA --list
    ;;
  list)
    $KAFKA --list
    ;;
  describe)
    $KAFKA --describe ${2:+--topic "$2"}
    ;;
  *)
    echo "usage: $0 {create|list|describe [topic]}" >&2
    exit 1
    ;;
esac
