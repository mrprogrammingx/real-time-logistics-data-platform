#!/usr/bin/env bash
# Phase 2 experiment: watch a Kafka consumer-group rebalance.
#
#   flowfleet.driver.locations has 6 partitions.
#   We run 3 consumers -> 2 partitions each, then kill one -> its 2 move to the survivors.
#
# Requires the stack to be up (make up) and the generator producing.
set -euo pipefail

GROUP="flowfleet.location-consumer"
DC="docker compose"
kafka_cg() { $DC exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 "$@"; }

echo "== scaling to 3 consumers =="
$DC up -d --scale location-consumer=3 --no-recreate location-consumer
sleep 20

echo
echo "== group members & partition assignment (expect ~2 partitions each) =="
kafka_cg --describe --group "$GROUP" --members --verbose
echo
kafka_cg --describe --group "$GROUP"

victim=$($DC ps -q location-consumer | head -1)
echo
echo "== killing one consumer ($victim) =="
docker kill "$victim" >/dev/null
sleep 15

echo
echo "== after rebalance (expect 3 + 3 across 2 survivors) =="
kafka_cg --describe --group "$GROUP" --members --verbose
echo
kafka_cg --describe --group "$GROUP"

echo
echo "grep the consumer logs for the reassignment:"
echo "  docker compose logs location-consumer | grep REBALANCE"
