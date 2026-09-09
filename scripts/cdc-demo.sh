#!/usr/bin/env bash
# End-to-end CDC demo: mutate an order through the API, see the Debezium event on Kafka.
# Requires: make up ; make cdc-register
set -euo pipefail

API="${API:-http://localhost:18080}"
need() { command -v "$1" >/dev/null || { echo "need $1"; exit 1; }; }
need jq

echo "== connector status =="
./scripts/connect.sh status | jq '{name, state: .connector.state, tasks: [.tasks[].state]}'

echo
echo "== create an order via the API =="
ORDER=$(curl -fsS -X POST "$API/api/v1/orders" -H 'content-type: application/json' -d '{
  "customerId":1,"restaurantId":1,"items":[{"name":"CDC demo","quantity":1,"unitPrice":3.50}]}')
ID=$(echo "$ORDER" | jq -r .id)
echo "$ORDER" | jq '{id,status,version}'

echo
echo "== PATCH it to CONFIRMED =="
curl -fsS -X PATCH "$API/api/v1/orders/$ID/status" -H 'content-type: application/json' \
  -d '{"status":"CONFIRMED"}' | jq '{id,status,version}'

echo
echo "== last CDC events for orders (5s window) — expect op=c then op=u =="
timeout 8 docker compose exec -T schema-registry kafka-avro-console-consumer \
  --bootstrap-server kafka:29092 --property schema.registry.url=http://localhost:8085 \
  --topic flowfleet.orders.cdc --from-beginning --timeout-ms 6000 2>/dev/null \
  | jq -c 'select(.after.id == '"$ID"') | {op, before: .before.status, after: .after.status, lsn: .source.lsn}' \
  || true

echo
echo "done. Full stream: make cdc-tail"
