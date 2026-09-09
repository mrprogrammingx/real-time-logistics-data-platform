#!/usr/bin/env bash
# End-to-end smoke test against a running FlowFleet API (make up).
# Creates a driver, creates an order, and walks the order to DELIVERED.
set -euo pipefail

BASE="${BASE:-http://localhost:18080}"
say() { printf '\n\033[36m== %s\033[0m\n' "$1"; }

need() { command -v "$1" >/dev/null || { echo "need $1"; exit 1; }; }
need curl
need jq

say "health"
curl -fsS "$BASE/actuator/health" | jq .

say "create driver"
DRIVER=$(curl -fsS -X POST "$BASE/api/v1/drivers" \
  -H 'content-type: application/json' \
  -d '{"name":"Smoke Driver","vehicleId":1}')
echo "$DRIVER" | jq .
DRIVER_ID=$(echo "$DRIVER" | jq -r .id)

say "driver -> AVAILABLE"
curl -fsS -X PATCH "$BASE/api/v1/drivers/$DRIVER_ID/status" \
  -H 'content-type: application/json' -d '{"status":"AVAILABLE"}' | jq '{id,status,version}'

say "create order"
ORDER=$(curl -fsS -X POST "$BASE/api/v1/orders" \
  -H 'content-type: application/json' \
  -d '{"customerId":1,"restaurantId":1,"items":[
        {"name":"Lahmajoun","quantity":3,"unitPrice":1.50},
        {"name":"Ayran","quantity":2,"unitPrice":0.80}]}')
echo "$ORDER" | jq .
ORDER_ID=$(echo "$ORDER" | jq -r .id)

say "walk order to DELIVERED"
for s in CONFIRMED PREPARING READY_FOR_PICKUP PICKED_UP OUT_FOR_DELIVERY DELIVERED; do
  curl -fsS -X PATCH "$BASE/api/v1/orders/$ORDER_ID/status" \
    -H 'content-type: application/json' -d "{\"status\":\"$s\"}" | jq -c '{status,version,updatedAt}'
done

say "illegal transition is rejected (expect HTTP 409)"
code=$(curl -s -o /dev/null -w '%{http_code}' -X PATCH "$BASE/api/v1/orders/$ORDER_ID/status" \
  -H 'content-type: application/json' -d '{"status":"CREATED"}')
echo "HTTP $code"
[[ "$code" == "409" ]] || { echo "expected 409"; exit 1; }

say "done ✅"
