#!/usr/bin/env bash
# Inspect the Schema Registry and demonstrate the DriverLocation v1 -> v2 evolution.
set -euo pipefail

SR="${SR:-http://localhost:18085}"
SUBJECT="flowfleet.driver.locations-value"
CMD="${1:-subjects}"

case "$CMD" in
  subjects)
    curl -fsS "$SR/subjects" | jq .
    ;;
  latest)
    curl -fsS "$SR/subjects/$SUBJECT/versions/latest" | jq '{version, id, schema: (.schema | fromjson)}'
    ;;
  versions)
    curl -fsS "$SR/subjects/$SUBJECT/versions" | jq .
    ;;
  config)
    curl -fsS "$SR/config" | jq .
    echo "subject $SUBJECT:"
    curl -fsS "$SR/config/$SUBJECT" | jq . 2>/dev/null || echo "  (inherits global)"
    ;;
  check-v1)
    # Would registering the historical v1 schema be compatible with the current one?
    v1=$(jq -c . services/events/src/test/resources/avro-history/driver-location.v1.avsc)
    curl -fsS -X POST -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
      --data "$(jq -n --arg s "$v1" '{schema: $s, schemaType: "AVRO"}')" \
      "$SR/compatibility/subjects/$SUBJECT/versions/latest" | jq .
    ;;
  *)
    echo "usage: $0 {subjects|latest|versions|config|check-v1}" >&2
    exit 1
    ;;
esac
