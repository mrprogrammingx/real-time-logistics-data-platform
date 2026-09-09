#!/usr/bin/env bash
# Manage the Debezium PostgreSQL source connector on the local Kafka Connect.
set -euo pipefail

CONNECT="${CONNECT:-http://localhost:18083}"
NAME="flowfleet-postgres-source"
CFG="kafka-connect/postgres-source.json"
CMD="${1:-status}"

case "$CMD" in
  register|create)
    curl -fsS -X POST -H 'Content-Type: application/json' --data "@$CFG" "$CONNECT/connectors" | jq .
    ;;
  update)
    jq '.config' "$CFG" | curl -fsS -X PUT -H 'Content-Type: application/json' \
      --data @- "$CONNECT/connectors/$NAME/config" | jq .
    ;;
  status)
    curl -fsS "$CONNECT/connectors/$NAME/status" | jq .
    ;;
  list)
    curl -fsS "$CONNECT/connectors" | jq .
    ;;
  plugins)
    curl -fsS "$CONNECT/connector-plugins" | jq '[.[].class]'
    ;;
  restart)
    curl -fsS -X POST "$CONNECT/connectors/$NAME/restart?includeTasks=true&onlyFailed=false" -w '\nHTTP %{http_code}\n'
    ;;
  pause)   curl -fsS -X PUT "$CONNECT/connectors/$NAME/pause"  -w 'HTTP %{http_code}\n' ;;
  resume)  curl -fsS -X PUT "$CONNECT/connectors/$NAME/resume" -w 'HTTP %{http_code}\n' ;;
  delete)  curl -fsS -X DELETE "$CONNECT/connectors/$NAME"     -w 'HTTP %{http_code}\n' ;;
  offsets)
    curl -fsS "$CONNECT/connectors/$NAME/offsets" | jq .
    ;;
  slot)
    # replication-slot health straight from Postgres
    docker compose exec -T postgres psql -U flowfleet -d flowfleet -c \
      "SELECT slot_name, active, wal_status, pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS retained FROM pg_replication_slots;"
    ;;
  *)
    echo "usage: $0 {register|update|status|list|plugins|restart|pause|resume|delete|offsets|slot}" >&2
    exit 1
    ;;
esac
