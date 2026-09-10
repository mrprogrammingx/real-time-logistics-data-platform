#!/usr/bin/env bash
# Phase 6 failure-engineering experiments against the running stack.
#   make up && make cdc-register && make flink-submit-all   (then wait a minute)
set -euo pipefail

FLINK="${FLINK:-http://localhost:18086}"
DC="docker compose"
CMD="${1:-help}"

jobs()      { curl -fsS "$FLINK/jobs/overview" | jq -c '.jobs[] | {name, state, start: .["start-time"]}'; }
job_id()    { curl -fsS "$FLINK/jobs/overview" | jq -r ".jobs[] | select(.name==\"flowfleet-$1\") | .jid"; }
metrics()   {
  local jid; jid=$(job_id "$1")
  echo "== $1 ($jid) =="
  curl -fsS "$FLINK/jobs/$jid" | jq '{state, restarts: .["status-counts"], vertices: [.vertices[] | {name, busy: .metrics["accumulated-busy-time"], backpressured: .metrics["accumulated-backpressured-time"], in: .metrics["read-records"], out: .metrics["write-records"]}]}'
  curl -fsS "$FLINK/jobs/$jid/checkpoints" | jq '{completed: .counts.completed, failed: .counts.failed, last: (.latest.completed | {duration: .end_to_end_duration, size: .state_size})}'
}

case "$CMD" in
  jobs)    jobs ;;
  metrics) metrics "${2:-driver-state}" ;;

  kill-tm)  ## kill a TaskManager, watch the job recover from its last checkpoint
    victim=$($DC ps -q flink-taskmanager | head -1)
    echo "before:"; metrics "${2:-driver-state}"
    echo; echo "killing taskmanager $victim ..."; docker kill "$victim" >/dev/null
    echo "waiting for RUNNING again ..."
    for _ in $(seq 1 60); do
      st=$(curl -fsS "$FLINK/jobs/overview" | jq -r '.jobs[0].state'); echo "  state=$st"
      [ "$st" = "RUNNING" ] && break; sleep 3
    done
    $DC up -d --no-recreate flink-taskmanager >/dev/null
    sleep 20; echo; echo "after:"; metrics "${2:-driver-state}"
    echo "-> compare restarts, and check the sinks: make timescale-peek (row counts should not have jumped)"
    ;;

  slow-sink)  ## resubmit the timescale sink with an N ms/record delay; watch backpressure
    ms="${2:?usage: chaos.sh slow-sink <ms>}"
    jid=$(job_id timescale-sink || true)
    [ -n "${jid:-}" ] && { echo "cancelling $jid"; $DC exec -T flink-jobmanager flink cancel "$jid" >/dev/null; sleep 3; }
    $DC exec -T flink-jobmanager flink run -d \
      -c com.flowfleet.flink.sink.TimescaleSinkJob /opt/flink/usrlib/flowfleet-flink-jobs.jar \
      --slow-map-ms "$ms"
    echo "give it a minute, then: chaos.sh metrics timescale-sink ; make lag"
    ;;

  malformed)  ## inject N non-Avro messages onto flowfleet.driver.locations -> DLQ
    n="${2:-5}"
    echo "producing $n junk messages ..."
    yes 'not-a-valid-avro-record' | head -n "$n" | \
      $DC exec -T kafka kafka-console-producer --bootstrap-server kafka:29092 --topic flowfleet.driver.locations
    sleep 5
    echo "DLQ tail (expect decode-failure entries):"
    timeout 8 $DC exec -T kafka kafka-console-consumer --bootstrap-server kafka:29092 \
      --topic flowfleet.driver.locations.dlq --timeout-ms 6000 2>/dev/null | grep -c decode-failure || true
    ;;

  duplicate)  ## replay flowfleet.driver.state into the timescale sink; row count must not change
    echo "before:"; $DC exec -T timescaledb psql -U flowfleet -d flowfleet -tc "SELECT count(*) FROM driver_state;"
    jid=$(job_id timescale-sink); $DC exec -T flink-jobmanager flink cancel "$jid" >/dev/null; sleep 3
    $DC exec -T kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
      --group flowfleet.flink.timescale-sink --topic flowfleet.driver.state --reset-offsets --to-earliest --execute >/dev/null
    $DC exec -T flink-jobmanager flink run -d \
      -c com.flowfleet.flink.sink.TimescaleSinkJob /opt/flink/usrlib/flowfleet-flink-jobs.jar >/dev/null
    echo "replaying ... (wait ~30s)"; sleep 30
    echo "after (should match):"; $DC exec -T timescaledb psql -U flowfleet -d flowfleet -tc "SELECT count(*) FROM driver_state;"
    ;;

  help|*) grep -E '^\s+[a-z-]+\)\s+##' "$0" | sed 's/)\s*##/ -/' ;;
esac
