#!/usr/bin/env bash
# FlowFleet load test (Phase 8): ramp the generator through throughput steps and
# sample the pipeline from Prometheus at the end of each hold.
#
#   scripts/loadtest.sh ramp                 # full ramp, writes load-testing/results/<ts>.csv
#   scripts/loadtest.sh rate 25000           # just set the generator to 25k ev/s
#   scripts/loadtest.sh sample               # one row of metrics right now
#   scripts/loadtest.sh report <file.csv>    # render a results CSV as a markdown table
#
# Env:
#   GEN_URL   generator base URL          (default http://localhost:18090)
#   PROM_URL  Prometheus base URL         (default http://localhost:19090)
#   STEPS     rates to ramp through       (default "1000 5000 10000 25000 50000")
#   HOLD      seconds to hold each step   (default 180)
#   WARMUP    seconds before sampling     (default 45)
set -euo pipefail

GEN_URL="${GEN_URL:-http://localhost:18090}"
PROM_URL="${PROM_URL:-http://localhost:19090}"
STEPS="${STEPS:-1000 5000 10000 25000 50000}"
HOLD="${HOLD:-180}"
WARMUP="${WARMUP:-45}"
OUTDIR="$(cd "$(dirname "$0")/.." && pwd)/load-testing/results"

need() { command -v "$1" >/dev/null || { echo "missing: $1" >&2; exit 1; }; }
need curl; need jq

# instant PromQL query -> scalar (or "NaN")
pq() {
  curl -sf --get "$PROM_URL/api/v1/query" --data-urlencode "query=$1" \
    | jq -r '.data.result[0].value[1] // "NaN"'
}

set_rate() {
  curl -sf -XPOST "$GEN_URL/api/load/rate?value=$1" | jq -c .
}

HEADER="target_rate,produced_per_s,ingest_per_s,e2e_p50_ms,e2e_p95_ms,e2e_p99_ms,src_lag,busy_ms_s,backpressure_ms_s,ckpt_dur_ms,ckpt_size_b,restarts"

sample_row() {
  local target="${1:-NaN}"
  local produced ingest p50 p95 p99 lag busy bp cd cs rs
  produced=$(pq 'sum(rate(flowfleet_generator_sent_total[1m]))')
  ingest=$(pq 'sum(rate(flink_taskmanager_job_task_operator_flowfleet_ingest_valid[1m]))')
  p50=$(pq 'max(flink_taskmanager_job_task_operator_flowfleet_ingest_latencyMs{quantile="0.5"})')
  p95=$(pq 'max(flink_taskmanager_job_task_operator_flowfleet_ingest_latencyMs{quantile="0.95"})')
  p99=$(pq 'max(flink_taskmanager_job_task_operator_flowfleet_ingest_latencyMs{quantile="0.99"})')
  lag=$(pq 'max(flink_taskmanager_job_task_operator_KafkaSourceReader_KafkaConsumer_records_lag_max)')
  busy=$(pq 'max(flink_taskmanager_job_task_busyTimeMsPerSecond)')
  bp=$(pq 'max(flink_taskmanager_job_task_backPressuredTimeMsPerSecond)')
  cd=$(pq 'max(flink_jobmanager_job_lastCheckpointDuration)')
  cs=$(pq 'max(flink_jobmanager_job_lastCheckpointSize)')
  rs=$(pq 'sum(flink_jobmanager_job_numRestarts)')
  printf '%s,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f,%.0f\n' \
    "$target" "$produced" "$ingest" "$p50" "$p95" "$p99" "$lag" "$busy" "$bp" "$cd" "$cs" "$rs" \
    2>/dev/null || printf '%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n' \
      "$target" "$produced" "$ingest" "$p50" "$p95" "$p99" "$lag" "$busy" "$bp" "$cd" "$cs" "$rs"
}

cmd_ramp() {
  mkdir -p "$OUTDIR"
  local out="$OUTDIR/$(date +%Y%m%d-%H%M%S).csv"
  echo "$HEADER" | tee "$out"
  for r in $STEPS; do
    echo ">>> step $r ev/s  (warmup ${WARMUP}s, hold ${HOLD}s)" >&2
    set_rate "$r" >/dev/null
    sleep "$WARMUP"
    sleep "$((HOLD - WARMUP))"
    sample_row "$r" | tee -a "$out"
  done
  set_rate 0 >/dev/null
  echo ">>> done -> $out" >&2
  cmd_report "$out"
}

cmd_report() {
  local f="${1:?csv file}"
  awk -F, 'NR==1{printf "|"; for(i=1;i<=NF;i++) printf " %s |",$i; printf "\n|"; for(i=1;i<=NF;i++) printf "---|"; printf "\n"; next}
           {printf "|"; for(i=1;i<=NF;i++) printf " %s |",$i; printf "\n"}' "$f"
}

case "${1:-}" in
  ramp)   cmd_ramp ;;
  rate)   set_rate "${2:?rate}" ;;
  sample) echo "$HEADER"; sample_row "$(curl -sf "$GEN_URL/api/load/rate" | jq -r .targetRatePerSec)" ;;
  report) cmd_report "${2:-}" ;;
  *) grep -E '^#( |$)' "$0" | sed 's/^# \{0,1\}//'; exit 1 ;;
esac
