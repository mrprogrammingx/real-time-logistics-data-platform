# Load testing (Phase 8)

Push the pipeline from 1k to 50k events/second, record where each ceiling is, fix
one thing at a time, autoscale.

## How the load is generated

The **`location-generator`** is the load tool — no k6/JMeter. It has two modes
(`services/location-generator/.../GeneratorRunner.java`):

* **legacy** — one sample per driver per tick (throughput = drivers ÷ tick).
* **rate** (`GENERATOR_TARGET_RATE > 0`) — a 100 ms tick emits exactly enough
  samples to hold the target rate, advancing a rotating window over the fleet.
  `RatePlan` carries the fractional remainder so the long-run average is exact.

The rate is changed **at runtime** so a ramp doesn't restart the pod (which would
reset the fleet and the Kafka producer):

```
curl -XPOST 'localhost:18090/api/load/rate?value=25000'
curl        'localhost:18090/api/load/rate'
```

Pool size caps the per-tick burst: to hold `R` ev/s you need
`drivers ≥ R / 10`. The load overlay sets 20 000.

## Running it

```bash
make load-up            # compose up with docker-compose.load.yml (24 partitions, TM x3 / 8 slots)
make cdc-register
make flink-submit-all
make load-ramp          # scripts/loadtest.sh ramp -> load-testing/results/<ts>.csv
```

`scripts/loadtest.sh ramp` sets each rate in `STEPS`, holds `HOLD` seconds, then
reads Prometheus (`/api/v1/query`) for produced rate, ingest rate, end-to-end
latency p50/p95/p99 (`flowfleet.ingest.latencyMs` — event time → Flink now),
Kafka source lag, busy vs backpressured ms/s, checkpoint duration/size, restarts.
Output is a CSV plus a markdown table.

On Kubernetes: `PROM_URL`/`GEN_URL` env point it at port-forwards, and
`values-load.yaml` turns on the Flink autoscaler + the consumer HPA.

## What to measure and expect

`docs/experiments/phase-8-load-and-tuning.md` — the methodology, the bottleneck
ladder (partitions → source parallelism → sink batch → checkpoint alignment →
state size), the tuning iterations, and a results table.

`docs/interview-questions/phase-8.md` — the numbers and the reasoning.

## `results/`

Committed CSVs + rendered tables from real runs. These are the numbers to quote.
