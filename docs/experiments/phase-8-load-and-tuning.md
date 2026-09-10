# Experiment: load, bottlenecks, tuning, autoscaling

**Setup:**

```bash
make load-up                       # docker-compose.yml + docker-compose.load.yml
make cdc-register
make flink-submit-all
# Grafana http://localhost:13000/d/flowfleet-flink  (Throughput + Latency panels are Phase 8)
```

Then ramp:

```bash
make load-ramp                     # STEPS="1000 5000 10000 25000 50000", HOLD=180s
#   -> load-testing/results/<ts>.csv + a markdown table
```

`loadtest.sh` POSTs each rate to `/api/load/rate`, holds, then reads Prometheus.

## Method

* One variable at a time. Re-run the ramp after each change; keep the CSV.
* Read the bottleneck off Grafana, don't guess: the operator at
  `busyTimeMsPerSecond ≈ 1000` with everything upstream `backPressured` is it.
* "Held the rate" = `produced ≈ ingested` and `src_lag` flat (not climbing) for
  the whole hold.

## The ramp (baseline → tuned)

### Iteration 0 — baseline (`values.yaml` defaults, 6 partitions, parallelism 2)

Expected: holds to ~8–10k ev/s, then the **source** saturates (6 partitions ×
~1.5k/subtask). `src_lag` climbs; source `busy ≈ 1000`; ingest *not* backpressured.

| target | produced/s | ingested/s | e2e p50 | e2e p99 | src_lag | busy | backpressure | ckpt ms | restarts |
|-------:|-----------:|-----------:|--------:|--------:|--------:|-----:|-------------:|--------:|---------:|
| 1 000  |            |            |         |         |         |      |              |         |          |
| 5 000  |            |            |         |         |         |      |              |         |          |
| 10 000 |            |            |         |         |         |      |              |         |          |
| 25 000 |            |            |         |         |         |      |              |         |          |
| 50 000 |            |            |         |         |         |      |              |         |          |

### Iteration 1 — widen the topic (24 partitions) + parallelism 4 (`values-load.yaml`)

Expected: source ceiling gone; next stop is the **JDBC sinks** around 20–30k —
sink operator busy, upstream backpressured, checkpoint duration up.

### Iteration 2 — sink batch 500→2000, `sink.parallelism` 4, `pipeline.object-reuse: true`

Expected: sinks keep up to ~40k; **RocksDB / checkpoint size** on `driver-state`
becomes the limiter as state grows — checkpoint duration approaches the 30 s
interval.

### Iteration 3 — `managed.fraction` 0.4→0.5, checkpoint interval 30s→60s

Expected: holds 50k with p99 latency bounded (single-node — the real ceiling is
disk IOPS for RocksDB and the one Postgres/ClickHouse container).

## Autoscaling

### Flink autoscaler

```bash
helm upgrade flowfleet ./helm/flowfleet -f helm/flowfleet/values.yaml -f helm/flowfleet/values-load.yaml
# or on compose: it's operator-only, so this part is the k8s path
kubectl -n flowfleet get flinksessionjob -w        # parallelism changes as load ramps
```

Ramp 5k → 50k → 5k and watch `job.autoscaler` move each job's parallelism to hold
`targetUtilization: 0.6`, savepoint-and-restore each time, then scale back down
after `stabilizationInterval`. Confirm exactly-once held: `make timescale-peek`
distinct `(driver_id, event_time)` count is monotonic, no gaps.

### Consumer HPA

```bash
kubectl -n flowfleet get hpa flowfleet-location-consumer -w
```

At 50k ev/s the consumers pass 70% CPU and the HPA adds replicas; each addition
logs a `ConsumerRebalanceListener` partition move (Phase 2). Scale-down waits out
the 120 s stabilization window so it doesn't rebalance on every tick.

## Results (fill in from your run)

```
date / machine:
baseline ceiling:        ___ k ev/s   (bottleneck: ______)
+ 24 partitions / par 4: ___ k ev/s   (bottleneck: ______)
+ sink batch / parallel: ___ k ev/s   (bottleneck: ______)
+ rocksdb / ckpt tuning: ___ k ev/s   (p50 ___ ms  p99 ___ ms)
autoscaler: 5k->50k took ___ s to converge, ___ savepoint-restarts, 0 row-count drift
```
