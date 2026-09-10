# Performance — throughput, latency, tuning, autoscaling (Phase 8)

> Status: **implemented**. Load tool: the `location-generator` in rate mode
> (`scripts/loadtest.sh`). Signals: `architecture/failure-recovery.md` runbook +
> the Grafana dashboard. Numbers: `docs/experiments/phase-8-load-and-tuning.md`.

## The throughput model

One `DriverLocation` is ~120 bytes on the wire (Avro + zstd). At the target rates:

| ev/s | MB/s (compressed) | per-partition ev/s at 24 partitions |
|-----:|------------------:|-----------------------------------:|
| 1 000 | ~0.1 | 42 |
| 10 000 | ~1 | 420 |
| 25 000 | ~2.5 | 1 040 |
| 50 000 | ~5 | 2 080 |

Nothing here is bandwidth-bound. The ceilings are all about **parallelism and
per-record work**, and they appear in this order as you turn the rate up.

## The bottleneck ladder

1. **Source partitions.** Flink's `KafkaSource` parallelism is capped at the
   partition count of `flowfleet.driver.locations` (6 by default). Past
   ~6 × (single-subtask rate) the source can't keep up and lag climbs with the
   source operator at `busyTimeMsPerSecond ≈ 1000`.
   → widen the topic (`GENERATOR_LOCATIONS_PARTITIONS`, `values-load.yaml`: 24)
   and raise job parallelism to match.

2. **Deserialize + ingest CPU.** `ConfluentRegistryAvroDeserializationSchema` +
   `LocationIngest` validation is the per-record hot path. Shows as the `ingest`
   operator busy while the source is *not* backpressured.
   → more parallelism; `pipeline.object-reuse: true` (safe here — records aren't
   retained past `processElement`); keep the schema-registry client's cache warm.

3. **Keyed state access (`driver-state`, `anomaly`).** RocksDB get/put per record.
   Bounded by disk IOPS and the managed-memory block cache.
   → `taskmanager.memory.managed.fraction` up (0.4 → 0.5 in the load overlay);
   incremental checkpoints (already on); enough parallelism that each subtask's
   key-group fits the cache.

4. **Sink round-trips (`timescale-sink`, `clickhouse-sink`).** `JdbcSink` flushes
   at 500 rows / 1 s. Under load the sink operator goes busy and backpressures
   everything upstream; checkpoint duration grows (buffers to drain).
   → bigger `JdbcExecutionOptions` batch; `sink.parallelism` up (watch the DB
   `max_connections` — one pool per subtask); unaligned checkpoints (already on)
   keep checkpoint duration bounded while you tune.

5. **Checkpoint size / duration.** As keyed state grows, each checkpoint moves
   more bytes. If duration approaches the interval, back-to-back checkpoints
   starve processing.
   → longer interval under sustained load; incremental + RocksDB; more
   parallelism spreads the state.

Rule from the runbook: the bottleneck is the operator at `busy ≈ 1000 ms/s` with
**every operator upstream of it** showing `backPressuredTimeMsPerSecond > 0`.

## The knobs (one variable at a time)

| Layer | Knob | Effect | Set in |
|---|---|---|---|
| Kafka | `driver.locations` partitions | source parallelism ceiling | `GENERATOR_LOCATIONS_PARTITIONS` |
| Producer | `linger.ms` / `batch.size` / `compression` | fewer, larger requests | `LocationProducer` (20 ms / 64 kB / zstd) |
| Flink | job `parallelism` | throughput ∝ parallelism until the next ceiling | `flink.jobs.<job>.parallelism` |
| Flink | `taskmanager.numberOfTaskSlots` | subtasks per TM | `flink.taskManager.slotsPerTaskManager` |
| Flink | `pipeline.object-reuse` | no per-record copy | `flink.extraConfig` |
| Flink | `taskmanager.memory.managed.fraction` | RocksDB block cache | `flink.extraConfig` / `values-load.yaml` |
| Flink | checkpoint interval | fewer snapshots vs. more replay on failure | `flink.checkpointing.interval` |
| Sink | JDBC batch size / interval | fewer round-trips | `JdbcSinks.EXEC_OPTIONS` |
| Sink | `sink.parallelism` + pool size | concurrent writers | job parallelism / connector opts |

## Autoscaling

**Flink** — the operator's built-in **job autoscaler**
(`flink.autoscaler.enabled`). It reads each operator's true processing rate and
busy time over `metricsWindow`, and rescales parallelism (through a savepoint) to
hold `targetUtilization` (0.6), bounded by `pipeline.max-parallelism` (24, = the
partition count). `stabilizationInterval` and `scaleDownMaxFactor` stop it
thrashing. This is *vertical within the job* — it changes parallelism, not pod
count; the operator adds TaskManagers to fit.

**location-consumer** — a CPU `HorizontalPodAutoscaler` (`locationConsumer.hpa`).
Every replica change triggers a **consumer-group rebalance** (Phase 2), so the
HPA `behavior.scaleDown` has a 120 s stabilization window. "Scale on Kafka lag"
instead of CPU needs `prometheus-adapter` exposing
`kafka_consumergroup_lag` as an external metric — noted, not wired.

**Kafka** — partitions don't scale down and adding them mid-stream rehashes keys.
Size `driver.locations` for the peak up front.

## What Phase 8 does *not* claim

The numbers in the experiment log are from the single-node compose / kind setup
on one machine — they show the *shape* of each ceiling and that a given knob
moves it, not a production capacity figure. A real number needs a real cluster
with real disks and network.
