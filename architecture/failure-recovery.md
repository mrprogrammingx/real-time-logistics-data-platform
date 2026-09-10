# Failure recovery & the experiments

> Status: **implemented** (Phase 6).
> - Poison messages: `flink/…/ingest/ResilientLocationDeserializer` + `LocationIngest` —
>   a bad record → DLQ with its Kafka offset, never a job failure. `LocationIngestTest`.
> - Late data: `WatermarkLatenessIT` proves a sample past the watermark → side output.
> - Idempotent replay: `TimescaleSinkIT` / `ClickHouseSinkIT` (Phase 5).
> - Cluster-level chaos (kill TaskManager, slow sink, replay): `scripts/chaos.sh` /
>   `make chaos-*`, walkthrough in `docs/experiments/phase-6-chaos.md`.
> - Metrics: Flink → Prometheus (:19090) → Grafana (:13000, "FlowFleet — Flink").

## 1. TaskManager failure

**Do:** `docker kill` / `kubectl delete pod` a Flink TaskManager mid-processing.

**Expect:**
* job transitions `RUNNING → RESTARTING → RUNNING`
* restores operator + keyed state from the **last completed checkpoint**
* Kafka source rewinds to the offsets stored *in that checkpoint* (not Kafka-committed ones)
* records between the last checkpoint and the crash are **reprocessed** → sinks see
  duplicates → idempotent sinks absorb them ([`idempotency.md`](idempotency.md))

**Measure:** downtime, number of reprocessed records, sink row counts vs. baseline (must
match), `lastCheckpointRestoreTimestamp`.

## 2. Slow sink → backpressure

**Do:** wrap a sink in `Thread.sleep(N ms)` per batch (or point ClickHouse at a throttled
network).

**Expect the chain:**
```
sink busyTimeMsPerSecond → 1000
   └─ output buffers exhausted
      └─ backPressuredTimeMsPerSecond rises on upstream operators
         └─ source emits slower
            └─ Kafka consumer lag (records-lag-max) climbs
               └─ checkpoint duration grows (buffer alignment); may hit checkpoint timeout
```

**Fix and re-measure (one variable at a time):**
| Lever | Effect |
|-------|--------|
| sink micro-batching (size/time) | fewer round-trips, higher throughput |
| `sink.parallelism` ↑ | more concurrent writers (watch DB connection limits) |
| connection pool size | removes a hidden serialization point |
| `AsyncFunction` for enrichment lookups | overlaps I/O latency |
| unaligned checkpoints | checkpoint no longer waits for buffers to drain |
| more source partitions + parallelism | more ingest headroom |

## 3. Malformed events → DLQ

**Do:** inject `{"driver_id": null, "latitude": 999, "longitude": 200}` and non-JSON bytes.

**Expect:** main pipeline unaffected; bad records on `flowfleet.driver.locations.dlq` with
`{ original, error, sourceTopic, partition, offset, ts }`. DLQ depth is a Grafana panel and
an alert.

## 4. Duplicate events

**Do:** re-produce a range of offsets; independently, force a checkpoint-recovery replay.

**Expect:** every sink ends at the same row counts / metric values as the
single-delivery baseline. One logical delivery = one row.

## 5. Out-of-order / late events

**Do:** shuffle event timestamps within a 10 s band; also send one sample 60 s late.

**Expect:**
* samples within `forBoundedOutOfOrderness(5s)` → placed in the correct event-time window
* the 60 s-late sample → `late-locations` side output, counted, **not** silently dropped
* window results are deterministic across re-runs

## 6. CDC connector restart

**Do:** restart the Debezium connector / Kafka Connect worker; separately, stop it for
10 min then resume.

**Expect:** resumes from last committed LSN; replication slot held WAL during the outage;
no gap in `source.lsn`; downstream idempotent consumers absorb the replayed tail. Alert if
`pg_replication_slots` lag crosses a threshold.

## 7. Kafka broker loss

**Do:** kill one of three brokers.

**Expect:** partitions with `replication.factor=3, min.insync.replicas=2` stay available;
leader election moves leadership; producers with `acks=all` block briefly then continue;
Flink source re-connects. `UnderReplicatedPartitions` > 0 is an alert.

---

## "Kafka lag suddenly increased 10×" — the runbook

1. **Lag** — which topic/partitions? uniform or skewed (hot key)?
2. **Consumer throughput** — `records-consumed-rate` dropped, or input rate spiked?
3. **Flink** — `busyTimeMsPerSecond` (CPU-bound?) vs `backPressuredTimeMsPerSecond`
   (downstream stall?), per operator.
4. **Checkpoints** — duration/size trending up? failures? → state or alignment problem.
5. **State** — RocksDB size, compaction stalls, disk IOPS.
6. **Sink** — sink latency p99, DB CPU / connections / lock waits / autovacuum.
7. **Partitions** — assignment balanced? one subtask doing all the work?
8. **Resources** — TM CPU throttling, GC pauses, network saturation.

Bottleneck is wherever `busy` ≈ 1000 ms/s and everything upstream is `backPressured`.
