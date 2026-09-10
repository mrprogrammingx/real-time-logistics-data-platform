# Interview questions — Phase 6 (failure engineering, checkpoints, backpressure, DLQ)

Answerable from `flink/…/ingest/`, `flink/…/sink/SlowMap`, `scripts/chaos.sh`, the
`monitoring/` stack, and the Phase 6 experiment log.

## Checkpoints & recovery

* **What happens when a TaskManager dies mid-processing?** The job goes
  `RUNNING → RESTARTING → RUNNING`; all operator + keyed state is restored from the **last
  completed checkpoint**; the Kafka source rewinds to the offsets *stored in that
  checkpoint* (not the offsets committed to Kafka). Everything between that checkpoint and
  the crash is **reprocessed** ⇒ downstream sees duplicates ⇒ the idempotent sinks absorb
  them (`ON CONFLICT DO NOTHING` / `ReplacingMergeTree`). Net effect: exactly-once *as
  observed*.
* **Checkpoint vs savepoint?** Checkpoint = automatic, owned by Flink, for failure
  recovery, can be incremental. Savepoint = manual, self-contained, for upgrades / rescale
  / migration between clusters.
* **Aligned vs unaligned checkpoints?** Aligned: a barrier waits at each operator for all
  input channels to reach it — under backpressure the alignment stalls and the checkpoint
  can time out. **Unaligned** (enabled here): the barrier overtakes buffered data, which is
  snapshotted as part of the checkpoint — checkpoint duration stays bounded under
  backpressure, at the cost of a larger checkpoint.
* **`RETAIN_ON_CANCELLATION`** so a `flink cancel` keeps the last checkpoint to resume from.

## Backpressure

* **Diagnose it:** `busyTimeMsPerSecond` ≈ 1000 on operator X and every operator *upstream*
  of X shows `backPressuredTimeMsPerSecond` > 0 ⇒ X is the bottleneck. Grafana's
  Backpressure panel; Flink UI's backpressure tab.
* **The chain:** slow sink → its input buffers fill → credit-based flow control stops
  granting credits upstream → the source slows → Kafka consumer lag climbs → (aligned)
  checkpoint alignment stalls.
* **Fixes, cheapest first:** bigger sink batch / fewer round-trips; `sink.parallelism` up
  (watch DB connection limits); async I/O for enrichment lookups; unaligned checkpoints;
  more Kafka partitions + source parallelism. `SlowMap` (`--slow-map-ms`) makes the
  experiment reproducible.

## Poison messages / DLQ

* **Where can a bad record kill the job?** In the deserializer, before any user code. A
  `SerializationException` out of `KafkaRecordDeserializationSchema.deserialize` fails the
  task → restart loop.
* **The fix:** `ResilientLocationDeserializer` catches it and emits a `ParsedLocation` with
  `error` + the raw bytes + Kafka `partition`/`offset` instead of throwing. `LocationIngest`
  routes `error != null` (and semantically-invalid-but-decoded) records to
  `flowfleet.driver.locations.dlq`; two counters (`flowfleet.ingest.valid` / `.dlq`) make
  the split a Grafana panel.
* **What goes in a DLQ record?** Enough to act on it: reason, error, source topic +
  partition + offset, and (for decode failures) the truncated raw bytes.

## Duplicates & out-of-order

* **Duplicates** come from: Debezium connector restart, Flink checkpoint recovery, producer
  resend. Handled by deterministic `event_id`s + idempotent sink writes — proven by
  `TimescaleSinkIT` / `ClickHouseSinkIT` (write, replay, assert one row).
* **Out-of-order** within `forBoundedOutOfOrderness(5s)` → placed in the correct event-time
  window. Past the watermark → `sideOutputLateData` → counted, routed to the DLQ, **never
  dropped**. `WatermarkLatenessIT` proves it with a source that emits watermarks under test
  control.

## Observability

* Flink → Prometheus reporter (`metrics.reporter.prom`) on :9250 (JM) / :9251 (TM),
  scraped by Prometheus (:19090), shown in Grafana (:13000). Key series: `numRecordsInPerSecond`,
  `busyTimeMsPerSecond`, `backPressuredTimeMsPerSecond`, `lastCheckpointDuration`,
  `lastCheckpointSize`, `numRestarts`, `KafkaConsumer_records_lag_max`.

## The 3 a.m. question: "Kafka lag jumped 10×"

`lag → consumer/source throughput → Flink busy vs backpressured (which operator?) →
checkpoint duration/size → RocksDB / state size → sink p99 latency → DB CPU / connections /
locks → partition skew (one hot key?) → TM CPU throttling / GC`. The bottleneck is wherever
`busy ≈ 1000` with everything upstream `backPressured`.
