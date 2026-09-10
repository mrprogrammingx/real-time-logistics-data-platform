# Interview questions — Phase 8 (load testing, tuning, autoscaling)

Answerable from `services/location-generator` (rate mode), `scripts/loadtest.sh`,
`architecture/performance.md`, and a real ramp in `load-testing/results/`.

## Load generation

* **How do you generate a controlled 50k ev/s?** The generator has a rate mode: a
  fixed 100 ms tick emits `RatePlan.eventsThisTick(rate, 100)` samples — the
  fractional remainder is carried so the long-run average is exact — by advancing
  a rotating window over the fleet. Pool size caps the burst: `drivers ≥ rate/10`.
* **Why change the rate over HTTP instead of an env var + restart?** A restart
  resets the fleet RNG and drops the Kafka producer's in-flight batches — you'd
  measure the warm-up, not steady state. `POST /api/load/rate` changes it live.
* **How do you know a step actually held?** `produced ≈ ingested` and Kafka source
  lag flat (not climbing) for the whole hold. Climbing lag = the pipeline is
  behind and the number is a lie.

## Measuring latency

* **What latency, measured where?** Event time (device stamp = wall clock at
  emit) → the moment Flink's `LocationIngest` processes it. A
  `DescriptiveStatisticsHistogram` metric (`flowfleet.ingest.latencyMs`), exposed
  by the Prometheus reporter as `..._latencyMs{quantile="0.99"}`.
* **Why clamp negative values to 0?** The device clock can be a hair ahead of the
  TaskManager clock; a negative "latency" is noise, not signal.
* **Why not measure all the way to the database row?** You could (stamp carried
  through to the sink), but ingest latency is where backlog shows up first and it
  needs no schema change. Producer→broker is on the generator's Kafka metrics;
  Flink→DB is the JDBC batch flush interval. Add them for end-to-end.
* **Aggregating percentiles across subtasks** — `max(...{quantile="0.99"})` is a
  p99-of-p99s, an upper bound. A true global p99 needs the raw observations
  (client-side histogram merge, or a single-subtask probe).

## Bottlenecks — the order they appear

1. **Source partitions** cap `KafkaSource` parallelism. Symptom: source
   `busy ≈ 1000`, source *not* backpressured, lag climbing.
2. **Deserialize + validate CPU** in the `ingest` operator.
3. **RocksDB** get/put on the keyed jobs — disk IOPS + block cache.
4. **JDBC sink round-trips** — sink busy, everything upstream backpressured,
   checkpoint duration up.
5. **Checkpoint size/duration** as state grows — approaching the interval starves
   processing.

The bottleneck is the operator at `busy ≈ 1000` with **every upstream operator**
`backPressured`.

## Tuning

* **First move for a partition-capped source?** More partitions on
  `driver.locations` + matching job parallelism. Adding partitions mid-stream
  rehashes keys, so size for peak up front.
* **Sink is the bottleneck — options, cheapest first?** Bigger JDBC batch (fewer
  round-trips) → `sink.parallelism` up (one connection pool per subtask — watch
  `max_connections`) → async I/O for any enrichment lookups.
* **`pipeline.object-reuse: true` — safe here?** Yes: no operator retains a record
  reference past `processElement`. It skips a per-record defensive copy.
* **Checkpoint duration creeping toward the interval — why bad, what do you do?**
  Back-to-back checkpoints starve processing and alignment stalls. Lengthen the
  interval under sustained load; incremental + RocksDB; more parallelism to spread
  state. Unaligned checkpoints keep *duration* bounded while you tune the rest.
* **Where does `linger.ms` help?** Producer side — 20 ms lets the generator batch
  under load, trading a few ms of latency for far fewer produce requests.

## Autoscaling

* **Flink autoscaler — what does it scale on?** Per-operator true processing rate
  vs. busy time over a metrics window; it rescales *parallelism* (via a savepoint)
  to hold `target.utilization` (0.6), bounded by `pipeline.max-parallelism`
  (= partition count). It adds TaskManagers to fit; it does not scale pods
  directly.
* **How does it avoid thrash?** `stabilization.interval` (no change is
  re-evaluated for that long), `scale-down.max-factor` (0.5 — halve at most), a
  utilization boundary band so small drifts don't trigger.
* **Why is autoscaling a Flink job harder than autoscaling a stateless service?**
  Every rescale is a stop-with-savepoint + redistribute key-groups + restore —
  seconds of downtime and state movement. You scale on sustained signal, not
  spikes, and you cap the frequency.
* **Consumer HPA + Kafka?** Every replica change rebalances the consumer group
  (partitions reassigned, brief pause). The HPA `scaleDown.stabilizationWindow` is
  120 s so it doesn't rebalance on every dip. Scaling on lag (not CPU) needs
  `prometheus-adapter` to expose `kafka_consumergroup_lag` as an external metric.
* **Can you autoscale Kafka partitions?** Not really — no scale-down, and adding
  them rehashes the key→partition mapping (breaks per-key ordering for in-flight
  keys). Provision for peak.

## The number to quote

"On a single-node setup it holds `N`k ev/s at p99 `X` ms after widening the topic
to 24 partitions, batching the sinks, and giving RocksDB more managed memory; the
source was the first ceiling at ~10k, the JDBC sinks the second at ~25k." — with
the CSV to back it. The *shape* of the ceilings and which knob moved each is the
transferable part; the absolute number is machine-specific.
