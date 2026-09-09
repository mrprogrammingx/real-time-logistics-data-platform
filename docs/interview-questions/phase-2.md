# Interview questions — Phase 2 (Kafka, producers/consumers, Avro, Schema Registry)

Answerable from this phase's code (`services/events`, `services/location-generator`,
`services/location-consumer`, `docker-compose.yml`).

## Topics & partitioning

* **Why partition at all?** Parallelism (one consumer per partition per group) and ordered
  sub-streams. **Why key `driver.locations` by `driver_id`?** So every sample for one
  driver lands on one partition in order — a single Flink subtask can then hold that
  driver's keyed state and see a monotonic track. See `Topics.java` / `kafka-design.md`.
* **What's the cost of that key?** A hot driver = a hot partition. Fine here (~1 msg/s per
  driver); if not, the key becomes `driver_id + time-bucket` and you relax ordering.
* **Why can't you just raise the partition count later?** `hash(key) % N` changes for
  existing keys → ordering guarantee per key is broken across the change. Partition counts
  are chosen once (`driver.locations` = 6 locally, 24 in prod).
* **`auto.create.topics.enable=false`** — why: stops typos silently creating 1-partition,
  wrongly-configured topics. Topics are created explicitly (`TopicAdmin` / `scripts/kafka-topics.sh`).

## Producer

* **`acks=all` + `enable.idempotence=true`** — `acks=all` waits for the ISR; idempotence
  gives each producer a PID + sequence number so a retv does not append a duplicate. Together:
  no loss on broker failover, no dupes from retries. (App-level resend after a full crash is
  still possible → sinks stay idempotent.)
* **`linger.ms=20` / `batch.size=64KB` / `compression=zstd`** — batch under load; zstd is
  the best ratio/CPU trade-off for JSON-ish Avro. `linger` trades latency for throughput.
* **`max.in.flight` with idempotence** — ≤5 is safe (broker dedupes/reorders within the
  window); >5 with idempotence is rejected.

## Consumer group & rebalancing

* **What triggers a rebalance?** A member joins/leaves/times out (`session.timeout.ms`,
  `max.poll.interval.ms`), or partition count changes.
* **`CooperativeStickyAssignor` vs `RangeAssignor`** — cooperative rebalances revoke only
  the partitions that must move, so the rest keep consuming (incremental, no stop-the-world).
* **`enable.auto.commit=false` + `commitSync()` after processing** — at-least-once: an
  offset is only committed for work that's actually done. Commit in `onPartitionsRevoked`
  before losing a partition.
* **Experiment (`make rebalance-demo`):** 6 partitions, 3 consumers → 2 each; kill one →
  its 2 partitions reassigned to the survivors (3+3). The `REBALANCE +/-` lines in
  `LocationConsumerLoop`'s log are the evidence.
* **Why does Flink not use the consumer group for correctness?** It assigns partitions
  itself and stores offsets in checkpoints, so recovery is exact regardless of Kafka-side
  commits.
* **Consumer lag** = `log-end-offset − committed-offset` per partition. `make lag`.

## Avro & Schema Registry

* **What's on the wire?** `magic byte | schema-id (4B) | avro binary`. Not the schema —
  consumers fetch + cache it by id.
* **Subject strategy** — `TopicNameStrategy` → `<topic>-value`. Alternatives: record-name,
  topic-record-name (for multi-type topics).
* **Compatibility modes** — BACKWARD (default; new reader reads old data), FORWARD (old
  reader reads new data), FULL (both), plus `*_TRANSITIVE`. We run **FULL**.
* **`DriverLocation` v1 → v2** — added 3 `["null",T]` fields with `default: null` ⇒ FULL
  compatible. `default` lets a v2 reader synthesize the field when reading v1 bytes, and a
  v1 reader skip it in v2 bytes. Breaking changes: no-default field, field removal, type
  narrowing, rename without alias. Proven by `DriverLocationSchemaEvolutionTest`.
* **`specific.avro.reader=true`** — deserialize into the generated `DriverLocation` class
  vs a generic `GenericRecord`.
* **Why Redpanda in tests but Confluent in compose?** Redpanda bundles a Kafka API + a
  Confluent-compatible Schema Registry in one container → fast, simple ITs. The deployed
  stack uses the real cp-kafka + cp-schema-registry the JD asks about.

## The simulator

* Deterministic for a seed (per-driver `RandomGenerator` seeded from `seed·k + id`), so a
  run is reproducible. Drives restaurant → customer routes on a haversine sphere
  (`GeoPoint.destination` / `moveToward`), matched to the geofences the API seeds, so
  Phase 4's geofencing job will see real ENTER/EXIT transitions.
* Throughput knob: `GENERATOR_DRIVERS` × (1 / `GENERATOR_TICK`). `GENERATOR_SPEEDUP` runs
  simulated time faster than wall time without changing the message rate.
