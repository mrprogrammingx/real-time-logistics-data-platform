# Idempotency & delivery guarantees

> Status: **implemented** (Phase 5) for the TimescaleDB + ClickHouse sinks
> (`flink/src/main/java/com/flowfleet/flink/sink/`). `TimescaleSinkIT` and
> `ClickHouseSinkIT` replay an identical batch and assert one logical row survives.
> Exactly-once via Kafka transactions, and BigQuery, remain design.

## Posture

**At-least-once transport + idempotent sinks.** Every hop before the sink may deliver a
record more than once (connector restart, Flink recovery from checkpoint, producer retry).
The sinks are built so that applying the same logical record twice has the same effect as
applying it once.

Exactly-once end-to-end (Kafka transactions + 2-phase-commit sinks) is implemented for the
`delivery.events` path as an experiment and its latency/throughput cost is measured — but
it is not the default, because idempotent sinks get the same *observable* result more
cheaply.

## The idempotency key

Every record that reaches a sink carries a stable `event_id`:

| Origin | `event_id` |
|--------|-----------|
| CDC-derived | `sha256(table \| pk \| lsn)` — deterministic from the WAL position |
| simulator location | `driver_id \| eventTime` (device is the authority; a resend is the same key) |
| Flink-derived event | `sha256(inputEventId \| jobName \| outputType)` — deterministic function of its cause |
| Flink window result | `key \| windowStart \| windowEnd \| metric` |

Deterministic keys matter: after a Flink restart the *same* input is reprocessed and must
produce the *same* `event_id`, or dedup fails.

## Per-sink strategy

### PostgreSQL / TimescaleDB
```sql
INSERT INTO delivery_events (event_id, order_id, type, payload, event_time)
VALUES (:event_id, :order_id, :type, :payload, :event_time)
ON CONFLICT (event_id) DO NOTHING;
```
* `event_id` is the PK (or a `UNIQUE` constraint).
* Batched with `reWriteBatchedInserts=true`; the whole batch is one transaction.
* For "latest state" tables (e.g. `driver_current_state`) use `ON CONFLICT (driver_id) DO
  UPDATE ... WHERE excluded.event_time > driver_current_state.event_time` — a
  **conditional upsert** so an out-of-order replay can't move state backwards.

### ClickHouse
No `ON CONFLICT`. Options, in order of preference:
* **`ReplacingMergeTree(version)`** keyed by `event_id`, `version = ingest_time` or `lsn`.
  Duplicates collapse at merge time; queries use `FINAL` or aggregate to dedup on read.
* **`INSERT ... DEDUPLICATE`** / `insert_deduplication_token = event_id` — ClickHouse keeps
  a window of recent insert-block hashes and drops exact re-inserts. Good for the
  Flink-retry case (same block re-sent).
* Partition by `toYYYYMMDD(event_time)`, order by `(type, order_id, event_id)`.

### BigQuery
* Stream into a staging table, then **`MERGE`** into the fact table on `event_id` on a
  schedule; or use the Storage Write API with `CreateWriteStream` exactly-once mode.
* Fact tables partitioned by `DATE(event_time)`, clustered by `(city, restaurant_id)`.

### Kafka (Flink → Kafka)
* `DeliveryGuarantee.AT_LEAST_ONCE` + downstream consumers dedup on `event_id`, **or**
* `DeliveryGuarantee.EXACTLY_ONCE` with `transactional.id` per subtask and
  `transaction.timeout.ms` > checkpoint interval + max downtime.

## What "the same record twice" must never do

* create two rows for one delivery
* double-count a metric in a window aggregate
* move a status backwards (DELIVERED → OUT_FOR_DELIVERY) because a stale replay arrived late
* fire the same alert twice (`delivery.alerts` consumers dedup on `event_id`)

## Test (Phase 6)

Re-publish a batch of `driver.locations` with duplicated offsets; kill a TaskManager
mid-window; assert row counts and metric values in every sink are identical to the
single-delivery baseline.
