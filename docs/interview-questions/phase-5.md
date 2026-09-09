# Interview questions — Phase 5 (sinks: batching, idempotency, OLTP vs OLAP)

Answerable from `flink/src/main/java/com/flowfleet/flink/sink/`, `database/timescaledb/`,
`database/clickhouse/`.

## Why two databases (and not one)

| Need | Store | Why not the other |
|------|-------|-------------------|
| "latest position per driver", "speed over the last 5 min", 10k writes/s of time-series | **TimescaleDB** — hypertables auto-partition by time, continuous aggregates, still SQL/Postgres | plain Postgres: index bloat + vacuum pressure; ClickHouse: no cheap point lookups / updates |
| "p95 geofence dwell by zone by hour over 90 days", ad-hoc slice-and-dice over billions of rows | **ClickHouse** — columnar, `PARTITION BY` + primary-key skip index, vectorised scans | Timescale/Postgres: row store, full scans; poor compression |

OLTP (PostgreSQL, Phase 1) is the *system of record*; these are *derived, disposable* read
models — rebuildable by replaying Kafka.

## Sink tuning (`JdbcSinks.java`)

* **Batching** — `JdbcExecutionOptions`: flush at **500 rows or 1 s**, whichever first. One
  round-trip per batch instead of per row. The interval bounds latency when the stream is slow.
* **Connection pool** — the Flink JDBC sink holds **one connection per sink subtask** for
  the life of the operator; parallelism = number of writers. No per-record connect.
* **Retries** — 3 attempts per failed batch. Safe here *because the writes are idempotent*
  (a half-applied batch re-applied is a no-op).
* **Backpressure** — a slow sink fills its input buffers → backpressure propagates to the
  source → Kafka lag climbs. Measured in Phase 6.

## Idempotency — two mechanisms

* **TimescaleDB** — the primary key includes the event's own timestamp
  (`driver_state (driver_id, event_time)`, `driver_speed_windows (driver_id, window_start)`),
  and the write is `INSERT ... ON CONFLICT (pk) DO NOTHING`. A Flink replay from a
  checkpoint re-inserts the *same* `(driver_id, event_time)` → conflict → no-op. "Current
  state" is a query (`driver_current_state` = `DISTINCT ON (driver_id) ... ORDER BY
  event_time DESC`), not a mutable row — the Timescale-idiomatic append-only pattern.
* **ClickHouse** — no `ON CONFLICT`. `ReplacingMergeTree(ingested_at)` with `ORDER BY
  (event_id)` collapses rows sharing an `event_id` at **merge time**, keeping the latest
  `ingested_at`. A replay re-inserts the same `event_id` and it is eventually deduplicated;
  reads use `FINAL` (or `GROUP BY event_id`) for exact dedup on demand.
* The `event_id`s themselves are **deterministic** (Phase 4: `sha256(driverId | geofenceId |
  transition | eventTime)`), so a reprocessed input produces the same id — without that,
  dedup fails.

## What "the same record twice" must never do

Two rows for one geofence transition; a double-counted metric; a status moved backwards by
a stale replay; a duplicate alert. `TimescaleSinkIT` / `ClickHouseSinkIT` write a batch,
replay the identical batch, and assert the row count / `FINAL` count is unchanged.

## Delivery semantics recap

At-least-once transport (Kafka, Debezium, Flink→sink) + **idempotent sinks** = the same
observable result as exactly-once, without Kafka transactions / 2-phase-commit latency.
Exactly-once end-to-end is implemented as an experiment in Phase 6 and its cost measured.

## Not yet built

BigQuery (`fact_*` / `dim_*`, `MERGE` on `event_id` or Storage Write API exactly-once) —
needs GCP credentials; deferred to the cloud-deploy step. A sink-side DLQ
(`flowfleet.sink.dlq`) for rows a sink rejects (constraint violation, type error).
