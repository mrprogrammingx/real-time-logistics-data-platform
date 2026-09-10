# Architecture

## The domain

FlowFleet is a food-delivery marketplace. Three actors:

* **customers** place **orders** against **restaurants**;
* **drivers** (on **vehicles**, working **shifts**) fulfil orders as **deliveries**;
* driver devices emit a continuous stream of **GPS locations**; the platform detects
  **geofence** enter/exit transitions (restaurant pickup zone, customer zone, warehouse).

The interesting engineering is in the movement stream and the delivery lifecycle:
high volume, event-time semantics, out-of-order arrival, and multiple downstream consumers
with very different read patterns.

## Layers

| # | Layer | Technology | Responsibility |
|---|-------|-----------|----------------|
| 1 | Operational store | PostgreSQL + PostGIS | System of record for orders/drivers/deliveries. OLTP. |
| 2 | Change data capture | Debezium + Kafka Connect | Turn row changes into an ordered event stream. |
| 3 | Log / transport | Apache Kafka | Durable, partitioned, replayable event backbone. |
| 4 | Stream processing | Apache Flink (Java) | Stateful enrichment, windows, timers, geofencing, anomaly detection. |
| 5 | Serving stores | TimescaleDB, ClickHouse, BigQuery | Operational time-series, interactive analytics, warehouse. |
| 6 | Observability | Prometheus + Grafana | Lag, throughput, checkpoint health, backpressure. |
| 7 | Platform | Kubernetes, Flink K8s Operator, Helm, ArgoCD | Deploy, scale, upgrade with savepoints, GitOps. See [`deployment.md`](deployment.md). |

## Data flow

```
 write path (OLTP)              stream path                     read path (OLAP / serving)
 ────────────────               ───────────                     ──────────────────────────
 POST /orders ──► orders table ──► Debezium ──► orders.cdc ─┐
 PATCH /status ─► UPDATE ───────► (WAL)     ──► ...          │
                                                            ├─► Flink ─► TimescaleDB  (driver speed, ETA, live ops)
 location-generator ───────────────────────► driver.locations ─┤        ─► ClickHouse   (event-level analytics)
                                             driver.shift-events│        ─► BigQuery     (fact/dim, historical)
                                                                │        ─► *.dlq        (poison messages)
                                                                └─► delivery.alerts (pickup timeout, anomalies)
```

## Why not just PostgreSQL for everything?

| Need | Fits | Doesn't fit |
|------|------|-------------|
| Transactional order writes, FKs, row-level correctness | **PostgreSQL** | ClickHouse (no real updates), BigQuery (no OLTP) |
| "Average speed per driver over the last 5 min", 10k writes/s of GPS | **TimescaleDB** hypertables, continuous aggregates | vanilla PostgreSQL (index bloat, vacuum pressure) |
| "p95 delivery time by city by hour over 90 days", ad-hoc slice/dice | **ClickHouse** MergeTree, columnar, partition + primary-key skip | PostgreSQL (row store, full scans) |
| Company-wide historical facts/dims, joins to finance & marketing data | **BigQuery** (partition + cluster, separation of storage/compute) | ClickHouse (operational, not a lake) |

Each store gets the shape of data it is good at. The stream processor is what fans the
single source of truth out to all of them, consistently.

## Consistency & delivery model

* Kafka + Debezium give **at-least-once** delivery and **per-key ordering** (partitioned by
  entity id).
* Flink provides **exactly-once** *state* via checkpoints.
* End-to-end we run **at-least-once transport + idempotent sinks** — see
  [`idempotency.md`](idempotency.md). Exactly-once end-to-end (Kafka transactions / 2PC
  sinks) is implemented and measured as an explicit experiment, not assumed.

## Module boundaries

* `services/common` has **no framework dependency**. The same `record`s describe a row in
  PostgreSQL, a JSON payload in the API, and (later) an Avro record in Kafka / a POJO in
  Flink. Persistence, JSON and Avro mapping each live in the module that needs them.
* `services/api` owns the schema (Flyway migrations) and is the only writer to PostgreSQL.
* Flink jobs are independent deployables sharing only `common` and the Avro schemas.
