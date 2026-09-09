# Kafka design

> Status: **design** (implemented in Phase 2). This document is the contract the producers,
> Debezium, and the Flink jobs are built against.

## Topics

| Topic | Source | Key | Partitions | Retention | Cleanup | Notes |
|-------|--------|-----|-----------:|-----------|---------|-------|
| `flowfleet.orders.cdc` | Debezium `public.orders` | `order_id` | 12 | 7d | delete | full before/after row images |
| `flowfleet.drivers.cdc` | Debezium `public.drivers` | `driver_id` | 12 | 7d | delete | |
| `flowfleet.deliveries.cdc` | Debezium `public.deliveries` | `order_id` | 12 | 7d | delete | keyed by order so an order + its delivery co-partition |
| `flowfleet.shifts.cdc` | Debezium `public.driver_shifts` | `driver_id` | 12 | 7d | delete | |
| `flowfleet.driver.locations` | `location-generator` | `driver_id` | 24 | 24h | delete | highest volume; short retention |
| `flowfleet.driver.geofence-events` | Flink geofencing job | `driver_id` | 12 | 7d | delete | ENTER/EXIT transitions |
| `flowfleet.delivery.events` | Flink delivery-analytics | `order_id` | 12 | 30d | delete | business-level lifecycle events |
| `flowfleet.delivery.alerts` | Flink (timers, anomaly) | `order_id` | 6 | 30d | delete | pickup timeout, impossible speed, GPS gap |
| `flowfleet.driver.locations.dlq` | Flink ingestion side-output | original key | 6 | 14d | delete | poison messages + error metadata |
| `flowfleet._schema-changes.flowfleet` | Debezium | n/a | 1 | infinite | delete | connector schema history |

Naming: `flowfleet.<domain>.<kind>`. `.cdc` = raw Debezium, `.events` = business events,
`.alerts` = actionable, `.dlq` = dead letters.

## Partition keys — why these

Events for one entity must be **totally ordered** and land on **one partition** so a single
Flink subtask sees them in order and can hold keyed state for that entity.

```
driver-8392 :  loc → loc → loc → GEOFENCE_ENTER(restaurant) → PICKED_UP → loc → GEOFENCE_ENTER(customer) → DELIVERED
              └──────────────────────── all on partition hash(8392) % N ────────────────────────┘
```

* driver-scoped topics → **`driver_id`**
* order-scoped topics → **`order_id`**
* `deliveries.cdc` is keyed by **`order_id`** (not `delivery_id`) so that an order and its
  delivery are co-partitioned and joinable without a network shuffle.

Trade-off: a hot driver/order is a hot partition. Acceptable here (per-entity rate is
~1 msg/s); if it weren't, the key would become `driver_id` + time-bucket and ordering
guarantees would be relaxed accordingly.

## Partition count

`driver.locations` at 10k drivers × 1 msg/s = 10k msg/s. 24 partitions ⇒ ~400 msg/s each,
comfortable headroom, and it caps Flink source parallelism at 24 (enough for the target
load). CDC topics are low-volume; 12 partitions is future headroom, not a current need.
Partition count can only go **up**, and increasing it breaks key→partition stability for
existing keys — so these are deliberately chosen once.

## Delivery semantics

| Hop | Guarantee | Mechanism |
|-----|-----------|-----------|
| Debezium → Kafka | at-least-once | connector offset commit after produce; duplicates possible on restart |
| `location-generator` → Kafka | at-least-once | `acks=all`, `enable.idempotence=true` (no dupes from retries), but app-level resend on crash still possible |
| Kafka → Flink source | exactly-once *into Flink state* | offsets stored in Flink checkpoints, not committed to Kafka for correctness |
| Flink → Kafka sink | configurable | `AT_LEAST_ONCE` (default) or `EXACTLY_ONCE` (Kafka transactions, `transaction.timeout.ms` > checkpoint interval) |
| Flink → JDBC/ClickHouse sink | at-least-once + idempotent | see [`idempotency.md`](idempotency.md) |

Default posture: **at-least-once transport, idempotent sinks.** Exactly-once via Kafka
transactions is enabled per-job where the extra latency is justified, and the throughput
cost is measured in Phase 6.

## Consumer groups & rebalancing (Phase 2 experiment)

* 6 partitions, 3 consumers in a group ⇒ 2 partitions each.
* Kill one consumer ⇒ group coordinator triggers a rebalance ⇒ its 2 partitions reassigned
  (cooperative-sticky assignor ⇒ only the moved partitions pause).
* Observe: `rebalance` count, partition assignment, consumer lag spike + recovery.
* This is why Flink does **not** use the Kafka consumer group for correctness — it manages
  partition assignment itself and stores offsets in checkpoints.

## Producer config baseline

```
acks=all
enable.idempotence=true
max.in.flight.requests.per.connection=5
compression.type=zstd
linger.ms=20
batch.size=64KB
```

## Schema Registry

All non-CDC payloads are **Avro** with a Confluent Schema Registry, compatibility
`BACKWARD` (new schema can read old data). `DriverLocation` v1 → v2 (adds nullable
`speed`, `heading`) is the worked schema-evolution example — see
[`../schemas/`](../schemas/) once Phase 2 lands.
