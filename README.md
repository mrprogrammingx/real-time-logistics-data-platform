# FlowFleet — Real-Time Logistics Data Platform

A production-style streaming platform for a fictional delivery company (FlowFleet), built
to exercise the exact stack a senior streaming/data engineer works with day to day:

> **PostgreSQL → Debezium (CDC) → Kafka → Apache Flink → TimescaleDB / ClickHouse / BigQuery → Grafana**

with Avro + Schema Registry, idempotent sinks, a dead-letter queue, checkpoint/savepoint
recovery, backpressure experiments, Kubernetes + the Flink Kubernetes Operator, Helm,
ArgoCD, and Prometheus/Grafana observability.

It is built **incrementally**. Each phase produces working code, tests, architecture notes,
and a set of interview questions it lets you answer from experience.

---

## Target architecture

```
                 OPERATIONAL SYSTEMS                      ANALYTICS / SERVING
          ┌──────────────────────────┐
          │ PostgreSQL + PostGIS     │
          │  customers  orders       │
          │  drivers    deliveries   │
          │  shifts     geofences    │
          └───────────┬──────────────┘
                      │ Debezium CDC (WAL, logical replication)
                      ▼
                 ┌─────────┐        high-volume events (10k+/s)
                 │  Kafka  │◀──────────────  location-generator
                 │         │
                 │ *.cdc   │
                 │ driver.locations
                 │ driver.geofence-events
                 │ *.events / *.dlq
                 └────┬────┘
                      │
                 ┌────▼─────────────┐
                 │   Apache Flink   │  keyed state · event time · watermarks
                 │                  │  windows · timers · side outputs
                 │  driver-state    │  checkpoints · savepoints
                 │  geofencing      │
                 │  delivery-analytics
                 │  anomaly-detection
                 └───┬────┬────┬────┘
                     │    │    │
            ┌────────▼─┐ ┌▼────────┐ ┌▼─────────┐        ┌──────────┐
            │Timescale │ │ClickHouse│ │ BigQuery │───────▶│ Grafana  │
            │   DB     │ │          │ │          │        │  / APIs  │
            └──────────┘ └──────────┘ └──────────┘        └──────────┘
```

---

## Current status

| Phase | Scope | State |
|------:|-------|-------|
| **1** | Java 21 domain model, PostgreSQL/PostGIS schema, Spring Boot operational API | ✅ **done** |
| **2** | Kafka topic design, Avro + Schema Registry, location simulator, consumer-group rebalancing | ✅ **done** |
| **3** | Debezium CDC from PostgreSQL — snapshot vs streaming, before/after, tombstones, slot & connector recovery | ✅ **done** |
| **4** | Flink jobs — keyed driver state, event-time windows, timers, geofencing (broadcast + JTS) | ✅ **done** |
| **5** | Flink sinks → TimescaleDB + ClickHouse — batching, retries, idempotent writes | ✅ **done** |
| 6 | Failure engineering: kill TaskManager, slow sink, malformed & duplicate events | ⬜ |
| 7 | Kubernetes, Flink K8s Operator, Helm, ArgoCD, Prometheus/Grafana | ⬜ |
| 8 | Load testing 1k→50k events/s, tuning, autoscaling | ⬜ |

Phase details and per-phase interview questions live in [`docs/`](docs/) and
[`architecture/`](architecture/).

---

## Phase 5 — what's here now

Flink **sink jobs** move the derived streams into the read stores, with the batching /
retry / idempotency a real sink needs. Both write mechanisms are proven replay-safe by a
Testcontainers IT (write a batch → replay the identical batch → one logical row survives).

| Job | From → To | Idempotency |
|-----|-----------|-------------|
| [`TimescaleSinkJob`](flink/src/main/java/com/flowfleet/flink/sink/TimescaleSinkJob.java) | `driver.state` + `driver.speed-windows` → TimescaleDB hypertables | PK includes the event's own timestamp; `INSERT … ON CONFLICT DO NOTHING` |
| [`ClickHouseSinkJob`](flink/src/main/java/com/flowfleet/flink/sink/ClickHouseSinkJob.java) | `driver.geofence-events` + `delivery.alerts` → ClickHouse | `ReplacingMergeTree(event_id)` collapses re-inserts at merge time |

`JdbcSinks` — `JdbcExecutionOptions` (flush at 500 rows or 1 s), 1 connection per subtask,
3 retries per batch. compose gains **`timescaledb`** (:15433) and **`clickhouse`** (:18123);
`make flink-submit-sinks`, `make timescale-peek`, `make clickhouse-peek`. Walkthrough in
[`docs/experiments/phase-5-idempotent-sinks.md`](docs/experiments/phase-5-idempotent-sinks.md).

Why two databases (OLTP vs OLAP), and BigQuery deferred: see
[`docs/interview-questions/phase-5.md`](docs/interview-questions/phase-5.md).

<details><summary>Phase 4 — Flink stateful processing (still here)</summary>

**`flink/`** — one module (Java 17 / Flink 1.20), one fat jar, one entry class per job.
Every stateful function has a Flink test-harness or MiniCluster test (no Kafka needed).

| Job | Flink feature it demonstrates |
|-----|------------------------------|
| [`DriverStateJob`](flink/src/main/java/com/flowfleet/flink/driverstate/DriverStateFunction.java) | `keyBy(driverId)` + `ValueState`; instantaneous speed derived from the position delta over the event-time gap; an event-time timer that expires state when a driver goes dark |
| [`DriverSpeedJob`](flink/src/main/java/com/flowfleet/flink/speed/DriverSpeedJob.java) | event time + watermarks (`forBoundedOutOfOrderness(5s)` + idleness); 1-minute `TumblingEventTimeWindows`; late data → side output, not dropped |
| [`GeofenceJob`](flink/src/main/java/com/flowfleet/flink/geofence/GeofenceFunction.java) | `KeyedBroadcastProcessFunction` — geofence polygons broadcast to every subtask, JTS point-in-polygon, only ENTER/EXIT transitions emitted |
| [`AnomalyJob`](flink/src/main/java/com/flowfleet/flink/anomaly/AnomalyFunction.java) | `KeyedProcessFunction` + timers — impossible-speed rule + GPS-gap timer → `flowfleet.delivery.alerts` |
| `GpsGuard` (in every job) | `ProcessFunction` side output — bad samples → `flowfleet.driver.locations.dlq` |

compose gains a **Flink session cluster** (`flink-jobmanager` + `flink-taskmanager`, UI on
:18086); `make flink-submit-all` deploys the jobs. Experiment walkthrough in
[`docs/experiments/phase-4-watermarks-windows.md`](docs/experiments/phase-4-watermarks-windows.md).

</details>

<details><summary>Phase 3 — Debezium CDC (still here)</summary>

* **`services/cdc`** — a reusable Debezium **embedded-engine** wrapper (`DebeziumCdcSource`)
  + a parsed view of the envelope (`CdcRecord`). `PostgresCdcIT` asserts snapshot (`op:r`)
  → INSERT/UPDATE/DELETE from the WAL with before/after → tombstone.
* **`services/connect`** — cp-kafka-connect + the Debezium PostgreSQL connector, as the
  `kafka-connect` service. **`kafka-connect/postgres-source.json`** — `pgoutput`,
  `snapshot.mode=initial`, Avro, `RegexRouter` → `flowfleet.orders.cdc`.
* **`V3`** — `REPLICA IDENTITY FULL`. `make cdc-register / cdc-status / cdc-slot / cdc-demo`.

</details>

<details><summary>Phase 2 — Kafka + Avro + location stream (still here)</summary>

* **`services/events`** — Avro schemas (`schemas/*.avsc` → generated `SpecificRecord`s),
  the [`Topics`](services/events/src/main/java/com/flowfleet/events/Topics.java) catalogue,
  and an `AdminClient`-based `TopicAdmin`. Shared by producers, consumers and (later) Flink.
* **`services/location-generator`** — simulates a fleet of drivers moving restaurant →
  customer on a haversine sphere and produces Avro `DriverLocation` events to
  `flowfleet.driver.locations` (`acks=all`, idempotent, keyed by `driver_id`). Deterministic
  per seed. Throughput = `GENERATOR_DRIVERS` / `GENERATOR_TICK`.
* **`services/location-consumer`** — a hand-rolled poll loop (manual commit,
  `CooperativeStickyAssignor`, a `ConsumerRebalanceListener` that logs partition moves).
  Scale it to see a rebalance.
* **compose** adds `kafka` (KRaft), `schema-registry` (compat = `FULL`) and `kafka-ui`.
* **Schema evolution:** `DriverLocation` v1 → v2 adds `speedKph` / `headingDegrees` /
  `vehicleType` as nullable-with-default ⇒ FULL-compatible, proven by a pure-Avro test.
* Tests: schema-compat unit test, deterministic simulator tests, and a **Redpanda**
  Testcontainers IT doing a real Avro → Schema Registry → Kafka → deserialize round trip.

</details>

<details><summary>Phase 1 — operational API (still here)</summary>

* **`services/common`** — framework-free domain model (`record`s + `enum`s): `Order`,
  `Driver`, `Delivery`, `DriverLocation`, `Geofence`, `DriverShift`, `GeoPoint`, plus the
  `OrderStatus` / `DeliveryStatus` state machines.
* **`services/api`** — Spring Boot 3.4 / Java 21 operational API on PostgreSQL:
  * `POST /api/v1/orders`, `GET /api/v1/orders/{id}`, `GET /api/v1/orders?status=`,
    `PATCH /api/v1/orders/{id}/status`
  * `POST /api/v1/drivers`, `GET /api/v1/drivers/{id}`, `GET /api/v1/drivers?status=`,
    `PATCH /api/v1/drivers/{id}/status`
  * Flyway migrations (`V1` schema + PostGIS, `V2` seed data)
  * Spring Data JDBC with `@Version` optimistic locking
  * Actuator health + Prometheus metrics, OpenAPI/Swagger UI
* **`docker-compose.yml`** — PostGIS + the API + Adminer.
* Tests: JUnit 5 unit tests for the domain, **Testcontainers** integration tests that run
  the real Flyway migrations against a real PostGIS container.

</details>

### Run it

```bash
# prerequisites: JDK 21, Docker. (Homebrew: brew install openjdk@21 maven)

make verify        # full build + unit + Testcontainers/Redpanda integration tests
make up            # start the whole stack (Postgres, API, Kafka, Schema Registry, generator, consumer)
make smoke         # API: create a driver + order, walk it to DELIVERED (needs jq)
make schemas       # show the registered DriverLocation schema
make lag           # consumer-group lag
make rebalance-demo  # scale consumers to 3, kill one, print the partition reassignment
make cdc-register  # register the Debezium PostgreSQL source connector
make cdc-demo      # change an order via the API, watch the CDC event land
make flink-submit-all  # deploy all six Flink jobs (4 processing + 2 sink)
make timescale-peek    # row counts + latest driver positions in TimescaleDB
make clickhouse-peek   # deduplicated counts in ClickHouse
make down          # stop (keep data)   |   make clean-data  (drop volumes)
```

The first `make up` builds five images from source (each runs Maven) and the full stack is
~14 containers — give Docker a few GB of headroom.

| Service | URL / address |
|---|---|
| API + Swagger | http://localhost:18080 · `/swagger-ui.html` |
| Adminer | http://localhost:18081 (`server=postgres db/user/pass=flowfleet`) |
| Kafka UI | http://localhost:18082 |
| Kafka Connect | http://localhost:18083/connectors |
| Schema Registry | http://localhost:18085/subjects |
| Flink UI | http://localhost:18086 |
| ClickHouse | http://localhost:18123 · `/play` |
| Kafka (host) | `localhost:19092` · Postgres `localhost:15432` · TimescaleDB `localhost:15433` |

> Host ports are non-standard on purpose so the stack co-exists with other local
> databases/apps. Change them in `docker-compose.yml`.

### Examples

```bash
# API
curl -s localhost:18080/api/v1/orders -H 'content-type: application/json' -d '{
  "customerId": 1, "restaurantId": 1,
  "items": [{"name":"Lahmajoun","quantity":3,"unitPrice":1.50}]
}' | jq

# watch the location stream
make kafka-tail
```

---

## Repository layout

```
.
├── pom.xml                     Maven reactor
├── Makefile                    developer entrypoints
├── docker-compose.yml          local stack (Phases 1–5)
├── schemas/                    canonical Avro .avsc files
├── kafka-connect/              Debezium connector config + notes
├── architecture/               design docs (Kafka, Flink, CDC, idempotency, recovery)
├── docs/                       phase roadmap + interview questions + experiments
├── database/                   database notes (schema is Flyway-managed in services/api)
├── scripts/                    smoke / kafka-topics / schema-registry / rebalance-demo / connect / cdc-demo / flink
├── flink/                      Flink jobs (Java 17, Flink 1.20) — one fat jar, one class per job
└── services/
    ├── common/                 domain model, no framework deps (Java 17)
    ├── events/                 Avro-generated events + Kafka topic catalogue (Java 17)
    ├── api/                    Spring Boot operational API + Flyway migrations
    ├── cdc/                    Debezium embedded-engine wrapper + envelope parser
    ├── connect/                Kafka Connect + Debezium PG connector image
    ├── location-generator/     driver GPS simulator → Kafka
    └── location-consumer/      consumer-group member (rebalance experiment)
```

Later phases add `helm/`, `argocd/`, `monitoring/`, and `load-testing/`.

---

## Toolchain

| | |
|---|---|
| Language | Java 21 (services) / Java 17 (Flink + shared libs) |
| Build | Maven (multi-module, `./mvnw`) |
| API | Spring Boot 3.4, Spring Data JDBC, Flyway |
| Streaming | Apache Kafka 3.8 (KRaft), Confluent Schema Registry, Avro 1.12 |
| CDC | Debezium 3.0 (Kafka Connect + embedded engine) |
| Stream processing | Apache Flink 1.20 (DataStream), JTS for geofencing |
| DB | PostgreSQL 16 + PostGIS 3.5 (`imresamu/postgis`, multi-arch) |
| Tests | JUnit 5, AssertJ, Mockito, Testcontainers (PostGIS + Redpanda), Flink test-harness + MiniCluster |
| Containers | Docker / Docker Compose |

> **Note on Docker API version:** Docker Engine 29+ requires API ≥ 1.44. The Testcontainers
> client is pinned to `1.44` for the test JVM in `services/api/pom.xml`
> (`-Ddocker.api.version=` to override for an older daemon).
