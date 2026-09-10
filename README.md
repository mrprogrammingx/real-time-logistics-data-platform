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
| **6** | Failure engineering — poison-message DLQ, late-data, checkpoint recovery, backpressure, Prometheus + Grafana | ✅ **done** |
| **7** | Kubernetes — Helm chart, Flink Kubernetes Operator (`FlinkDeployment` + `FlinkSessionJob`, savepoint upgrades), Argo CD (app-of-apps) | ✅ **done** |
| 8 | Load testing 1k→50k events/s, tuning, autoscaling | ⬜ next |

Phase details and per-phase interview questions live in [`docs/`](docs/) and
[`architecture/`](architecture/).

---

## Phase 7 — what's here now

**The stack on Kubernetes, delivered by GitOps.** `docker-compose.yml` still runs
Phases 1–6 locally; Phase 7 packages the same topology as a Helm chart and hands the
Flink jobs to the Flink Kubernetes Operator.

* **[`helm/flowfleet/`](helm/flowfleet)** — one umbrella chart, one first-party template per
  component (datastores as StatefulSets, Kafka KRaft, the Spring services, the two sink
  DBs). One `ConfigMap` + `Secret`, `envFrom` everywhere. `values-kind.yaml` for a laptop
  cluster. Renders offline; CI validates every manifest against real CRD schemas.
* **Flink** — a session `FlinkDeployment` + one `FlinkSessionJob` per job
  ([`flink-session.yaml`](helm/flowfleet/templates/flink-session.yaml) /
  [`flink-jobs.yaml`](helm/flowfleet/templates/flink-jobs.yaml)). `upgradeMode: savepoint` —
  editing a job's CR makes the operator stop-with-savepoint and restore. RocksDB state on a
  PVC (or S3); optional Kubernetes HA. A tiny in-cluster artifact server feeds the job jar
  (session jobs need a remote `jarURI`).
* **[`argocd/`](argocd)** — app-of-apps: Flink Operator (sync-wave −2) → kube-prometheus-stack
  (−1) → the flowfleet chart (0), with `ignoreDifferences` for the fields the operator
  writes back.
* **[`scripts/k8s.sh`](scripts/k8s.sh)** / `make k8s-up` — kind cluster + cert-manager +
  operator + build/load images + `helm install`. `make k8s-jobs` / `k8s-ui` / `k8s-savepoint`.

Design + trade-offs (session vs application mode, state storage, sync waves):
[`architecture/deployment.md`](architecture/deployment.md) and [`k8s/README.md`](k8s/README.md).
Savepoint-upgrade experiment: [`docs/experiments/phase-7-savepoint-upgrade.md`](docs/experiments/phase-7-savepoint-upgrade.md).
Interview Q&A: [`docs/interview-questions/phase-7.md`](docs/interview-questions/phase-7.md).

<details><summary>Phase 6 — failure engineering (still here)</summary>

* **Poison messages** — [`ResilientLocationDeserializer`](flink/src/main/java/com/flowfleet/flink/ingest/ResilientLocationDeserializer.java)
  never throws: an undecodable Kafka record becomes a `ParsedLocation` with its error +
  offset. [`LocationIngest`](flink/src/main/java/com/flowfleet/flink/ingest/LocationIngest.java)
  routes decode failures *and* structurally-invalid samples to
  `flowfleet.driver.locations.dlq`, with `flowfleet.ingest.{valid,dlq}` counters. One
  ingestion gate shared by all four jobs.
* **Late data** — `WatermarkLatenessIT` feeds a sample behind the watermark (via a source
  that emits watermarks under test control) and asserts it lands on the late side output,
  not the window.
* **Chaos** — [`scripts/chaos.sh`](scripts/chaos.sh) / `make chaos-{kill-tm,slow-sink,malformed,duplicate}`:
  kill a TaskManager and watch checkpoint recovery; a `SlowMap` (`--slow-map-ms`) to induce
  backpressure; inject non-Avro messages; replay a topic and confirm the sink row count
  holds.
* **Metrics** — Flink → Prometheus (:19090) → **Grafana** (:13000, dashboard *FlowFleet — Flink*):
  throughput, backpressure, checkpoint duration/size, restarts, Kafka source lag, ingest
  valid vs DLQ.

Walkthrough with fill-in results: [`docs/experiments/phase-6-chaos.md`](docs/experiments/phase-6-chaos.md).
Interview Q&A: [`docs/interview-questions/phase-6.md`](docs/interview-questions/phase-6.md).

</details>

<details><summary>Phase 5 — Flink sinks → TimescaleDB + ClickHouse (still here)</summary>

Flink **sink jobs** move the derived streams into the read stores with the batching / retry
/ idempotency a real sink needs; both proven replay-safe by a Testcontainers IT.

| Job | From → To | Idempotency |
|-----|-----------|-------------|
| `TimescaleSinkJob` | `driver.state` + `driver.speed-windows` → TimescaleDB hypertables | PK includes the event's own timestamp; `INSERT … ON CONFLICT DO NOTHING` |
| `ClickHouseSinkJob` | `driver.geofence-events` + `delivery.alerts` → ClickHouse | `ReplacingMergeTree(event_id)` collapses re-inserts at merge time |

`JdbcSinks` — batched (500 rows / 1 s), 3 retries. `make timescale-peek` / `clickhouse-peek`.

</details>

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
make chaos-kill-tm     # kill a TaskManager, watch checkpoint recovery
make chaos-malformed   # inject non-Avro messages, watch them land on the DLQ
make down          # stop (keep data)   |   make clean-data  (drop volumes)
```

The first `make up` builds five images from source (each runs Maven) and the full stack is
~16 containers — give Docker a few GB of headroom.

| Service | URL / address |
|---|---|
| API + Swagger | http://localhost:18080 · `/swagger-ui.html` |
| Adminer · Kafka UI | http://localhost:18081 · http://localhost:18082 |
| Kafka Connect · Schema Registry | http://localhost:18083/connectors · http://localhost:18085/subjects |
| Flink UI | http://localhost:18086 |
| ClickHouse | http://localhost:18123/play |
| Prometheus · Grafana | http://localhost:19090 · http://localhost:13000 (`admin`/`flowfleet`) |
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
├── docker-compose.yml          local stack (Phases 1–6, ~16 containers)
├── schemas/                    canonical Avro .avsc files
├── kafka-connect/              Debezium connector config + notes
├── database/                   TimescaleDB + ClickHouse sink DDL
├── monitoring/                 Prometheus scrape config + Grafana provisioning
├── helm/flowfleet/             the platform Helm chart (Phase 7)
├── argocd/                     Argo CD app-of-apps (Phase 7)
├── k8s/                        Flink Operator + kube-prometheus-stack values, kind cluster
├── architecture/               design docs (Kafka, Flink, CDC, idempotency, recovery, deployment)
├── docs/                       phase roadmap + interview questions + experiments
├── scripts/                    smoke / kafka / schema-registry / connect / cdc-demo / flink / chaos / k8s
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

Phase 8 adds load-test tooling under `scripts/`.

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
| Sinks | TimescaleDB 2.17, ClickHouse 24.8 (via `flink-connector-jdbc`) |
| Observability | Prometheus + Grafana; Flink Prometheus reporter, Micrometer |
| DB | PostgreSQL 16 + PostGIS 3.5 (`imresamu/postgis`, multi-arch) |
| Tests | JUnit 5, AssertJ, Mockito, Testcontainers (PostGIS · Redpanda · Timescale · ClickHouse), Flink test-harness + MiniCluster |
| Containers | Docker / Docker Compose |
| Kubernetes | Helm 3, Flink Kubernetes Operator 1.10, Argo CD, kube-prometheus-stack; `kind` for local (`scripts/k8s.sh`) |

> **Note on Docker API version:** Docker Engine 29+ requires API ≥ 1.44. The Testcontainers
> client is pinned to `1.44` for the test JVM in `services/api/pom.xml`
> (`-Ddocker.api.version=` to override for an older daemon).
