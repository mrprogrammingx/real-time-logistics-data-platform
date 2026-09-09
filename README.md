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
| 2 | Kafka topic design, Java producers/consumers, rebalancing experiments | ⬜ next |
| 3 | Debezium CDC from PostgreSQL, snapshot vs streaming, connector recovery | ⬜ |
| 4 | Flink jobs: driver state, watermarks/windows, timers, geofencing | ⬜ |
| 5 | Production sinks: Timescale/ClickHouse/BigQuery, idempotency, DLQ | ⬜ |
| 6 | Failure engineering: kill TaskManager, slow sink, malformed & duplicate events | ⬜ |
| 7 | Kubernetes, Flink K8s Operator, Helm, ArgoCD, Prometheus/Grafana | ⬜ |
| 8 | Load testing 1k→50k events/s, tuning, autoscaling | ⬜ |

Phase details and per-phase interview questions live in [`docs/`](docs/) and
[`architecture/`](architecture/).

---

## Phase 1 — what's here now

* **`services/common`** — framework-free domain model (`record`s + `enum`s): `Order`,
  `Driver`, `Delivery`, `DriverLocation`, `Geofence`, `DriverShift`, `GeoPoint`, plus the
  `OrderStatus` / `DeliveryStatus` state machines. Reused later by the services *and* the
  Flink jobs.
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

### Run it

```bash
# prerequisites: JDK 21, Docker. (Homebrew: brew install openjdk@21 maven)

make verify        # full build + unit + Testcontainers integration tests
make up            # start Postgres + API + Adminer via docker compose
make smoke         # create a driver + order, walk it to DELIVERED (needs jq)
make down          # stop (keep data)   |   make clean-data  (drop the volume)
```

* API — http://localhost:18080
* Swagger UI — http://localhost:18080/swagger-ui.html
* Health — http://localhost:18080/actuator/health
* Adminer — http://localhost:18081 (`server=postgres db=flowfleet user=flowfleet pass=flowfleet`)
* Postgres — `localhost:15432` (`db=flowfleet user=flowfleet pass=flowfleet`)

> Host ports (`15432` / `18080` / `18081`) are non-standard on purpose so the stack
> co-exists with other local databases/apps. Change them in `docker-compose.yml`.

### Example

```bash
curl -s localhost:18080/api/v1/orders -H 'content-type: application/json' -d '{
  "customerId": 1, "restaurantId": 1,
  "items": [{"name":"Lahmajoun","quantity":3,"unitPrice":1.50}]
}' | jq
```

---

## Repository layout

```
.
├── pom.xml                     Maven reactor (Java 21)
├── Makefile                    developer entrypoints
├── docker-compose.yml          Phase 1 local stack
├── architecture/               design docs (Kafka, Flink, CDC, idempotency, recovery)
├── docs/                       phase plans + interview questions
├── database/                   database notes (schema is Flyway-managed in services/api)
├── scripts/                    smoke.sh and friends
└── services/
    ├── common/                 domain model (no framework deps)
    └── api/                    Spring Boot operational API + Flyway migrations
```

Later phases add `flink/`, `schemas/` (Avro), `kafka-connect/`, `helm/`, `argocd/`,
`monitoring/`, and `load-testing/`.

---

## Toolchain

| | |
|---|---|
| Language | Java 21 |
| Build | Maven (multi-module, `./mvnw`) |
| API | Spring Boot 3.4, Spring Data JDBC, Flyway |
| DB | PostgreSQL 16 + PostGIS 3.4 |
| Tests | JUnit 5, AssertJ, Mockito, Testcontainers |
| Containers | Docker / Docker Compose |

> **Note on Docker API version:** Docker Engine 29+ requires API ≥ 1.44. The Testcontainers
> client is pinned to `1.44` for the test JVM in `services/api/pom.xml`
> (`-Ddocker.api.version=` to override for an older daemon).
