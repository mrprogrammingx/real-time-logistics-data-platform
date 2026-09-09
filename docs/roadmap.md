# Build roadmap

Incremental. Each phase must end with: **working code + tests + a short design note + the
interview questions it lets you answer from experience.**

```
MVP ──────────────► PRODUCTION ────────────► PLATFORM ──────────► SCALE
Postgres + Kafka    state / windows          Kubernetes           load testing
CDC                 watermarks / timers      Helm                 tuning
Flink               idempotency / DLQ        Flink K8s Operator   failure tests
ClickHouse          checkpoints              Prometheus/Grafana   autoscaling
                                             ArgoCD
```

| Phase | Focus | Key deliverable | Notes |
|------:|-------|-----------------|-------|
| **1 ✅** | Java 21 domain + OLTP | `services/common`, `services/api`, PostGIS schema, docker-compose, Testcontainers | done |
| **2 ✅** | Kafka | `services/events` (Avro + `Topics` + `TopicAdmin`), `location-generator`, `location-consumer`, Kafka + Schema Registry + Kafka UI in compose, `DriverLocation` v1→v2 evolution, rebalance experiment (`make rebalance-demo`) | done |
| **3 ✅** | Debezium CDC | `services/cdc` (embedded-engine wrapper + `CdcRecord` + `PostgresCdcIT` proving snapshot/streaming/op c·r·u·d/before·after/tombstone), `services/connect` (Kafka Connect + Debezium PG connector image), `kafka-connect/postgres-source.json`, `V3` REPLICA IDENTITY FULL, `make cdc-*` + recovery experiment | done |
| **4 ✅** | Flink core | `flink/` module (Java 17 / Flink 1.20): `DriverStateJob` (keyed state + event-time timer), `DriverSpeedJob` (tumbling event-time windows + late-data side output), `GeofenceJob` (`KeyedBroadcastProcessFunction` + JTS), `AnomalyJob` (timers). Session cluster in compose, `make flink-*`, 14 harness/MiniCluster tests | done |
| **5 ✅** | Production sinks | `TimescaleSinkJob` (`driver.state` + `driver.speed-windows` → Timescale hypertables, `ON CONFLICT DO NOTHING`), `ClickHouseSinkJob` (`geofence-events` + `alerts` → `ReplacingMergeTree(event_id)`), batched/retrying `JdbcSink`, `TimescaleSinkIT` + `ClickHouseSinkIT` proving replay-idempotency. BigQuery + sink-DLQ deferred | [`idempotency.md`](../architecture/idempotency.md) |
| **6** | Failure engineering | kill TaskManager, slow-sink backpressure, malformed→DLQ, duplicate + out-of-order tests, checkpoint recovery — all with metrics | [`failure-recovery.md`](../architecture/failure-recovery.md) |
| **7** | Kubernetes + observability | Helm charts, Flink K8s Operator (`FlinkDeployment`/`FlinkSessionJob`), ArgoCD, Prometheus + Grafana dashboards (lag, checkpoint, backpressure, state size) | |
| **8** | Load + optimization | `location-generator` 1k→10k→25k→50k events/s, record throughput / p50-p99 / lag / checkpoint duration, then tune + autoscale | numbers to quote in interviews |

## `location-generator` (built Phase 2, scales in Phase 8)

Simulates N drivers each emitting ~1 GPS sample/s along a realistic route:
`start → drive to restaurant → drive to customer → new job`. Not real drivers — synthetic
haversine movement with plausible speed/heading, deterministic per seed. Phase 4 adds
deliberately malformed samples (for the DLQ) and deliberately late ones (for watermarks).
Phase 8 pushes it to 10k–50k events/s.
