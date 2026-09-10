# Monitoring

Prometheus + Grafana, brought up by `docker-compose.yml` (Phase 6).

| | |
|---|---|
| Prometheus | http://localhost:19090 — scrape targets under Status → Targets |
| Grafana | http://localhost:13000 — anonymous viewer; login `admin` / `flowfleet` to edit |
| Flink dashboard | http://localhost:13000/d/flowfleet-flink |

## What's scraped

- **Flink** — the `PrometheusReporter` (bundled in the image, `flink/Dockerfile` moves it
  to `lib/`) exposes metrics on `:9250` (JobManager) and `:9251` (TaskManager).
- **Spring services** — `api`, `location-generator`, `location-consumer` on
  `/actuator/prometheus` (Micrometer).

`monitoring/prometheus/prometheus.yml` — scrape config.
`monitoring/grafana/provisioning/` — datasource + dashboard provider (file-based).
`monitoring/grafana/dashboards/flink.json` — the starter dashboard (throughput,
backpressure, checkpoint duration/size, restarts, Kafka source lag, ingest valid/DLQ).

## Custom metrics

`LocationIngest` registers `flowfleet.ingest.valid` and `flowfleet.ingest.dlq` counters;
the reporter exposes them as
`flink_taskmanager_job_task_operator_flowfleet_ingest_{valid,dlq}`.

Phase 7 adds Kafka-broker metrics (JMX exporter) and per-connector Debezium metrics, and
moves this to the Kubernetes monitoring stack.
