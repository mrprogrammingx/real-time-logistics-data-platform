# FlowFleet — developer entrypoints.
# Every target is safe to run repeatedly.

SHELL := /bin/bash

# Pin the JDK to 21 regardless of what's first on PATH (Homebrew keg is not linked).
JAVA_HOME := $(shell /opt/homebrew/bin/brew --prefix openjdk@21 2>/dev/null)/libexec/openjdk.jdk/Contents/Home
export JAVA_HOME
MVN := ./mvnw
COMPOSE := docker compose

.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

## ----- build & test -----------------------------------------------------------

.PHONY: build
build: ## Compile everything and run unit tests (no Docker needed)
	$(MVN) -B clean test

.PHONY: verify
verify: ## Full build incl. Testcontainers integration tests (Docker required)
	$(MVN) -B clean verify

.PHONY: package
package: ## Build all service fat-jars, skipping tests
	$(MVN) -B clean package -DskipTests

## ----- local stack ----------------------------------------------------------

.PHONY: up
up: ## Start the whole stack (~14 containers). Then: make cdc-register && make flink-submit-all
	$(COMPOSE) up -d --build
	@echo "API        : http://localhost:18080     Swagger: /swagger-ui.html"
	@echo "Adminer    : http://localhost:18081     Kafka UI: http://localhost:18082"
	@echo "Connect    : http://localhost:18083     Schema Reg: http://localhost:18085/subjects"
	@echo "Flink UI   : http://localhost:18086     ClickHouse: http://localhost:18123/play"
	@echo "Kafka 19092 · Postgres 15432 · TimescaleDB 15433"

.PHONY: down
down: ## Stop the stack, keep the database volume
	$(COMPOSE) down

.PHONY: clean-data
clean-data: ## Stop the stack AND delete the database volume
	$(COMPOSE) down -v

.PHONY: logs
logs: ## Tail logs from all services
	$(COMPOSE) logs -f --tail=100

.PHONY: psql
psql: ## Open a psql shell in the running Postgres container
	$(COMPOSE) exec postgres psql -U flowfleet -d flowfleet

## ----- kafka --------------------------------------------------------------

.PHONY: kafka-topics
kafka-topics: ## Create + list the FlowFleet topics
	./scripts/kafka-topics.sh create

.PHONY: kafka-describe
kafka-describe: ## Describe all topics (partitions, leaders)
	./scripts/kafka-topics.sh describe

.PHONY: kafka-tail
kafka-tail: ## Print DriverLocation events off the topic as JSON (Ctrl-C to stop)
	$(COMPOSE) exec schema-registry kafka-avro-console-consumer \
		--bootstrap-server kafka:29092 --property schema.registry.url=http://localhost:8085 \
		--topic flowfleet.driver.locations --from-beginning --property print.key=true

.PHONY: schemas
schemas: ## Show registered subjects and the current DriverLocation schema
	./scripts/schema-registry.sh subjects && ./scripts/schema-registry.sh latest

.PHONY: rebalance-demo
rebalance-demo: ## Scale consumers to 3, kill one, show the partition reassignment
	./scripts/rebalance-demo.sh

.PHONY: lag
lag: ## Show consumer-group lag for the location consumers
	$(COMPOSE) exec kafka kafka-consumer-groups --bootstrap-server kafka:29092 \
		--describe --group flowfleet.location-consumer

## ----- cdc (debezium) ---------------------------------------------------------

.PHONY: cdc-register
cdc-register: ## Register the Debezium PostgreSQL source connector
	./scripts/connect.sh register

.PHONY: cdc-status
cdc-status: ## Connector + task state
	./scripts/connect.sh status

.PHONY: cdc-slot
cdc-slot: ## PostgreSQL replication-slot health (active? WAL retained?)
	./scripts/connect.sh slot

.PHONY: cdc-tail
cdc-tail: ## Print orders.cdc events as JSON (Ctrl-C to stop)
	$(COMPOSE) exec schema-registry kafka-avro-console-consumer \
		--bootstrap-server kafka:29092 --property schema.registry.url=http://localhost:8085 \
		--topic flowfleet.orders.cdc --from-beginning --property print.key=true

.PHONY: cdc-demo
cdc-demo: ## Change an order via the API and watch the CDC event land
	./scripts/cdc-demo.sh

## ----- flink ------------------------------------------------------------------

.PHONY: flink-submit-all
flink-submit-all: ## Submit all six Flink jobs (4 processing + 2 sink) to the session cluster
	./scripts/flink.sh submit-all

.PHONY: flink-submit-jobs
flink-submit-jobs: ## Submit only the processing jobs (driver-state/speed/geofence/anomaly)
	./scripts/flink.sh submit-jobs

.PHONY: flink-submit-sinks
flink-submit-sinks: ## Submit only the sink jobs (timescale + clickhouse)
	./scripts/flink.sh submit-sinks

.PHONY: flink-list
flink-list: ## List running Flink jobs
	./scripts/flink.sh list

.PHONY: flink-cancel-all
flink-cancel-all: ## Cancel all running Flink jobs
	./scripts/flink.sh cancel-all

.PHONY: flink-tail
flink-tail: ## Print driver.state snapshots as JSON (Ctrl-C to stop)
	$(COMPOSE) exec schema-registry kafka-avro-console-consumer \
		--bootstrap-server kafka:29092 --property schema.registry.url=http://localhost:8085 \
		--topic flowfleet.driver.state --property print.key=true

## ----- sinks ----------------------------------------------------------------

.PHONY: timescale-psql
timescale-psql: ## psql shell into TimescaleDB
	$(COMPOSE) exec timescaledb psql -U flowfleet -d flowfleet

.PHONY: timescale-peek
timescale-peek: ## Row counts + latest driver positions in TimescaleDB
	$(COMPOSE) exec timescaledb psql -U flowfleet -d flowfleet -c \
	  "SELECT 'driver_state' t, count(*) FROM driver_state UNION ALL SELECT 'speed_windows', count(*) FROM driver_speed_windows;" \
	  -c "SELECT driver_id, event_time, round(derived_speed_kph::numeric,1) kph FROM driver_current_state ORDER BY driver_id LIMIT 10;"

.PHONY: clickhouse-client
clickhouse-client: ## clickhouse-client shell
	$(COMPOSE) exec clickhouse clickhouse-client -u flowfleet --password flowfleet -d flowfleet

.PHONY: clickhouse-peek
clickhouse-peek: ## Deduplicated row counts in ClickHouse
	$(COMPOSE) exec clickhouse clickhouse-client -u flowfleet --password flowfleet -q \
	  "SELECT 'geofence_events' t, count() c FROM flowfleet.geofence_events FINAL UNION ALL SELECT 'delivery_alerts', count() FROM flowfleet.delivery_alerts FINAL"

## ----- failure engineering (phase 6) ----------------------------------------

.PHONY: chaos-kill-tm
chaos-kill-tm: ## Kill a TaskManager, watch the job recover from its last checkpoint
	./scripts/chaos.sh kill-tm

.PHONY: chaos-slow-sink
chaos-slow-sink: ## Resubmit the Timescale sink with a 20ms/record delay (backpressure)
	./scripts/chaos.sh slow-sink 20

.PHONY: chaos-malformed
chaos-malformed: ## Inject 5 non-Avro messages -> watch them land on the DLQ
	./scripts/chaos.sh malformed 5

.PHONY: chaos-duplicate
chaos-duplicate: ## Replay driver.state into the sink; TimescaleDB row count must not change
	./scripts/chaos.sh duplicate

.PHONY: chaos-metrics
chaos-metrics: ## Dump Flink checkpoint / backpressure / restart metrics for a job
	./scripts/chaos.sh metrics $(JOB)

.PHONY: grafana
grafana: ## Open the Grafana Flink dashboard
	@echo "http://localhost:13000/d/flowfleet-flink  (anonymous viewer; admin/flowfleet)"

## ----- kubernetes (phase 7) -----------------------------------------------

.PHONY: helm-sync
helm-sync: ## Copy the canonical DB init SQL + Grafana dashboard into the Helm chart
	cp database/timescaledb/init.sql helm/flowfleet/files/db/timescaledb-init.sql
	cp database/clickhouse/init.sql  helm/flowfleet/files/db/clickhouse-init.sql
	cp monitoring/grafana/dashboards/flink.json helm/flowfleet/files/dashboards/flink.json
	@echo "chart files synced — commit if changed"

.PHONY: helm-lint
helm-lint: ## helm lint + render the chart with both value sets
	helm lint helm/flowfleet
	helm template flowfleet helm/flowfleet -n flowfleet >/dev/null
	helm template flowfleet helm/flowfleet -n flowfleet -f helm/flowfleet/values-kind.yaml >/dev/null
	@echo "chart OK"

.PHONY: k8s-up
k8s-up: ## kind create + operators + build/load images + helm install (see k8s/README.md)
	./scripts/k8s.sh up

.PHONY: k8s-deploy
k8s-deploy: ## helm upgrade --install into an existing cluster
	./scripts/k8s.sh deploy

.PHONY: k8s-images
k8s-images: ## Rebuild the 5 images and load them into the kind cluster
	./scripts/k8s.sh images

.PHONY: k8s-jobs
k8s-jobs: ## FlinkDeployment / FlinkSessionJob status
	./scripts/k8s.sh jobs

.PHONY: k8s-ui
k8s-ui: ## Port-forward the Flink UI (localhost:8081)
	./scripts/k8s.sh ui

.PHONY: k8s-savepoint
k8s-savepoint: ## Trigger a savepoint for one job:  make k8s-savepoint JOB=driver-state
	./scripts/k8s.sh savepoint $(JOB)

.PHONY: k8s-down
k8s-down: ## Delete the kind cluster
	./scripts/k8s.sh down

## ----- demo ---------------------------------------------------------------

.PHONY: smoke
smoke: ## Hit the API end to end (needs `make up` first)
	./scripts/smoke.sh
