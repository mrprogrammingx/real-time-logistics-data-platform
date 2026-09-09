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
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
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
up: ## Start the whole stack (Postgres, API, Kafka, Schema Registry, generator, consumer)
	$(COMPOSE) up -d --build
	@echo "API        : http://localhost:18080     Swagger: /swagger-ui.html"
	@echo "Adminer    : http://localhost:18081     (server=postgres db=flowfleet user=flowfleet pass=flowfleet)"
	@echo "Kafka UI   : http://localhost:18082"
	@echo "Schema Reg : http://localhost:18085/subjects"
	@echo "Generator  : http://localhost:18090/actuator/prometheus"
	@echo "Kafka      : localhost:19092   Postgres: localhost:15432"

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

## ----- demo ---------------------------------------------------------------

.PHONY: smoke
smoke: ## Hit the API end to end (needs `make up` first)
	./scripts/smoke.sh
