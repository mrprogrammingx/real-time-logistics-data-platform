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
package: ## Build the API fat-jar, skipping tests
	$(MVN) -B -pl services/api -am clean package -DskipTests

## ----- local stack ----------------------------------------------------------

.PHONY: up
up: ## Start Postgres + API + Adminer (builds the API image)
	$(COMPOSE) up -d --build
	@echo "API      : http://localhost:18080"
	@echo "Swagger  : http://localhost:18080/swagger-ui.html"
	@echo "Health   : http://localhost:18080/actuator/health"
	@echo "Adminer  : http://localhost:18081  (server=postgres db=flowfleet user=flowfleet pass=flowfleet)"
	@echo "Postgres : localhost:15432  (db=flowfleet user=flowfleet pass=flowfleet)"

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

## ----- demo ---------------------------------------------------------------

.PHONY: smoke
smoke: ## Hit the API end to end (needs `make up` first)
	./scripts/smoke.sh
