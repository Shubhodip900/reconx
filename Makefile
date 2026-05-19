# ── ReconX — root Makefile ────────────────────────────────────────────────────
#
# FULL STACK (single command, recommended):
#   make up          — build images + start entire stack via Docker Compose
#   make down        — stop and remove containers
#   make logs        — tail logs from all services
#
# INDIVIDUAL SERVICES (for development, each in its own terminal):
#   make run-ingestion
#   make run-engine
#   make run-resolution
#   make run-gateway
#
# BUILD:
#   make build-all   — build all service binaries locally
#
# TESTS:
#   make test-all    — run tests for all services
#
# UTILITIES:
#   make lint-all    — lint all services
#   make proto       — regenerate Go proto bindings
#   make help        — show this message

.PHONY: up down logs ps \
        build-all build-ingestion build-engine build-resolution build-gateway \
        run-ingestion run-engine run-resolution run-gateway \
        test-all test-ingestion test-engine test-resolution test-gateway \
        lint-all proto help

# ── Full-stack Docker Compose ─────────────────────────────────────────────────

## up: Build images and start the full ReconX stack (postgres + all services + prometheus)
up:
	docker compose up --build

## up-detach: Start the stack in the background
up-detach:
	docker compose up --build -d

## down: Stop and remove all containers (data volumes preserved)
down:
	docker compose down

## down-volumes: Stop containers AND delete all volumes (clears all data)
down-volumes:
	docker compose down -v

## logs: Tail logs from all running services
logs:
	docker compose logs -f

## ps: Show status of all compose services
ps:
	docker compose ps

# ── Build all binaries locally ────────────────────────────────────────────────

## build-all: Build every service binary on the local machine
build-all: build-ingestion build-engine build-resolution build-gateway

build-ingestion:
	$(MAKE) -C services/ingestion build

build-engine:
	cargo build --manifest-path services/engine/Cargo.toml

build-resolution:
	$(MAKE) -C services/resolution build

build-gateway:
	$(MAKE) -C services/gateway build

# ── Run services locally (each in its own terminal) ──────────────────────────
# Prerequisite: PostgreSQL must be running locally (or via `docker compose up postgres`).

## run-ingestion: Run only the Ingestion Service locally
run-ingestion:
	$(MAKE) -C services/ingestion run

## run-engine: Run only the Reconciliation Engine locally
run-engine:
	cargo run --manifest-path services/engine/Cargo.toml

## run-resolution: Run only the Resolution Service locally
run-resolution:
	$(MAKE) -C services/resolution run

## run-gateway: Run only the API Gateway locally
run-gateway:
	$(MAKE) -C services/gateway run

## run-postgres: Start only PostgreSQL via Docker (for local dev)
run-postgres:
	docker compose up postgres -d

# ── Test all services ─────────────────────────────────────────────────────────

## test-all: Run tests for every service
test-all: test-ingestion test-engine test-resolution test-gateway

test-ingestion:
	$(MAKE) -C services/ingestion test

test-engine:
	cargo test --manifest-path services/engine/Cargo.toml

test-resolution:
	$(MAKE) -C services/resolution test

test-gateway:
	$(MAKE) -C services/gateway test

# ── Lint ──────────────────────────────────────────────────────────────────────

## lint-all: Lint every service
lint-all:
	$(MAKE) -C services/ingestion lint
	cargo clippy --manifest-path services/engine/Cargo.toml -- -D warnings
	$(MAKE) -C services/resolution lint
	$(MAKE) -C services/gateway lint

# ── Proto ─────────────────────────────────────────────────────────────────────

## proto: Regenerate Go proto bindings (requires protoc + plugins)
proto:
	$(MAKE) -C services/ingestion proto

# ── Help ──────────────────────────────────────────────────────────────────────

## help: Show available make targets
help:
	@echo "ReconX — available targets:"
	@sed -n 's/^## //p' $(MAKEFILE_LIST) | column -t -s ':'
