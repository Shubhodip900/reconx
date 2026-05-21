# ReconX — Developer Setup Guide

## Prerequisites

| Tool | Minimum version | Check |
|---|---|---|
| Docker | 24+ | `docker --version` |
| Docker Compose | v2 (plugin) | `docker compose version` |
| Go | 1.23+ | `go version` |
| Rust | 1.82+ | `rustc --version` |
| `protoc` | 25+ | `protoc --version` *(proto regen only)* |

Install Rust via [rustup](https://rustup.rs) if not already present:

```bash
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
rustup update stable
```

---

## Option 1 — Full Docker Compose (fastest)

Builds all images and starts every service plus Prometheus in one command.

```bash
make up
# or in the background:
make up-detach
```

| Service | Address |
|---|---|
| Public REST API (Gateway) | http://localhost:8090 |
| Ingestion HTTP | http://localhost:8080 |
| Ingestion gRPC | localhost:50051 |
| Engine gRPC | localhost:50052 |
| Resolution HTTP | http://localhost:8082 |
| Resolution gRPC | localhost:50053 |
| Prometheus UI | http://localhost:9191 |

Stop everything (data preserved):

```bash
make down
```

Stop and wipe all volumes:

```bash
make down-volumes
```

---

## Option 2 — Local binaries + Docker postgres

Runs service binaries directly on the host. Useful for faster iteration (no image rebuild).

```bash
make run-all
```

What this does:
1. Starts PostgreSQL via `docker compose up postgres -d`
2. Waits for postgres to pass its health check
3. Builds all four service binaries (`make build-all`)
4. Launches each service as a background process with the correct env vars
5. Traps Ctrl+C and kills all processes on exit

Skip the build step if binaries are already up to date:

```bash
bash scripts/run-local.sh --no-build
```

Service logs are written to:

```
/tmp/reconx-ingestion.log
/tmp/reconx-engine.log
/tmp/reconx-resolution.log
/tmp/reconx-gateway.log
```

---

## Option 3 — Individual services (one terminal each)

Start postgres first:

```bash
make run-postgres
```

Then, in separate terminals:

```bash
make run-ingestion    # :50051 gRPC | :8080 HTTP | :9090 metrics
make run-engine       # :50052 gRPC | :9091 metrics
make run-resolution   # :50053 gRPC | :8082 HTTP | :9092 metrics
make run-gateway      # :8090 HTTP  | :9093 metrics
```

Each service reads its configuration from environment variables. Defaults are suitable for local development when all services run on `localhost`. See `README.md` for the full environment variable reference.

---

## Database

All services share a single PostgreSQL database (`reconx`). Schema migrations run automatically on startup — no manual migration step required.

| Credential | Value |
|---|---|
| Host | `localhost:5432` |
| Database | `reconx` |
| User | `reconx` |
| Password | `reconx` |

Connect directly:

```bash
docker compose exec postgres psql -U reconx -d reconx
```

---

## Build

Build all service binaries locally:

```bash
make build-all
```

Build a single service:

```bash
make build-ingestion    # → services/ingestion/bin/reconx-ingestion
make build-engine       # → services/engine/target/release/reconx-engine
make build-resolution   # → services/resolution/bin/reconx-resolution
make build-gateway      # → services/gateway/bin/reconx-gateway
```

---

## Tests

```bash
make test-all           # run all service test suites

# Or per service:
make test-ingestion
make test-engine        # cargo test (no DB required)
make test-resolution
make test-gateway
```

---

## Linting

```bash
make lint-all
```

This runs `golangci-lint` for Go services and `cargo clippy -- -D warnings` for the engine.

---

## Proto regeneration

Requires `protoc`, `protoc-gen-go`, and `protoc-gen-go-grpc`:

```bash
make proto
```

Generated Go bindings are committed under `proto/gen/go/` so this step is only needed when `.proto` files change.

---

## Smoke test

After starting the stack (any option above):

```bash
# Ingest two records with a discrepancy
curl -s -X POST http://localhost:8080/ingest/vendor_portal \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key":"setup-001-v","transaction_ref":"SETUP-001","amount":"10000.00","currency":"INR","event_time":"2024-01-15T10:30:00Z"}'

curl -s -X POST http://localhost:8080/ingest/erp_system \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key":"setup-001-e","transaction_ref":"SETUP-001","amount":"9800.00","currency":"INR","event_time":"2024-01-15T10:31:00Z"}'

# Wait ~5 s for the engine to process, then query state
sleep 6
grpcurl -plaintext \
  -d '{"transaction_ref":"SETUP-001"}' \
  localhost:50052 reconx.engine.ReconciliationEngine/GetReconState

# Auto-resolve the mismatch
curl -s -X POST http://localhost:8082/v1/resolve/auto/SETUP-001 \
  -H "Content-Type: application/json" \
  -d '{"strategy":"latest_record"}'

# Verify it is now RESOLVED
grpcurl -plaintext \
  -d '{"transaction_ref":"SETUP-001"}' \
  localhost:50052 reconx.engine.ReconciliationEngine/GetReconState
```

Expected final status: `"status": "RESOLVED"`.
