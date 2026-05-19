# ReconX — Distributed Reconciliation Engine

A production-grade, distributed reconciliation engine designed to automatically match, validate, and resolve inconsistencies between datasets originating from multiple independent systems.

## Problem Statement

Modern enterprises receive data from procurement platforms, vendor portals, payment gateways, and internal ERP systems — all asynchronously, with delays, and with conflicting values.

**Example:** A vendor invoice says ₹10,000. Your ERP recorded ₹9,800. The payment gateway processed ₹10,000. Which is correct? Who resolves it? How is the decision tracked?

ReconX automates this process end-to-end.

---

## Architecture

```
               +----------------------+
               |   CLI / Dashboard    |
               +----------+-----------+
                          |
                       gRPC API
                          |
               +----------------------+
               |   API Gateway (Go)   |
               +----------+-----------+
                          |
       ------------------------------------------------
       |                  |                           |
+----------------+  +------------------+  +----------------+
| Ingestion Svc  |  | Reconciliation   |  | Resolution Svc |
| (Go)  :50051   |  | Engine (Rust)    |  | (Go)           |
|       :8080    |  |       :50052     |  |                |
+----------------+  +------------------+  +----------------+
       |                  |                           |
       ------------------------------------------------
                          |
                 +-------------------+
                 | PostgreSQL        |
                 | ingestion_records |
                 | recon_state       |
                 | recon_match_details|
                 | recon_audit_log   |
                 +-------------------+

       +-------------------+
       | Kafka (optional)  |
       +-------------------+

Observability: Prometheus + Grafana + ELK + Alertmanager
```

---

## Tech Stack

| Component | Technology | Why |
|---|---|---|
| Ingestion Service | Go | Lightweight goroutines, fast gRPC support |
| Reconciliation Engine | Rust | Memory safety, exact decimal arithmetic, zero-cost concurrency |
| Resolution Service | Go | Network services, concurrency |
| Storage | PostgreSQL | Strong consistency, complex queries, transactions |
| Transport | gRPC | High performance, strong typing via proto contracts |
| Messaging | Kafka (optional) | Event-driven decoupling, scalability |
| Metrics | Prometheus + Grafana | Industry-standard observability |
| Logging | Zap (Go) / tracing (Rust) | High-performance structured logging |

---

## Repository Structure

```
reconx/
├── proto/                              # gRPC contract definitions
│   ├── common.proto                    # Shared types (ReconStatus, Metadata, Error)
│   ├── ingestion.proto                 # IngestionService API
│   ├── engine.proto                    # ReconciliationEngine API
│   ├── resolution.proto                # ResolutionService API
│   └── gen/
│       └── go/                         # Generated Go bindings
│           ├── common/
│           ├── ingestion/
│           ├── engine/
│           └── resolution/
│
├── services/
│   ├── ingestion/                      # Ingestion Service (Go) ✅
│   │   ├── cmd/
│   │   │   └── main.go                 # Entrypoint
│   │   ├── internal/
│   │   │   ├── adapters/               # Source connectors (gRPC/webhook/file/kafka/rest/db)
│   │   │   ├── pipeline/               # Enrich → Validate → Normalize stage chain
│   │   │   ├── server/                 # gRPC IngestionService implementation
│   │   │   ├── idempotency/            # Idempotent receiver (PostgreSQL-backed)
│   │   │   ├── ratelimit/              # Per-source token bucket
│   │   │   ├── dlq/                    # Dead letter queue (PostgreSQL)
│   │   │   ├── storage/                # ingestion_records persistence
│   │   │   ├── metrics/                # Prometheus metrics
│   │   │   └── config/                 # Viper + env var configuration
│   │   ├── Makefile
│   │   └── Dockerfile
│   │
│   └── engine/                         # Reconciliation Engine (Rust) ✅
│       ├── src/
│       │   ├── main.rs                 # Entrypoint — startup, gRPC server, worker, metrics
│       │   ├── config.rs               # Configuration structs (layered: file + env vars)
│       │   ├── error.rs                # Unified EngineError + tonic::Status conversions
│       │   ├── metrics.rs              # Prometheus metrics registry
│       │   ├── db/
│       │   │   ├── models.rs           # IngestionRecord, ReconState, MatchDetail, AuditLog
│       │   │   └── queries.rs          # All SQL queries + schema migrations
│       │   ├── engine/
│       │   │   ├── matcher.rs          # Core matching logic (exact/tolerance/majority)
│       │   │   ├── rules.rs            # Configurable rule set + Tolerance type
│       │   │   └── worker.rs           # Background reconciliation worker (Tokio task)
│       │   └── grpc/
│       │       ├── proto.rs            # tonic::include_proto! bindings
│       │       └── server.rs           # GetReconState + ReTriggerMatch handlers
│       ├── config/
│       │   └── default.toml            # Default configuration values
│       ├── build.rs                    # tonic-build proto compilation
│       ├── Cargo.toml
│       ├── Makefile
│       └── Dockerfile
│
└── docs/
    ├── ingestion-service.md            # Ingestion Service full documentation
    └── reconciliation-engine.md       # Reconciliation Engine full documentation
```

---

## Services

### Ingestion Service (Go) — `services/ingestion/` ✅

The data entry layer. Accepts records from multiple sources, validates and normalizes them, and persists them for the Reconciliation Engine.

**Supported ingestion paths:**

| Transport | Endpoint / Config | Use Case |
|---|---|---|
| gRPC `SubmitRecord` | `:50051` | Other microservices, typed API |
| gRPC `BulkStreamIngest` | `:50051` | High-throughput batch loads |
| HTTP Webhook | `POST :8080/ingest/{source}` | SaaS platforms, vendor callbacks |
| File Upload | `POST :8080/ingest/file` | ERP exports, bank statements, CSV dumps |
| Kafka Consumer | configurable topic | Event-driven upstream systems |
| REST Poller | configurable URL + interval | Legacy systems with polling APIs |
| DB Poller | configurable SQL + watermark | Systems accessible only via SQL |

**Key features:**
- Idempotent processing (PostgreSQL-backed, 24h TTL)
- Per-source rate limiting (token bucket, configurable RPS)
- 3-stage pipeline: Enrich → Validate → Normalize
- Dead letter queue for failed records
- Prometheus metrics on all operations
- Graceful shutdown with drain

See **[docs/ingestion-service.md](docs/ingestion-service.md)** for full documentation.

---

### Reconciliation Engine (Rust) — `services/engine/` ✅

Core brain of the system. Continuously polls for unprocessed transactions, groups records by `transaction_ref`, applies configurable matching logic, and stores the result.

**Matching strategies:**

| Strategy | Description | Use case |
|---|---|---|
| `exact` | All amounts must be identical | Zero-tolerance financial systems |
| `tolerance` | Amounts within `max(abs_tol, pct% of ref)` are matched | Rounding-tolerant reconciliation |
| `majority` | Largest agreement group wins; outliers flagged | 3+ source comparison |

**Key features:**
- `rust_decimal` for all amount arithmetic — no IEEE 754 rounding errors
- Configurable `expected_sources` — wait for all required systems before deciding
- Pending timeout — escalates to MISMATCHED when sources are silent
- gRPC `ReTriggerMatch` — force immediate re-evaluation of any transaction
- Full audit trail in `recon_audit_log`
- Inline reconciliation on first `GetReconState` query (best-effort)
- Prometheus metrics on all operations
- Graceful shutdown via broadcast signal

See **[docs/reconciliation-engine.md](docs/reconciliation-engine.md)** for full documentation.

---

### Resolution Service (Go) — `services/resolution/` 🚧

Applies resolution strategies: auto-resolve by business rules, manual review queue, or retry. Exposes the `ListMismatches` streaming API for dashboards.

---

## Quick Start

### Prerequisites

- Go 1.23+
- Rust 1.82+ (`rustup update`)
- PostgreSQL 16+
- `protoc` + plugins (for proto regeneration only)

### 1. Start PostgreSQL

```bash
docker run -d \
  --name reconx-postgres \
  -e POSTGRES_USER=reconx \
  -e POSTGRES_PASSWORD=reconx \
  -e POSTGRES_DB=reconx \
  -p 5432:5432 \
  postgres:16-alpine
```

### 2. Start the Ingestion Service

```bash
cd services/ingestion
make build
./bin/reconx-ingestion
```

Starts three listeners:
- `:50051` — gRPC API
- `:8080` — HTTP API (webhooks + file upload + health)
- `:9090` — Prometheus metrics

### 3. Ingest test records

```bash
# Vendor says ₹10,000
curl -X POST http://localhost:8080/ingest/vendor_portal \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "inv-2024-001-vendor",
    "transaction_ref": "INV-2024-001",
    "amount": "10000.00",
    "currency": "INR",
    "event_time": "2024-01-15T10:30:00Z"
  }'

# ERP says ₹9,800 (discrepancy!)
curl -X POST http://localhost:8080/ingest/erp_system \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "inv-2024-001-erp",
    "transaction_ref": "INV-2024-001",
    "amount": "9800.00",
    "currency": "INR",
    "event_time": "2024-01-15T10:31:00Z"
  }'
```

### 4. Start the Reconciliation Engine

```bash
cd services/engine
cargo run
```

Starts two listeners:
- `:50052` — gRPC API
- `:9091` — Prometheus metrics + health

Within `poll_interval_secs` (default: 5s), the engine will automatically detect and process the `INV-2024-001` transaction.

### 5. Query reconciliation state

```bash
grpcurl -plaintext \
  -d '{"transaction_ref":"INV-2024-001"}' \
  localhost:50052 reconx.engine.ReconciliationEngine/GetReconState
```

Expected response (₹200 discrepancy detected):
```json
{
  "transaction_ref": "INV-2024-001",
  "status": "MISMATCHED",
  "details": [
    { "system_name": "vendor_portal", "discrepancy_found": false },
    { "system_name": "erp_system",    "discrepancy_found": true  }
  ],
  "last_updated": 1705316400000
}
```

### 6. Force re-evaluation after correction

```bash
grpcurl -plaintext \
  -d '{"transaction_ref":"INV-2024-001"}' \
  localhost:50052 reconx.engine.ReconciliationEngine/ReTriggerMatch
```

### 7. Check metrics

```bash
# Ingestion Service
curl http://localhost:9090/metrics | grep reconx_ingestion_

# Reconciliation Engine
curl http://localhost:9091/metrics | grep reconx_engine_
```

---

## Configuration

### Ingestion Service (prefix `RECONX_`)

```env
RECONX_GRPC_PORT=50051
RECONX_HTTP_PORT=8080
RECONX_DATABASE_DSN=postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable
RECONX_KAFKA_ENABLED=false
RECONX_RATELIMIT_DEFAULT_RPS=1000
RECONX_LOG_LEVEL=info
```

See [docs/ingestion-service.md](docs/ingestion-service.md) for the full list.

### Reconciliation Engine (prefix `RECONX_ENGINE__`)

```env
RECONX_ENGINE__GRPC__PORT=50052
RECONX_ENGINE__DATABASE__DSN=postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable
RECONX_ENGINE__ENGINE__MATCH_STRATEGY=tolerance
RECONX_ENGINE__ENGINE__AMOUNT_TOLERANCE_PCT=1.0
RECONX_ENGINE__ENGINE__AMOUNT_TOLERANCE_ABS=0.50
RECONX_ENGINE__ENGINE__POLL_INTERVAL_SECS=5
RECONX_ENGINE__ENGINE__EXPECTED_SOURCES='["vendor_portal","erp_system"]'
RECONX_ENGINE__LOG__LEVEL=info
```

See [docs/reconciliation-engine.md](docs/reconciliation-engine.md) for the full list.

---

## Development

### Ingestion Service

```bash
cd services/ingestion
make test          # run tests
make lint          # golangci-lint
make build         # compile binary
make docker        # build Docker image
make proto         # regenerate Go proto bindings
```

### Reconciliation Engine

```bash
cd services/engine
cargo test         # run unit tests (no DB required)
cargo test -- --nocapture  # with stdout
cargo clippy       # lints
cargo fmt          # format
cargo build --release
make docker        # build Docker image (from repo root)
```

---

## Proto Contracts

| Service | File | Status |
|---|---|---|
| Ingestion | `proto/ingestion.proto` | ✅ Final |
| Reconciliation Engine | `proto/engine.proto` | ✅ Final |
| Resolution | `proto/resolution.proto` | ✅ Final |
| Common types | `proto/common.proto` | ✅ Final |

---

## Status

| Component | Status |
|---|---|
| Proto contracts | ✅ Complete |
| Ingestion Service (Go) | ✅ Complete |
| Reconciliation Engine (Rust) | ✅ Complete |
| Resolution Service (Go) | 🚧 In Progress |
| API Gateway (Go) | 🚧 Planned |
| Observability stack (Grafana/ELK) | 🚧 Planned |
| Docker Compose (full stack) | 🚧 Planned |

---

## Documentation

- [Ingestion Service](docs/ingestion-service.md) — Architecture, adapters, pipeline, API reference, configuration
- [Reconciliation Engine](docs/reconciliation-engine.md) — Matching logic, database schema, gRPC API, worker internals, configuration
