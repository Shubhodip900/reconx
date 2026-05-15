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
| (Go)           |  | Engine (Rust)    |  | (Go)           |
+----------------+  +------------------+  +----------------+
       |                  |                           |
       ------------------------------------------------
                          |
                 +-------------------+
                 | PostgreSQL        |
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
| Reconciliation Engine | Rust | Memory safety, high-performance core logic |
| Resolution Service | Go | Network services, concurrency |
| Storage | PostgreSQL | Strong consistency, complex queries, transactions |
| Transport | gRPC | High performance, strong typing via proto contracts |
| Messaging | Kafka (optional) | Event-driven decoupling, scalability |
| Metrics | Prometheus + Grafana | Industry-standard observability |
| Logging | Zap (structured JSON) | High-performance structured logging |

---

## Repository Structure

```
reconx/
├── proto/                         # gRPC contract definitions
│   ├── common.proto               # Shared types (ReconStatus, Metadata, Error)
│   ├── ingestion.proto            # IngestionService API
│   ├── engine.proto               # ReconciliationEngine API
│   ├── resolution.proto           # ResolutionService API
│   └── gen/
│       └── go/                    # Generated Go bindings
│           ├── common/
│           ├── ingestion/
│           ├── engine/
│           └── resolution/
│
├── services/
│   └── ingestion/                 # Ingestion Service (Go)
│       ├── cmd/
│       │   └── main.go            # Entrypoint
│       ├── internal/
│       │   ├── adapters/          # Source connectors
│       │   │   ├── adapter.go     # SourceAdapter interface
│       │   │   ├── rest.go        # REST polling adapter
│       │   │   ├── webhook.go     # HTTP webhook receiver
│       │   │   ├── kafka.go       # Kafka consumer
│       │   │   ├── file.go        # File upload (NDJSON/CSV)
│       │   │   └── db.go          # Database polling adapter
│       │   ├── pipeline/
│       │   │   ├── pipeline.go    # Stage chain + NormalizedRecord model
│       │   │   ├── validate.go    # Validation stage
│       │   │   └── normalize.go   # Normalization + enrichment stages
│       │   ├── server/
│       │   │   └── server.go      # gRPC IngestionService implementation
│       │   ├── idempotency/
│       │   │   └── idempotency.go # Idempotent receiver (PostgreSQL)
│       │   ├── ratelimit/
│       │   │   └── ratelimit.go   # Per-source token bucket
│       │   ├── dlq/
│       │   │   └── dlq.go         # Dead letter queue (PostgreSQL)
│       │   ├── storage/
│       │   │   └── storage.go     # PostgreSQL record persistence
│       │   ├── metrics/
│       │   │   └── metrics.go     # Prometheus metrics
│       │   └── config/
│       │       └── config.go      # Configuration (Viper + env vars)
│       ├── Makefile
│       └── Dockerfile
│
└── docs/
    └── ingestion-service.md       # Full ingestion service documentation
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

### Reconciliation Engine (Rust) — `services/engine/` 🚧

Core brain of the system. Groups records by `transaction_ref`, compares values across source systems, and detects discrepancies.

### Resolution Service (Go) — `services/resolution/` 🚧

Applies resolution strategies: auto-resolve by business rules, manual review queue, or retry. Exposes the `ListMismatches` streaming API for dashboards.

---

## Quick Start

### Prerequisites

- Go 1.23+
- PostgreSQL 16+
- `protoc` + `protoc-gen-go` + `protoc-gen-go-grpc` (for proto regeneration only)

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

### 2. Build and run the Ingestion Service

```bash
cd services/ingestion
make build
./bin/reconx-ingestion
```

The service starts three listeners:
- `:50051` — gRPC API
- `:8080` — HTTP API (webhooks + file upload + health)
- `:9090` — Prometheus metrics

### 3. Send a test record

```bash
# Via webhook (HTTP)
curl -X POST http://localhost:8080/ingest/vendor_portal \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "inv-2024-001",
    "transaction_ref": "INV-2024-001",
    "amount": "10000.00",
    "currency": "INR",
    "event_time": "2024-01-15T10:30:00Z"
  }'

# Via gRPC (requires grpcurl)
grpcurl -plaintext \
  -d '{"idempotency_key":"inv-001","transaction_ref":"INV-001","metadata":{"source_system":"vendor_portal"}}' \
  localhost:50051 reconx.ingestion.IngestionService/SubmitRecord
```

### 4. Check metrics

```bash
curl http://localhost:9090/metrics | grep reconx_
```

---

## Configuration

All configuration is driven by environment variables (prefix `RECONX_`):

```env
RECONX_GRPC_PORT=50051
RECONX_HTTP_PORT=8080
RECONX_DATABASE_DSN=postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable
RECONX_KAFKA_ENABLED=false
RECONX_RATELIMIT_DEFAULT_RPS=1000
RECONX_LOG_LEVEL=info
```

See [docs/ingestion-service.md#11-configuration](docs/ingestion-service.md#11-configuration) for the full list.

---

## Development

### Regenerating proto bindings

```bash
cd services/ingestion
make proto
```

### Running tests

```bash
cd services/ingestion
make test
```

### Build Docker image

```bash
cd services/ingestion
make docker
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
| Reconciliation Engine (Rust) | 🚧 In Progress |
| Resolution Service (Go) | 🚧 In Progress |
| API Gateway (Go) | 🚧 Planned |
| Observability stack | 🚧 Planned |
| Docker Compose (full stack) | 🚧 Planned |

---

## Documentation

- [Ingestion Service](docs/ingestion-service.md) — Architecture, adapters, pipeline, API reference, configuration
