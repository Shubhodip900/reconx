# ReconX Ingestion Service

## Table of Contents

1. [Overview](#1-overview)
2. [Architecture](#2-architecture)
3. [Data Flow](#3-data-flow)
4. [Source Adapters](#4-source-adapters)
5. [Processing Pipeline](#5-processing-pipeline)
6. [Idempotency](#6-idempotency)
7. [Rate Limiting](#7-rate-limiting)
8. [Dead Letter Queue (DLQ)](#8-dead-letter-queue-dlq)
9. [Storage Schema](#9-storage-schema)
10. [API Reference](#10-api-reference)
11. [Configuration](#11-configuration)
12. [Metrics & Observability](#12-metrics--observability)
13. [Running Locally](#13-running-locally)
14. [Design Decisions](#14-design-decisions)

---

## 1. Overview

The **Ingestion Service** is the data entry point for the ReconX distributed reconciliation engine. Its sole responsibility is:

> Accept raw records from any source system, validate and normalize them into a canonical format, and persist them for the Reconciliation Engine to compare.

It does **not** perform matching or conflict resolution — those are the Reconciliation Engine's (Rust) and Resolution Service's (Go) concerns.

### Key capabilities

| Capability | Description |
|---|---|
| Multi-source ingestion | gRPC, REST webhook, file upload, Kafka, database poll |
| Idempotent processing | Duplicate records return the original response — never double-processed |
| Schema normalization | All sources produce the same canonical `NormalizedRecord` |
| Dead letter queue | Failed records are persisted for retry/inspection |
| Per-source rate limiting | Prevents a single source from overwhelming the pipeline |
| Prometheus metrics | Full observability out of the box |
| Graceful shutdown | Drains in-flight requests before exiting |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                      Ingestion Service                          │
│                                                                 │
│  ┌────────────┐  ┌──────────┐  ┌──────────┐  ┌─────────────┐  │
│  │ gRPC       │  │ Webhook  │  │ File     │  │ Kafka       │  │
│  │ Server     │  │ Handler  │  │ Handler  │  │ Consumer    │  │
│  │ :50051     │  │ :8080    │  │ :8080    │  │ (optional)  │  │
│  └─────┬──────┘  └────┬─────┘  └────┬─────┘  └──────┬──────┘  │
│        │              │              │                │          │
│        └──────────────┴──────────────┴────────────────┘          │
│                               │                                  │
│                        ┌──────▼──────┐                          │
│                        │  Rate Limit │  (per source_system)     │
│                        └──────┬──────┘                          │
│                               │                                  │
│                        ┌──────▼──────┐                          │
│                        │ Idempotency │  (PostgreSQL)             │
│                        │   Check     │                           │
│                        └──────┬──────┘                          │
│                               │                                  │
│                   ┌───────────▼──────────┐                      │
│                   │   Processing Pipeline │                      │
│                   │                      │                      │
│                   │  ① Enrich            │  stamp server time   │
│                   │  ② Validate          │  reject bad data     │
│                   │  ③ Normalize         │  parse JSON payload  │
│                   └──────────┬───────────┘                      │
│                         ┌────┴────┐                             │
│                    OK ◄──┤         ├──► Error → DLQ             │
│                         └────┬────┘                             │
│                              │                                   │
│                      ┌───────▼───────┐                          │
│                      │  PostgreSQL   │  ingestion_records        │
│                      └───────────────┘                          │
│                                                                  │
│  ┌──────────────┐                                               │
│  │ Prometheus   │  :9090/metrics                                │
│  └──────────────┘                                               │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Data Flow

### Single record (gRPC `SubmitRecord`)

```
Client
  │
  │  IngestRequest{idempotency_key, transaction_ref, payload, metadata}
  │
  ▼
Rate Limiter ──► 429 Resource Exhausted (if exceeded)
  │
  ▼
Idempotency Store ──► Return cached response (if duplicate key)
  │ (new key)
  ▼
Pipeline:
  ① Enrich   — stamp ServerReceivedAt (UTC), set adapter_type tag
  ② Validate — check required fields, payload size, currency, clock skew
  ③ Normalize — parse JSON payload → amount (decimal), currency, timestamp
  │
  ├─ Error ──► DLQ table + return error response
  │
  ▼
PostgreSQL: INSERT INTO ingestion_records
  │
  ▼
Idempotency: store response with TTL (default 24h)
  │
  ▼
Return IngestResponse{internal_id, success: true}
```

### Bulk stream (gRPC `BulkStreamIngest`)

```
Client ──[stream IngestRequest, IngestRequest, ...]──►
                                                      │
                              For each record: SubmitRecord() ─► same pipeline
                                                      │
◄── BulkSummary{total_processed, total_failed, duration_ms}
```

### HTTP adapters (webhook / file)

```
POST /ingest/{source_system}  (webhook)
POST /ingest/file             (multipart upload)
        │
        ▼
   Parse body (JSON object / JSON array / NDJSON / CSV)
        │
        ▼
   Push to adapter queue (bounded channel, 10,000 slots)
        │
        ▼
   Background worker calls SubmitRecord() for each record
        │
        ▼
   Same pipeline as gRPC path
```

---

## 4. Source Adapters

### 4.1 gRPC Adapter (primary)

The gRPC adapter is the primary integration path for other ReconX microservices.

**Service definition** (`proto/ingestion.proto`):

```protobuf
service IngestionService {
    rpc SubmitRecord(IngestRequest) returns (IngestResponse);
    rpc BulkStreamIngest(stream IngestRequest) returns (BulkSummary);
}
```

**Example call** (using `grpcurl`):

```bash
grpcurl -plaintext -d '{
  "idempotency_key": "inv-2024-001",
  "transaction_ref": "INV-2024-001",
  "payload": "<base64-encoded-json>",
  "metadata": {
    "source_system": "vendor_portal",
    "trace_id": "abc-123"
  }
}' localhost:50051 reconx.ingestion.IngestionService/SubmitRecord
```

### 4.2 Webhook Adapter

Receives HTTP POST from any system that can call a URL.

**Endpoint**: `POST /ingest/{source_system}`

**Headers**:
- `Content-Type: application/json`
- `X-Trace-Id: <optional trace ID>`

**Payload formats**:

```json
// Single record
{
  "idempotency_key": "inv-2024-001",
  "transaction_ref": "INV-2024-001",
  "amount": "10000.00",
  "currency": "INR",
  "event_time": "2024-01-15T10:30:00Z"
}
```

```json
// JSON array (multiple records in one POST)
[
  {"idempotency_key": "inv-001", "transaction_ref": "TXN-001", ...},
  {"idempotency_key": "inv-002", "transaction_ref": "TXN-002", ...}
]
```

```
// NDJSON (newline-delimited, one per line)
{"idempotency_key": "inv-001", "transaction_ref": "TXN-001"}
{"idempotency_key": "inv-002", "transaction_ref": "TXN-002"}
```

**Response**:

```json
{"queued": 2, "trace_id": "abc-123"}
```

**Example**:

```bash
curl -X POST http://localhost:8080/ingest/vendor_portal \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key":"inv-001","transaction_ref":"INV-001","amount":"10000","currency":"INR"}'
```

### 4.3 File Upload Adapter

For batch ingestion from ERP systems, bank exports, and legacy data dumps.

**Endpoint**: `POST /ingest/file`

**Accepted formats**: `.json`, `.ndjson`, `.csv`

**Required form fields**:
- `file` — the file to upload
- `source_system` — the originating system label

**CSV format**: The header row must contain `idempotency_key` and `transaction_ref`. Additional columns are passed through as tags.

```csv
idempotency_key,transaction_ref,amount,currency,event_time
inv-001,INV-2024-001,10000.00,INR,2024-01-15T10:30:00Z
inv-002,INV-2024-002,9800.00,INR,2024-01-15T11:00:00Z
```

**Example**:

```bash
curl -X POST http://localhost:8080/ingest/file \
  -F "source_system=erp_sap" \
  -F "file=@invoices.csv"
```

### 4.4 Kafka Consumer Adapter

For event-driven architectures where upstream systems publish to Kafka.

**Configuration** (environment variables):

```env
RECONX_KAFKA_ENABLED=true
RECONX_KAFKA_BROKERS=kafka1:9092,kafka2:9092
RECONX_KAFKA_TOPIC=reconx.records.raw
RECONX_KAFKA_GROUP_ID=reconx-ingestion
```

**Message format**: JSON, same as webhook payload. The Kafka message key is used as the `idempotency_key` if not present in the value. Kafka headers `X-Trace-Id` or `trace_id` are extracted for distributed tracing.

### 4.5 REST Poller Adapter

For legacy systems with no event publishing capability.

Configured programmatically — polls an HTTP endpoint on a fixed schedule and parses the NDJSON response:

```go
poller := adapters.NewRESTPoller(adapters.RESTPollerConfig{
    ID:           "rest-legacy-erp",
    SourceSystem: "legacy_erp",
    URL:          "https://erp.company.internal/api/v1/transactions/export",
    PollInterval: 5 * time.Minute,
    Headers:      map[string]string{"Authorization": "Bearer " + token},
}, logger)
go poller.Start(ctx, adapterQueue)
```

### 4.6 Database Poll Adapter

For source systems accessible only via SQL (no API, no Kafka).

Uses a high-watermark cursor pattern — queries for records updated since the last poll:

```go
poller, _ := adapters.NewDBPoller(adapters.DBPollConfig{
    ID:              "db-oracle-erp",
    SourceSystem:    "oracle_erp",
    Driver:          "postgres",
    DSN:             "postgres://user:pass@oracle-proxy:5432/erp",
    Query:           "SELECT id, ref, amount, currency, updated_at FROM invoices WHERE updated_at > $1 ORDER BY updated_at ASC",
    PollInterval:    2 * time.Minute,
    WatermarkColumn: "updated_at",
}, logger)
go poller.Start(ctx, adapterQueue)
```

---

## 5. Processing Pipeline

Records pass through three sequential stages after rate limiting and idempotency checks:

### Stage 1: Enrich

Stamps server-side metadata that clients cannot set:
- `ServerReceivedAt` — UTC wall clock when the server received the record
- `adapter_type` tag — which transport delivered this record

### Stage 2: Validate

Enforces correctness before any parsing occurs. Fails fast with a specific error code:

| Rule | Error Code | Description |
|---|---|---|
| `idempotency_key` present | `missing_field` | Required field |
| `transaction_ref` present | `missing_field` | Required field |
| `source_system` present | `missing_field` | Required field |
| `transaction_ref` is printable ASCII | `invalid_format` | Rejects binary/control characters |
| Payload ≤ 32 MB | `payload_too_large` | OOM protection |
| Currency is ISO 4217 | `invalid_currency` | Rejects unknown currency codes |
| Timestamp not >5min in the future | `future_timestamp` | Clock skew protection |
| Amount is non-negative | `negative_amount` | Rejects negative values |

### Stage 3: Normalize

Parses the raw JSON payload and populates canonical fields:

- `amount` → `decimal.Decimal` (never `float64` — arbitrary precision)
- `currency` → UPPER case string
- `event_time` → `time.Time` UTC (supports RFC3339, RFC3339Nano, ISO 8601)
- `extra` fields → preserved in `Tags` map
- Non-JSON payloads → stored as opaque bytes with `parse_warning` tag

---

## 6. Idempotency

The service implements the **Idempotent Receiver** pattern.

### How it works

1. Client sends `IngestRequest` with a unique `idempotency_key`
2. On first submission: process the record, store the response keyed to `idempotency_key`
3. On re-submission (same key): return the stored response **without re-processing**

### Storage

Idempotency records are stored in PostgreSQL (`ingestion_idempotency` table) with a configurable TTL (default: 24 hours).

```sql
CREATE TABLE ingestion_idempotency (
    idempotency_key TEXT        PRIMARY KEY,
    response_json   JSONB       NOT NULL,
    source_system   TEXT        NOT NULL,
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
```

### Choosing idempotency keys

Keys must be **stable across retries** and **unique per logical event**:

| Good key | Bad key |
|---|---|
| `invoice-2024-001-v1` | UUID generated at call time |
| `order-{order_id}-payment` | timestamp |
| `{source_system}-{event_id}` | sequential counter (might repeat) |

---

## 7. Rate Limiting

Per-source token bucket rate limiting prevents any single upstream from overwhelming the pipeline.

### Configuration

```yaml
ratelimit:
  default_rps: 1000.0
  overrides:
    vendor_portal: 500.0
    legacy_erp:    100.0
    kafka:         5000.0
```

Or via environment variables:

```env
RECONX_RATELIMIT_DEFAULT_RPS=1000
```

When a source exceeds its limit, gRPC returns `ResourceExhausted (429)`. HTTP adapters buffer in the adapter queue (10,000 slots) before applying backpressure.

---

## 8. Dead Letter Queue (DLQ)

Records that fail validation or storage are written to the DLQ table (`ingestion_dlq`) for operator review and retry.

### DLQ table schema

```sql
CREATE TABLE ingestion_dlq (
    id              BIGSERIAL    PRIMARY KEY,
    idempotency_key TEXT         NOT NULL,
    transaction_ref TEXT,
    source_system   TEXT         NOT NULL,
    adapter_type    TEXT,
    raw_payload     BYTEA,
    error_stage     TEXT,        -- "validate" | "normalize" | "storage"
    error_reason    TEXT,        -- "missing_field" | "payload_too_large" | ...
    error_message   TEXT,
    retry_count     INT          DEFAULT 0,
    created_at      TIMESTAMPTZ  DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ  DEFAULT NOW()
);
```

### Retry flow

A background worker (to be implemented in the Resolution Service) calls `dlq.Dequeue()` to fetch entries with `retry_count < max_retries`, re-submits them, and calls `dlq.Delete()` on success or `dlq.MarkRetried()` on failure.

Records that exhaust all retries remain in the DLQ for manual intervention.

---

## 9. Storage Schema

Successfully processed records land in `ingestion_records`:

```sql
CREATE TABLE ingestion_records (
    internal_id         TEXT        PRIMARY KEY,    -- server-assigned UUID
    idempotency_key     TEXT        NOT NULL UNIQUE,
    transaction_ref     TEXT        NOT NULL,       -- cross-system join key
    source_system       TEXT        NOT NULL,
    adapter_type        TEXT,
    amount              TEXT,                       -- decimal string (never float)
    currency            TEXT,
    record_timestamp    TIMESTAMPTZ,               -- source event time (UTC)
    server_received_at  TIMESTAMPTZ NOT NULL,      -- ingestion server time (UTC)
    raw_payload         BYTEA,                     -- original bytes preserved
    payload_schema      TEXT,
    tags                JSONB,
    trace_id            TEXT,
    status              TEXT        DEFAULT 'PENDING',  -- PENDING | MATCHED | ...
    created_at          TIMESTAMPTZ DEFAULT NOW()
);
```

The `transaction_ref` column is the foreign key that the Reconciliation Engine uses to group records across source systems for comparison.

---

## 10. API Reference

### gRPC API

See `proto/ingestion.proto` for the full contract.

| Method | Description |
|---|---|
| `SubmitRecord` | Submit a single record synchronously |
| `BulkStreamIngest` | Stream multiple records; returns aggregate summary |

### HTTP API

| Method | Path | Description |
|---|---|---|
| `POST` | `/ingest/{source_system}` | Webhook receiver |
| `POST` | `/ingest/file` | Multipart file upload |
| `GET` | `/health` | Health check |
| `GET` | `:9090/metrics` | Prometheus metrics |

---

## 11. Configuration

All settings are configurable via environment variables (prefix: `RECONX_`) or `config.yaml`.

| Variable | Default | Description |
|---|---|---|
| `RECONX_GRPC_PORT` | `50051` | gRPC server port |
| `RECONX_HTTP_PORT` | `8080` | HTTP server port |
| `RECONX_METRICS_PORT` | `9090` | Prometheus metrics port |
| `RECONX_DATABASE_DSN` | `postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable` | PostgreSQL DSN |
| `RECONX_KAFKA_ENABLED` | `false` | Enable Kafka consumer |
| `RECONX_KAFKA_BROKERS` | `localhost:9092` | Kafka broker list |
| `RECONX_KAFKA_TOPIC` | `reconx.records.raw` | Kafka topic |
| `RECONX_RATELIMIT_DEFAULT_RPS` | `1000` | Default rate limit (RPS) |
| `RECONX_DLQ_MAX_RETRIES` | `3` | Max DLQ retry attempts |
| `RECONX_IDEMPOTENCY_TTL` | `24h` | Idempotency key TTL |
| `RECONX_LOG_LEVEL` | `info` | Log level: debug/info/warn/error |

---

## 12. Metrics & Observability

### Prometheus metrics (`:9090/metrics`)

| Metric | Type | Labels | Description |
|---|---|---|---|
| `reconx_ingestion_records_total` | Counter | `source_system`, `adapter_type`, `status` | Total records received |
| `reconx_ingestion_duration_seconds` | Histogram | `source_system`, `adapter_type` | End-to-end pipeline latency |
| `reconx_ingestion_validation_failures_total` | Counter | `source_system`, `failure_reason` | Validation failures by rule |
| `reconx_ingestion_dlq_depth` | Gauge | `source_system` | Current DLQ backlog |
| `reconx_ingestion_active_grpc_streams` | Gauge | — | Active streaming connections |
| `reconx_ingestion_idempotency_hits_total` | Counter | `source_system` | Duplicates detected |
| `reconx_ingestion_rate_limited_total` | Counter | `source_system` | Rate-limited rejections |
| `reconx_ingestion_payload_size_bytes` | Histogram | `source_system` | Payload size distribution |

### Structured logs (JSON)

Every record log entry includes:
```json
{
  "level": "info",
  "msg": "record ingested",
  "internal_id": "uuid",
  "tx_ref": "INV-2024-001",
  "source": "vendor_portal",
  "ts": "2024-01-15T10:30:00.123Z"
}
```

---

## 13. Running Locally

### Prerequisites

- Go 1.23+
- PostgreSQL 14+
- (Optional) Kafka 3.x

### Quick start

```bash
# 1. Start PostgreSQL
docker run -d \
  --name reconx-postgres \
  -e POSTGRES_USER=reconx \
  -e POSTGRES_PASSWORD=reconx \
  -e POSTGRES_DB=reconx \
  -p 5432:5432 \
  postgres:16-alpine

# 2. Build and run the ingestion service
cd services/ingestion
make build
./bin/reconx-ingestion

# 3. Submit a test record (gRPC)
grpcurl -plaintext \
  -d '{"idempotency_key":"test-001","transaction_ref":"INV-001","metadata":{"source_system":"test"}}' \
  localhost:50051 reconx.ingestion.IngestionService/SubmitRecord

# 4. Submit via webhook
curl -X POST http://localhost:8080/ingest/test_vendor \
  -H "Content-Type: application/json" \
  -d '{"idempotency_key":"test-002","transaction_ref":"INV-002","amount":"5000","currency":"INR"}'

# 5. View metrics
curl http://localhost:9090/metrics | grep reconx_
```

### Docker

```bash
# From the repo root
docker build -f services/ingestion/Dockerfile -t reconx/ingestion:latest .
docker run -p 50051:50051 -p 8080:8080 -p 9090:9090 \
  -e RECONX_DATABASE_DSN="postgres://reconx:reconx@host.docker.internal:5432/reconx?sslmode=disable" \
  reconx/ingestion:latest
```

---

## 14. Design Decisions

### Why separate validation and normalization stages?

Validation rejects structurally invalid records **before** parsing begins. This prevents expensive parsing operations on garbage data and provides precise error codes. (Pattern borrowed from `moov-io/ach` and `moov-io/fed`.)

### Why `decimal.Decimal` instead of `float64` for amounts?

IEEE 754 floating-point arithmetic produces rounding errors for decimal values. `₹9,800.00 * 1.1825` (GST) produces different results in float64 vs decimal arithmetic. In financial reconciliation, a single-paisa discrepancy causes a mismatch. `shopspring/decimal` provides arbitrary-precision decimal arithmetic.

### Why PostgreSQL for idempotency instead of Redis?

For financial data, idempotency records must be durable. A Redis restart without persistence would cause re-processing of already-handled records. PostgreSQL `ON CONFLICT DO NOTHING` provides the same atomic semantics with full durability.

### Why preserve raw payload (`RawPayload`)?

The Reconciliation Engine compares what each system *actually said*, not our interpretation of it. Dispute resolution (e.g., vendor says ₹10,000 but we stored ₹9,800 due to a normalization bug) requires access to the original bytes. Raw payload is the audit trail.

### Why bounded adapter queue channel?

The 10,000-slot bounded channel between HTTP adapters and the pipeline worker provides **natural backpressure**: when the pipeline is slow (e.g., database saturated), the channel fills up, the HTTP handler blocks on `chan <- rec`, and the HTTP server applies backpressure to clients via slow response times. This prevents OOM from unbounded queuing.
