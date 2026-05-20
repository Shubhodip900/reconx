# Resolution Service

The Resolution Service is the third and final processing layer of ReconX. It consumes MISMATCHED transactions produced by the Reconciliation Engine and resolves them through one of three paths:

1. **Manual resolution** — an operator picks the authoritative source via gRPC.
2. **Automatic resolution** — a deterministic strategy (amount-based, time-based, or priority-based) picks the winner via HTTP REST API.
3. **Retry worker** — a background process periodically re-triggers the engine's matcher in case new data has arrived; auto-resolves exhausted entries if configured.

---

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                  Resolution Service                      │
│                                                         │
│  gRPC :50053          HTTP REST :8082    Metrics :9092  │
│  ┌────────────┐       ┌─────────────┐                   │
│  │ gRPC Server│       │ HTTP Handler│                   │
│  │ ResolveMan │       │ /auto/{ref} │                   │
│  │ ListMismat │       │ /retry/{ref}│                   │
│  └─────┬──────┘       │ /audit/{ref}│                   │
│        │              │ /retry-queue│                   │
│        │              │ /mismatches │                   │
│        │              └──────┬──────┘                   │
│        │                     │                          │
│        └──────────┬──────────┘                          │
│                   │                                     │
│          ┌────────▼────────┐  ┌───────────────────┐    │
│          │   DB Layer      │  │  Retry Worker      │    │
│          │  resolution_    │  │  (background loop) │    │
│          │  records        │  │  ← Engine gRPC     │    │
│          │  retry_queue    │  │    :50052           │    │
│          └────────┬────────┘  └───────────────────┘    │
└───────────────────┼─────────────────────────────────────┘
                    │
              PostgreSQL
```

---

## Ports

| Listener | Default | Purpose |
|---|---|---|
| gRPC | `:50053` | `ResolveManually`, `ListMismatches` |
| HTTP REST | `:8082` | Auto-resolve, retry, audit, queue APIs |
| Prometheus | `:9092` | Metrics scrape endpoint + `/health` |

---

## Database Schema

Tables **owned** by this service (created at startup via `RunMigrations`):

### `resolution_records`

One row per resolved transaction. Re-resolving the same `transaction_ref` is idempotent via `ON CONFLICT DO UPDATE`.

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL` | PK |
| `transaction_ref` | `TEXT UNIQUE` | Links to `recon_state` |
| `resolution_type` | `TEXT` | `MANUAL` or `AUTO` |
| `chosen_source` | `TEXT` | The winning system name |
| `resolution_reason` | `TEXT` | Human-readable explanation |
| `resolver_id` | `TEXT` | Operator ID or `system:api` / `system:retry_worker` |
| `strategy` | `TEXT` | Auto-resolve strategy used (empty for MANUAL) |
| `resolved_at` | `TIMESTAMPTZ` | When the decision was made |

### `resolution_retry_queue`

Tracks automatic retry state per transaction.

| Column | Type | Notes |
|---|---|---|
| `id` | `BIGSERIAL` | PK |
| `transaction_ref` | `TEXT UNIQUE` | Links to `recon_state` |
| `attempt_count` | `INT` | How many retries have run |
| `max_attempts` | `INT` | Ceiling; when reached → EXHAUSTED |
| `last_attempted_at` | `TIMESTAMPTZ` | Nullable |
| `next_retry_at` | `TIMESTAMPTZ` | Worker picks rows where `<= NOW()` |
| `status` | `TEXT` | `PENDING` \| `EXHAUSTED` \| `RESOLVED` |
| `requested_by` | `TEXT` | Who enqueued the retry |
| `created_at` | `TIMESTAMPTZ` | |
| `updated_at` | `TIMESTAMPTZ` | |

Tables **read/written** from other services:

| Table | Owner | Access |
|---|---|---|
| `recon_state` | Engine | Read status; write `RESOLVED` |
| `recon_match_details` | Engine | Read for strategy queries |
| `recon_audit_log` | Engine (shared) | Append audit entries |
| `ingestion_records` | Ingestion | Read for timing/amount strategies |

---

## gRPC API

Proto file: `proto/resolution.proto`

### `ResolveManually`

Allows a human operator to choose the authoritative source for a MISMATCHED transaction.

```
rpc ResolveManually(ResolutionRequest) returns (ResolutionResponse)
```

**Request:**

```protobuf
message ResolutionRequest {
  string transaction_ref    = 1; // required
  string chosen_source      = 2; // required — winning system name
  string resolution_reason  = 3; // optional free text
  string resolver_id        = 4; // required — operator identifier
}
```

**Response:**

```protobuf
message ResolutionResponse {
  bool             success    = 1;
  ReconStatus      new_status = 2; // always RESOLVED on success
}
```

**Error codes:**

| Code | Condition |
|---|---|
| `INVALID_ARGUMENT` | Missing `transaction_ref`, `chosen_source`, or `resolver_id` |
| `NOT_FOUND` | Transaction does not exist in `recon_state` |
| `FAILED_PRECONDITION` | Transaction is PENDING or MATCHED |
| `INTERNAL` | Database error |

### `ListMismatches`

Server-side streaming of all MISMATCHED transactions with optional pagination and source filtering.

```
rpc ListMismatches(FilterRequest) returns (stream StateResponse)
```

**Request:**

```protobuf
message FilterRequest {
  int32  page_size    = 1; // default 20, max 100
  string page_token   = 2; // cursor (transaction_ref of last seen item)
  string source_filter = 3; // filter by system_name in recon_match_details
}
```

**Response stream:** `engine.StateResponse` messages, one per MISMATCHED transaction.

---

## HTTP REST API

Base URL: `http://localhost:8082`

All endpoints return `Content-Type: application/json`. Errors use the envelope:

```json
{ "error": "human-readable message" }
```

### `POST /v1/resolve/auto/{ref}`

Automatically resolves a MISMATCHED (or previously EXHAUSTED) transaction using a configurable strategy.

**Request body** (all fields optional):

```json
{
  "strategy": "latest_record",
  "source_priority": "payment_gateway,erp_system",
  "resolver_id": "cron:daily-sweep"
}
```

| Field | Default | Description |
|---|---|---|
| `strategy` | `cfg.auto_resolve.default_strategy` | Resolution strategy |
| `source_priority` | `cfg.auto_resolve.source_priority` | Comma-separated priority list (for `source_priority` strategy) |
| `resolver_id` | `"system:api"` | Identifies who triggered the resolution |

**Resolution strategies:**

| Strategy | Description |
|---|---|
| `source_priority` | First source from a priority list that has submitted a record wins |
| `latest_record` | Source with the most recent `server_received_at` wins |
| `highest_amount` | Source reporting the highest monetary amount wins |
| `lowest_amount` | Source reporting the lowest monetary amount wins |
| `first_submitted` | Source with the earliest `server_received_at` wins |

**Response `200 OK`:**

```json
{
  "transaction_ref": "INV-2024-001",
  "chosen_source": "payment_gateway",
  "strategy": "source_priority",
  "reason": "source_priority: \"payment_gateway\" is highest-priority source present",
  "resolved_at": "2024-01-15T11:00:00Z"
}
```

**Error codes:**

| HTTP | Condition |
|---|---|
| `404` | Transaction not found |
| `409` | Transaction is not MISMATCHED (e.g. already MATCHED or PENDING) |
| `400` | Unknown strategy string |
| `422` | Strategy ran but could not determine a winner (e.g. no amount data) |

---

### `POST /v1/resolve/retry/{ref}`

Enqueues a MISMATCHED transaction for the background retry worker. If the transaction already has an EXHAUSTED queue entry, it is reset to PENDING (attempt count resets to 0).

**Request body** (all fields optional):

```json
{
  "requested_by": "ops@company.com",
  "max_attempts": 5
}
```

**Response `200 OK`:**

```json
{
  "transaction_ref": "INV-2024-001",
  "max_attempts": 5,
  "message": "transaction enqueued for retry"
}
```

**Error codes:**

| HTTP | Condition |
|---|---|
| `404` | Transaction not found |
| `409` | Transaction is not MISMATCHED (cannot retry RESOLVED or MATCHED) |

---

### `GET /v1/resolve/audit/{ref}`

Returns the complete audit trail for a transaction in chronological order.

**Response `200 OK`:**

```json
{
  "transaction_ref": "INV-2024-001",
  "count": 3,
  "entries": [
    {
      "id": 1,
      "transaction_ref": "INV-2024-001",
      "event_type": "RETRY_ENQUEUED",
      "old_status": "MISMATCHED",
      "new_status": "MISMATCHED",
      "detail": "{\"max_attempts\":\"5\",\"requested_by\":\"ops@company.com\"}",
      "created_at": "2024-01-15T10:35:00Z"
    },
    {
      "id": 2,
      "event_type": "RETRY_ATTEMPT",
      "old_status": "MISMATCHED",
      "new_status": "MISMATCHED",
      "detail": "{\"attempt\":1,\"max_attempts\":5,\"next_retry_at\":\"2024-01-15T11:35:00Z\"}",
      "created_at": "2024-01-15T10:35:30Z"
    },
    {
      "id": 3,
      "event_type": "AUTO_RESOLUTION",
      "old_status": "MISMATCHED",
      "new_status": "RESOLVED",
      "detail": "{\"chosen_source\":\"payment_gateway\",\"strategy\":\"source_priority\"}",
      "created_at": "2024-01-15T11:00:00Z"
    }
  ]
}
```

---

### `GET /v1/resolve/retry-queue`

Lists retry queue entries with cursor-based pagination.

**Query parameters:**

| Param | Default | Description |
|---|---|---|
| `page_size` | `50` (max `200`) | Number of entries per page |
| `page_token` | — | `transaction_ref` cursor from previous response |
| `status` | — | Filter: `PENDING` \| `EXHAUSTED` \| `RESOLVED` |

**Response `200 OK`:**

```json
{
  "count": 2,
  "next_page_token": "INV-2024-002",
  "entries": [
    {
      "transaction_ref": "INV-2024-001",
      "attempt_count": 2,
      "max_attempts": 5,
      "last_attempted_at": "2024-01-15T10:35:30Z",
      "next_retry_at": "2024-01-15T11:35:30Z",
      "status": "PENDING",
      "requested_by": "ops@company.com",
      "created_at": "2024-01-15T10:34:00Z"
    }
  ]
}
```

---

### `GET /v1/resolve/mismatches`

Lists MISMATCHED transactions with cursor-based pagination. HTTP alternative to the gRPC `ListMismatches` stream.

**Query parameters:**

| Param | Default | Description |
|---|---|---|
| `page_size` | `20` (max `100`) | Number of entries per page |
| `page_token` | — | `transaction_ref` cursor from previous response |
| `source` | — | Filter by source system name |

**Response `200 OK`:**

```json
{
  "count": 1,
  "next_page_token": "INV-2024-001",
  "entries": [
    {
      "transaction_ref": "INV-2024-001",
      "last_updated": "2024-01-15T10:31:00Z",
      "details": [
        { "system_name": "vendor_portal", "discrepancy_found": false },
        { "system_name": "erp_system",    "discrepancy_found": true  }
      ]
    }
  ]
}
```

---

### `GET /health`

Liveness probe. Returns `200 OK` if the process is running.

```json
{ "status": "ok", "service": "reconx-resolution" }
```

---

## Retry Worker

The retry worker is a background goroutine that automatically re-triggers matching for MISMATCHED transactions.

### Flow

```
every poll_interval_secs:
  1. Refresh RetryQueueDepth / RetryQueueExhausted Prometheus gauges
  2. Fetch up to batch_size PENDING entries where next_retry_at <= NOW()
  3. For each entry:
       a. Call engine.ReTriggerMatch(transaction_ref) via gRPC
       b. If engine returns MATCHED or RESOLVED:
            → MarkRetryResolved; write audit log
       c. If engine returns MISMATCHED and attempt < max_attempts:
            → IncrementRetryAttempt with next_retry_at = now + backoff
       d. If attempt >= max_attempts:
            → MarkRetryExhausted; write audit log
            → If auto_apply_on_exhaustion=true: run default strategy → apply resolution
```

### Backoff formula

```
backoff = min(base_backoff_secs × 2^(attempt-1), max_backoff_secs)
next_retry_at = now + backoff
```

With defaults (`base=60s`, `max=3600s`):

| Attempt | Backoff |
|---|---|
| 1 | 60 s |
| 2 | 120 s |
| 3 | 240 s |
| 4 | 480 s |
| 5 | 960 s (capped at 3600 s) |

### Audit event types

| Event | Trigger |
|---|---|
| `RETRY_ENQUEUED` | `POST /v1/resolve/retry/{ref}` called |
| `RETRY_ATTEMPT` | Worker attempted re-match; still MISMATCHED |
| `RETRY_RESOLVED` | Engine returned MATCHED/RESOLVED |
| `RETRY_ENGINE_ERROR` | gRPC call to engine failed |
| `RETRY_EXHAUSTED` | Max attempts reached |
| `AUTO_RESOLUTION` | Auto-resolve strategy applied (worker or API) |
| `MANUAL_RESOLUTION` | `ResolveManually` gRPC called |

---

## Configuration

Environment variable prefix: `RECONX_RESOLUTION_`

```env
# gRPC server
RECONX_RESOLUTION_GRPC_PORT=50053

# HTTP REST API
RECONX_RESOLUTION_HTTP_PORT=8082

# Prometheus metrics
RECONX_RESOLUTION_METRICS_PORT=9092
RECONX_RESOLUTION_METRICS_PATH=/metrics

# PostgreSQL
RECONX_RESOLUTION_DATABASE_DSN=postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable
RECONX_RESOLUTION_DATABASE_MAX_OPEN_CONNS=10
RECONX_RESOLUTION_DATABASE_MAX_IDLE_CONNS=3
RECONX_RESOLUTION_DATABASE_CONN_MAX_LIFETIME=5m

# Engine gRPC client
RECONX_RESOLUTION_ENGINE_ADDRESS=localhost:50052
RECONX_RESOLUTION_ENGINE_DIAL_TIMEOUT=5s
RECONX_RESOLUTION_ENGINE_REQUEST_TIMEOUT=10s

# Retry worker
RECONX_RESOLUTION_RETRY_ENABLED=true
RECONX_RESOLUTION_RETRY_POLL_INTERVAL_SECS=30
RECONX_RESOLUTION_RETRY_MAX_ATTEMPTS=5
RECONX_RESOLUTION_RETRY_BASE_BACKOFF_SECS=60
RECONX_RESOLUTION_RETRY_MAX_BACKOFF_SECS=3600
RECONX_RESOLUTION_RETRY_BATCH_SIZE=50

# Auto-resolve
RECONX_RESOLUTION_AUTO_RESOLVE_DEFAULT_STRATEGY=latest_record
RECONX_RESOLUTION_AUTO_RESOLVE_SOURCE_PRIORITY=payment_gateway,erp_system,vendor_portal
RECONX_RESOLUTION_AUTO_RESOLVE_AUTO_APPLY_ON_EXHAUSTION=false
```

A YAML config file (`config.yaml`) placed in the working directory or `/etc/reconx/resolution/` is also supported (Viper).

---

## Prometheus Metrics

| Metric | Type | Labels | Description |
|---|---|---|---|
| `reconx_resolution_resolutions_total` | Counter | `resolver_id` | Successful manual resolutions |
| `reconx_resolution_resolution_errors_total` | Counter | `error_kind` | Failed resolution attempts |
| `reconx_resolution_list_mismatches_streamed_total` | Counter | — | Records streamed via ListMismatches |
| `reconx_resolution_rpc_duration_seconds` | Histogram | `method` | gRPC handler latency |
| `reconx_resolution_auto_resolutions_total` | Counter | `strategy`, `outcome` | Auto-resolve operations |
| `reconx_resolution_auto_resolution_duration_seconds` | Histogram | `strategy` | Auto-resolve latency |
| `reconx_resolution_retry_attempts_total` | Counter | `outcome` | Retry worker attempts |
| `reconx_resolution_retry_queue_depth` | Gauge | — | Current PENDING queue entries |
| `reconx_resolution_retry_queue_exhausted` | Gauge | — | Current EXHAUSTED queue entries |
| `reconx_resolution_retry_worker_cycle_duration_seconds` | Histogram | — | Per-cycle worker duration |
| `reconx_resolution_retry_worker_errors_total` | Counter | — | Unrecoverable worker errors |
| `reconx_resolution_http_requests_total` | Counter | `method`, `path`, `status` | HTTP REST request count |
| `reconx_resolution_http_request_duration_seconds` | Histogram | `method`, `path` | HTTP REST latency |

---

## Running the Service

```bash
cd services/resolution

# Development
go run ./cmd/...

# Build
go build -o bin/reconx-resolution ./cmd/...
./bin/reconx-resolution

# Docker (from repo root)
docker build -f services/resolution/Dockerfile -t reconx-resolution .
```

### Quick-start example

```bash
# 1. Trigger auto-resolve with source priority
curl -X POST http://localhost:8082/v1/resolve/auto/INV-2024-001 \
  -H "Content-Type: application/json" \
  -d '{
    "strategy": "source_priority",
    "source_priority": "payment_gateway,erp_system",
    "resolver_id": "ops-team"
  }'

# 2. Enqueue for retry (engine will re-check on next data arrival)
curl -X POST http://localhost:8082/v1/resolve/retry/INV-2024-001 \
  -H "Content-Type: application/json" \
  -d '{"requested_by": "ops@company.com", "max_attempts": 3}'

# 3. View full audit trail
curl http://localhost:8082/v1/resolve/audit/INV-2024-001

# 4. List all mismatches (page 1)
curl "http://localhost:8082/v1/resolve/mismatches?page_size=20"

# 5. List retry queue — EXHAUSTED entries needing attention
curl "http://localhost:8082/v1/resolve/retry-queue?status=EXHAUSTED"

# 6. Manual resolution via gRPC
grpcurl -plaintext \
  -d '{
    "transaction_ref": "INV-2024-001",
    "chosen_source": "payment_gateway",
    "resolution_reason": "Payment gateway is always authoritative for settled transactions",
    "resolver_id": "alice@company.com"
  }' \
  localhost:50053 reconx.resolution.ResolutionService/ResolveManually
```

---

## Internal Package Structure

```
services/resolution/
├── cmd/
│   └── main.go                  # Entrypoint — wires all components
├── internal/
│   ├── api/
│   │   └── handler.go           # HTTP REST handlers (Go 1.22+ ServeMux routing)
│   ├── config/
│   │   └── config.go            # Viper config: gRPC, HTTP, DB, engine, retry, auto-resolve
│   ├── db/
│   │   └── db.go                # All PostgreSQL queries + RunMigrations
│   ├── engine/
│   │   └── client.go            # gRPC client for Reconciliation Engine
│   ├── metrics/
│   │   └── metrics.go           # Prometheus metrics + Register()
│   ├── resolver/
│   │   └── strategies.go        # Conflict resolution strategies (5 implementations)
│   ├── retry/
│   │   └── worker.go            # Background retry worker with exponential backoff
│   └── server/
│       └── server.go            # gRPC server (ResolveManually, ListMismatches)
└── go.mod
```
