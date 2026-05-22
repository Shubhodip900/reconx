# API Gateway

The API Gateway is the single public-facing HTTP entry point for the ReconX platform. All client traffic enters here; the gateway routes requests to the appropriate upstream service over gRPC or HTTP.

> **Default port:** `:8090`  
> **Metrics port:** `:9093` (Prometheus, no auth)  
> **Language:** Go  
> **Source:** `services/gateway/`

---

## Table of Contents

1. [Architecture](#1-architecture)
2. [Authentication](#2-authentication)
3. [Route Overview](#3-route-overview)
4. [Ingestion Routes](#4-ingestion-routes)
5. [Reconciliation Engine Routes](#5-reconciliation-engine-routes)
6. [Resolution Routes](#6-resolution-routes)
7. [Health & Observability](#7-health--observability)
8. [Error Format](#8-error-format)
9. [Configuration](#9-configuration)
10. [Running Locally](#10-running-locally)

---

## 1. Architecture

```
                        ┌──────────────────────────────────────────┐
Client (REST)  ──────►  │            API Gateway :8090              │
                        │                                          │
                        │  ┌──────────────────────────────────┐   │
                        │  │   Auth Middleware (X-API-Key)     │   │
                        │  └──────────────┬───────────────────┘   │
                        │                 │                        │
                        │  ┌──────────────▼───────────────────┐   │
                        │  │         HTTP ServeMux             │   │
                        │  └──┬───────────┬────────────┬──────┘   │
                        │     │           │            │           │
                        │  gRPC        gRPC         HTTP proxy    │
                        └──┬──┴───────────┴──────────────┴────────┘
                           │
             ┌─────────────┼─────────────┐
             ▼             ▼             ▼
     Ingestion :50051  Engine :50052  Resolution :50053 (gRPC)
                                      Resolution :8082  (HTTP)
```

The gateway has two servers:

| Server | Port | Purpose |
|---|---|---|
| API server | `:8090` | All REST endpoints (auth-protected) |
| Metrics server | `:9093` | `/metrics` (Prometheus scrape, no auth) |

---

## 2. Authentication

All `/v1/*` routes require the `X-API-Key` header.

**Header:**

```
X-API-Key: <your-api-key>
```

The key is configured via the `RECONX_GATEWAY_API_KEY` environment variable. If the variable is empty or unset, authentication is **disabled** (useful for local development).

**Exempt routes** — no key required:

- `GET /health`
- `GET /metrics` (served on a separate port)

**Unauthenticated response:**

```http
HTTP/1.1 401 Unauthorized
Content-Type: application/json

{"error": "unauthorized"}
```

---

## 3. Route Overview

| Method | Gateway path | Backend | Notes |
|---|---|---|---|
| `POST` | `/v1/ingest` | Ingestion gRPC | Submit a single record |
| `GET` | `/v1/recon/{ref}` | Engine gRPC | Get reconciliation state |
| `POST` | `/v1/recon/{ref}/retrigger` | Engine gRPC | Re-trigger matching |
| `POST` | `/v1/resolution/{ref}` | Resolution gRPC | Manual resolution |
| `GET` | `/v1/resolution/mismatches` | Resolution gRPC | List mismatched transactions |
| `POST` | `/v1/resolution/{ref}/auto` | Resolution HTTP | Auto-resolve with strategy |
| `POST` | `/v1/resolution/{ref}/retry` | Resolution HTTP | Enqueue for retry worker |
| `GET` | `/v1/resolution/{ref}/audit` | Resolution HTTP | Full audit trail |
| `GET` | `/v1/resolution/retry-queue` | Resolution HTTP | List retry queue |
| `GET` | `/health` | Gateway (aggregate) | Liveness + upstream health |

---

## 4. Ingestion Routes

### `POST /v1/ingest`

Submit a single financial record for reconciliation. Delegates to `Ingestion.SubmitRecord` via gRPC.

**Request headers:**

```
Content-Type: application/json
X-API-Key: <key>
```

**Request body:**

```json
{
  "idempotency_key": "inv-2024-001",
  "transaction_ref": "INV-2024-001",
  "payload": "<base64-encoded JSON>",
  "source_system": "payment_gateway",
  "trace_id": "abc-123",
  "tags": {
    "region": "IN"
  }
}
```

| Field | Required | Description |
|---|---|---|
| `idempotency_key` | yes | Stable unique key; duplicate submissions return the original response |
| `transaction_ref` | yes | Cross-system join key used by the Reconciliation Engine |
| `payload` | no | Base64-encoded raw record bytes |
| `source_system` | yes | Name of the originating system |
| `trace_id` | no | Distributed tracing ID |
| `tags` | no | Arbitrary key-value metadata |

**Response `202 Accepted`:**

```json
{
  "internal_id": "01HXYZ...",
  "success": true
}
```

**Error codes:**

| HTTP | Condition |
|---|---|
| `400` | Invalid JSON body |
| `409` | Duplicate `idempotency_key` with conflicting data |
| `429` | Source system rate limit exceeded |
| `503` | Ingestion service unavailable |

---

## 5. Reconciliation Engine Routes

### `GET /v1/recon/{ref}`

Returns the current reconciliation state for a transaction.

**Path parameter:** `ref` — the `transaction_ref` value.

**Response `200 OK`:**

```json
{
  "transaction_ref": "INV-2024-001",
  "status": "MISMATCHED",
  "details": [
    {"system_name": "payment_gateway", "discrepancy_found": false},
    {"system_name": "erp_system",      "discrepancy_found": true}
  ],
  "last_updated_ms": 1705312260000
}
```

| Status | Meaning |
|---|---|
| `PENDING` | Waiting for all source systems to submit records |
| `MATCHED` | All sources agree — reconciliation complete |
| `MISMATCHED` | Discrepancy detected between sources |
| `RESOLVED` | Mismatch resolved (manually or automatically) |

**Error codes:**

| HTTP | Condition |
|---|---|
| `400` | Missing `ref` |
| `404` | Transaction not found |
| `503` | Engine unavailable |

---

### `POST /v1/recon/{ref}/retrigger`

Re-triggers the matching algorithm for a transaction. Useful after new records arrive or data is corrected.

**Path parameter:** `ref` — the `transaction_ref` value.

**Request body:** none required.

**Response `200 OK`:** same shape as `GET /v1/recon/{ref}`.

**Error codes:** same as `GET /v1/recon/{ref}`.

---

## 6. Resolution Routes

### `POST /v1/resolution/{ref}`

Manual resolution: an operator explicitly picks the authoritative source. Delegates to `Resolution.ResolveManually` via gRPC.

**Request body:**

```json
{
  "chosen_source": "payment_gateway",
  "resolution_reason": "Payment gateway is the system of record for settled transactions",
  "resolver_id": "alice@company.com"
}
```

| Field | Required | Description |
|---|---|---|
| `chosen_source` | yes | System name of the authoritative record |
| `resolution_reason` | no | Free-text explanation |
| `resolver_id` | yes | Operator identifier |

**Response `200 OK`:**

```json
{
  "success": true,
  "new_status": "RESOLVED"
}
```

**Error codes:**

| HTTP | Condition |
|---|---|
| `400` | Missing required fields |
| `404` | Transaction not found |
| `409` | Transaction is PENDING or MATCHED — cannot resolve |
| `503` | Resolution service unavailable |

---

### `GET /v1/resolution/mismatches`

Lists all MISMATCHED transactions. Streams from `Resolution.ListMismatches` gRPC and buffers into a JSON array.

**Query parameters:**

| Param | Default | Description |
|---|---|---|
| `page_size` | `20` (max `100`) | Records per page |
| `page_token` | — | Cursor from previous response (`transaction_ref` of last item) |
| `source_filter` | — | Filter by source system name |

**Response `200 OK`:**

```json
{
  "items": [
    {
      "transaction_ref": "INV-2024-001",
      "status": "MISMATCHED",
      "details": [
        {"system_name": "vendor_portal", "discrepancy_found": false},
        {"system_name": "erp_system",    "discrepancy_found": true}
      ],
      "last_updated_ms": 1705312260000
    }
  ],
  "next_page_token": "INV-2024-001"
}
```

---

### `POST /v1/resolution/{ref}/auto`

Automatically resolves a MISMATCHED transaction using a configurable strategy. Proxied to the Resolution Service HTTP API.

**Request body** (all fields optional):

```json
{
  "strategy": "latest_record",
  "source_priority": "payment_gateway,erp_system",
  "resolver_id": "cron:nightly-sweep"
}
```

| Field | Default | Description |
|---|---|---|
| `strategy` | service config default | Resolution strategy (see below) |
| `source_priority` | service config | Comma-separated priority list for `source_priority` strategy |
| `resolver_id` | `"system:api"` | Caller identifier |

**Resolution strategies:**

| Strategy | Winner |
|---|---|
| `source_priority` | First source in the priority list that submitted a record |
| `latest_record` | Source with the most recent `server_received_at` |
| `highest_amount` | Source reporting the highest monetary amount |
| `lowest_amount` | Source reporting the lowest monetary amount |
| `first_submitted` | Source with the earliest `server_received_at` |

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
| `409` | Transaction is not MISMATCHED |
| `400` | Unknown strategy string |
| `422` | Strategy ran but could not determine a winner |
| `502` | Resolution service HTTP unreachable |

---

### `POST /v1/resolution/{ref}/retry`

Enqueues a MISMATCHED transaction for the background retry worker. The worker will re-trigger the Engine's matcher periodically. If the transaction already has an EXHAUSTED retry entry, it is reset to PENDING. Proxied to the Resolution Service HTTP API.

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
| `502` | Resolution service HTTP unreachable |

---

### `GET /v1/resolution/{ref}/audit`

Returns the complete audit trail for a transaction in chronological order. Proxied to the Resolution Service HTTP API.

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
      "detail": "{\"attempt\":1,\"max_attempts\":5}",
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

**Audit event types:**

| Event | Trigger |
|---|---|
| `RETRY_ENQUEUED` | `POST /v1/resolution/{ref}/retry` |
| `RETRY_ATTEMPT` | Worker attempted re-match; still MISMATCHED |
| `RETRY_RESOLVED` | Engine returned MATCHED/RESOLVED |
| `RETRY_EXHAUSTED` | Max attempts reached |
| `AUTO_RESOLUTION` | Auto-resolve strategy applied |
| `MANUAL_RESOLUTION` | Manual `ResolveManually` gRPC call |

**Error codes:**

| HTTP | Condition |
|---|---|
| `404` | Transaction not found or no audit entries |
| `502` | Resolution service HTTP unreachable |

---

### `GET /v1/resolution/retry-queue`

Lists entries in the retry queue with cursor-based pagination. Proxied to the Resolution Service HTTP API.

**Query parameters:**

| Param | Default | Description |
|---|---|---|
| `page_size` | `50` (max `200`) | Records per page |
| `page_token` | — | Cursor from previous response |
| `status` | — | Filter: `PENDING` \| `EXHAUSTED` \| `RESOLVED` |

**Response `200 OK`:**

```json
{
  "count": 1,
  "next_page_token": "INV-2024-001",
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

**Error codes:**

| HTTP | Condition |
|---|---|
| `502` | Resolution service HTTP unreachable |

---

## 7. Health & Observability

### `GET /health`

Liveness and readiness probe. **No authentication required.**

Pings each upstream service's `/health` endpoint concurrently with a 5-second timeout and returns the aggregate status.

**Response `200 OK` (all healthy):**

```json
{
  "status": "ok",
  "services": {
    "ingestion":  "ok",
    "engine":     "ok",
    "resolution": "ok"
  }
}
```

**Response `503 Service Unavailable` (any upstream unhealthy):**

```json
{
  "status": "degraded",
  "services": {
    "ingestion":  "ok",
    "engine":     "unhealthy",
    "resolution": "ok"
  }
}
```

### `GET /metrics` (port `:9093`)

Standard Prometheus text exposition. Served on a separate port — not exposed to clients, only to the internal Prometheus scraper.

**Gateway-emitted metrics:**

| Metric | Type | Labels | Description |
|---|---|---|---|
| `reconx_gateway_http_requests_total` | Counter | `method`, `path`, `status_code` | Total HTTP requests |
| `reconx_gateway_http_duration_seconds` | Histogram | `method`, `path` | Handler latency |
| `reconx_gateway_upstream_errors_total` | Counter | `service` | gRPC / HTTP upstream errors by service name |

---

## 8. Error Format

All error responses use a consistent JSON envelope:

```json
{"error": "human-readable message"}
```

Standard HTTP status codes used by the gateway:

| Code | Meaning |
|---|---|
| `400` | Bad request — invalid JSON body or missing required fields |
| `401` | Unauthorized — missing or invalid `X-API-Key` |
| `404` | Not found — transaction or resource does not exist |
| `409` | Conflict — operation not valid for the current state |
| `422` | Unprocessable — strategy ran but produced no result |
| `429` | Too many requests — upstream rate limit exceeded |
| `502` | Bad gateway — HTTP upstream (Resolution REST) unreachable |
| `503` | Service unavailable — gRPC upstream down, or health check degraded |

---

## 9. Configuration

Environment variable prefix: `RECONX_GATEWAY_`

| Variable | Default | Description |
|---|---|---|
| `RECONX_GATEWAY_HTTP_PORT` | `8090` | Public API server port |
| `RECONX_GATEWAY_METRICS_PORT` | `9093` | Prometheus metrics port |
| `RECONX_GATEWAY_METRICS_PATH` | `/metrics` | Prometheus metrics path |
| `RECONX_GATEWAY_API_KEY` | `""` | Shared API key. Empty = auth disabled |
| `RECONX_GATEWAY_INGESTION_ADDRESS` | `localhost:50051` | Ingestion gRPC address |
| `RECONX_GATEWAY_ENGINE_ADDRESS` | `localhost:50052` | Engine gRPC address |
| `RECONX_GATEWAY_RESOLUTION_ADDRESS` | `localhost:50053` | Resolution gRPC address |
| `RECONX_GATEWAY_RESOLUTION_HTTP_ADDRESS` | `http://localhost:8082` | Resolution HTTP REST base URL |
| `RECONX_GATEWAY_INGESTION_HEALTH_URL` | `http://localhost:8080/health` | Ingestion health endpoint |
| `RECONX_GATEWAY_ENGINE_HEALTH_URL` | `http://localhost:9091/health` | Engine health endpoint |
| `RECONX_GATEWAY_RESOLUTION_HEALTH_URL` | `http://localhost:8082/health` | Resolution health endpoint |
| `RECONX_GATEWAY_LOG_LEVEL` | `info` | Log level: `debug`\|`info`\|`warn`\|`error` |

A YAML config file (`config.yaml`) placed in the working directory or `/etc/reconx/gateway/` is also supported (Viper).

---

## 10. Running Locally

```bash
cd services/gateway

# Development (all services running on localhost with default ports)
go run ./cmd/...

# Build
go build -o bin/reconx-gateway ./cmd/...
./bin/reconx-gateway

# With API key enabled
RECONX_GATEWAY_API_KEY=devkey go run ./cmd/...

# Docker (from repo root)
docker compose up gateway
```

### Quick-start examples

```bash
BASE=http://localhost:8090
KEY=devkey   # set RECONX_GATEWAY_API_KEY=devkey when running

# 1. Submit a record
curl -X POST $BASE/v1/ingest \
  -H "X-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "idempotency_key": "inv-001",
    "transaction_ref": "INV-2024-001",
    "source_system": "payment_gateway"
  }'

# 2. Check reconciliation state
curl -H "X-API-Key: $KEY" $BASE/v1/recon/INV-2024-001

# 3. Re-trigger matching
curl -X POST -H "X-API-Key: $KEY" $BASE/v1/recon/INV-2024-001/retrigger

# 4. Manually resolve a mismatch
curl -X POST $BASE/v1/resolution/INV-2024-001 \
  -H "X-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "chosen_source": "payment_gateway",
    "resolution_reason": "PG is authoritative for settled payments",
    "resolver_id": "alice@company.com"
  }'

# 5. Auto-resolve with latest_record strategy
curl -X POST $BASE/v1/resolution/INV-2024-001/auto \
  -H "X-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"strategy": "latest_record"}'

# 6. Enqueue for retry
curl -X POST $BASE/v1/resolution/INV-2024-001/retry \
  -H "X-API-Key: $KEY" \
  -H "Content-Type: application/json" \
  -d '{"requested_by": "ops@company.com", "max_attempts": 3}'

# 7. View audit trail
curl -H "X-API-Key: $KEY" $BASE/v1/resolution/INV-2024-001/audit

# 8. List retry queue (EXHAUSTED entries only)
curl -H "X-API-Key: $KEY" "$BASE/v1/resolution/retry-queue?status=EXHAUSTED"

# 9. List all mismatches
curl -H "X-API-Key: $KEY" "$BASE/v1/resolution/mismatches?page_size=20"

# 10. Aggregate health (no key required)
curl $BASE/health
```

---

## Internal Package Structure

```
services/gateway/
├── cmd/
│   └── main.go                        # Entrypoint — wires all components
├── internal/
│   ├── clients/
│   │   └── clients.go                 # gRPC stubs + ResolutionHTTPClient
│   ├── config/
│   │   └── config.go                  # Viper config: ports, addresses, API key
│   ├── handlers/
│   │   └── handlers.go                # HTTP handlers + proxy helpers + health
│   ├── metrics/
│   │   └── metrics.go                 # Prometheus metrics
│   └── middleware/
│       └── auth.go                    # X-API-Key authentication middleware
└── go.mod
```
