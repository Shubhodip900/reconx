# Reconciliation Engine — Architecture & Developer Reference

> **Service:** `services/engine/`  
> **Language:** Rust (edition 2021)  
> **gRPC port:** `50052`  
> **Metrics port:** `9091`  
> **Proto contract:** `proto/engine.proto`

---

## 1. Purpose

The Reconciliation Engine is the core brain of ReconX. It continuously reads records produced by the Ingestion Service, groups them by `transaction_ref`, and determines whether all participating source systems agree on the transaction data.

**Key responsibilities:**

| Responsibility | Where |
|---|---|
| Poll for unprocessed transactions | `engine/worker.rs` |
| Apply matching logic with configurable tolerance | `engine/matcher.rs` |
| Enforce configurable rules (strategy, expected sources, timeout) | `engine/rules.rs` |
| Persist reconciliation state and audit trail | `db/queries.rs` |
| Expose current state via gRPC | `grpc/server.rs` |
| Propagate status back to `ingestion_records` | `db/queries.rs` |
| Emit Prometheus metrics | `metrics.rs` |

---

## 2. Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  Reconciliation Engine (Rust)                │
│                                                             │
│  ┌──────────────┐    ┌──────────────────────────────────┐  │
│  │  gRPC Server  │    │   Background Worker (Tokio task)  │  │
│  │  :50052       │    │                                  │  │
│  │               │    │  every poll_interval_secs:        │  │
│  │ GetReconState ├───►│   1. query pending tx_refs        │  │
│  │ ReTriggerMatch│    │   2. fetch ingestion_records      │  │
│  └──────┬───────┘    │   3. run matcher                  │  │
│         │            │   4. upsert recon_state            │  │
│         │            │   5. upsert match_details          │  │
│         │            │   6. append audit_log              │  │
│         │            │   7. update ingestion_records      │  │
│         │            └──────────────┬───────────────────┘  │
│         │                           │                       │
│  ┌──────▼───────────────────────────▼──────────────────┐   │
│  │              PostgreSQL                              │   │
│  │  ingestion_records (read)  recon_state (write)       │   │
│  │  recon_match_details (write)  recon_audit_log (write)│   │
│  └──────────────────────────────────────────────────────┘   │
│                                                             │
│  ┌─────────────────────┐                                    │
│  │  Prometheus /metrics │  :9091                            │
│  └─────────────────────┘                                    │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Data Flow

```
Ingestion Service                   Reconciliation Engine
─────────────────                   ─────────────────────
vendor_portal  ──► ingestion_records ──► worker polls
erp_system     ──► ingestion_records ──► group by transaction_ref
payment_gw     ──► ingestion_records ──► matcher runs
                                    ──► recon_state updated
                                    ──► ingestion_records.status updated
```

**Cross-system join key:** `transaction_ref` — all participating systems must use the same value for the same business event. It is validated as printable ASCII by the Ingestion Service.

---

## 4. Database Schema

The engine creates three tables at startup (idempotent `CREATE TABLE IF NOT EXISTS`):

### 4.1 `recon_state`

One row per `transaction_ref`. Updated on every reconciliation attempt.

| Column | Type | Description |
|---|---|---|
| `transaction_ref` | `TEXT PK` | Cross-system join key |
| `status` | `TEXT` | `PENDING` \| `MATCHED` \| `MISMATCHED` \| `RESOLVED` |
| `record_count` | `INT` | Total ingested records for this transaction |
| `matched_sources` | `JSONB` | JSON array of sources that matched |
| `mismatched_sources` | `JSONB` | JSON array of sources with discrepancies |
| `retry_count` | `INT` | Number of matching attempts made |
| `max_retries` | `INT` | Copied from config at first insert |
| `last_processed_at` | `TIMESTAMPTZ` | When the engine last ran for this transaction |
| `resolved_at` | `TIMESTAMPTZ` | Set when status becomes `MATCHED` or `RESOLVED` |
| `created_at` | `TIMESTAMPTZ` | First seen |
| `updated_at` | `TIMESTAMPTZ` | Last modified |

### 4.2 `recon_match_details`

One row per `(transaction_ref, system_name)`. Upserted on every run.

| Column | Type | Description |
|---|---|---|
| `id` | `BIGSERIAL PK` | Auto-increment |
| `transaction_ref` | `TEXT FK` | References `recon_state` |
| `system_name` | `TEXT` | Source system name |
| `internal_id` | `TEXT` | `ingestion_records.internal_id` reference |
| `amount` | `TEXT` | Amount as stored (decimal string) |
| `currency` | `TEXT` | ISO 4217 currency code |
| `data_captured` | `BYTEA` | Raw payload bytes (returned in gRPC response) |
| `discrepancy_found` | `BOOL` | True if this source diverges from the reference |
| `discrepancy_details` | `JSONB` | Structured description of the discrepancy |
| `processed_at` | `TIMESTAMPTZ` | When this detail was last written |

**Unique constraint:** `(transaction_ref, system_name)` — ensures idempotent upserts.

### 4.3 `recon_audit_log`

Append-only. Every state transition, retry, and retrigger is recorded.

| Column | Type | Description |
|---|---|---|
| `id` | `BIGSERIAL PK` | Auto-increment |
| `transaction_ref` | `TEXT` | Which transaction |
| `event_type` | `TEXT` | `MATCH_ATTEMPT` \| `STATUS_CHANGE` \| `RETRY` \| `RETRIGGER` \| `ERROR` \| `TIMEOUT` |
| `old_status` | `TEXT` | Status before this event |
| `new_status` | `TEXT` | Status after this event |
| `details` | `JSONB` | Amounts compared, tolerance used, mismatch reason, etc. |
| `created_at` | `TIMESTAMPTZ` | Event timestamp |

---

## 5. Matching Logic

### 5.1 Status lifecycle

```
                    ┌──────────────────────────────┐
New ingestion ──►   │         PENDING              │
                    │  (waiting for more sources    │
                    │   or retrying after mismatch) │
                    └────────┬─────────────┬────────┘
                             │             │
                    all agree│             │diverge / timeout
                             ▼             ▼
                         MATCHED       MISMATCHED
                             │             │
                             │             │ (resolution service)
                             └──────┬──────┘
                                    ▼
                                RESOLVED
```

### 5.2 Matching strategies

| Strategy | Behaviour | Best for |
|---|---|---|
| `exact` | All amounts must be bit-identical | Zero-tolerance financial reconciliation |
| `tolerance` | `|a - b| ≤ max(abs_tol, pct% of max(a,b))` | Rounding-difference-tolerant matching |
| `majority` | Largest agreement bucket wins; outliers flagged | 3+ source comparison |

### 5.3 Step-by-step algorithm (`engine/matcher.rs`)

1. **Deduplicate** — if a source submitted multiple records, only the latest is used.
2. **Expected sources check** — if `expected_sources` is configured, wait until all are present. After `pending_timeout_secs`, escalate to `MISMATCHED`.
3. **Minimum source check** — need ≥ 2 distinct sources to compare. After timeout, escalate.
4. **Parse amounts** — `Decimal::from_str()` from the stored TEXT column. Parsing errors → `EngineError::InvalidAmount`.
5. **Currency consistency** — if sources report different currencies, immediately `MISMATCHED` (separate from amount check).
6. **Amount comparison** — apply the configured strategy. Each source gets a `SourceResult` with `discrepancy_found` and optional `discrepancy_details` JSON.

### 5.4 Tolerance example

Config:
```toml
amount_tolerance_pct = 1.0   # 1%
amount_tolerance_abs = "0.50" # ₹0.50
```

For a reference of ₹10,000:
- Effective tolerance = max(₹100.00, ₹0.50) = **₹100.00**
- ₹9,920 → diff = ₹80 → **MATCHED**
- ₹9,850 → diff = ₹150 → **MISMATCHED**

---

## 6. gRPC API

Proto file: `proto/engine.proto`

```protobuf
service ReconciliationEngine {
    rpc GetReconState   (StateRequest) returns (StateResponse);
    rpc ReTriggerMatch  (StateRequest) returns (StateResponse);
}

message StateRequest  { string transaction_ref = 1; }
message StateResponse {
    string                    transaction_ref = 1;
    reconx.common.ReconStatus status          = 2;
    repeated MatchDetail      details          = 3;
    int64                     last_updated     = 4; // unix ms
}
message MatchDetail {
    string system_name       = 1;
    bytes  data_captured     = 2; // raw payload from ingestion
    bool   discrepancy_found = 3;
}
```

### `GetReconState`

Returns the current state for a `transaction_ref`.

**Behaviour:**
- If the transaction has never been processed, an immediate inline reconciliation is triggered before responding (best-effort).
- Returns `NOT_FOUND` if no ingested records exist for that `transaction_ref`.

**Example (grpcurl):**
```bash
grpcurl -plaintext \
  -d '{"transaction_ref":"INV-2024-001"}' \
  localhost:50052 reconx.engine.ReconciliationEngine/GetReconState
```

### `ReTriggerMatch`

Forces a fresh reconciliation attempt regardless of current state.

**Use cases:**
- A late record arrived and you want an immediate re-evaluation.
- A bug was fixed and previously-incorrect records have been corrected.
- The Resolution Service marks a MISMATCHED transaction as needing re-check.

**Behaviour:**
- Resets `retry_count` to 0 and `status` to `PENDING` in `recon_state`.
- Appends a `RETRIGGER` audit log entry.
- Runs the matcher synchronously.
- Returns the updated `StateResponse`.

---

## 7. Background Worker

The worker (`engine/worker.rs`) runs in a dedicated Tokio task.

**Poll loop:**

```
Every poll_interval_secs:
  1. backoff_cutoff = now() - retry_backoff_secs
  2. SELECT DISTINCT transaction_ref FROM ingestion_records
        LEFT JOIN recon_state ...
        WHERE [never processed] OR [PENDING + retries remaining + backoff elapsed]
        LIMIT batch_size
  3. For each transaction_ref: reconcile_one()
```

**`reconcile_one()` internals:**

```
1. Fetch old_status from recon_state (for audit)
2. SELECT all ingestion_records WHERE transaction_ref = ?
3. Project to RecordView[]
4. matcher::match_records() → MatchOutcome
5. upsert_recon_state()
6. For each source: upsert_match_detail()
7. insert_audit_log("MATCH_ATTEMPT", old_status, new_status, details)
8. If status ≠ PENDING: UPDATE ingestion_records SET status = ?
9. Increment Prometheus counters
```

---

## 8. Configuration Reference

All values can be overridden via environment variables with prefix `RECONX_ENGINE__`:

| Key | Default | Description |
|---|---|---|
| `grpc.port` | `50052` | gRPC server port |
| `database.dsn` | `postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable` | PostgreSQL DSN |
| `database.max_connections` | `20` | Connection pool max size |
| `database.min_connections` | `2` | Connection pool min idle |
| `database.connect_timeout_secs` | `10` | Pool acquire timeout |
| `database.idle_timeout_secs` | `300` | Idle connection lifetime |
| `metrics.port` | `9091` | Prometheus + health HTTP port |
| `engine.poll_interval_secs` | `5` | Worker poll frequency |
| `engine.batch_size` | `100` | Max transaction_refs per tick |
| `engine.amount_tolerance_pct` | `0.0` | Percentage tolerance |
| `engine.amount_tolerance_abs` | `"0.00"` | Absolute decimal tolerance |
| `engine.max_retries` | `3` | Max matching attempts |
| `engine.retry_backoff_secs` | `30` | Minimum seconds between retries |
| `engine.pending_timeout_secs` | `300` | Seconds before PENDING escalates to MISMATCHED |
| `engine.expected_sources` | `[]` | Required source systems (empty = any 2+) |
| `engine.match_strategy` | `"tolerance"` | `exact` \| `tolerance` \| `majority` |
| `log.level` | `"info"` | `trace`\|`debug`\|`info`\|`warn`\|`error` |
| `log.format` | `"json"` | `json` \| `text` |

**Environment variable examples:**

```bash
# Use majority strategy with 1% tolerance
RECONX_ENGINE__ENGINE__MATCH_STRATEGY=majority
RECONX_ENGINE__ENGINE__AMOUNT_TOLERANCE_PCT=1.0

# Require three specific sources
RECONX_ENGINE__ENGINE__EXPECTED_SOURCES='["vendor_portal","erp_system","payment_gateway"]'

# Point to remote DB
RECONX_ENGINE__DATABASE__DSN=postgres://user:pass@db.prod:5432/reconx?sslmode=require
```

---

## 9. Prometheus Metrics

All metrics are under the `reconx_engine_` namespace. Scraped at `:9091/metrics`.

| Metric | Type | Labels | Description |
|---|---|---|---|
| `reconx_engine_reconciliations_total` | Counter | `status` | Total attempts by outcome |
| `reconx_engine_active_mismatches` | Gauge | — | Currently open mismatches |
| `reconx_engine_retriggers_total` | Counter | — | Manual ReTriggerMatch calls |
| `reconx_engine_worker_cycle_duration_seconds` | Histogram | — | Worker tick duration |
| `reconx_engine_worker_batch_size` | Histogram | — | Transactions per tick |
| `reconx_engine_worker_errors_total` | Counter | — | Unrecoverable worker errors |
| `reconx_engine_recon_errors_total` | Counter | `transaction_ref` | Per-transaction errors |
| `reconx_engine_grpc_requests_total` | Counter | `method` | gRPC calls by method |
| `reconx_engine_grpc_request_duration_seconds` | Histogram | `method` | gRPC handler latency |

---

## 10. Quick Start

### Prerequisites

- Rust 1.82+ (`rustup update`)
- `protoc` (protobuf compiler) — `apt install protobuf-compiler` / `brew install protobuf`
- PostgreSQL 16+ running and accessible

### Run locally

```bash
# 1. Start PostgreSQL (if not already running)
docker run -d \
  --name reconx-postgres \
  -e POSTGRES_USER=reconx \
  -e POSTGRES_PASSWORD=reconx \
  -e POSTGRES_DB=reconx \
  -p 5432:5432 \
  postgres:16-alpine

# 2. Build and run
cd services/engine
RECONX_ENGINE__LOG__FORMAT=text \
RECONX_ENGINE__LOG__LEVEL=debug \
cargo run
```

The engine starts three listeners:
- `:50052` — gRPC API
- `:9091/metrics` — Prometheus metrics
- `:9091/health` — Health check

### Test the gRPC API

```bash
# Query state (will trigger immediate reconciliation if records exist)
grpcurl -plaintext \
  -d '{"transaction_ref":"INV-2024-001"}' \
  localhost:50052 reconx.engine.ReconciliationEngine/GetReconState

# Force re-evaluation
grpcurl -plaintext \
  -d '{"transaction_ref":"INV-2024-001"}' \
  localhost:50052 reconx.engine.ReconciliationEngine/ReTriggerMatch
```

### Run tests

```bash
cd services/engine
cargo test
# or with output:
cargo test -- --nocapture
```

### Build Docker image

```bash
# From repo root:
docker build \
  -f services/engine/Dockerfile \
  -t reconx/engine:latest \
  .
```

---

## 11. Development Notes

### Why Rust?

The reconciliation engine was chosen to be written in Rust because:
- **Memory safety without GC** — critical for a long-running stateful service
- **`rust_decimal`** — exact decimal arithmetic (IEEE 754 floating-point would introduce reconciliation errors in financial comparisons)
- **Tokio** — async concurrency model maps cleanly to poll/process/persist workloads
- **Zero-cost abstractions** — the hot matching loop allocates nothing on the heap for matched transactions

### Why not `float64` for amounts?

IEEE 754 double-precision can represent at most ~15 significant decimal digits. Financial reconciliation routinely involves values like `₹9,99,999.99` (8 significant digits, fine) but arithmetic on them introduces rounding:

```
10000.00 - 9800.00 = 200.0  // fine
10000.10 - 9800.10 = 199.99999999999997  // NOT fine for exact match
```

`rust_decimal` uses 96-bit integer arithmetic internally — no rounding errors.

### Adding a new matching strategy

1. Add a variant to `MatchStrategy` in `engine/rules.rs`
2. Add the parsing arm in `MatchStrategy::from_str()`
3. Implement the strategy as a `fn compare_xxx(records: &[ParsedRecord], rules: &RuleSet) -> MatchOutcome`
4. Add the arm to the `match rules.strategy` block in `engine/matcher.rs`
5. Add a unit test

### Extending the audit trail

The `recon_audit_log` table accepts any `event_type` string and a free-form `JSONB` details column. To add a new event type, simply call `queries::insert_audit_log()` with an appropriate `event_type` constant and a `serde_json::json!({})` details object.
