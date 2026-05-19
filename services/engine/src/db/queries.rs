/// db/queries.rs — all SQL queries executed by the Reconciliation Engine.
///
/// Uses `sqlx` with runtime query dispatch (no compile-time macro checking)
/// to keep build simple without requiring a live database connection.
/// All queries are async and return `Result<_, EngineError>`.
use chrono::{DateTime, Utc};
use serde_json::Value as JsonValue;
use sqlx::{Pool, Postgres, Row};

use crate::db::models::{IngestionRecord, ReconAuditLog, ReconMatchDetail, ReconState};
use crate::error::{EngineError, Result};

// ─────────────────────────────────────────────────────────────────────────────
// Schema migration — runs on startup, idempotent
// ─────────────────────────────────────────────────────────────────────────────

const MIGRATION_SQL: &str = r#"
-- Reconciliation state (one row per business transaction)
CREATE TABLE IF NOT EXISTS recon_state (
    transaction_ref     TEXT PRIMARY KEY,
    status              TEXT NOT NULL DEFAULT 'PENDING',
    record_count        INT  NOT NULL DEFAULT 0,
    matched_sources     JSONB NOT NULL DEFAULT '[]'::jsonb,
    mismatched_sources  JSONB NOT NULL DEFAULT '[]'::jsonb,
    retry_count         INT  NOT NULL DEFAULT 0,
    max_retries         INT  NOT NULL DEFAULT 3,
    last_processed_at   TIMESTAMPTZ,
    resolved_at         TIMESTAMPTZ,
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_recon_state_status
    ON recon_state (status);

CREATE INDEX IF NOT EXISTS idx_recon_state_last_processed
    ON recon_state (last_processed_at);

-- Per-source-system match details (one row per source per transaction)
CREATE TABLE IF NOT EXISTS recon_match_details (
    id                  BIGSERIAL PRIMARY KEY,
    transaction_ref     TEXT NOT NULL REFERENCES recon_state(transaction_ref) ON DELETE CASCADE,
    system_name         TEXT NOT NULL,
    internal_id         TEXT NOT NULL,
    amount              TEXT,
    currency            TEXT,
    data_captured       BYTEA,
    discrepancy_found   BOOLEAN NOT NULL DEFAULT FALSE,
    discrepancy_details JSONB,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (transaction_ref, system_name)
);

CREATE INDEX IF NOT EXISTS idx_recon_match_details_ref
    ON recon_match_details (transaction_ref);

-- Append-only audit log (full history of every state change)
CREATE TABLE IF NOT EXISTS recon_audit_log (
    id              BIGSERIAL PRIMARY KEY,
    transaction_ref TEXT NOT NULL,
    event_type      TEXT NOT NULL,
    old_status      TEXT,
    new_status      TEXT,
    details         JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_recon_audit_log_ref
    ON recon_audit_log (transaction_ref, created_at DESC);
"#;

/// Run schema migrations. Safe to call every startup (all DDL is idempotent).
pub async fn run_migrations(pool: &Pool<Postgres>) -> Result<()> {
    sqlx::query(MIGRATION_SQL)
        .execute(pool)
        .await
        .map_err(EngineError::Database)?;
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Worker queries — called by the background reconciliation loop
// ─────────────────────────────────────────────────────────────────────────────

/// Fetch `limit` distinct `transaction_ref` values that require reconciliation.
///
/// A transaction needs reconciliation if:
///   a) It exists in `ingestion_records` but has no `recon_state` row yet, OR
///   b) Its `recon_state` is PENDING, has retries remaining, and the backoff
///      window has elapsed.
pub async fn get_pending_transaction_refs(
    pool: &Pool<Postgres>,
    limit: i64,
    backoff_cutoff: DateTime<Utc>,
) -> Result<Vec<String>> {
    let rows = sqlx::query(
        r#"
        SELECT DISTINCT ir.transaction_ref
        FROM ingestion_records ir
        LEFT JOIN recon_state rs ON ir.transaction_ref = rs.transaction_ref
        WHERE
            -- Never processed
            rs.transaction_ref IS NULL
            OR (
                -- Pending with retries remaining and backoff elapsed
                rs.status = 'PENDING'
                AND rs.retry_count < rs.max_retries
                AND (
                    rs.last_processed_at IS NULL
                    OR rs.last_processed_at < $1
                )
            )
        ORDER BY ir.transaction_ref
        LIMIT $2
        "#,
    )
    .bind(backoff_cutoff)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(EngineError::Database)?;

    Ok(rows.iter().map(|r| r.get::<String, _>("transaction_ref")).collect())
}

/// Fetch all ingested records for a given `transaction_ref`.
pub async fn get_records_by_transaction_ref(
    pool: &Pool<Postgres>,
    transaction_ref: &str,
) -> Result<Vec<IngestionRecord>> {
    sqlx::query_as::<_, IngestionRecord>(
        r#"
        SELECT
            internal_id, idempotency_key, transaction_ref, source_system,
            adapter_type, amount, currency, record_timestamp,
            server_received_at, raw_payload, payload_schema, tags,
            trace_id, status, created_at
        FROM ingestion_records
        WHERE transaction_ref = $1
        ORDER BY server_received_at ASC
        "#,
    )
    .bind(transaction_ref)
    .fetch_all(pool)
    .await
    .map_err(EngineError::Database)
}

/// Create or update a `recon_state` row.
///
/// On conflict (row already exists):
/// - Always update status, record_count, matched/mismatched sources, timestamps.
/// - Increment retry_count atomically.
/// - Set resolved_at only when transitioning to MATCHED or RESOLVED.
pub async fn upsert_recon_state(
    pool: &Pool<Postgres>,
    transaction_ref: &str,
    status: &str,
    record_count: i32,
    matched_sources: &[String],
    mismatched_sources: &[String],
    max_retries: i32,
) -> Result<()> {
    let matched_json = serde_json::to_value(matched_sources).map_err(EngineError::Serialization)?;
    let mismatch_json =
        serde_json::to_value(mismatched_sources).map_err(EngineError::Serialization)?;
    let resolved_at: Option<DateTime<Utc>> = if status == "MATCHED" || status == "RESOLVED" {
        Some(Utc::now())
    } else {
        None
    };

    sqlx::query(
        r#"
        INSERT INTO recon_state (
            transaction_ref, status, record_count,
            matched_sources, mismatched_sources,
            retry_count, max_retries,
            last_processed_at, resolved_at,
            created_at, updated_at
        ) VALUES (
            $1, $2, $3, $4, $5,
            1, $6,
            NOW(), $7,
            NOW(), NOW()
        )
        ON CONFLICT (transaction_ref) DO UPDATE SET
            status              = EXCLUDED.status,
            record_count        = EXCLUDED.record_count,
            matched_sources     = EXCLUDED.matched_sources,
            mismatched_sources  = EXCLUDED.mismatched_sources,
            retry_count         = recon_state.retry_count + 1,
            last_processed_at   = NOW(),
            resolved_at         = CASE
                WHEN EXCLUDED.status IN ('MATCHED', 'RESOLVED') THEN NOW()
                ELSE recon_state.resolved_at
            END,
            updated_at          = NOW()
        "#,
    )
    .bind(transaction_ref)
    .bind(status)
    .bind(record_count)
    .bind(matched_json)
    .bind(mismatch_json)
    .bind(max_retries)
    .bind(resolved_at)
    .execute(pool)
    .await
    .map_err(EngineError::Database)?;

    Ok(())
}

/// Upsert a match detail row for a single source system within a transaction.
///
/// Called once per source system per reconciliation attempt. On conflict
/// (same transaction_ref + system_name), overwrites with the latest data.
pub async fn upsert_match_detail(
    pool: &Pool<Postgres>,
    transaction_ref: &str,
    system_name: &str,
    internal_id: &str,
    amount: Option<&str>,
    currency: Option<&str>,
    data_captured: Option<&[u8]>,
    discrepancy_found: bool,
    discrepancy_details: Option<JsonValue>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO recon_match_details (
            transaction_ref, system_name, internal_id,
            amount, currency, data_captured,
            discrepancy_found, discrepancy_details, processed_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, NOW())
        ON CONFLICT (transaction_ref, system_name) DO UPDATE SET
            internal_id         = EXCLUDED.internal_id,
            amount              = EXCLUDED.amount,
            currency            = EXCLUDED.currency,
            data_captured       = EXCLUDED.data_captured,
            discrepancy_found   = EXCLUDED.discrepancy_found,
            discrepancy_details = EXCLUDED.discrepancy_details,
            processed_at        = NOW()
        "#,
    )
    .bind(transaction_ref)
    .bind(system_name)
    .bind(internal_id)
    .bind(amount)
    .bind(currency)
    .bind(data_captured)
    .bind(discrepancy_found)
    .bind(discrepancy_details)
    .execute(pool)
    .await
    .map_err(EngineError::Database)?;

    Ok(())
}

/// Append a row to the audit log. Never updates existing rows.
pub async fn insert_audit_log(
    pool: &Pool<Postgres>,
    transaction_ref: &str,
    event_type: &str,
    old_status: Option<&str>,
    new_status: Option<&str>,
    details: Option<JsonValue>,
) -> Result<()> {
    sqlx::query(
        r#"
        INSERT INTO recon_audit_log
            (transaction_ref, event_type, old_status, new_status, details)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(transaction_ref)
    .bind(event_type)
    .bind(old_status)
    .bind(new_status)
    .bind(details)
    .execute(pool)
    .await
    .map_err(EngineError::Database)?;

    Ok(())
}

/// Update `ingestion_records.status` for all records with this transaction_ref.
/// Called after a final MATCHED/MISMATCHED decision is made so that external
/// readers of the ingestion table see an up-to-date status.
pub async fn update_ingestion_status(
    pool: &Pool<Postgres>,
    transaction_ref: &str,
    status: &str,
) -> Result<()> {
    sqlx::query(
        "UPDATE ingestion_records SET status = $1 WHERE transaction_ref = $2",
    )
    .bind(status)
    .bind(transaction_ref)
    .execute(pool)
    .await
    .map_err(EngineError::Database)?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// gRPC read queries
// ─────────────────────────────────────────────────────────────────────────────

/// Fetch the current reconciliation state for a `transaction_ref`.
/// Returns `None` if the transaction has never been processed.
pub async fn get_recon_state(
    pool: &Pool<Postgres>,
    transaction_ref: &str,
) -> Result<Option<ReconState>> {
    sqlx::query_as::<_, ReconState>(
        r#"
        SELECT
            transaction_ref, status, record_count,
            matched_sources, mismatched_sources,
            retry_count, max_retries,
            last_processed_at, resolved_at,
            created_at, updated_at
        FROM recon_state
        WHERE transaction_ref = $1
        "#,
    )
    .bind(transaction_ref)
    .fetch_optional(pool)
    .await
    .map_err(EngineError::Database)
}

/// Fetch all per-source match details for a `transaction_ref`.
pub async fn get_match_details(
    pool: &Pool<Postgres>,
    transaction_ref: &str,
) -> Result<Vec<ReconMatchDetail>> {
    sqlx::query_as::<_, ReconMatchDetail>(
        r#"
        SELECT
            id, transaction_ref, system_name, internal_id,
            amount, currency, data_captured,
            discrepancy_found, discrepancy_details, processed_at
        FROM recon_match_details
        WHERE transaction_ref = $1
        ORDER BY processed_at ASC
        "#,
    )
    .bind(transaction_ref)
    .fetch_all(pool)
    .await
    .map_err(EngineError::Database)
}

/// Fetch all audit log entries for a `transaction_ref` in chronological order.
pub async fn get_audit_log(
    pool: &Pool<Postgres>,
    transaction_ref: &str,
) -> Result<Vec<ReconAuditLog>> {
    sqlx::query_as::<_, ReconAuditLog>(
        r#"
        SELECT id, transaction_ref, event_type, old_status, new_status, details, created_at
        FROM recon_audit_log
        WHERE transaction_ref = $1
        ORDER BY created_at ASC
        "#,
    )
    .bind(transaction_ref)
    .fetch_all(pool)
    .await
    .map_err(EngineError::Database)
}

/// Hard-reset `recon_state.retry_count` to 0 and set status back to PENDING.
/// Used by the gRPC `ReTriggerMatch` RPC before re-running the matcher.
pub async fn reset_for_retrigger(pool: &Pool<Postgres>, transaction_ref: &str) -> Result<()> {
    sqlx::query(
        r#"
        UPDATE recon_state
        SET
            status            = 'PENDING',
            retry_count       = 0,
            last_processed_at = NULL,
            updated_at        = NOW()
        WHERE transaction_ref = $1
        "#,
    )
    .bind(transaction_ref)
    .execute(pool)
    .await
    .map_err(EngineError::Database)?;

    Ok(())
}
