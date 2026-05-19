/// db/models.rs — database model structs for the Reconciliation Engine.
///
/// `IngestionRecord` mirrors the `ingestion_records` table written by the
/// Ingestion Service (read-only from the engine's perspective).
///
/// `ReconState`, `ReconMatchDetail`, and `ReconAuditLog` are the engine's
/// own tables, written exclusively by this service.
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

// ─────────────────────────────────────────────────────────────────────────────
// Ingestion Service table (read-only)
// ─────────────────────────────────────────────────────────────────────────────

/// A normalized record ingested by the Ingestion Service.
/// The engine reads from this table to drive reconciliation.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct IngestionRecord {
    pub internal_id: String,
    pub idempotency_key: String,
    pub transaction_ref: String,
    pub source_system: String,
    pub adapter_type: String,
    /// Monetary amount stored as TEXT to preserve exact decimal representation.
    pub amount: Option<String>,
    pub currency: Option<String>,
    pub record_timestamp: Option<DateTime<Utc>>,
    pub server_received_at: DateTime<Utc>,
    pub raw_payload: Option<Vec<u8>>,
    pub payload_schema: Option<String>,
    /// Arbitrary key-value tags (JSONB).
    pub tags: Option<JsonValue>,
    pub trace_id: Option<String>,
    /// Status from the ingestion perspective: 'PENDING' | 'MATCHED' | 'MISMATCHED' | 'RESOLVED'
    pub status: String,
    pub created_at: DateTime<Utc>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Reconciliation Engine tables (read-write)
// ─────────────────────────────────────────────────────────────────────────────

/// Current reconciliation state for a single `transaction_ref`.
/// One row per business transaction, updated in-place on each matching attempt.
#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct ReconState {
    pub transaction_ref: String,
    /// 'PENDING' | 'MATCHED' | 'MISMATCHED' | 'RESOLVED'
    pub status: String,
    /// Total number of ingested records associated with this transaction.
    pub record_count: i32,
    /// JSON array of source system names whose amounts matched.
    pub matched_sources: JsonValue,
    /// JSON array of source system names that had discrepancies.
    pub mismatched_sources: JsonValue,
    /// How many reconciliation attempts have been made so far.
    pub retry_count: i32,
    /// Maximum allowed attempts before giving up (copied from config at creation).
    pub max_retries: i32,
    pub last_processed_at: Option<DateTime<Utc>>,
    /// Set when status transitions to MATCHED or RESOLVED.
    pub resolved_at: Option<DateTime<Utc>>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

impl ReconState {
    /// Helper: decode matched_sources JSON array into `Vec<String>`.
    pub fn matched_sources_list(&self) -> Vec<String> {
        json_to_string_vec(&self.matched_sources)
    }

    /// Helper: decode mismatched_sources JSON array into `Vec<String>`.
    pub fn mismatched_sources_list(&self) -> Vec<String> {
        json_to_string_vec(&self.mismatched_sources)
    }
}

/// Per-source-system match result for a single `transaction_ref`.
/// One row per (transaction_ref, system_name) pair.
#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct ReconMatchDetail {
    pub id: i64,
    pub transaction_ref: String,
    /// The source system name (e.g. "vendor_portal", "erp_system").
    pub system_name: String,
    /// The internal_id of the ingested record this detail refers to.
    pub internal_id: String,
    /// Monetary amount as stored in ingestion_records.
    pub amount: Option<String>,
    pub currency: Option<String>,
    /// Raw payload bytes — returned to callers via the gRPC `data_captured` field.
    pub data_captured: Option<Vec<u8>>,
    /// True if this source's amount diverges from the reference amount.
    pub discrepancy_found: bool,
    /// Structured detail about the discrepancy (JSONB).
    pub discrepancy_details: Option<JsonValue>,
    pub processed_at: DateTime<Utc>,
}

/// Append-only audit trail for every state transition or matching attempt.
#[derive(Debug, Clone, sqlx::FromRow, Serialize, Deserialize)]
pub struct ReconAuditLog {
    pub id: i64,
    pub transaction_ref: String,
    /// 'MATCH_ATTEMPT' | 'STATUS_CHANGE' | 'RETRY' | 'RETRIGGER' | 'ERROR' | 'TIMEOUT'
    pub event_type: String,
    pub old_status: Option<String>,
    pub new_status: Option<String>,
    /// Free-form JSONB details (e.g. amounts compared, tolerance used, mismatch reason).
    pub details: Option<JsonValue>,
    pub created_at: DateTime<Utc>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

fn json_to_string_vec(v: &JsonValue) -> Vec<String> {
    v.as_array()
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

// ─────────────────────────────────────────────────────────────────────────────
// Intermediate types used by the matching logic
// ─────────────────────────────────────────────────────────────────────────────

/// Lightweight view of an ingestion record, projected for the matcher.
#[derive(Debug, Clone)]
pub struct RecordView {
    pub internal_id: String,
    pub source_system: String,
    pub amount_str: Option<String>,
    pub currency: Option<String>,
    pub raw_payload: Option<Vec<u8>>,
    pub server_received_at: DateTime<Utc>,
}

impl From<&IngestionRecord> for RecordView {
    fn from(r: &IngestionRecord) -> Self {
        Self {
            internal_id: r.internal_id.clone(),
            source_system: r.source_system.clone(),
            amount_str: r.amount.clone(),
            currency: r.currency.clone(),
            raw_payload: r.raw_payload.clone(),
            server_received_at: r.server_received_at,
        }
    }
}
