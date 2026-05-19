/// error.rs — unified error hierarchy for the Reconciliation Engine.
///
/// `EngineError` is used throughout the internal layers.
/// `tonic::Status` conversions are applied at the gRPC boundary only.
use thiserror::Error;

/// Top-level engine error type.
#[derive(Debug, Error)]
pub enum EngineError {
    // ── Database errors ────────────────────────────────────────────────────
    #[error("database error: {0}")]
    Database(#[from] sqlx::Error),

    // ── Matching / reconciliation errors ──────────────────────────────────
    #[error("no records found for transaction_ref '{0}'")]
    NotFound(String),

    #[error("insufficient sources for transaction_ref '{0}': got {1}, need at least 2")]
    InsufficientSources(String, usize),

    #[error("invalid amount '{value}' for source '{source}': {reason}")]
    InvalidAmount {
        source: String,
        value: String,
        reason: String,
    },

    #[error("currency mismatch for transaction_ref '{0}': {1}")]
    CurrencyMismatch(String, String),

    // ── Configuration errors ───────────────────────────────────────────────
    #[error("configuration error: {0}")]
    Config(#[from] config::ConfigError),

    #[error("invalid match strategy '{0}': must be one of exact|tolerance|majority")]
    InvalidMatchStrategy(String),

    #[error("invalid tolerance value '{0}': {1}")]
    InvalidTolerance(String, String),

    // ── gRPC / transport errors ────────────────────────────────────────────
    #[error("gRPC transport error: {0}")]
    Transport(#[from] tonic::transport::Error),

    // ── Serialization errors ───────────────────────────────────────────────
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    // ── Decimal parsing ────────────────────────────────────────────────────
    #[error("decimal parse error: {0}")]
    DecimalParse(#[from] rust_decimal::Error),

    // ── Generic / internal errors ──────────────────────────────────────────
    #[error("internal error: {0}")]
    Internal(String),
}

/// Convenience alias used throughout the codebase.
pub type Result<T> = std::result::Result<T, EngineError>;

/// Convert `EngineError` → `tonic::Status` at the gRPC boundary.
impl From<EngineError> for tonic::Status {
    fn from(err: EngineError) -> Self {
        match &err {
            EngineError::NotFound(r) => {
                tonic::Status::not_found(format!("no reconciliation state for '{r}'"))
            }
            EngineError::InsufficientSources(r, n) => tonic::Status::failed_precondition(format!(
                "transaction '{r}' has only {n} source(s); need at least 2 to reconcile"
            )),
            EngineError::Database(e) => {
                tracing::error!(error = %e, "database error");
                tonic::Status::internal("database error")
            }
            EngineError::InvalidAmount { source, value, reason } => {
                tonic::Status::invalid_argument(format!(
                    "invalid amount '{value}' from source '{source}': {reason}"
                ))
            }
            EngineError::CurrencyMismatch(r, detail) => {
                tonic::Status::failed_precondition(format!(
                    "currency mismatch for '{r}': {detail}"
                ))
            }
            _ => {
                tracing::error!(error = %err, "internal engine error");
                tonic::Status::internal(err.to_string())
            }
        }
    }
}
