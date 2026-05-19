/// config.rs — all configuration for the Reconciliation Engine.
///
/// Priority order (highest wins):
///   1. Environment variables  (prefix `RECONX_ENGINE_`, separator `__`)
///   2. config/default.toml
///
/// Example env overrides:
///   RECONX_ENGINE_GRPC__PORT=50052
///   RECONX_ENGINE_DATABASE__DSN=postgres://...
///   RECONX_ENGINE_ENGINE__POLL_INTERVAL_SECS=10
use config::{Config, ConfigError, Environment, File};
use serde::Deserialize;

/// Top-level configuration object — cloned and passed via `Arc<Settings>`.
#[derive(Debug, Deserialize, Clone)]
pub struct Settings {
    pub grpc: GrpcConfig,
    pub database: DatabaseConfig,
    pub metrics: MetricsConfig,
    pub engine: EngineConfig,
    pub log: LogConfig,
}

/// gRPC server binding.
#[derive(Debug, Deserialize, Clone)]
pub struct GrpcConfig {
    /// Port the gRPC server listens on (default: 50052)
    pub port: u16,
}

/// PostgreSQL connection settings.
#[derive(Debug, Deserialize, Clone)]
pub struct DatabaseConfig {
    /// Full DSN, e.g. postgres://reconx:reconx@localhost:5432/reconx?sslmode=disable
    pub dsn: String,
    /// Max connections in the pool
    pub max_connections: u32,
    /// Min idle connections kept alive
    pub min_connections: u32,
    /// Seconds to wait for a connection from the pool before timing out
    pub connect_timeout_secs: u64,
    /// Seconds an idle connection can live before being reaped
    pub idle_timeout_secs: u64,
}

/// Prometheus metrics HTTP endpoint.
#[derive(Debug, Deserialize, Clone)]
pub struct MetricsConfig {
    /// Port for the /metrics HTTP endpoint (default: 9091)
    pub port: u16,
}

/// Core reconciliation engine tuning parameters.
#[derive(Debug, Deserialize, Clone)]
pub struct EngineConfig {
    /// How often the background worker polls for new work (seconds)
    pub poll_interval_secs: u64,

    /// Maximum number of transaction_refs processed per worker tick
    pub batch_size: i64,

    /// Percentage tolerance for amount comparison (e.g. 0.01 = 1%).
    /// If the relative difference between two amounts is within this threshold,
    /// the records are considered matched.
    pub amount_tolerance_pct: f64,

    /// Absolute amount tolerance as a decimal string (e.g. "0.01").
    /// The engine uses max(pct_tolerance, abs_tolerance) per comparison.
    pub amount_tolerance_abs: String,

    /// Maximum number of reconciliation attempts before giving up
    pub max_retries: i32,

    /// Seconds to wait between retry attempts (backoff)
    pub retry_backoff_secs: u64,

    /// After this many seconds in PENDING state, the engine escalates to
    /// MISMATCHED (i.e., required sources never arrived)
    pub pending_timeout_secs: u64,

    /// If non-empty, a transaction is only considered complete when ALL
    /// listed source systems have submitted a record. Leave empty to
    /// reconcile any 2+ source systems dynamically.
    pub expected_sources: Vec<String>,

    /// Matching strategy: "exact" | "tolerance" | "majority"
    ///   exact      — all amounts must be identical (no tolerance)
    ///   tolerance  — amounts within configured tolerance are matched
    ///   majority   — sources in the majority bucket win; outliers are flagged
    pub match_strategy: String,
}

/// Logging configuration.
#[derive(Debug, Deserialize, Clone)]
pub struct LogConfig {
    /// tracing level filter: "trace" | "debug" | "info" | "warn" | "error"
    pub level: String,
    /// Output format: "json" (default, structured) | "text" (human-readable)
    pub format: String,
}

impl Settings {
    /// Load configuration, layering defaults → file → environment.
    pub fn load() -> Result<Self, ConfigError> {
        Config::builder()
            // Layer 1: defaults from config/default.toml
            .add_source(File::with_name("config/default").required(false))
            // Layer 2: environment variables (RECONX_ENGINE___ separator)
            .add_source(
                Environment::with_prefix("RECONX_ENGINE")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?
            .try_deserialize()
    }
}
