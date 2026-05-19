/// main.rs — Reconciliation Engine entrypoint.
///
/// Startup sequence:
///   1. Load configuration (config/default.toml + RECONX_ENGINE_* env vars)
///   2. Initialise structured logging (JSON in prod, human-readable in dev)
///   3. Connect to PostgreSQL and run schema migrations
///   4. Compile rule set from config
///   5. Create shared Prometheus metrics registry
///   6. Build gRPC server (GetReconState + ReTriggerMatch)
///   7. Build Prometheus HTTP metrics endpoint
///   8. Spawn background reconciliation worker
///   9. Wait for SIGTERM/SIGINT → broadcast shutdown → drain goroutines → exit
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, routing::get, Router};
use prometheus::Registry;
use tokio::signal;
use tokio::sync::broadcast;
use tonic::transport::Server;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod config;
mod db;
mod engine;
mod error;
mod grpc;
mod metrics;

use config::Settings;
use engine::{rules::RuleSet, worker::ReconciliationWorker};
use grpc::{
    proto::ReconciliationEngineServer,
    server::ReconEngineService,
};
use metrics::Metrics;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // ── 1. Configuration ───────────────────────────────────────────────────────
    let settings = Settings::load().map_err(|e| {
        eprintln!("FATAL: failed to load configuration: {e}");
        e
    })?;

    // ── 2. Logging ─────────────────────────────────────────────────────────────
    init_logging(&settings.log.level, &settings.log.format);

    tracing::info!(
        grpc_port = settings.grpc.port,
        metrics_port = settings.metrics.port,
        poll_interval_secs = settings.engine.poll_interval_secs,
        batch_size = settings.engine.batch_size,
        match_strategy = %settings.engine.match_strategy,
        "ReconX Reconciliation Engine starting"
    );

    // ── 3. PostgreSQL connection pool ──────────────────────────────────────────
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(settings.database.max_connections)
        .min_connections(settings.database.min_connections)
        .acquire_timeout(Duration::from_secs(settings.database.connect_timeout_secs))
        .idle_timeout(Duration::from_secs(settings.database.idle_timeout_secs))
        .connect(&settings.database.dsn)
        .await
        .map_err(|e| {
            tracing::error!(error = %e, dsn = %settings.database.dsn, "failed to connect to PostgreSQL");
            e
        })?;

    tracing::info!("PostgreSQL pool established");

    // ── 4. Schema migrations ───────────────────────────────────────────────────
    db::queries::run_migrations(&pool).await?;
    tracing::info!("database migrations applied");

    // ── 5. Compile reconciliation rules ───────────────────────────────────────
    let rules = Arc::new(RuleSet::from_config(&settings.engine)?);
    tracing::info!(
        strategy = ?rules.strategy,
        abs_tolerance = %rules.tolerance.absolute,
        pct_tolerance = rules.tolerance.percentage,
        expected_sources = ?rules.expected_sources,
        "rule set compiled"
    );

    // ── 6. Metrics registry ────────────────────────────────────────────────────
    let registry = Registry::new_custom(Some("reconx".to_string()), None)
        .expect("metrics registry");
    let metrics = Arc::new(Metrics::new(&registry));
    let registry = Arc::new(registry);

    // ── 7. Background worker ───────────────────────────────────────────────────
    // Keep `shutdown_tx` alive until the end of main — `.send(())` signals shutdown.
    let (shutdown_tx, _initial_rx) = broadcast::channel::<()>(1);

    let pool_arc = Arc::new(pool);
    let worker = Arc::new(ReconciliationWorker::new(
        pool_arc.clone(),
        rules.clone(),
        Arc::new(settings.engine.clone()),
        metrics.clone(),
    ));

    let worker_handle = worker.clone().spawn(shutdown_tx.subscribe());

    // ── 8. gRPC server ─────────────────────────────────────────────────────────
    let grpc_addr = format!("0.0.0.0:{}", settings.grpc.port)
        .parse()
        .expect("invalid gRPC listen address");

    let svc = ReconEngineService {
        pool: pool_arc.clone(),
        worker: worker.clone(),
        metrics: metrics.clone(),
    };

    let grpc_server = Server::builder()
        .add_service(ReconciliationEngineServer::new(svc))
        // Enable gRPC server reflection for grpcurl / Postman
        .add_service(
            tonic_reflection::server::Builder::configure()
                .register_encoded_file_descriptor_set(include_bytes!(concat!(
                    env!("OUT_DIR"),
                    "/engine_descriptor.bin"
                )))
                .build_v1()
                .expect("reflection service"),
        );

    tracing::info!(addr = %grpc_addr, "gRPC server listening");
    let grpc_shutdown_rx = shutdown_tx.subscribe();
    let grpc_handle = tokio::spawn(async move {
        if let Err(e) = grpc_server
            .serve_with_shutdown(grpc_addr, async move {
                let mut rx = grpc_shutdown_rx;
                let _ = rx.recv().await;
            })
            .await
        {
            tracing::error!(error = %e, "gRPC server error");
        }
    });

    // ── 9. Prometheus HTTP metrics endpoint ────────────────────────────────────
    let metrics_addr = format!("0.0.0.0:{}", settings.metrics.port)
        .parse::<std::net::SocketAddr>()
        .expect("invalid metrics listen address");

    let metrics_registry = registry.clone();
    let metrics_app = Router::new()
        .route("/metrics", get(metrics_handler))
        .route("/health", get(health_handler))
        .with_state(metrics_registry);

    let metrics_listener = tokio::net::TcpListener::bind(metrics_addr).await?;
    tracing::info!(addr = %metrics_addr, "metrics + health endpoint listening");

    let metrics_handle = tokio::spawn(async move {
        if let Err(e) = axum::serve(metrics_listener, metrics_app).await {
            tracing::error!(error = %e, "metrics server error");
        }
    });

    // ── 10. Wait for shutdown signal ───────────────────────────────────────────
    wait_for_signal().await;
    tracing::info!("shutdown signal received — draining workers");

    let _ = shutdown_tx.send(());

    // Allow up to 30 seconds for the worker to finish its current batch
    let drain_timeout = Duration::from_secs(30);
    let _ = tokio::time::timeout(drain_timeout, worker_handle).await;

    // Cancel gRPC and metrics servers
    grpc_handle.abort();
    metrics_handle.abort();

    tracing::info!("ReconX Reconciliation Engine stopped");
    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// HTTP handlers (metrics endpoint)
// ─────────────────────────────────────────────────────────────────────────────

async fn metrics_handler(State(registry): State<Arc<Registry>>) -> String {
    use prometheus::Encoder;
    let encoder = prometheus::TextEncoder::new();
    let metric_families = registry.gather();
    let mut buf = Vec::new();
    encoder.encode(&metric_families, &mut buf).unwrap_or(());
    String::from_utf8(buf).unwrap_or_default()
}

async fn health_handler() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "status": "ok",
        "service": "reconx-engine",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

// ─────────────────────────────────────────────────────────────────────────────
// Logging initialisation
// ─────────────────────────────────────────────────────────────────────────────

fn init_logging(level: &str, format: &str) {
    let env_filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(level));

    if format == "json" {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer().json())
            .init();
    } else {
        tracing_subscriber::registry()
            .with(env_filter)
            .with(fmt::layer().pretty())
            .init();
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Signal handling
// ─────────────────────────────────────────────────────────────────────────────

async fn wait_for_signal() {
    let ctrl_c = async {
        signal::ctrl_c().await.expect("ctrl-c handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => { tracing::info!("received SIGINT (Ctrl-C)") }
        _ = terminate => { tracing::info!("received SIGTERM") }
    }
}
