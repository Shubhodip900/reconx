/// metrics.rs — Prometheus metrics registry for the Reconciliation Engine.
///
/// All metrics are registered under the `reconx_engine_` namespace so they
/// are clearly distinct from the Ingestion Service's `reconx_ingestion_` metrics
/// when scraped by the same Prometheus instance.
///
/// The `Metrics` struct is constructed once at startup and shared via `Arc`.
use prometheus::{
    exponential_buckets, histogram_opts, opts, register_counter_vec_with_registry,
    register_counter_with_registry, register_gauge_with_registry,
    register_histogram_vec_with_registry, register_histogram_with_registry, CounterVec,
    Gauge, Histogram, HistogramVec, Registry, Counter,
};

/// All Prometheus metrics for the engine.  Pass `Arc<Metrics>` everywhere.
#[derive(Clone)]
pub struct Metrics {
    // ── Reconciliation outcomes ────────────────────────────────────────────
    /// Total reconciliation attempts, labelled by final status.
    ///   Labels: status = MATCHED | MISMATCHED | PENDING
    pub recon_total: CounterVec,

    /// Number of currently open mismatches (transitions: +1 on MISMATCHED, -1 on MATCHED).
    pub active_mismatches: Gauge,

    /// Total times ReTriggerMatch was called via gRPC.
    pub retriggers_total: Counter,

    // ── Worker health ──────────────────────────────────────────────────────
    /// Duration of each worker poll cycle (seconds).
    pub worker_cycle_duration: Histogram,

    /// Records processed per worker batch tick.
    pub worker_batch_size: Histogram,

    /// Total unrecoverable worker errors.
    pub worker_errors_total: Counter,

    /// Per-transaction reconciliation errors (labelled by transaction_ref).
    /// Note: high-cardinality label — only keep in development mode.
    pub recon_errors_total: CounterVec,

    // ── gRPC layer ─────────────────────────────────────────────────────────
    /// Total gRPC calls per method.
    ///   Labels: method = GetReconState | ReTriggerMatch
    pub grpc_requests_total: CounterVec,

    /// gRPC handler latency per method (seconds).
    pub grpc_request_duration: HistogramVec,
}

impl Metrics {
    /// Create and register all metrics. Panics on name collision (programming error).
    pub fn new(registry: &Registry) -> Self {
        let recon_total = register_counter_vec_with_registry!(
            opts!(
                "reconx_engine_reconciliations_total",
                "Total reconciliation attempts grouped by outcome status"
            ),
            &["status"],
            registry
        )
        .expect("reconx_engine_reconciliations_total");

        let active_mismatches = register_gauge_with_registry!(
            opts!(
                "reconx_engine_active_mismatches",
                "Current number of transactions in MISMATCHED state"
            ),
            registry
        )
        .expect("reconx_engine_active_mismatches");

        let retriggers_total = register_counter_with_registry!(
            opts!(
                "reconx_engine_retriggers_total",
                "Total number of manual ReTriggerMatch calls received"
            ),
            registry
        )
        .expect("reconx_engine_retriggers_total");

        let worker_cycle_duration = register_histogram_with_registry!(
            histogram_opts!(
                "reconx_engine_worker_cycle_duration_seconds",
                "Time spent per worker poll cycle",
                // buckets from 10ms to 30s
                exponential_buckets(0.01, 3.0, 9).unwrap()
            ),
            registry
        )
        .expect("reconx_engine_worker_cycle_duration_seconds");

        let worker_batch_size = register_histogram_with_registry!(
            histogram_opts!(
                "reconx_engine_worker_batch_size",
                "Number of transaction_refs processed per worker tick",
                vec![0.0, 1.0, 5.0, 10.0, 25.0, 50.0, 100.0, 250.0, 500.0]
            ),
            registry
        )
        .expect("reconx_engine_worker_batch_size");

        let worker_errors_total = register_counter_with_registry!(
            opts!(
                "reconx_engine_worker_errors_total",
                "Total unrecoverable errors in the background worker loop"
            ),
            registry
        )
        .expect("reconx_engine_worker_errors_total");

        let recon_errors_total = register_counter_vec_with_registry!(
            opts!(
                "reconx_engine_recon_errors_total",
                "Per-transaction reconciliation errors (high-cardinality label)"
            ),
            &["transaction_ref"],
            registry
        )
        .expect("reconx_engine_recon_errors_total");

        let grpc_requests_total = register_counter_vec_with_registry!(
            opts!(
                "reconx_engine_grpc_requests_total",
                "Total gRPC requests received, labelled by RPC method"
            ),
            &["method"],
            registry
        )
        .expect("reconx_engine_grpc_requests_total");

        let grpc_request_duration = register_histogram_vec_with_registry!(
            histogram_opts!(
                "reconx_engine_grpc_request_duration_seconds",
                "gRPC handler latency per method",
                exponential_buckets(0.001, 2.0, 11).unwrap()
            ),
            &["method"],
            registry
        )
        .expect("reconx_engine_grpc_request_duration_seconds");

        // Pre-initialise label combinations to avoid "no data" gaps
        for status in &["MATCHED", "MISMATCHED", "PENDING"] {
            recon_total.with_label_values(&[status]);
        }
        for method in &["GetReconState", "ReTriggerMatch"] {
            grpc_requests_total.with_label_values(&[method]);
            grpc_request_duration.with_label_values(&[method]);
        }

        Self {
            recon_total,
            active_mismatches,
            retriggers_total,
            worker_cycle_duration,
            worker_batch_size,
            worker_errors_total,
            recon_errors_total,
            grpc_requests_total,
            grpc_request_duration,
        }
    }

    /// Render all metrics in Prometheus text format.
    pub fn render(&self, registry: &Registry) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let metric_families = registry.gather();
        let mut buf = Vec::new();
        encoder.encode(&metric_families, &mut buf).unwrap_or(());
        String::from_utf8(buf).unwrap_or_default()
    }
}
