/// engine/worker.rs — background reconciliation worker.
///
/// The worker runs in a dedicated Tokio task, polling PostgreSQL on a
/// configurable interval. For each batch of unprocessed `transaction_ref`s:
///
/// 1. Fetch all ingested records for that transaction.
/// 2. Run the matcher to produce a `MatchOutcome`.
/// 3. Persist the result to `recon_state` + `recon_match_details`.
/// 4. Append an entry to `recon_audit_log`.
/// 5. Update `ingestion_records.status` for external visibility.
/// 6. Emit Prometheus metrics.
///
/// The worker is cleanly stopped via a `tokio::sync::broadcast` shutdown signal.
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use serde_json::json;
use sqlx::{Pool, Postgres};

use crate::config::EngineConfig;
use crate::db::{models::RecordView, queries};
use crate::engine::{matcher, rules::RuleSet};
use crate::metrics::Metrics;

/// Owned handle to the background worker.
pub struct ReconciliationWorker {
    pool: Arc<Pool<Postgres>>,
    rules: Arc<RuleSet>,
    config: Arc<EngineConfig>,
    metrics: Arc<Metrics>,
}

impl ReconciliationWorker {
    pub fn new(
        pool: Arc<Pool<Postgres>>,
        rules: Arc<RuleSet>,
        config: Arc<EngineConfig>,
        metrics: Arc<Metrics>,
    ) -> Self {
        Self {
            pool,
            rules,
            config,
            metrics,
        }
    }

    /// Spawn the worker onto the current Tokio runtime.
    ///
    /// Takes `Arc<Self>` so the same instance can be shared with the gRPC server.
    /// Returns a `JoinHandle` you can await for clean shutdown.
    /// Call `shutdown_tx.send(())` to stop.
    pub fn spawn(
        self: std::sync::Arc<Self>,
        mut shutdown_rx: tokio::sync::broadcast::Receiver<()>,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let interval = Duration::from_secs(self.config.poll_interval_secs);
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

            tracing::info!(
                poll_interval_secs = self.config.poll_interval_secs,
                batch_size = self.config.batch_size,
                strategy = %self.config.match_strategy,
                "reconciliation worker started"
            );

            loop {
                tokio::select! {
                    biased;

                    _ = shutdown_rx.recv() => {
                        tracing::info!("reconciliation worker received shutdown signal");
                        break;
                    }

                    _ = ticker.tick() => {
                        let timer = self.metrics.worker_cycle_duration.start_timer();
                        if let Err(e) = self.process_batch().await {
                            tracing::error!(error = %e, "worker batch failed");
                            self.metrics.worker_errors_total.inc();
                        }
                        timer.observe_duration();
                    }
                }
            }

            tracing::info!("reconciliation worker stopped");
        })
    }

    /// Process one batch of pending transaction_refs.
    async fn process_batch(&self) -> anyhow::Result<()> {
        let backoff_cutoff =
            Utc::now() - chrono::Duration::seconds(self.config.retry_backoff_secs as i64);

        let refs = queries::get_pending_transaction_refs(
            &self.pool,
            self.config.batch_size,
            backoff_cutoff,
        )
        .await?;

        if refs.is_empty() {
            tracing::debug!("no pending transactions in this batch");
            return Ok(());
        }

        tracing::info!(count = refs.len(), "processing reconciliation batch");
        self.metrics.worker_batch_size.observe(refs.len() as f64);

        for transaction_ref in refs {
            if let Err(e) = self.reconcile_one(&transaction_ref).await {
                tracing::warn!(
                    transaction_ref = %transaction_ref,
                    error = %e,
                    "reconciliation failed for transaction"
                );
                self.metrics
                    .recon_errors_total
                    .with_label_values(&[&transaction_ref])
                    .inc();
            }
        }

        Ok(())
    }

    /// Reconcile a single `transaction_ref` end-to-end.
    pub async fn reconcile_one(&self, transaction_ref: &str) -> anyhow::Result<()> {
        let span = tracing::info_span!("reconcile", transaction_ref = %transaction_ref);
        let _enter = span.enter();

        // ── 1. Fetch current state (for audit: old_status) ────────────────
        let old_status = queries::get_recon_state(&self.pool, transaction_ref)
            .await?
            .map(|s| s.status)
            .unwrap_or_else(|| "NEW".to_string());

        // ── 2. Fetch all ingested records ──────────────────────────────────
        let records = queries::get_records_by_transaction_ref(&self.pool, transaction_ref).await?;

        if records.is_empty() {
            tracing::warn!("no ingestion records found; skipping");
            return Ok(());
        }

        let first_seen_at = records
            .iter()
            .map(|r| r.server_received_at)
            .min()
            .unwrap_or_else(Utc::now);

        // ── 3. Project to RecordView for the matcher ───────────────────────
        let views: Vec<RecordView> = records.iter().map(RecordView::from).collect();

        // ── 4. Run matching logic ──────────────────────────────────────────
        let outcome = matcher::match_records(&views, &self.rules, first_seen_at)?;
        let new_status = outcome.status_str();

        tracing::info!(
            old_status = %old_status,
            new_status = %new_status,
            record_count = records.len(),
            "reconciliation outcome"
        );

        // ── 5. Persist recon_state ─────────────────────────────────────────
        queries::upsert_recon_state(
            &self.pool,
            transaction_ref,
            new_status,
            records.len() as i32,
            &outcome.matched_sources(),
            &outcome.mismatched_sources(),
            self.rules.max_retries,
        )
        .await?;

        // ── 6. Persist per-source match details ───────────────────────────
        for sr in outcome.source_results() {
            queries::upsert_match_detail(
                &self.pool,
                transaction_ref,
                &sr.source_system,
                &sr.internal_id,
                sr.amount.as_ref().map(|d| d.to_string()).as_deref(),
                sr.currency.as_deref(),
                sr.data_captured.as_deref(),
                sr.discrepancy_found,
                sr.discrepancy_details.clone(),
            )
            .await?;
        }

        // ── 7. Audit log ──────────────────────────────────────────────────
        queries::insert_audit_log(
            &self.pool,
            transaction_ref,
            "MATCH_ATTEMPT",
            Some(&old_status),
            Some(new_status),
            Some(outcome.audit_details()),
        )
        .await?;

        // ── 8. Update ingestion_records.status (cross-service propagation) ─
        if new_status != "PENDING" {
            queries::update_ingestion_status(&self.pool, transaction_ref, new_status).await?;
        }

        // ── 9. Metrics ────────────────────────────────────────────────────
        self.metrics
            .recon_total
            .with_label_values(&[new_status])
            .inc();

        if new_status == "MISMATCHED" {
            self.metrics.active_mismatches.inc();
        } else if old_status == "MISMATCHED" && new_status == "MATCHED" {
            self.metrics.active_mismatches.dec();
        }

        Ok(())
    }
}
