/// grpc/server.rs — gRPC service implementation for the Reconciliation Engine.
///
/// Implements two RPCs defined in engine.proto:
///
///   GetReconState    — Returns the current reconciliation state for a
///                      given `transaction_ref`. If no state exists yet,
///                      triggers an immediate reconciliation attempt.
///
///   ReTriggerMatch   — Forces the engine to re-evaluate all records for
///                      a `transaction_ref`, resetting the retry counter
///                      and running the matcher synchronously.
use std::sync::Arc;

use sqlx::{Pool, Postgres};
use tonic::{Request, Response, Status};

use crate::db::queries;
use crate::engine::worker::ReconciliationWorker;
use crate::grpc::proto::{
    MatchDetail, ReconStatus, ReconciliationEngine, StateRequest, StateResponse,
};
use crate::metrics::Metrics;

/// Shared service state injected via `Arc`.
pub struct ReconEngineService {
    pub pool: Arc<Pool<Postgres>>,
    pub worker: Arc<ReconciliationWorker>,
    pub metrics: Arc<Metrics>,
}

#[tonic::async_trait]
impl ReconciliationEngine for ReconEngineService {
    // ── GetReconState ─────────────────────────────────────────────────────────
    /// Return the current reconciliation state for `transaction_ref`.
    ///
    /// If the transaction has never been processed, the engine runs the
    /// matcher inline (best-effort) before returning so callers always
    /// get a meaningful response on first query.
    async fn get_recon_state(
        &self,
        request: Request<StateRequest>,
    ) -> Result<Response<StateResponse>, Status> {
        let transaction_ref = request.into_inner().transaction_ref;
        validate_transaction_ref(&transaction_ref)?;

        let _timer = self
            .metrics
            .grpc_request_duration
            .with_label_values(&["GetReconState"])
            .start_timer();

        self.metrics
            .grpc_requests_total
            .with_label_values(&["GetReconState"])
            .inc();

        tracing::info!(transaction_ref = %transaction_ref, "GetReconState called");

        // Try to fetch existing state
        let state_opt = queries::get_recon_state(&self.pool, &transaction_ref)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // If not found, try immediate reconciliation so the caller sees something useful
        if state_opt.is_none() {
            tracing::debug!(
                transaction_ref = %transaction_ref,
                "no existing state — triggering immediate reconciliation"
            );
            if let Err(e) = self.worker.reconcile_one(&transaction_ref).await {
                tracing::warn!(
                    transaction_ref = %transaction_ref,
                    error = %e,
                    "inline reconciliation failed"
                );
                // Not fatal — fall through and return NOT_FOUND if still missing
            }
        }

        // Re-fetch after potential inline reconciliation
        let state = queries::get_recon_state(&self.pool, &transaction_ref)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::not_found(format!(
                    "no ingested records found for transaction_ref '{transaction_ref}'"
                ))
            })?;

        let details = queries::get_match_details(&self.pool, &transaction_ref)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(build_state_response(state, details)?))
    }

    // ── ReTriggerMatch ────────────────────────────────────────────────────────
    /// Force the engine to re-evaluate a `transaction_ref`.
    ///
    /// This is useful after a new record arrives late, after a manual data
    /// correction, or when the resolution service marks a MISMATCHED
    /// transaction as needing re-check.
    ///
    /// Steps:
    ///   1. Reset `retry_count` and status to PENDING.
    ///   2. Run the matcher synchronously.
    ///   3. Return the updated state.
    async fn re_trigger_match(
        &self,
        request: Request<StateRequest>,
    ) -> Result<Response<StateResponse>, Status> {
        let transaction_ref = request.into_inner().transaction_ref;
        validate_transaction_ref(&transaction_ref)?;

        let _timer = self
            .metrics
            .grpc_request_duration
            .with_label_values(&["ReTriggerMatch"])
            .start_timer();

        self.metrics
            .grpc_requests_total
            .with_label_values(&["ReTriggerMatch"])
            .inc();

        tracing::info!(transaction_ref = %transaction_ref, "ReTriggerMatch called");

        // Reset state so the worker treats this as a fresh attempt
        queries::reset_for_retrigger(&self.pool, &transaction_ref)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Append audit entry for the manual retrigger
        let _ = queries::insert_audit_log(
            &self.pool,
            &transaction_ref,
            "RETRIGGER",
            None,
            Some("PENDING"),
            Some(serde_json::json!({ "triggered_by": "grpc_ReTriggerMatch" })),
        )
        .await;

        // Run matching synchronously so the caller sees fresh results
        self.worker
            .reconcile_one(&transaction_ref)
            .await
            .map_err(|e| Status::internal(format!("reconciliation failed: {e}")))?;

        self.metrics.retriggers_total.inc();

        // Return updated state
        let state = queries::get_recon_state(&self.pool, &transaction_ref)
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .ok_or_else(|| {
                Status::internal(format!(
                    "reconciliation ran but produced no state for '{transaction_ref}'"
                ))
            })?;

        let details = queries::get_match_details(&self.pool, &transaction_ref)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        Ok(Response::new(build_state_response(state, details)?))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Map a status string from the database to the proto `ReconStatus` i32.
fn status_to_i32(status: &str) -> i32 {
    match status {
        "MATCHED" => ReconStatus::Matched as i32,
        "MISMATCHED" => ReconStatus::Mismatched as i32,
        "RESOLVED" => ReconStatus::Resolved as i32,
        _ => ReconStatus::Pending as i32,
    }
}

/// Construct the `StateResponse` proto message from database rows.
fn build_state_response(
    state: crate::db::models::ReconState,
    details: Vec<crate::db::models::ReconMatchDetail>,
) -> Result<StateResponse, Status> {
    let match_details: Vec<MatchDetail> = details
        .into_iter()
        .map(|d| MatchDetail {
            system_name: d.system_name,
            data_captured: d.data_captured.unwrap_or_default(),
            discrepancy_found: d.discrepancy_found,
        })
        .collect();

    let last_updated = state
        .last_processed_at
        .unwrap_or(state.updated_at)
        .timestamp_millis();

    Ok(StateResponse {
        transaction_ref: state.transaction_ref,
        status: status_to_i32(&state.status),
        details: match_details,
        last_updated,
    })
}

/// Validate that `transaction_ref` is non-empty and printable ASCII only
/// (matches the ingestion service validation rule).
fn validate_transaction_ref(r: &str) -> Result<(), Status> {
    if r.is_empty() {
        return Err(Status::invalid_argument("transaction_ref must not be empty"));
    }
    if !r.chars().all(|c| ('\x20'..='\x7E').contains(&c)) {
        return Err(Status::invalid_argument(
            "transaction_ref must contain only printable ASCII characters",
        ));
    }
    Ok(())
}
