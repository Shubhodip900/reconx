// Package retry implements the background retry worker for the Resolution Service.
//
// The worker periodically scans the resolution_retry_queue for PENDING entries
// that are due (next_retry_at <= NOW()), calls the engine's ReTriggerMatch gRPC
// for each one, and updates the queue based on the result:
//
//   - If the engine returns MATCHED or RESOLVED → mark queue entry RESOLVED.
//   - If the engine returns MISMATCHED → increment attempt_count; compute next
//     retry time using exponential backoff (min(base * 2^attempt, max)).
//   - If attempt_count reaches max_attempts → mark EXHAUSTED.
//     If AutoApplyOnExhaustion is configured, the auto-resolver runs automatically.
//
// All outcomes are recorded in the audit log and Prometheus metrics.
package retry

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"time"

	"go.uber.org/zap"

	commonpb "github.com/reconx/proto/gen/go/common"
	"github.com/reconx/services/resolution/internal/config"
	"github.com/reconx/services/resolution/internal/db"
	engineclient "github.com/reconx/services/resolution/internal/engine"
	"github.com/reconx/services/resolution/internal/metrics"
	"github.com/reconx/services/resolution/internal/resolver"
)

// Worker is the background retry worker.
type Worker struct {
	database    *sql.DB
	engine      *engineclient.Client
	resolver    *resolver.Resolver
	cfg         config.RetryConfig
	autoCfg     config.AutoResolveConfig
	log         *zap.Logger
}

// New creates a new retry Worker.
func New(
	database *sql.DB,
	engine *engineclient.Client,
	res *resolver.Resolver,
	cfg config.RetryConfig,
	autoCfg config.AutoResolveConfig,
	log *zap.Logger,
) *Worker {
	return &Worker{
		database: database,
		engine:   engine,
		resolver: res,
		cfg:      cfg,
		autoCfg:  autoCfg,
		log:      log,
	}
}

// Start runs the worker loop until ctx is cancelled.
// Call this in a goroutine: go worker.Start(ctx).
func (w *Worker) Start(ctx context.Context) {
	if !w.cfg.Enabled {
		w.log.Info("retry worker is disabled — skipping startup")
		return
	}

	interval := time.Duration(w.cfg.PollIntervalSecs) * time.Second
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	w.log.Info("retry worker started",
		zap.Duration("poll_interval", interval),
		zap.Int("max_attempts", w.cfg.MaxAttempts),
		zap.Int("base_backoff_secs", w.cfg.BaseBackoffSecs),
		zap.Int("max_backoff_secs", w.cfg.MaxBackoffSecs),
		zap.Int("batch_size", w.cfg.BatchSize),
		zap.Bool("auto_apply_on_exhaustion", w.autoCfg.AutoApplyOnExhaustion),
	)

	// Run one immediate cycle on startup to catch any backlog.
	w.runCycle(ctx)

	for {
		select {
		case <-ctx.Done():
			w.log.Info("retry worker stopping (context cancelled)")
			return
		case <-ticker.C:
			w.runCycle(ctx)
		}
	}
}

// runCycle executes one poll cycle: fetch due retries and process them.
func (w *Worker) runCycle(ctx context.Context) {
	timer := prometheus_timer(metrics.RetryWorkerCycleDuration)
	defer timer()

	// Update queue depth gauge
	w.updateQueueGauges(ctx)

	pending, err := db.GetPendingRetries(ctx, w.database, w.cfg.BatchSize)
	if err != nil {
		w.log.Error("failed to fetch pending retries", zap.Error(err))
		metrics.RetryWorkerErrors.Inc()
		return
	}

	if len(pending) == 0 {
		w.log.Debug("no pending retries in this cycle")
		return
	}

	w.log.Info("retry worker processing batch", zap.Int("count", len(pending)))

	for _, entry := range pending {
		if ctx.Err() != nil {
			break // shutdown signal
		}
		if err := w.retryOne(ctx, entry); err != nil {
			w.log.Warn("retry failed for transaction",
				zap.String("transaction_ref", entry.TransactionRef),
				zap.Error(err),
			)
			metrics.RetryAttemptsTotal.WithLabelValues("error").Inc()
		}
	}
}

// retryOne processes a single retry queue entry.
func (w *Worker) retryOne(ctx context.Context, entry db.RetryQueueRow) error {
	txRef := entry.TransactionRef
	attempt := entry.AttemptCount + 1 // this will be the new attempt number

	w.log.Info("retrying transaction",
		zap.String("transaction_ref", txRef),
		zap.Int("attempt", attempt),
		zap.Int("max_attempts", entry.MaxAttempts),
	)

	// ── 1. Call engine to re-trigger matching ─────────────────────────────────
	state, err := w.engine.ReTriggerMatch(ctx, txRef)
	if err != nil {
		// Engine call failed — increment attempt and back off
		nextRetry := w.nextRetryTime(attempt)
		_ = db.IncrementRetryAttempt(ctx, w.database, txRef, nextRetry)
		_ = w.writeAuditLog(ctx, txRef, "RETRY_ENGINE_ERROR", "MISMATCHED", "MISMATCHED", map[string]interface{}{
			"attempt": attempt,
			"error":   err.Error(),
		})
		return fmt.Errorf("engine ReTriggerMatch: %w", err)
	}

	// ── 2. Evaluate engine outcome ────────────────────────────────────────────
	statusStr := protoStatusToString(state.Status)

	switch statusStr {
	case "MATCHED", "RESOLVED":
		// Great — the engine resolved it. Mark queue entry as resolved.
		w.log.Info("transaction matched after retry",
			zap.String("transaction_ref", txRef),
			zap.Int("attempt", attempt),
			zap.String("status", statusStr),
		)
		if err := db.MarkRetryResolved(ctx, w.database, txRef); err != nil {
			w.log.Warn("failed to mark retry as resolved", zap.String("transaction_ref", txRef), zap.Error(err))
		}
		_ = w.writeAuditLog(ctx, txRef, "RETRY_RESOLVED", "MISMATCHED", statusStr, map[string]interface{}{
			"attempt":          attempt,
			"engine_status":    statusStr,
			"resolution_type":  "engine_retrigger",
		})
		metrics.RetryAttemptsTotal.WithLabelValues("matched").Inc()
		return nil

	case "MISMATCHED":
		// Still mismatched — check if we've hit the limit
		if attempt >= entry.MaxAttempts {
			return w.handleExhausted(ctx, txRef, attempt)
		}

		// Schedule the next retry with exponential backoff
		nextRetry := w.nextRetryTime(attempt)
		if err := db.IncrementRetryAttempt(ctx, w.database, txRef, nextRetry); err != nil {
			return fmt.Errorf("increment retry attempt: %w", err)
		}

		_ = w.writeAuditLog(ctx, txRef, "RETRY_ATTEMPT", "MISMATCHED", "MISMATCHED", map[string]interface{}{
			"attempt":         attempt,
			"max_attempts":    entry.MaxAttempts,
			"next_retry_at":   nextRetry.Format(time.RFC3339),
			"engine_status":   statusStr,
		})

		w.log.Info("transaction still mismatched — scheduled next retry",
			zap.String("transaction_ref", txRef),
			zap.Int("attempt", attempt),
			zap.Time("next_retry_at", nextRetry),
		)
		metrics.RetryAttemptsTotal.WithLabelValues("still_mismatched").Inc()
		return nil

	default:
		// PENDING — engine needs more data. Back off and try again.
		nextRetry := w.nextRetryTime(attempt)
		if err := db.IncrementRetryAttempt(ctx, w.database, txRef, nextRetry); err != nil {
			return fmt.Errorf("increment retry attempt for pending: %w", err)
		}
		w.log.Debug("transaction still pending — backing off",
			zap.String("transaction_ref", txRef),
			zap.String("engine_status", statusStr),
		)
		metrics.RetryAttemptsTotal.WithLabelValues("still_mismatched").Inc()
		return nil
	}
}

// handleExhausted marks a transaction as exhausted and optionally auto-resolves it.
func (w *Worker) handleExhausted(ctx context.Context, txRef string, attempt int) error {
	w.log.Warn("retry attempts exhausted",
		zap.String("transaction_ref", txRef),
		zap.Int("attempt", attempt),
		zap.Bool("auto_apply_on_exhaustion", w.autoCfg.AutoApplyOnExhaustion),
	)

	if err := db.MarkRetryExhausted(ctx, w.database, txRef); err != nil {
		return fmt.Errorf("mark retry exhausted: %w", err)
	}

	_ = w.writeAuditLog(ctx, txRef, "RETRY_EXHAUSTED", "MISMATCHED", "MISMATCHED", map[string]interface{}{
		"attempt":              attempt,
		"auto_apply_strategy":  w.autoCfg.AutoApplyOnExhaustion,
		"default_strategy":     w.autoCfg.DefaultStrategy,
	})

	metrics.RetryAttemptsTotal.WithLabelValues("exhausted").Inc()

	// ── Auto-resolve on exhaustion if configured ──────────────────────────────
	if w.autoCfg.AutoApplyOnExhaustion {
		strategy, err := resolver.ParseStrategy(w.autoCfg.DefaultStrategy)
		if err != nil {
			w.log.Error("invalid auto-resolve strategy in config",
				zap.String("strategy", w.autoCfg.DefaultStrategy),
				zap.Error(err),
			)
			return nil // Don't propagate — log and leave for manual intervention
		}

		strategyConfig := map[string]string{
			"source_priority": w.autoCfg.SourcePriority,
		}

		result, err := w.resolver.Resolve(ctx, txRef, strategy, strategyConfig)
		if err != nil {
			w.log.Error("auto-resolve on exhaustion failed",
				zap.String("transaction_ref", txRef),
				zap.String("strategy", string(strategy)),
				zap.Error(err),
			)
			metrics.AutoResolutionsTotal.WithLabelValues(string(strategy), "failed").Inc()
			return nil // Leave for manual intervention
		}

		// Apply the resolution
		if err := w.applyAutoResolution(ctx, txRef, result, "system:retry_worker"); err != nil {
			w.log.Error("failed to apply auto-resolution after exhaustion",
				zap.String("transaction_ref", txRef),
				zap.Error(err),
			)
			return nil
		}

		// Mark queue entry as resolved
		_ = db.MarkRetryResolved(ctx, w.database, txRef)

		metrics.AutoResolutionsTotal.WithLabelValues(string(strategy), "success").Inc()
		w.log.Info("auto-resolved exhausted transaction",
			zap.String("transaction_ref", txRef),
			zap.String("chosen_source", result.ChosenSource),
			zap.String("strategy", string(strategy)),
		)
	}

	return nil
}

// applyAutoResolution writes the resolution record and updates recon_state.
func (w *Worker) applyAutoResolution(
	ctx context.Context,
	txRef string,
	result *resolver.Result,
	resolverID string,
) error {
	if err := db.InsertResolutionRecord(
		ctx, w.database,
		txRef,
		"AUTO",
		result.ChosenSource,
		result.Reason,
		resolverID,
		string(result.Strategy),
	); err != nil {
		return fmt.Errorf("insert auto-resolution record: %w", err)
	}

	if err := db.UpdateReconStateToResolved(ctx, w.database, txRef); err != nil {
		return fmt.Errorf("update recon state to RESOLVED: %w", err)
	}

	_ = w.writeAuditLog(ctx, txRef, "AUTO_RESOLUTION", "MISMATCHED", "RESOLVED", map[string]interface{}{
		"chosen_source": result.ChosenSource,
		"strategy":      string(result.Strategy),
		"reason":        result.Reason,
		"resolver_id":   resolverID,
	})
	return nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

// nextRetryTime computes the next retry timestamp using capped exponential backoff.
// Formula: now + min(baseSecs * 2^attempt, maxSecs)
func (w *Worker) nextRetryTime(attempt int) time.Time {
	base := float64(w.cfg.BaseBackoffSecs)
	maxB := float64(w.cfg.MaxBackoffSecs)
	backoff := base * math.Pow(2, float64(attempt-1))
	if backoff > maxB {
		backoff = maxB
	}
	return time.Now().Add(time.Duration(backoff) * time.Second)
}

// writeAuditLog is a fire-and-forget helper that converts a map to JSON and writes to recon_audit_log.
func (w *Worker) writeAuditLog(
	ctx context.Context,
	txRef, eventType, oldStatus, newStatus string,
	detail map[string]interface{},
) error {
	detailJSON, _ := json.Marshal(detail)
	_, err := w.database.ExecContext(ctx, `
		INSERT INTO recon_audit_log
			(transaction_ref, event_type, old_status, new_status, details, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())`,
		txRef, eventType, oldStatus, newStatus, detailJSON,
	)
	if err != nil {
		w.log.Warn("failed to write audit log",
			zap.String("transaction_ref", txRef),
			zap.String("event_type", eventType),
			zap.Error(err),
		)
	}
	return err
}

// updateQueueGauges refreshes the RetryQueueDepth and RetryQueueExhausted gauges.
func (w *Worker) updateQueueGauges(ctx context.Context) {
	pending, exhausted, _, err := db.RetryQueueStats(ctx, w.database)
	if err != nil {
		w.log.Warn("failed to fetch retry queue stats", zap.Error(err))
		return
	}
	metrics.RetryQueueDepth.Set(float64(pending))
	metrics.RetryQueueExhausted.Set(float64(exhausted))
}

// protoStatusToString maps a common.ReconStatus enum value to a canonical string.
func protoStatusToString(status commonpb.ReconStatus) string {
	switch status {
	case commonpb.ReconStatus_MATCHED:
		return "MATCHED"
	case commonpb.ReconStatus_MISMATCHED:
		return "MISMATCHED"
	case commonpb.ReconStatus_RESOLVED:
		return "RESOLVED"
	default:
		return "PENDING"
	}
}

// prometheus_timer returns a function that, when called, observes the elapsed time.
func prometheus_timer(obs interface{ Observe(float64) }) func() {
	start := time.Now()
	return func() {
		obs.Observe(time.Since(start).Seconds())
	}
}
