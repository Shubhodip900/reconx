// Package api implements the HTTP REST API for the Resolution Service.
//
// Endpoints:
//
//	POST /v1/resolve/auto/{ref}     — trigger automatic resolution with a strategy
//	POST /v1/resolve/retry/{ref}    — enqueue a transaction for the retry worker
//	GET  /v1/resolve/audit/{ref}    — full audit trail for a transaction
//	GET  /v1/resolve/retry-queue    — list retry queue entries (paginated)
//	GET  /v1/resolve/mismatches     — list MISMATCHED transactions (paginated)
//	GET  /health                    — liveness probe
//
// All non-health endpoints record Prometheus metrics via the metrics package.
// Method+path routing requires Go 1.22+.
package api

import (
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strconv"
	"time"

	"go.uber.org/zap"

	"github.com/reconx/services/resolution/internal/config"
	"github.com/reconx/services/resolution/internal/db"
	"github.com/reconx/services/resolution/internal/metrics"
	"github.com/reconx/services/resolution/internal/resolver"
)

// Handler holds dependencies for all HTTP REST handlers.
type Handler struct {
	db       *sql.DB
	resolver *resolver.Resolver
	cfg      *config.Config
	log      *zap.Logger
}

// New creates a Handler backed by the given dependencies.
func New(database *sql.DB, res *resolver.Resolver, cfg *config.Config, log *zap.Logger) *Handler {
	return &Handler{db: database, resolver: res, cfg: cfg, log: log}
}

// RegisterRoutes registers all REST endpoints on mux.
// Uses Go 1.22+ method+path pattern syntax.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /v1/resolve/auto/{ref}", h.wrap("POST", "/v1/resolve/auto/{ref}", h.autoResolve))
	mux.HandleFunc("POST /v1/resolve/retry/{ref}", h.wrap("POST", "/v1/resolve/retry/{ref}", h.enqueueRetry))
	mux.HandleFunc("GET /v1/resolve/audit/{ref}", h.wrap("GET", "/v1/resolve/audit/{ref}", h.getAuditTrail))
	mux.HandleFunc("GET /v1/resolve/retry-queue", h.wrap("GET", "/v1/resolve/retry-queue", h.getRetryQueue))
	mux.HandleFunc("GET /v1/resolve/mismatches", h.wrap("GET", "/v1/resolve/mismatches", h.listMismatches))
	mux.HandleFunc("GET /health", h.health)
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /v1/resolve/auto/{ref}
// ─────────────────────────────────────────────────────────────────────────────

type autoResolveRequest struct {
	// Strategy overrides the global default. One of:
	// source_priority | latest_record | highest_amount | lowest_amount | first_submitted
	Strategy string `json:"strategy"`
	// SourcePriority is the comma-separated priority list for source_priority strategy.
	// Falls back to cfg.AutoResolve.SourcePriority if empty.
	SourcePriority string `json:"source_priority"`
	// ResolverID identifies the caller. Defaults to "system:api".
	ResolverID string `json:"resolver_id"`
}

type autoResolveResponse struct {
	TransactionRef string `json:"transaction_ref"`
	ChosenSource   string `json:"chosen_source"`
	Strategy       string `json:"strategy"`
	Reason         string `json:"reason"`
	ResolvedAt     string `json:"resolved_at"`
}

// autoResolve handles POST /v1/resolve/auto/{ref}.
//
// Flow:
//  1. Validate the transaction exists and is MISMATCHED (or already RESOLVED for idempotency).
//  2. Parse the resolution strategy from the request body (falls back to config default).
//  3. Run the chosen strategy via the Resolver.
//  4. Persist the resolution record and update recon_state → RESOLVED.
//  5. Mark the retry queue entry RESOLVED (if one exists).
//  6. Append an audit log entry.
func (h *Handler) autoResolve(w http.ResponseWriter, r *http.Request) {
	txRef := r.PathValue("ref")
	if txRef == "" {
		writeError(w, http.StatusBadRequest, "transaction_ref is required")
		return
	}

	var req autoResolveRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	// ── Check current state ───────────────────────────────────────────────────
	state, err := db.GetReconState(r.Context(), h.db, txRef)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "transaction not found")
			return
		}
		h.log.Error("GetReconState failed", zap.String("ref", txRef), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	if state.Status == "PENDING" || state.Status == "MATCHED" {
		writeError(w, http.StatusConflict,
			"transaction is "+state.Status+" and cannot be auto-resolved")
		return
	}

	// ── Determine strategy ────────────────────────────────────────────────────
	strategyStr := req.Strategy
	if strategyStr == "" {
		strategyStr = h.cfg.AutoResolve.DefaultStrategy
	}
	strat, err := resolver.ParseStrategy(strategyStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	sourcePriority := req.SourcePriority
	if sourcePriority == "" {
		sourcePriority = h.cfg.AutoResolve.SourcePriority
	}

	// ── Run resolver ──────────────────────────────────────────────────────────
	start := time.Now()
	result, err := h.resolver.Resolve(r.Context(), txRef, strat, map[string]string{
		"source_priority": sourcePriority,
	})
	metrics.AutoResolutionDuration.WithLabelValues(string(strat)).Observe(time.Since(start).Seconds())
	if err != nil {
		h.log.Warn("auto-resolve strategy failed",
			zap.String("ref", txRef),
			zap.String("strategy", string(strat)),
			zap.Error(err),
		)
		metrics.AutoResolutionsTotal.WithLabelValues(string(strat), "failed").Inc()
		writeError(w, http.StatusUnprocessableEntity, "resolution strategy failed: "+err.Error())
		return
	}

	resolverID := req.ResolverID
	if resolverID == "" {
		resolverID = "system:api"
	}

	// ── Persist resolution ────────────────────────────────────────────────────
	if err := db.InsertResolutionRecord(r.Context(), h.db,
		txRef, "AUTO",
		result.ChosenSource,
		result.Reason,
		resolverID,
		string(strat),
	); err != nil {
		h.log.Error("InsertResolutionRecord failed", zap.String("ref", txRef), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to persist resolution")
		return
	}

	if err := db.UpdateReconStateToResolved(r.Context(), h.db, txRef); err != nil {
		h.log.Error("UpdateReconStateToResolved failed", zap.String("ref", txRef), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to update recon state")
		return
	}

	// Clean up retry queue entry if present — fire-and-forget.
	_ = db.MarkRetryResolved(r.Context(), h.db, txRef)

	// Audit log — fire-and-forget.
	_ = db.InsertAuditLog(r.Context(), h.db,
		txRef, "AUTO_RESOLUTION",
		state.Status, "RESOLVED",
		map[string]string{
			"chosen_source": result.ChosenSource,
			"strategy":      string(strat),
			"reason":        result.Reason,
			"resolver_id":   resolverID,
		},
	)

	metrics.AutoResolutionsTotal.WithLabelValues(string(strat), "success").Inc()
	h.log.Info("auto-resolved transaction via HTTP API",
		zap.String("ref", txRef),
		zap.String("chosen_source", result.ChosenSource),
		zap.String("strategy", string(strat)),
		zap.String("resolver_id", resolverID),
	)

	writeJSON(w, http.StatusOK, autoResolveResponse{
		TransactionRef: txRef,
		ChosenSource:   result.ChosenSource,
		Strategy:       string(strat),
		Reason:         result.Reason,
		ResolvedAt:     time.Now().UTC().Format(time.RFC3339),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// POST /v1/resolve/retry/{ref}
// ─────────────────────────────────────────────────────────────────────────────

type enqueueRetryRequest struct {
	// RequestedBy identifies who triggered the retry. Defaults to "api".
	RequestedBy string `json:"requested_by"`
	// MaxAttempts overrides the global retry.max_attempts config.
	MaxAttempts int `json:"max_attempts"`
}

type enqueueRetryResponse struct {
	TransactionRef string `json:"transaction_ref"`
	MaxAttempts    int    `json:"max_attempts"`
	Message        string `json:"message"`
}

// enqueueRetry handles POST /v1/resolve/retry/{ref}.
//
// Adds a MISMATCHED transaction to the retry queue so the background retry
// worker will attempt to re-trigger matching. If the transaction already has
// an EXHAUSTED queue entry, it is reset to PENDING (attempt_count = 0).
func (h *Handler) enqueueRetry(w http.ResponseWriter, r *http.Request) {
	txRef := r.PathValue("ref")
	if txRef == "" {
		writeError(w, http.StatusBadRequest, "transaction_ref is required")
		return
	}

	var req enqueueRetryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
		writeError(w, http.StatusBadRequest, "invalid JSON body: "+err.Error())
		return
	}

	// Only MISMATCHED transactions can be enqueued. (An EXHAUSTED retry queue
	// entry still has recon_state = MISMATCHED, so the check covers both cases.)
	state, err := db.GetReconState(r.Context(), h.db, txRef)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			writeError(w, http.StatusNotFound, "transaction not found")
			return
		}
		h.log.Error("GetReconState failed", zap.String("ref", txRef), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}
	if state.Status != "MISMATCHED" {
		writeError(w, http.StatusConflict,
			"transaction is "+state.Status+"; only MISMATCHED transactions can be enqueued for retry")
		return
	}

	maxAttempts := req.MaxAttempts
	if maxAttempts <= 0 {
		maxAttempts = h.cfg.Retry.MaxAttempts
	}
	requestedBy := req.RequestedBy
	if requestedBy == "" {
		requestedBy = "api"
	}

	if err := db.EnqueueRetry(r.Context(), h.db, txRef, maxAttempts, requestedBy); err != nil {
		h.log.Error("EnqueueRetry failed", zap.String("ref", txRef), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "failed to enqueue retry")
		return
	}

	_ = db.InsertAuditLog(r.Context(), h.db,
		txRef, "RETRY_ENQUEUED",
		state.Status, state.Status,
		map[string]string{
			"requested_by": requestedBy,
			"max_attempts": strconv.Itoa(maxAttempts),
		},
	)

	h.log.Info("transaction enqueued for retry via HTTP API",
		zap.String("ref", txRef),
		zap.String("requested_by", requestedBy),
		zap.Int("max_attempts", maxAttempts),
	)

	writeJSON(w, http.StatusOK, enqueueRetryResponse{
		TransactionRef: txRef,
		MaxAttempts:    maxAttempts,
		Message:        "transaction enqueued for retry",
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/resolve/audit/{ref}
// ─────────────────────────────────────────────────────────────────────────────

type auditEntry struct {
	ID             int64   `json:"id"`
	TransactionRef string  `json:"transaction_ref"`
	EventType      string  `json:"event_type"`
	OldStatus      *string `json:"old_status,omitempty"`
	NewStatus      *string `json:"new_status,omitempty"`
	Detail         string  `json:"detail"`
	CreatedAt      string  `json:"created_at"`
}

type auditTrailResponse struct {
	TransactionRef string       `json:"transaction_ref"`
	Entries        []auditEntry `json:"entries"`
	Count          int          `json:"count"`
}

// getAuditTrail handles GET /v1/resolve/audit/{ref}.
// Returns the complete audit trail for a transaction in chronological order.
func (h *Handler) getAuditTrail(w http.ResponseWriter, r *http.Request) {
	txRef := r.PathValue("ref")
	if txRef == "" {
		writeError(w, http.StatusBadRequest, "transaction_ref is required")
		return
	}

	rows, err := db.GetAuditTrail(r.Context(), h.db, txRef)
	if err != nil {
		h.log.Error("GetAuditTrail failed", zap.String("ref", txRef), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	entries := make([]auditEntry, 0, len(rows))
	for _, row := range rows {
		entries = append(entries, auditEntry{
			ID:             row.ID,
			TransactionRef: row.TransactionRef,
			EventType:      row.EventType,
			OldStatus:      row.OldStatus,
			NewStatus:      row.NewStatus,
			Detail:         string(row.Details),
			CreatedAt:      row.CreatedAt.UTC().Format(time.RFC3339),
		})
	}

	writeJSON(w, http.StatusOK, auditTrailResponse{
		TransactionRef: txRef,
		Entries:        entries,
		Count:          len(entries),
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/resolve/retry-queue
// ─────────────────────────────────────────────────────────────────────────────

type retryQueueEntry struct {
	TransactionRef  string  `json:"transaction_ref"`
	AttemptCount    int     `json:"attempt_count"`
	MaxAttempts     int     `json:"max_attempts"`
	LastAttemptedAt *string `json:"last_attempted_at,omitempty"`
	NextRetryAt     string  `json:"next_retry_at"`
	Status          string  `json:"status"`
	RequestedBy     string  `json:"requested_by,omitempty"`
	CreatedAt       string  `json:"created_at"`
}

type retryQueueResponse struct {
	Entries       []retryQueueEntry `json:"entries"`
	Count         int               `json:"count"`
	NextPageToken string            `json:"next_page_token,omitempty"`
}

// getRetryQueue handles GET /v1/resolve/retry-queue.
//
// Query parameters:
//   - page_size   int    (default 50, max 200)
//   - page_token  string (transaction_ref cursor from previous page)
//   - status      string (PENDING | EXHAUSTED | RESOLVED)
func (h *Handler) getRetryQueue(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pageSize, _ := strconv.Atoi(q.Get("page_size"))
	pageToken := q.Get("page_token")
	statusFilter := q.Get("status")

	rows, err := db.GetRetryQueue(r.Context(), h.db, pageSize, pageToken, statusFilter)
	if err != nil {
		h.log.Error("GetRetryQueue failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	entries := make([]retryQueueEntry, 0, len(rows))
	for _, row := range rows {
		e := retryQueueEntry{
			TransactionRef: row.TransactionRef,
			AttemptCount:   row.AttemptCount,
			MaxAttempts:    row.MaxAttempts,
			NextRetryAt:    row.NextRetryAt.UTC().Format(time.RFC3339),
			Status:         row.Status,
			RequestedBy:    row.RequestedBy,
			CreatedAt:      row.CreatedAt.UTC().Format(time.RFC3339),
		}
		if row.LastAttemptedAt != nil {
			s := row.LastAttemptedAt.UTC().Format(time.RFC3339)
			e.LastAttemptedAt = &s
		}
		entries = append(entries, e)
	}

	// Cursor for next page is the last entry's transaction_ref.
	var nextToken string
	if len(entries) > 0 {
		nextToken = entries[len(entries)-1].TransactionRef
	}

	writeJSON(w, http.StatusOK, retryQueueResponse{
		Entries:       entries,
		Count:         len(entries),
		NextPageToken: nextToken,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /v1/resolve/mismatches
// ─────────────────────────────────────────────────────────────────────────────

type mismatchEntry struct {
	TransactionRef string        `json:"transaction_ref"`
	LastUpdated    string        `json:"last_updated"`
	Details        []matchDetail `json:"details"`
}

type matchDetail struct {
	SystemName       string `json:"system_name"`
	DiscrepancyFound bool   `json:"discrepancy_found"`
}

type listMismatchesResponse struct {
	Entries       []mismatchEntry `json:"entries"`
	Count         int             `json:"count"`
	NextPageToken string          `json:"next_page_token,omitempty"`
}

// listMismatches handles GET /v1/resolve/mismatches.
//
// Query parameters:
//   - page_size   int    (default 20, max 100)
//   - page_token  string (transaction_ref cursor from previous page)
//   - source      string (filter by source system name)
func (h *Handler) listMismatches(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	rawSize := q.Get("page_size")
	pageSize64, _ := strconv.ParseInt(rawSize, 10, 32)
	pageToken := q.Get("page_token")
	sourceFilter := q.Get("source")

	rows, err := db.ListMismatched(r.Context(), h.db, int32(pageSize64), pageToken, sourceFilter)
	if err != nil {
		h.log.Error("ListMismatched failed", zap.Error(err))
		writeError(w, http.StatusInternalServerError, "database error")
		return
	}

	entries := make([]mismatchEntry, 0, len(rows))
	for _, row := range rows {
		details := make([]matchDetail, 0, len(row.Details))
		for _, d := range row.Details {
			details = append(details, matchDetail{
				SystemName:       d.SystemName,
				DiscrepancyFound: d.DiscrepancyFound,
			})
		}
		entries = append(entries, mismatchEntry{
			TransactionRef: row.TransactionRef,
			LastUpdated:    row.LastUpdated.UTC().Format(time.RFC3339),
			Details:        details,
		})
	}

	var nextToken string
	if len(entries) > 0 {
		nextToken = entries[len(entries)-1].TransactionRef
	}

	writeJSON(w, http.StatusOK, listMismatchesResponse{
		Entries:       entries,
		Count:         len(entries),
		NextPageToken: nextToken,
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// GET /health
// ─────────────────────────────────────────────────────────────────────────────

// health handles GET /health.
func (h *Handler) health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "reconx-resolution",
	})
}

// ─────────────────────────────────────────────────────────────────────────────
// Middleware and helpers
// ─────────────────────────────────────────────────────────────────────────────

// statusRecorder wraps http.ResponseWriter to capture the written status code
// for Prometheus metrics.
type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

// wrap returns a HandlerFunc that records HTTPRequestsTotal and HTTPRequestDuration
// for the given method and path label.
func (h *Handler) wrap(method, path string, fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		fn(rec, r)
		metrics.HTTPRequestsTotal.WithLabelValues(method, path, strconv.Itoa(rec.status)).Inc()
		metrics.HTTPRequestDuration.WithLabelValues(method, path).Observe(time.Since(start).Seconds())
	}
}

// writeJSON encodes v as JSON and writes it with the given HTTP status code.
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// errorResponse is the standard error envelope returned by all REST endpoints.
type errorResponse struct {
	Error string `json:"error"`
}

// writeError writes a JSON error response with the given HTTP status code.
func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, errorResponse{Error: msg})
}
