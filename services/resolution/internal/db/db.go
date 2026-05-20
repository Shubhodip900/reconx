// Package db manages PostgreSQL access for the Resolution Service.
//
// Schema owned by this service:
//   - resolution_records      — one row per resolved transaction (manual or auto)
//   - resolution_retry_queue  — tracks automatic retry state per transaction
//
// Tables it reads (owned by other services):
//   - recon_state             — reads status, last_updated (owned by engine)
//   - recon_match_details     — reads system_name, discrepancy_found (owned by engine)
//   - recon_audit_log         — appends audit entries (shared write convention)
//   - ingestion_records       — reads source timing for resolver strategies
package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// RunMigrations ensures all resolution-owned tables exist.
// Idempotent — safe to call on every startup.
func RunMigrations(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, migrationSQL)
	return err
}

const migrationSQL = `
-- ── Resolution records ────────────────────────────────────────────────────────
-- One row per resolved transaction. On re-resolution (idempotency) the row is
-- updated via ON CONFLICT DO UPDATE so the latest decision always wins.
CREATE TABLE IF NOT EXISTS resolution_records (
    id                BIGSERIAL   PRIMARY KEY,
    transaction_ref   TEXT        NOT NULL,
    resolution_type   TEXT        NOT NULL DEFAULT 'MANUAL', -- MANUAL | AUTO
    chosen_source     TEXT        NOT NULL,
    resolution_reason TEXT        NOT NULL,
    resolver_id       TEXT        NOT NULL,
    strategy          TEXT,                                  -- auto-resolve strategy used, if any
    resolved_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT resolution_records_transaction_ref_unique UNIQUE (transaction_ref)
);

CREATE INDEX IF NOT EXISTS idx_resolution_records_transaction_ref
    ON resolution_records (transaction_ref);

CREATE INDEX IF NOT EXISTS idx_resolution_records_resolver_id
    ON resolution_records (resolver_id);

CREATE INDEX IF NOT EXISTS idx_resolution_records_resolved_at
    ON resolution_records (resolved_at DESC);

-- ── Retry queue ───────────────────────────────────────────────────────────────
-- Tracks automatic retry attempts for MISMATCHED transactions.
-- The retry worker polls this table for rows where next_retry_at <= NOW().
CREATE TABLE IF NOT EXISTS resolution_retry_queue (
    id                BIGSERIAL   PRIMARY KEY,
    transaction_ref   TEXT        NOT NULL,
    attempt_count     INT         NOT NULL DEFAULT 0,
    max_attempts      INT         NOT NULL DEFAULT 5,
    last_attempted_at TIMESTAMPTZ,
    next_retry_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- PENDING: waiting for next retry
    -- EXHAUSTED: max_attempts reached, needs manual intervention or auto-resolve
    -- RESOLVED: engine matched or resolution service resolved it
    status            TEXT        NOT NULL DEFAULT 'PENDING',
    requested_by      TEXT,                  -- who enqueued this retry
    created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT resolution_retry_queue_transaction_ref_unique UNIQUE (transaction_ref)
);

CREATE INDEX IF NOT EXISTS idx_retry_queue_next_retry
    ON resolution_retry_queue (next_retry_at)
    WHERE status = 'PENDING';

CREATE INDEX IF NOT EXISTS idx_retry_queue_status
    ON resolution_retry_queue (status);
`

// ─────────────────────────────────────────────────────────────────────────────
// Shared types
// ─────────────────────────────────────────────────────────────────────────────

// ReconState is a minimal projection of the recon_state row needed here.
type ReconState struct {
	TransactionRef string
	Status         string
	LastUpdated    time.Time
}

// MatchDetail is a minimal projection of recon_match_details for streaming.
type MatchDetail struct {
	SystemName       string
	DiscrepancyFound bool
}

// MismatchedRow is one row returned by ListMismatched.
type MismatchedRow struct {
	TransactionRef string
	LastUpdated    time.Time
	Details        []MatchDetail
}

// RetryQueueRow is one row from resolution_retry_queue.
type RetryQueueRow struct {
	TransactionRef  string
	AttemptCount    int
	MaxAttempts     int
	LastAttemptedAt *time.Time
	NextRetryAt     time.Time
	Status          string
	RequestedBy     string
	CreatedAt       time.Time
}

// AuditLogRow is one row from recon_audit_log.
type AuditLogRow struct {
	ID             int64
	TransactionRef string
	EventType      string
	OldStatus      *string
	NewStatus      *string
	Details        []byte
	CreatedAt      time.Time
}

// ─────────────────────────────────────────────────────────────────────────────
// recon_state queries (read-only from resolution service perspective)
// ─────────────────────────────────────────────────────────────────────────────

// GetReconState fetches the current state for a transaction ref.
// Returns sql.ErrNoRows if not found.
func GetReconState(ctx context.Context, db *sql.DB, txRef string) (*ReconState, error) {
	row := db.QueryRowContext(ctx,
		`SELECT transaction_ref, status, last_updated
		   FROM recon_state
		  WHERE transaction_ref = $1`,
		txRef,
	)
	s := &ReconState{}
	if err := row.Scan(&s.TransactionRef, &s.Status, &s.LastUpdated); err != nil {
		return nil, err
	}
	return s, nil
}

// UpdateReconStateToResolved marks the transaction RESOLVED in the engine's table.
func UpdateReconStateToResolved(ctx context.Context, db *sql.DB, txRef string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE recon_state
		   SET status = 'RESOLVED', last_updated = NOW()
		 WHERE transaction_ref = $1`,
		txRef,
	)
	return err
}

// ─────────────────────────────────────────────────────────────────────────────
// resolution_records queries
// ─────────────────────────────────────────────────────────────────────────────

// InsertResolutionRecord writes the resolution decision.
// Uses ON CONFLICT DO UPDATE so re-resolving the same ref is idempotent.
func InsertResolutionRecord(
	ctx context.Context,
	db *sql.DB,
	txRef, resolutionType, chosenSource, reason, resolverID, strategy string,
) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO resolution_records
			(transaction_ref, resolution_type, chosen_source, resolution_reason, resolver_id, strategy, resolved_at)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (transaction_ref) DO UPDATE
		   SET resolution_type   = EXCLUDED.resolution_type,
		       chosen_source     = EXCLUDED.chosen_source,
		       resolution_reason = EXCLUDED.resolution_reason,
		       resolver_id       = EXCLUDED.resolver_id,
		       strategy          = EXCLUDED.strategy,
		       resolved_at       = NOW()`,
		txRef, resolutionType, chosenSource, reason, resolverID, strategy,
	)
	return err
}

// ─────────────────────────────────────────────────────────────────────────────
// recon_audit_log queries
// ─────────────────────────────────────────────────────────────────────────────

// InsertAuditLog appends an entry to the shared recon_audit_log table.
func InsertAuditLog(
	ctx context.Context,
	db *sql.DB,
	txRef, eventType, oldStatus, newStatus string,
	detail map[string]string,
) error {
	detailJSON, err := json.Marshal(detail)
	if err != nil {
		return fmt.Errorf("marshal audit detail: %w", err)
	}
	_, err = db.ExecContext(ctx, `
		INSERT INTO recon_audit_log
			(transaction_ref, event_type, old_status, new_status, detail, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())`,
		txRef, eventType, oldStatus, newStatus, detailJSON,
	)
	return err
}

// InsertAuditLogJSON appends an entry with a raw JSON detail blob.
func InsertAuditLogJSON(
	ctx context.Context,
	db *sql.DB,
	txRef, eventType string,
	oldStatus, newStatus *string,
	detailJSON []byte,
) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO recon_audit_log
			(transaction_ref, event_type, old_status, new_status, detail, created_at)
		VALUES ($1, $2, $3, $4, $5, NOW())`,
		txRef, eventType, oldStatus, newStatus, detailJSON,
	)
	return err
}

// GetAuditTrail retrieves the full audit log for a transaction in chronological order.
func GetAuditTrail(ctx context.Context, db *sql.DB, txRef string) ([]AuditLogRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT id, transaction_ref, event_type, old_status, new_status, detail, created_at
		  FROM recon_audit_log
		 WHERE transaction_ref = $1
		 ORDER BY created_at ASC`,
		txRef,
	)
	if err != nil {
		return nil, fmt.Errorf("get audit trail: %w", err)
	}
	defer rows.Close()

	var result []AuditLogRow
	for rows.Next() {
		var r AuditLogRow
		if err := rows.Scan(
			&r.ID, &r.TransactionRef, &r.EventType,
			&r.OldStatus, &r.NewStatus, &r.Details, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// ─────────────────────────────────────────────────────────────────────────────
// recon_match_details + recon_state — list mismatches (gRPC streaming)
// ─────────────────────────────────────────────────────────────────────────────

// ListMismatched returns MISMATCHED transactions with cursor-based pagination.
//
//   - pageSize:    max rows to return (clamped to 100)
//   - pageToken:   transaction_ref of the last item seen (empty for first page)
//   - sourceFilter: if non-empty, only include rows where at least one
//     recon_match_details.system_name matches
func ListMismatched(
	ctx context.Context,
	db *sql.DB,
	pageSize int32,
	pageToken string,
	sourceFilter string,
) ([]MismatchedRow, error) {
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 20
	}

	var (
		rows *sql.Rows
		err  error
	)

	switch {
	case pageToken == "" && sourceFilter == "":
		rows, err = db.QueryContext(ctx, `
			SELECT transaction_ref, last_updated
			  FROM recon_state
			 WHERE status = 'MISMATCHED'
			 ORDER BY transaction_ref ASC
			 LIMIT $1`,
			pageSize,
		)
	case pageToken != "" && sourceFilter == "":
		rows, err = db.QueryContext(ctx, `
			SELECT transaction_ref, last_updated
			  FROM recon_state
			 WHERE status = 'MISMATCHED'
			   AND transaction_ref > $2
			 ORDER BY transaction_ref ASC
			 LIMIT $1`,
			pageSize, pageToken,
		)
	case pageToken == "" && sourceFilter != "":
		rows, err = db.QueryContext(ctx, `
			SELECT DISTINCT rs.transaction_ref, rs.last_updated
			  FROM recon_state rs
			  JOIN recon_match_details rmd
			    ON rmd.transaction_ref = rs.transaction_ref
			   AND rmd.system_name = $2
			 WHERE rs.status = 'MISMATCHED'
			 ORDER BY rs.transaction_ref ASC
			 LIMIT $1`,
			pageSize, sourceFilter,
		)
	default:
		rows, err = db.QueryContext(ctx, `
			SELECT DISTINCT rs.transaction_ref, rs.last_updated
			  FROM recon_state rs
			  JOIN recon_match_details rmd
			    ON rmd.transaction_ref = rs.transaction_ref
			   AND rmd.system_name = $3
			 WHERE rs.status = 'MISMATCHED'
			   AND rs.transaction_ref > $2
			 ORDER BY rs.transaction_ref ASC
			 LIMIT $1`,
			pageSize, pageToken, sourceFilter,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("list mismatched: %w", err)
	}
	defer rows.Close()

	var result []MismatchedRow
	for rows.Next() {
		var r MismatchedRow
		if err := rows.Scan(&r.TransactionRef, &r.LastUpdated); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if len(result) == 0 {
		return result, nil
	}

	// Fetch match details for all returned refs in one query.
	refs := make([]any, len(result))
	for i, r := range result {
		refs[i] = r.TransactionRef
	}
	placeholders := ""
	for i := range refs {
		if i > 0 {
			placeholders += ","
		}
		placeholders += fmt.Sprintf("$%d", i+1)
	}

	detailRows, err := db.QueryContext(ctx,
		fmt.Sprintf(`SELECT transaction_ref, system_name, discrepancy_found
			           FROM recon_match_details
			          WHERE transaction_ref IN (%s)
			          ORDER BY transaction_ref`, placeholders),
		refs...,
	)
	if err != nil {
		return nil, fmt.Errorf("fetch match details: %w", err)
	}
	defer detailRows.Close()

	idx := make(map[string]int, len(result))
	for i, r := range result {
		idx[r.TransactionRef] = i
	}

	for detailRows.Next() {
		var txRef, sysName string
		var discrepancy bool
		if err := detailRows.Scan(&txRef, &sysName, &discrepancy); err != nil {
			return nil, err
		}
		if i, ok := idx[txRef]; ok {
			result[i].Details = append(result[i].Details, MatchDetail{
				SystemName:       sysName,
				DiscrepancyFound: discrepancy,
			})
		}
	}
	return result, detailRows.Err()
}

// ─────────────────────────────────────────────────────────────────────────────
// resolution_retry_queue queries
// ─────────────────────────────────────────────────────────────────────────────

// EnqueueRetry inserts or resets a retry queue entry for a transaction.
// If an EXHAUSTED entry exists, it is reset to PENDING with attempt_count = 0.
// If a PENDING entry already exists, it is left untouched (returns nil).
func EnqueueRetry(
	ctx context.Context,
	db *sql.DB,
	txRef string,
	maxAttempts int,
	requestedBy string,
) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO resolution_retry_queue
			(transaction_ref, attempt_count, max_attempts, next_retry_at, status, requested_by, created_at, updated_at)
		VALUES ($1, 0, $2, NOW(), 'PENDING', $3, NOW(), NOW())
		ON CONFLICT (transaction_ref) DO UPDATE
		   SET max_attempts  = EXCLUDED.max_attempts,
		       next_retry_at = NOW(),
		       requested_by  = EXCLUDED.requested_by,
		       status        = CASE
		           WHEN resolution_retry_queue.status = 'EXHAUSTED' THEN 'PENDING'
		           ELSE resolution_retry_queue.status
		       END,
		       attempt_count = CASE
		           WHEN resolution_retry_queue.status = 'EXHAUSTED' THEN 0
		           ELSE resolution_retry_queue.attempt_count
		       END,
		       updated_at    = NOW()`,
		txRef, maxAttempts, requestedBy,
	)
	return err
}

// GetPendingRetries returns up to `limit` retry queue entries that are due.
// "Due" means: status = 'PENDING' AND next_retry_at <= NOW().
func GetPendingRetries(ctx context.Context, db *sql.DB, limit int) ([]RetryQueueRow, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT transaction_ref, attempt_count, max_attempts,
		       last_attempted_at, next_retry_at, status, COALESCE(requested_by,''), created_at
		  FROM resolution_retry_queue
		 WHERE status = 'PENDING'
		   AND next_retry_at <= NOW()
		 ORDER BY next_retry_at ASC
		 LIMIT $1`,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("get pending retries: %w", err)
	}
	defer rows.Close()

	var result []RetryQueueRow
	for rows.Next() {
		var r RetryQueueRow
		if err := rows.Scan(
			&r.TransactionRef, &r.AttemptCount, &r.MaxAttempts,
			&r.LastAttemptedAt, &r.NextRetryAt, &r.Status, &r.RequestedBy, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// IncrementRetryAttempt updates the attempt counter and schedules the next retry
// using exponential backoff: next = now + min(base * 2^attempt, max).
func IncrementRetryAttempt(
	ctx context.Context,
	db *sql.DB,
	txRef string,
	nextRetryAt time.Time,
) error {
	_, err := db.ExecContext(ctx, `
		UPDATE resolution_retry_queue
		   SET attempt_count     = attempt_count + 1,
		       last_attempted_at = NOW(),
		       next_retry_at     = $2,
		       updated_at        = NOW()
		 WHERE transaction_ref = $1`,
		txRef, nextRetryAt,
	)
	return err
}

// MarkRetryExhausted marks a retry queue entry as EXHAUSTED (max retries reached).
func MarkRetryExhausted(ctx context.Context, db *sql.DB, txRef string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE resolution_retry_queue
		   SET status     = 'EXHAUSTED',
		       updated_at = NOW()
		 WHERE transaction_ref = $1`,
		txRef,
	)
	return err
}

// MarkRetryResolved marks a retry queue entry as RESOLVED (engine matched it).
func MarkRetryResolved(ctx context.Context, db *sql.DB, txRef string) error {
	_, err := db.ExecContext(ctx, `
		UPDATE resolution_retry_queue
		   SET status     = 'RESOLVED',
		       updated_at = NOW()
		 WHERE transaction_ref = $1`,
		txRef,
	)
	return err
}

// GetRetryQueue returns paginated retry queue entries.
func GetRetryQueue(
	ctx context.Context,
	db *sql.DB,
	pageSize int,
	pageToken string,
	statusFilter string,
) ([]RetryQueueRow, error) {
	if pageSize <= 0 || pageSize > 200 {
		pageSize = 50
	}

	var (
		rows *sql.Rows
		err  error
	)

	switch {
	case pageToken == "" && statusFilter == "":
		rows, err = db.QueryContext(ctx, `
			SELECT transaction_ref, attempt_count, max_attempts,
			       last_attempted_at, next_retry_at, status, COALESCE(requested_by,''), created_at
			  FROM resolution_retry_queue
			 ORDER BY next_retry_at ASC
			 LIMIT $1`,
			pageSize,
		)
	case pageToken != "" && statusFilter == "":
		rows, err = db.QueryContext(ctx, `
			SELECT transaction_ref, attempt_count, max_attempts,
			       last_attempted_at, next_retry_at, status, COALESCE(requested_by,''), created_at
			  FROM resolution_retry_queue
			 WHERE transaction_ref > $2
			 ORDER BY next_retry_at ASC
			 LIMIT $1`,
			pageSize, pageToken,
		)
	case pageToken == "" && statusFilter != "":
		rows, err = db.QueryContext(ctx, `
			SELECT transaction_ref, attempt_count, max_attempts,
			       last_attempted_at, next_retry_at, status, COALESCE(requested_by,''), created_at
			  FROM resolution_retry_queue
			 WHERE status = $2
			 ORDER BY next_retry_at ASC
			 LIMIT $1`,
			pageSize, statusFilter,
		)
	default:
		rows, err = db.QueryContext(ctx, `
			SELECT transaction_ref, attempt_count, max_attempts,
			       last_attempted_at, next_retry_at, status, COALESCE(requested_by,''), created_at
			  FROM resolution_retry_queue
			 WHERE status = $3
			   AND transaction_ref > $2
			 ORDER BY next_retry_at ASC
			 LIMIT $1`,
			pageSize, pageToken, statusFilter,
		)
	}
	if err != nil {
		return nil, fmt.Errorf("get retry queue: %w", err)
	}
	defer rows.Close()

	var result []RetryQueueRow
	for rows.Next() {
		var r RetryQueueRow
		if err := rows.Scan(
			&r.TransactionRef, &r.AttemptCount, &r.MaxAttempts,
			&r.LastAttemptedAt, &r.NextRetryAt, &r.Status, &r.RequestedBy, &r.CreatedAt,
		); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

// RetryQueueStats returns aggregate counts by status.
func RetryQueueStats(ctx context.Context, db *sql.DB) (pending, exhausted, resolved int, err error) {
	row := db.QueryRowContext(ctx, `
		SELECT
		    COUNT(*) FILTER (WHERE status = 'PENDING')   AS pending,
		    COUNT(*) FILTER (WHERE status = 'EXHAUSTED') AS exhausted,
		    COUNT(*) FILTER (WHERE status = 'RESOLVED')  AS resolved
		  FROM resolution_retry_queue`)
	err = row.Scan(&pending, &exhausted, &resolved)
	return
}

// ─────────────────────────────────────────────────────────────────────────────
// Resolver helper queries (read from engine tables)
// ─────────────────────────────────────────────────────────────────────────────

// GetSourcesByLatest returns the source_system name of the record with the
// most recent server_received_at for the given transaction_ref.
func GetSourcesByLatest(ctx context.Context, db *sql.DB, txRef string) (string, error) {
	var source string
	err := db.QueryRowContext(ctx, `
		SELECT source_system
		  FROM ingestion_records
		 WHERE transaction_ref = $1
		 ORDER BY server_received_at DESC
		 LIMIT 1`,
		txRef,
	).Scan(&source)
	return source, err
}

// GetSourcesByFirst returns the source_system of the record submitted first.
func GetSourcesByFirst(ctx context.Context, db *sql.DB, txRef string) (string, error) {
	var source string
	err := db.QueryRowContext(ctx, `
		SELECT source_system
		  FROM ingestion_records
		 WHERE transaction_ref = $1
		 ORDER BY server_received_at ASC
		 LIMIT 1`,
		txRef,
	).Scan(&source)
	return source, err
}

// GetSourceByHighestAmount returns the source_system reporting the highest amount.
func GetSourceByHighestAmount(ctx context.Context, db *sql.DB, txRef string) (string, error) {
	var source string
	err := db.QueryRowContext(ctx, `
		SELECT source_system
		  FROM recon_match_details
		 WHERE transaction_ref = $1
		   AND amount IS NOT NULL
		 ORDER BY CAST(amount AS NUMERIC) DESC
		 LIMIT 1`,
		txRef,
	).Scan(&source)
	return source, err
}

// GetSourceByLowestAmount returns the source_system reporting the lowest amount.
func GetSourceByLowestAmount(ctx context.Context, db *sql.DB, txRef string) (string, error) {
	var source string
	err := db.QueryRowContext(ctx, `
		SELECT source_system
		  FROM recon_match_details
		 WHERE transaction_ref = $1
		   AND amount IS NOT NULL
		 ORDER BY CAST(amount AS NUMERIC) ASC
		 LIMIT 1`,
		txRef,
	).Scan(&source)
	return source, err
}

// GetPresentSources returns all source_system names that have submitted records
// for a transaction_ref, in submission order.
func GetPresentSources(ctx context.Context, db *sql.DB, txRef string) ([]string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT DISTINCT source_system
		  FROM ingestion_records
		 WHERE transaction_ref = $1`,
		txRef,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		sources = append(sources, s)
	}
	return sources, rows.Err()
}
