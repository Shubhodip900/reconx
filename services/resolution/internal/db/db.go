// Package db manages PostgreSQL access for the Resolution Service.
//
// Schema owned by this service:
//   - resolution_records  — one row per resolved transaction
//
// Tables it reads (owned by other services):
//   - recon_state         — reads status, last_updated (owned by engine)
//   - recon_match_details — reads system_name, discrepancy_found (owned by engine)
//   - recon_audit_log     — appends audit entries (shared write convention)
package db

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// RunMigrations ensures the resolution_records table exists.
// Idempotent — safe to call on every startup.
func RunMigrations(ctx context.Context, db *sql.DB) error {
	_, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS resolution_records (
			id               BIGSERIAL PRIMARY KEY,
			transaction_ref  TEXT      NOT NULL,
			chosen_source    TEXT      NOT NULL,
			resolution_reason TEXT     NOT NULL,
			resolver_id      TEXT      NOT NULL,
			resolved_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

			CONSTRAINT resolution_records_transaction_ref_unique UNIQUE (transaction_ref)
		);

		CREATE INDEX IF NOT EXISTS idx_resolution_records_transaction_ref
			ON resolution_records (transaction_ref);
	`)
	return err
}

// ReconState is a minimal projection of the recon_state row needed here.
type ReconState struct {
	TransactionRef string
	Status         string
	LastUpdated    time.Time
}

// MatchDetail is a minimal projection of recon_match_details needed for streaming.
type MatchDetail struct {
	SystemName       string
	DiscrepancyFound bool
}

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

// InsertResolutionRecord writes the resolution decision.
// Uses ON CONFLICT DO UPDATE so re-resolving the same ref is idempotent.
func InsertResolutionRecord(ctx context.Context, db *sql.DB, txRef, chosenSource, reason, resolverID string) error {
	_, err := db.ExecContext(ctx, `
		INSERT INTO resolution_records
			(transaction_ref, chosen_source, resolution_reason, resolver_id, resolved_at)
		VALUES ($1, $2, $3, $4, NOW())
		ON CONFLICT (transaction_ref) DO UPDATE
		   SET chosen_source     = EXCLUDED.chosen_source,
		       resolution_reason = EXCLUDED.resolution_reason,
		       resolver_id       = EXCLUDED.resolver_id,
		       resolved_at       = NOW()`,
		txRef, chosenSource, reason, resolverID,
	)
	return err
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

// InsertAuditLog appends an entry to the shared recon_audit_log table.
func InsertAuditLog(ctx context.Context, db *sql.DB, txRef, eventType, oldStatus, newStatus string, detail map[string]string) error {
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

// MismatchedRow is one row returned by ListMismatches.
type MismatchedRow struct {
	TransactionRef string
	LastUpdated    time.Time
	Details        []MatchDetail
}

// ListMismatched returns MISMATCHED transactions with cursor-based pagination.
//
//   - pageSize:  max rows to return (clamped to 100)
//   - pageToken: transaction_ref of the last item seen (empty for first page)
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

	// Build cursor predicate.
	cursorPred := ""
	if pageToken != "" {
		cursorPred = "AND rs.transaction_ref > $3"
	}

	// When sourceFilter is active, restrict to transactions that have a matching
	// system_name in recon_match_details.
	sourceJoin := ""
	if sourceFilter != "" {
		sourceJoin = `
		  JOIN recon_match_details rmd_f
		    ON rmd_f.transaction_ref = rs.transaction_ref
		   AND rmd_f.system_name = $4`
	}

	query := fmt.Sprintf(`
		SELECT rs.transaction_ref, rs.last_updated
		  FROM recon_state rs
		  %s
		 WHERE rs.status = 'MISMATCHED'
		   %s
		 ORDER BY rs.transaction_ref ASC
		 LIMIT $1
		OFFSET 0`,
		sourceJoin, cursorPred,
	)

	args := []any{pageSize, pageSize} // $1, placeholder
	// Rebuild args correctly (we use positional $N):
	args = []any{pageSize}
	if pageToken != "" {
		args = append(args, pageToken)  // replaces $2 but we want $3 slot
		// Rewrite: $1=pageSize, $2 unused, $3=pageToken... easier to just use named approach
		// Actually let's keep it simple:
	}

	// Simpler approach: always pass all 4 args, ignore unused ones.
	// Use a fixed query with all slots present.
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
	default: // both set
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

	// Fetch match details for all returned refs in one query.
	if len(result) == 0 {
		return result, nil
	}
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

	// Index result by transaction_ref for O(1) assignment.
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
