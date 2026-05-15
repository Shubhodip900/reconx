// Package dlq (Dead Letter Queue) stores records that have exhausted all
// processing attempts. Failed records are persisted to PostgreSQL for
// manual inspection, replay, or purge by operators.
//
// DLQ table: ingestion_dlq(id, idempotency_key, source_system, payload, error, retry_count, created_at)
package dlq

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

// Entry represents a failed ingestion record stored in the DLQ.
type Entry struct {
	ID             int64
	IdempotencyKey string
	TransactionRef string
	SourceSystem   string
	AdapterType    string
	RawPayload     []byte
	ErrorStage     string
	ErrorReason    string
	ErrorMessage   string
	RetryCount     int
	CreatedAt      time.Time
	LastAttemptAt  time.Time
}

// Queue manages dead-letter entries in PostgreSQL.
type Queue struct {
	db         *sql.DB
	tableName  string
	maxRetries int
}

// New creates a Queue and ensures the DLQ table exists.
func New(db *sql.DB, tableName string, maxRetries int) (*Queue, error) {
	q := &Queue{db: db, tableName: tableName, maxRetries: maxRetries}
	if err := q.migrate(context.Background()); err != nil {
		return nil, fmt.Errorf("dlq migrate: %w", err)
	}
	return q, nil
}

func (q *Queue) migrate(ctx context.Context) error {
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    id              BIGSERIAL    PRIMARY KEY,
    idempotency_key TEXT         NOT NULL,
    transaction_ref TEXT         NOT NULL DEFAULT '',
    source_system   TEXT         NOT NULL,
    adapter_type    TEXT         NOT NULL DEFAULT '',
    raw_payload     BYTEA,
    error_stage     TEXT         NOT NULL DEFAULT '',
    error_reason    TEXT         NOT NULL DEFAULT '',
    error_message   TEXT         NOT NULL DEFAULT '',
    retry_count     INT          NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    last_attempt_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_dlq_source ON %s(source_system, created_at);
CREATE INDEX IF NOT EXISTS idx_dlq_idem   ON %s(idempotency_key);
`, q.tableName, q.tableName, q.tableName)
	_, err := q.db.ExecContext(ctx, ddl)
	return err
}

// Enqueue inserts a failed record into the DLQ.
func (q *Queue) Enqueue(ctx context.Context, e *Entry) error {
	query := fmt.Sprintf(`
INSERT INTO %s (idempotency_key, transaction_ref, source_system, adapter_type,
                raw_payload, error_stage, error_reason, error_message, retry_count)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
`, q.tableName)
	_, err := q.db.ExecContext(ctx, query,
		e.IdempotencyKey, e.TransactionRef, e.SourceSystem, e.AdapterType,
		e.RawPayload, e.ErrorStage, e.ErrorReason, e.ErrorMessage, e.RetryCount,
	)
	return err
}

// Dequeue fetches up to limit entries eligible for retry (retry_count < maxRetries).
func (q *Queue) Dequeue(ctx context.Context, limit int) ([]*Entry, error) {
	query := fmt.Sprintf(`
SELECT id, idempotency_key, transaction_ref, source_system, adapter_type,
       raw_payload, error_stage, error_reason, error_message, retry_count,
       created_at, last_attempt_at
FROM %s
WHERE retry_count < $1
ORDER BY created_at ASC
LIMIT $2
`, q.tableName)
	rows, err := q.db.QueryContext(ctx, query, q.maxRetries, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var entries []*Entry
	for rows.Next() {
		e := &Entry{}
		if err := rows.Scan(
			&e.ID, &e.IdempotencyKey, &e.TransactionRef, &e.SourceSystem, &e.AdapterType,
			&e.RawPayload, &e.ErrorStage, &e.ErrorReason, &e.ErrorMessage, &e.RetryCount,
			&e.CreatedAt, &e.LastAttemptAt,
		); err != nil {
			return nil, err
		}
		entries = append(entries, e)
	}
	return entries, rows.Err()
}

// MarkRetried increments the retry counter and updates last_attempt_at.
func (q *Queue) MarkRetried(ctx context.Context, id int64) error {
	query := fmt.Sprintf(`
UPDATE %s SET retry_count = retry_count + 1, last_attempt_at = NOW()
WHERE id = $1
`, q.tableName)
	_, err := q.db.ExecContext(ctx, query, id)
	return err
}

// Delete removes a successfully replayed entry from the DLQ.
func (q *Queue) Delete(ctx context.Context, id int64) error {
	query := fmt.Sprintf(`DELETE FROM %s WHERE id = $1`, q.tableName)
	_, err := q.db.ExecContext(ctx, query, id)
	return err
}

// Depth returns the current number of entries per source system.
func (q *Queue) Depth(ctx context.Context) (map[string]int64, error) {
	query := fmt.Sprintf(`
SELECT source_system, COUNT(*) FROM %s GROUP BY source_system
`, q.tableName)
	rows, err := q.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make(map[string]int64)
	for rows.Next() {
		var src string
		var cnt int64
		if err := rows.Scan(&src, &cnt); err != nil {
			return nil, err
		}
		result[src] = cnt
	}
	return result, rows.Err()
}
