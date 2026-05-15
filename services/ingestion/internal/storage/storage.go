// Package storage persists normalized ingestion records to PostgreSQL.
// It also maintains the records table that the Reconciliation Engine reads.
package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/lib/pq"
)

// Record is the persisted form of a normalized ingestion record.
type Record struct {
	InternalID       string
	IdempotencyKey   string
	TransactionRef   string
	SourceSystem     string
	AdapterType      string
	Amount           string // stored as text to preserve precision
	Currency         string
	RecordTimestamp  time.Time
	ServerReceivedAt time.Time
	RawPayload       []byte
	PayloadSchema    string
	Tags             map[string]string
	TraceID          string
	Status           string // PENDING | MATCHED | MISMATCHED | RESOLVED
}

// Store handles PostgreSQL operations for ingested records.
type Store struct {
	db *sql.DB
}

// New creates a Store and ensures required tables exist.
func New(db *sql.DB) (*Store, error) {
	s := &Store{db: db}
	if err := s.migrate(context.Background()); err != nil {
		return nil, fmt.Errorf("storage migrate: %w", err)
	}
	return s, nil
}

func (s *Store) migrate(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS ingestion_records (
    internal_id         TEXT        PRIMARY KEY,
    idempotency_key     TEXT        NOT NULL UNIQUE,
    transaction_ref     TEXT        NOT NULL,
    source_system       TEXT        NOT NULL,
    adapter_type        TEXT        NOT NULL DEFAULT '',
    amount              TEXT        NOT NULL DEFAULT '0',
    currency            TEXT        NOT NULL DEFAULT '',
    record_timestamp    TIMESTAMPTZ,
    server_received_at  TIMESTAMPTZ NOT NULL,
    raw_payload         BYTEA,
    payload_schema      TEXT        NOT NULL DEFAULT '',
    tags                JSONB       NOT NULL DEFAULT '{}',
    trace_id            TEXT        NOT NULL DEFAULT '',
    status              TEXT        NOT NULL DEFAULT 'PENDING',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_records_txref   ON ingestion_records(transaction_ref);
CREATE INDEX IF NOT EXISTS idx_records_source  ON ingestion_records(source_system, server_received_at);
CREATE INDEX IF NOT EXISTS idx_records_status  ON ingestion_records(status);
CREATE INDEX IF NOT EXISTS idx_records_idem    ON ingestion_records(idempotency_key);
`
	_, err := s.db.ExecContext(ctx, ddl)
	return err
}

// Insert persists a new ingestion record.
// Returns an error if the idempotency_key already exists (constraint violation).
func (s *Store) Insert(ctx context.Context, r *Record) error {
	tagsJSON, err := json.Marshal(r.Tags)
	if err != nil {
		return fmt.Errorf("storage: marshal tags: %w", err)
	}

	const q = `
INSERT INTO ingestion_records
    (internal_id, idempotency_key, transaction_ref, source_system, adapter_type,
     amount, currency, record_timestamp, server_received_at, raw_payload,
     payload_schema, tags, trace_id, status)
VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
ON CONFLICT (idempotency_key) DO NOTHING
`
	var recTS *time.Time
	if !r.RecordTimestamp.IsZero() {
		recTS = &r.RecordTimestamp
	}

	_, err = s.db.ExecContext(ctx, q,
		r.InternalID, r.IdempotencyKey, r.TransactionRef, r.SourceSystem, r.AdapterType,
		r.Amount, r.Currency, recTS, r.ServerReceivedAt, r.RawPayload,
		r.PayloadSchema, tagsJSON, r.TraceID, r.Status,
	)
	return err
}

// GetByTransactionRef fetches all records for a given transaction reference.
func (s *Store) GetByTransactionRef(ctx context.Context, txRef string) ([]*Record, error) {
	const q = `
SELECT internal_id, idempotency_key, transaction_ref, source_system, adapter_type,
       amount, currency, record_timestamp, server_received_at, raw_payload,
       payload_schema, tags, trace_id, status
FROM ingestion_records
WHERE transaction_ref = $1
ORDER BY server_received_at ASC
`
	rows, err := s.db.QueryContext(ctx, q, txRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRecords(rows)
}

// GetBySource fetches the latest records from a given source system.
func (s *Store) GetBySource(ctx context.Context, sourceSystem string, limit int) ([]*Record, error) {
	const q = `
SELECT internal_id, idempotency_key, transaction_ref, source_system, adapter_type,
       amount, currency, record_timestamp, server_received_at, raw_payload,
       payload_schema, tags, trace_id, status
FROM ingestion_records
WHERE source_system = $1
ORDER BY server_received_at DESC
LIMIT $2
`
	rows, err := s.db.QueryContext(ctx, q, sourceSystem, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanRecords(rows)
}

func scanRecords(rows *sql.Rows) ([]*Record, error) {
	var records []*Record
	for rows.Next() {
		r := &Record{}
		var tagsJSON []byte
		var recTS sql.NullTime
		if err := rows.Scan(
			&r.InternalID, &r.IdempotencyKey, &r.TransactionRef, &r.SourceSystem, &r.AdapterType,
			&r.Amount, &r.Currency, &recTS, &r.ServerReceivedAt, &r.RawPayload,
			&r.PayloadSchema, &tagsJSON, &r.TraceID, &r.Status,
		); err != nil {
			return nil, err
		}
		if recTS.Valid {
			r.RecordTimestamp = recTS.Time
		}
		if err := json.Unmarshal(tagsJSON, &r.Tags); err != nil {
			r.Tags = make(map[string]string)
		}
		records = append(records, r)
	}
	return records, rows.Err()
}
