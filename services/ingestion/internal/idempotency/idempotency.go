// Package idempotency implements the Idempotent Receiver pattern (Fowler, 2020).
// Records arriving with a previously-seen idempotency_key are de-duplicated:
// the original response is returned without re-processing the record.
//
// Storage backend: PostgreSQL (durable, auditable — required for financial data).
// The idempotency table is: ingestion_idempotency(key TEXT PK, response_json JSONB, expires_at TIMESTAMPTZ)
package idempotency

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	_ "github.com/lib/pq" // PostgreSQL driver
)

// ErrNotFound is returned when a key has no stored entry (first-time submission).
var ErrNotFound = errors.New("idempotency: key not found")

// Store persists idempotency keys and their associated responses.
type Store struct {
	db  *sql.DB
	ttl time.Duration
}

// CachedResponse is the stored response payload for a given idempotency key.
type CachedResponse struct {
	InternalID string    `json:"internal_id"`
	Success    bool      `json:"success"`
	ErrorCode  string    `json:"error_code,omitempty"`
	ErrorMsg   string    `json:"error_msg,omitempty"`
	StoredAt   time.Time `json:"stored_at"`
}

// New creates an idempotency Store connected to the given database.
// It ensures the required table exists.
func New(db *sql.DB, ttl time.Duration) (*Store, error) {
	s := &Store{db: db, ttl: ttl}
	if err := s.migrate(context.Background()); err != nil {
		return nil, fmt.Errorf("idempotency migrate: %w", err)
	}
	return s, nil
}

// migrate creates the idempotency table if it does not exist.
func (s *Store) migrate(ctx context.Context) error {
	const ddl = `
CREATE TABLE IF NOT EXISTS ingestion_idempotency (
    idempotency_key TEXT        PRIMARY KEY,
    response_json   JSONB       NOT NULL,
    source_system   TEXT        NOT NULL DEFAULT '',
    expires_at      TIMESTAMPTZ NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_idempotency_expires ON ingestion_idempotency(expires_at);
`
	_, err := s.db.ExecContext(ctx, ddl)
	return err
}

// Get returns the cached response for key, or ErrNotFound if absent or expired.
func (s *Store) Get(ctx context.Context, key string) (*CachedResponse, error) {
	const q = `
SELECT response_json FROM ingestion_idempotency
WHERE idempotency_key = $1 AND expires_at > NOW()
`
	row := s.db.QueryRowContext(ctx, q, key)
	var raw []byte
	if err := row.Scan(&raw); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("idempotency get: %w", err)
	}
	resp := &CachedResponse{}
	if err := json.Unmarshal(raw, resp); err != nil {
		return nil, fmt.Errorf("idempotency decode: %w", err)
	}
	return resp, nil
}

// Set stores a response under key with the configured TTL.
// If the key already exists (race condition), it is ignored (ON CONFLICT DO NOTHING).
func (s *Store) Set(ctx context.Context, key, sourceSystem string, resp *CachedResponse) error {
	raw, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("idempotency encode: %w", err)
	}
	const q = `
INSERT INTO ingestion_idempotency (idempotency_key, response_json, source_system, expires_at)
VALUES ($1, $2, $3, $4)
ON CONFLICT (idempotency_key) DO NOTHING
`
	expiresAt := time.Now().Add(s.ttl)
	_, err = s.db.ExecContext(ctx, q, key, raw, sourceSystem, expiresAt)
	return err
}

// Purge deletes all expired idempotency entries. Run this on a schedule.
func (s *Store) Purge(ctx context.Context) (int64, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM ingestion_idempotency WHERE expires_at <= NOW()`)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
