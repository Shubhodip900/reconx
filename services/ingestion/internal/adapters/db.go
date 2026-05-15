// DB Poll Adapter — queries a foreign/source database on a schedule.
// Used to integrate with systems that do not publish events but expose data
// via SQL (legacy ERP systems, banking core systems, etc.).
//
// The adapter queries for records modified since the last poll using a
// "high-watermark" cursor (last_seen_id or updated_at timestamp).
// This pattern is identical to the CDC (Change Data Capture) polling approach
// used by Debezium when binlog is unavailable.
package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/reconx/services/ingestion/internal/pipeline"
)

// DBPollConfig configures a database polling adapter.
type DBPollConfig struct {
	// ID uniquely identifies this adapter instance.
	ID string

	// SourceSystem labels all records from this database.
	SourceSystem string

	// DSN is the database connection string.
	DSN string

	// Driver is the database driver name (e.g., "postgres", "mysql").
	Driver string

	// Query is the SQL to execute on each poll cycle.
	// It must accept a single argument: the high-watermark value (last polled timestamp).
	// Example: "SELECT id, ref, amount, currency, updated_at FROM transactions WHERE updated_at > $1 ORDER BY updated_at ASC"
	Query string

	// PollInterval determines how frequently the query is executed.
	PollInterval time.Duration

	// WatermarkColumn is the column used for cursor-based polling.
	// Defaults to "updated_at".
	WatermarkColumn string
}

// DBPoller polls a SQL database for new/updated records.
type DBPoller struct {
	cfg       DBPollConfig
	db        *sql.DB
	watermark time.Time
	log       *zap.Logger
}

// NewDBPoller creates a DBPoller and opens a connection to the source database.
func NewDBPoller(cfg DBPollConfig, log *zap.Logger) (*DBPoller, error) {
	if cfg.WatermarkColumn == "" {
		cfg.WatermarkColumn = "updated_at"
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 30 * time.Second
	}

	db, err := sql.Open(cfg.Driver, cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("db poll: open %s: %w", cfg.Driver, err)
	}
	db.SetMaxOpenConns(5)
	db.SetMaxIdleConns(2)

	return &DBPoller{
		cfg:       cfg,
		db:        db,
		watermark: time.Now().Add(-24 * time.Hour), // start 24h back on first poll
		log:       log.With(zap.String("adapter", cfg.ID)),
	}, nil
}

func (d *DBPoller) ID() string                     { return d.cfg.ID }
func (d *DBPoller) AdapterType() pipeline.AdapterType { return pipeline.AdapterDB }

// Start polls the database on PollInterval until ctx is cancelled.
func (d *DBPoller) Start(ctx context.Context, out chan<- *pipeline.NormalizedRecord) error {
	defer d.db.Close()
	ticker := time.NewTicker(d.cfg.PollInterval)
	defer ticker.Stop()

	d.log.Info("DB poller started", zap.Duration("interval", d.cfg.PollInterval))

	for {
		select {
		case <-ctx.Done():
			d.log.Info("DB poller stopped")
			return ctx.Err()
		case <-ticker.C:
			newWatermark, err := d.poll(ctx, out)
			if err != nil {
				d.log.Warn("DB poll cycle failed", zap.Error(err))
				continue
			}
			if !newWatermark.IsZero() {
				d.watermark = newWatermark
			}
		}
	}
}

// poll executes the query, emits records, and returns the new high-watermark.
func (d *DBPoller) poll(ctx context.Context, out chan<- *pipeline.NormalizedRecord) (time.Time, error) {
	rows, err := d.db.QueryContext(ctx, d.cfg.Query, d.watermark)
	if err != nil {
		return time.Time{}, fmt.Errorf("db query: %w", err)
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return time.Time{}, err
	}

	var latestWatermark time.Time
	for rows.Next() {
		// Scan all columns into interface{} values.
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			d.log.Warn("scan row failed", zap.Error(err))
			continue
		}

		// Build a map for JSON encoding.
		rowMap := make(map[string]interface{}, len(cols))
		for i, col := range cols {
			rowMap[col] = vals[i]
		}

		// Extract watermark column value to advance cursor.
		if wm, ok := rowMap[d.cfg.WatermarkColumn]; ok {
			if t, ok := wm.(time.Time); ok && t.After(latestWatermark) {
				latestWatermark = t
			}
		}

		// Extract known fields for the record skeleton.
		txRef := stringVal(rowMap, "transaction_ref", "ref", "reference_id", "order_id", "invoice_id")
		idempKey := stringVal(rowMap, "idempotency_key", "id")
		if idempKey == "" {
			idempKey = uuid.New().String()
		}

		payload, _ := json.Marshal(rowMap)

		rec := &pipeline.NormalizedRecord{
			IdempotencyKey: idempKey,
			TransactionRef: txRef,
			SourceSystem:   d.cfg.SourceSystem,
			AdapterType:    pipeline.AdapterDB,
			RawPayload:     payload,
			PayloadSchema:  "db_row.v1",
		}

		select {
		case out <- rec:
		case <-ctx.Done():
			return latestWatermark, ctx.Err()
		}
	}
	return latestWatermark, rows.Err()
}

// stringVal looks for the first matching key in a map and returns its string value.
func stringVal(m map[string]interface{}, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch val := v.(type) {
			case string:
				return val
			case []byte:
				return string(val)
			default:
				if val != nil {
					return fmt.Sprintf("%v", val)
				}
			}
		}
	}
	return ""
}
