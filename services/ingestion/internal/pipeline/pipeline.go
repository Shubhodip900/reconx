// Package pipeline defines the canonical data model and processing pipeline
// for the ReconX ingestion service.
//
// Architecture (inspired by moov-io/fed and enterprise reconciliation patterns):
//
//	RawBytes → Parse → Validate → Normalize → Enrich → Store → Publish
//
// Each stage is a composable function that operates on a NormalizedRecord.
// Failed records are emitted to the Dead Letter Queue (DLQ) for retry.
package pipeline

import (
	"context"
	"time"

	"github.com/shopspring/decimal"
)

// AdapterType identifies the ingestion transport used.
type AdapterType string

const (
	AdapterGRPC    AdapterType = "grpc"
	AdapterREST    AdapterType = "rest"
	AdapterWebhook AdapterType = "webhook"
	AdapterKafka   AdapterType = "kafka"
	AdapterFile    AdapterType = "file"
	AdapterDB      AdapterType = "db_poll"
)

// NormalizedRecord is the canonical internal representation of an ingested record.
// All source-specific formats are transformed into this struct before processing.
// The raw payload is always preserved for audit and dispute resolution.
type NormalizedRecord struct {
	// InternalID is a server-assigned UUID, always unique within ReconX.
	InternalID string

	// IdempotencyKey is the client-provided deduplication key.
	// Resubmission of the same key returns the original response without reprocessing.
	IdempotencyKey string

	// TransactionRef is the cross-system join key shared by all systems
	// that participate in a given business transaction.
	// Example: invoice number, order ID, payment reference.
	TransactionRef string

	// SourceSystem identifies the originating system.
	// Examples: "vendor_portal", "erp_sap", "payment_gateway_stripe".
	SourceSystem string

	// AdapterType records which transport delivered this record.
	AdapterType AdapterType

	// Amount is the normalized monetary value using arbitrary-precision decimal.
	// Never use float64 for financial amounts.
	Amount decimal.Decimal

	// Currency is the ISO 4217 currency code (e.g., "INR", "USD").
	Currency string

	// RecordTimestamp is the event time as reported by the source system,
	// normalized to UTC.
	RecordTimestamp time.Time

	// ServerReceivedAt is stamped by the ingestion server at entry point,
	// independent of any client-provided timestamp.
	ServerReceivedAt time.Time

	// RawPayload stores the original bytes exactly as received.
	// Never discarded — required for audit trail and dispute resolution.
	RawPayload []byte

	// PayloadSchema identifies the format/version of the raw payload.
	// Examples: "invoice.v1.json", "order.v2.proto".
	PayloadSchema string

	// Tags are arbitrary key-value metadata passed through from source.
	Tags map[string]string

	// TraceID enables distributed tracing correlation across services.
	TraceID string

	// RetryCount tracks how many times this record has been retried after failure.
	RetryCount int
}

// Stage is a pipeline processing function.
// A stage receives a record, performs its operation, and returns an error on failure.
// A non-nil error halts the pipeline and routes the record to the DLQ.
type Stage func(ctx context.Context, rec *NormalizedRecord) error

// Pipeline chains multiple stages together.
// Stages run sequentially; the first error short-circuits execution.
type Pipeline struct {
	stages []Stage
}

// New creates a Pipeline with the given ordered stages.
func New(stages ...Stage) *Pipeline {
	return &Pipeline{stages: stages}
}

// Run executes all stages in order against the given record.
// Returns the first error encountered, or nil if all stages succeed.
func (p *Pipeline) Run(ctx context.Context, rec *NormalizedRecord) error {
	for _, stage := range p.stages {
		if err := stage(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

// StageError wraps a pipeline error with context about which stage failed.
type StageError struct {
	Stage  string
	Reason string
	Err    error
}

func (e *StageError) Error() string {
	return e.Stage + ": " + e.Reason + ": " + e.Err.Error()
}

func (e *StageError) Unwrap() error { return e.Err }

// NewStageError constructs a StageError.
func NewStageError(stage, reason string, err error) *StageError {
	return &StageError{Stage: stage, Reason: reason, Err: err}
}
