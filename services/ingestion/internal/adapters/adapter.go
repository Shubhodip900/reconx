// Package adapters defines the SourceAdapter interface and shared types.
// Every ingestion source implements this interface, enabling plug-and-play
// addition of new data sources without modifying the core pipeline.
//
// Supported adapters:
//   - REST poller  – periodically pulls from an HTTP endpoint
//   - Webhook      – receives HTTP POST from upstream systems
//   - Kafka        – consumes from a Kafka topic
//   - File         – parses uploaded NDJSON / CSV files
//   - DB poll      – queries a foreign database on a schedule
package adapters

import (
	"context"

	"github.com/reconx/services/ingestion/internal/pipeline"
)

// SourceAdapter is the common interface for all ingestion adapters.
// Adapters are responsible for:
//  1. Fetching / receiving raw records from a specific transport.
//  2. Converting them into pipeline.NormalizedRecord skeletons
//     (IdempotencyKey, TransactionRef, SourceSystem, RawPayload, AdapterType).
//  3. Emitting them on the provided channel.
//
// The core pipeline handles validation, normalization, and storage.
type SourceAdapter interface {
	// ID returns a stable, human-readable identifier for this adapter instance.
	// Examples: "rest-erp-sap", "kafka-vendor-portal", "file-upload".
	ID() string

	// AdapterType returns the transport category.
	AdapterType() pipeline.AdapterType

	// Start begins consuming records and sending them to out.
	// It blocks until ctx is cancelled. Start should be called in a goroutine.
	Start(ctx context.Context, out chan<- *pipeline.NormalizedRecord) error
}

// RecordHandler is an alternative callback-based interface for push adapters.
type RecordHandler func(ctx context.Context, rec *pipeline.NormalizedRecord) error
