// Package metrics registers and exposes Prometheus metrics for the ingestion service.
// All counters and histograms follow the reconx_ namespace.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// Ingestion operation status labels.
const (
	StatusSuccess   = "success"
	StatusDuplicate = "duplicate"
	StatusFailed    = "failed"
	StatusDLQ       = "dlq"
)

var (
	// RecordsIngested counts total records received by the ingestion service.
	RecordsIngested = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconx_ingestion_records_total",
		Help: "Total number of records received by the ingestion service.",
	}, []string{"source_system", "adapter_type", "status"})

	// IngestionDuration tracks end-to-end processing latency per record.
	IngestionDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "reconx_ingestion_duration_seconds",
		Help:    "End-to-end ingestion pipeline duration in seconds.",
		Buckets: []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0},
	}, []string{"source_system", "adapter_type"})

	// ValidationFailures counts records that failed validation by rule.
	ValidationFailures = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconx_ingestion_validation_failures_total",
		Help: "Total number of records that failed validation.",
	}, []string{"source_system", "failure_reason"})

	// DLQDepth tracks the current number of records in the dead-letter table.
	DLQDepth = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "reconx_ingestion_dlq_depth",
		Help: "Current number of records awaiting re-processing in the DLQ.",
	}, []string{"source_system"})

	// ActiveConnections tracks current open gRPC streaming connections.
	ActiveConnections = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "reconx_ingestion_active_grpc_streams",
		Help: "Number of currently active gRPC streaming connections.",
	})

	// BulkStreamRecords tracks records processed via BulkStreamIngest.
	BulkStreamRecords = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconx_ingestion_bulk_stream_records_total",
		Help: "Total records received via the BulkStreamIngest RPC.",
	}, []string{"source_system", "status"})

	// IdempotencyHits counts idempotency key cache hits (duplicates detected).
	IdempotencyHits = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconx_ingestion_idempotency_hits_total",
		Help: "Total number of duplicate records detected via idempotency keys.",
	}, []string{"source_system"})

	// RateLimitedRequests counts requests rejected due to rate limiting.
	RateLimitedRequests = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "reconx_ingestion_rate_limited_total",
		Help: "Total requests rejected due to per-source rate limits.",
	}, []string{"source_system"})

	// PayloadSizeBytes tracks incoming payload sizes.
	PayloadSizeBytes = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "reconx_ingestion_payload_size_bytes",
		Help:    "Size of raw payload bytes received per record.",
		Buckets: prometheus.ExponentialBuckets(64, 4, 10),
	}, []string{"source_system"})

	// AdapterPollDuration tracks how long adapter polling cycles take.
	AdapterPollDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "reconx_ingestion_adapter_poll_duration_seconds",
		Help:    "Duration of adapter polling/fetch cycles.",
		Buckets: []float64{0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0},
	}, []string{"adapter_type", "source_system"})
)
