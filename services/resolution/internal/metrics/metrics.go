// Package metrics exposes Prometheus metrics for the Resolution Service.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// ── Manual resolution ────────────────────────────────────────────────────

	// ResolutionsTotal counts successful ResolveManually calls by resolver_id.
	ResolutionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "resolutions_total",
			Help:      "Total number of manual resolutions performed.",
		},
		[]string{"resolver_id"},
	)

	// ResolutionErrorsTotal counts failed resolution calls by error kind.
	ResolutionErrorsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "resolution_errors_total",
			Help:      "Total number of failed resolution attempts.",
		},
		[]string{"error_kind"},
	)

	// ListMismatchesStreamed counts StateResponse messages sent via ListMismatches.
	ListMismatchesStreamed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "list_mismatches_streamed_total",
			Help:      "Total MISMATCHED records streamed via ListMismatches.",
		},
	)

	// RPCDuration tracks gRPC handler latency.
	RPCDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "rpc_duration_seconds",
			Help:      "Latency of gRPC handlers in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method"},
	)

	// ── Auto-resolution ───────────────────────────────────────────────────────

	// AutoResolutionsTotal counts auto-resolve operations by strategy and outcome.
	AutoResolutionsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "auto_resolutions_total",
			Help:      "Total number of auto-resolve operations by strategy and outcome.",
		},
		[]string{"strategy", "outcome"}, // outcome: "success" | "failed"
	)

	// AutoResolutionDuration tracks auto-resolve latency by strategy.
	AutoResolutionDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "auto_resolution_duration_seconds",
			Help:      "Latency of auto-resolve operations in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"strategy"},
	)

	// ── Retry worker ──────────────────────────────────────────────────────────

	// RetryAttemptsTotal counts retry attempts by outcome.
	// outcome: "matched" | "still_mismatched" | "error" | "exhausted"
	RetryAttemptsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "retry_attempts_total",
			Help:      "Total retry attempts made by the retry worker, by outcome.",
		},
		[]string{"outcome"},
	)

	// RetryQueueDepth is the current number of PENDING entries in the retry queue.
	RetryQueueDepth = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "retry_queue_depth",
			Help:      "Current number of PENDING entries in the resolution_retry_queue.",
		},
	)

	// RetryQueueExhausted is the current number of EXHAUSTED entries in the retry queue.
	RetryQueueExhausted = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "retry_queue_exhausted",
			Help:      "Current number of EXHAUSTED (max retries reached) entries requiring intervention.",
		},
	)

	// RetryWorkerCycleDuration tracks how long each worker poll cycle takes.
	RetryWorkerCycleDuration = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "retry_worker_cycle_duration_seconds",
			Help:      "Duration of each retry worker poll cycle in seconds.",
			Buckets:   []float64{0.01, 0.05, 0.1, 0.5, 1, 5, 10, 30},
		},
	)

	// RetryWorkerErrors counts unrecoverable errors during worker poll cycles.
	RetryWorkerErrors = prometheus.NewCounter(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "retry_worker_errors_total",
			Help:      "Total unrecoverable errors encountered by the retry worker.",
		},
	)

	// ── HTTP API ──────────────────────────────────────────────────────────────

	// HTTPRequestsTotal counts HTTP REST API requests by method, path, and status.
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "http_requests_total",
			Help:      "Total HTTP REST API requests by method, path, and status code.",
		},
		[]string{"method", "path", "status"},
	)

	// HTTPRequestDuration tracks HTTP REST API handler latency.
	HTTPRequestDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "reconx",
			Subsystem: "resolution",
			Name:      "http_request_duration_seconds",
			Help:      "Latency of HTTP REST API handlers in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)
)

// Register adds all metrics to the provided registry.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(
		// gRPC
		ResolutionsTotal,
		ResolutionErrorsTotal,
		ListMismatchesStreamed,
		RPCDuration,
		// Auto-resolve
		AutoResolutionsTotal,
		AutoResolutionDuration,
		// Retry worker
		RetryAttemptsTotal,
		RetryQueueDepth,
		RetryQueueExhausted,
		RetryWorkerCycleDuration,
		RetryWorkerErrors,
		// HTTP API
		HTTPRequestsTotal,
		HTTPRequestDuration,
	)
}
