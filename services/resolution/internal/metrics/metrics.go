// Package metrics exposes Prometheus metrics for the Resolution Service.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
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

	// ResolutionErrorsTotal counts failed ResolveManually calls by error kind.
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
)

// Register adds all metrics to the provided registry.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(
		ResolutionsTotal,
		ResolutionErrorsTotal,
		ListMismatchesStreamed,
		RPCDuration,
	)
}
