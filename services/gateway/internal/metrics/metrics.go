// Package metrics exposes Prometheus metrics for the API Gateway.
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	// HTTPRequestsTotal counts requests by method, path, and status code.
	HTTPRequestsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "gateway",
			Name:      "http_requests_total",
			Help:      "Total HTTP requests handled by the gateway.",
		},
		[]string{"method", "path", "status_code"},
	)

	// HTTPDuration tracks HTTP handler latency by path.
	HTTPDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: "reconx",
			Subsystem: "gateway",
			Name:      "http_duration_seconds",
			Help:      "Latency of HTTP handlers in seconds.",
			Buckets:   prometheus.DefBuckets,
		},
		[]string{"method", "path"},
	)

	// UpstreamErrors counts gRPC errors returned by upstream services.
	UpstreamErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "reconx",
			Subsystem: "gateway",
			Name:      "upstream_errors_total",
			Help:      "Total upstream gRPC errors by service.",
		},
		[]string{"service"},
	)
)

// Register adds all metrics to the provided registry.
func Register(reg prometheus.Registerer) {
	reg.MustRegister(
		HTTPRequestsTotal,
		HTTPDuration,
		UpstreamErrors,
	)
}
