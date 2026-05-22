// ReconX API Gateway — main entrypoint.
//
// The gateway is the single public-facing HTTP entry point for the ReconX
// platform. It translates REST JSON calls into gRPC calls to the three
// upstream services (Ingestion, Engine, Resolution) and returns JSON responses.
// Four resolution routes are forwarded to the Resolution Service's HTTP REST API.
//
// API surface:
//
//	POST   /v1/ingest                               → Ingestion.SubmitRecord (gRPC)
//	GET    /v1/recon/{transaction_ref}              → Engine.GetReconState (gRPC)
//	POST   /v1/recon/{transaction_ref}/retrigger    → Engine.ReTriggerMatch (gRPC)
//	POST   /v1/resolution/{transaction_ref}         → Resolution.ResolveManually (gRPC)
//	GET    /v1/resolution/mismatches                → Resolution.ListMismatches (gRPC stream)
//	POST   /v1/resolution/{ref}/auto               → Resolution HTTP /v1/resolve/auto/{ref}
//	POST   /v1/resolution/{ref}/retry              → Resolution HTTP /v1/resolve/retry/{ref}
//	GET    /v1/resolution/{ref}/audit              → Resolution HTTP /v1/resolve/audit/{ref}
//	GET    /v1/resolution/retry-queue              → Resolution HTTP /v1/resolve/retry-queue
//	GET    /health                                  → aggregate health (auth-exempt)
//	GET    /metrics                                 → Prometheus metrics (separate port)
//
// Authentication:
//
//	All /v1/* routes require the X-API-Key header to match RECONX_GATEWAY_API_KEY.
//	GET /health is exempt. The metrics server has no auth.
//
// Startup sequence:
//  1. Load configuration
//  2. Register Prometheus metrics
//  3. Dial upstream gRPC services + create Resolution HTTP client
//  4. Start HTTP API server (with auth middleware)
//  5. Start metrics/health server (separate port)
//  6. Block until SIGTERM / SIGINT → graceful shutdown
package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"

	"github.com/reconx/services/gateway/internal/clients"
	"github.com/reconx/services/gateway/internal/config"
	"github.com/reconx/services/gateway/internal/handlers"
	"github.com/reconx/services/gateway/internal/metrics"
	"github.com/reconx/services/gateway/internal/middleware"
)

func main() {
	// ── Logger ───────────────────────────────────────────────────────────────
	log, _ := zap.NewProduction()
	defer log.Sync()

	log.Info("ReconX API Gateway starting")

	// ── Configuration ─────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config", zap.Error(err))
	}
	log.Info("configuration loaded",
		zap.Int("http_port", cfg.HTTP.Port),
		zap.Int("metrics_port", cfg.Metrics.Port),
		zap.String("ingestion", cfg.Ingestion.Address),
		zap.String("engine", cfg.Engine.Address),
		zap.String("resolution_grpc", cfg.Resolution.Address),
		zap.String("resolution_http", cfg.Resolution.HTTPAddress),
		zap.Bool("auth_enabled", cfg.APIKey != ""),
	)

	// ── Prometheus metrics ────────────────────────────────────────────────────
	reg := prometheus.NewRegistry()
	metrics.Register(reg)

	// ── Upstream clients ──────────────────────────────────────────────────────
	upstreams, err := clients.New(
		cfg.Ingestion.Address,
		cfg.Engine.Address,
		cfg.Resolution.Address,
		cfg.Resolution.HTTPAddress,
	)
	if err != nil {
		log.Fatal("failed to dial upstream services", zap.Error(err))
	}
	defer upstreams.Close()

	log.Info("upstream clients ready",
		zap.String("ingestion_grpc", cfg.Ingestion.Address),
		zap.String("engine_grpc", cfg.Engine.Address),
		zap.String("resolution_grpc", cfg.Resolution.Address),
		zap.String("resolution_http", cfg.Resolution.HTTPAddress),
	)

	// ── HTTP API Server ───────────────────────────────────────────────────────
	// Auth middleware wraps the entire API mux; /health is exempt inside the
	// middleware itself so liveness probes work without a key.
	apiHandler := middleware.APIKeyAuth(cfg.APIKey)(handlers.New(upstreams, cfg, log))

	apiServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      apiHandler,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// ── Metrics Server ────────────────────────────────────────────────────────
	// Served on a separate port — no auth, not publicly exposed.
	metricsMux := http.NewServeMux()
	metricsMux.Handle(cfg.Metrics.Path, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Metrics.Port),
		Handler: metricsMux,
	}

	// ── Start servers ─────────────────────────────────────────────────────────
	go func() {
		log.Info("API server listening", zap.Int("port", cfg.HTTP.Port))
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("API server error", zap.Error(err))
		}
	}()

	go func() {
		log.Info("metrics server listening", zap.Int("port", cfg.Metrics.Port))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutdown signal received — draining connections")

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()

	_ = apiServer.Shutdown(shutCtx)
	_ = metricsServer.Shutdown(shutCtx)

	log.Info("ReconX API Gateway shut down cleanly")
}
