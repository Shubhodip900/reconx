// ReconX API Gateway — main entrypoint.
//
// The gateway is the single public-facing HTTP entry point for the ReconX
// platform. It translates REST JSON calls into gRPC calls to the three
// upstream services (Ingestion, Engine, Resolution) and returns JSON responses.
//
// API surface:
//
//	POST   /v1/ingest                           → Ingestion.SubmitRecord
//	GET    /v1/recon/{transaction_ref}           → Engine.GetReconState
//	POST   /v1/recon/{transaction_ref}/retrigger → Engine.ReTriggerMatch
//	POST   /v1/resolution/{transaction_ref}      → Resolution.ResolveManually
//	GET    /v1/resolution/mismatches             → Resolution.ListMismatches
//	GET    /health                               → aggregate health
//	GET    /metrics                              → Prometheus metrics
//
// Startup sequence:
//  1. Load configuration
//  2. Register Prometheus metrics
//  3. Dial upstream gRPC services
//  4. Start HTTP API server
//  5. Start metrics/health server
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
		zap.String("resolution", cfg.Resolution.Address),
	)

	// ── Prometheus metrics ────────────────────────────────────────────────────
	reg := prometheus.NewRegistry()
	metrics.Register(reg)

	// ── Upstream gRPC clients ─────────────────────────────────────────────────
	upstreams, err := clients.New(
		cfg.Ingestion.Address,
		cfg.Engine.Address,
		cfg.Resolution.Address,
	)
	if err != nil {
		log.Fatal("failed to dial upstream services", zap.Error(err))
	}
	defer upstreams.Close()

	log.Info("upstream gRPC clients ready",
		zap.String("ingestion", cfg.Ingestion.Address),
		zap.String("engine", cfg.Engine.Address),
		zap.String("resolution", cfg.Resolution.Address),
	)

	// ── HTTP API Server ───────────────────────────────────────────────────────
	apiServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      handlers.New(upstreams, log),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
		IdleTimeout:  120 * time.Second,
	}

	// ── Metrics + Health Server ───────────────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle(cfg.Metrics.Path, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"status":"ok","service":"reconx-gateway"}`))
	})
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
