// ReconX Resolution Service — main entrypoint.
//
// This service allows human operators to manually resolve MISMATCHED transactions
// and provides a streaming view of all outstanding mismatches for dashboards.
//
// Startup sequence:
//  1. Load configuration (env vars / config.yaml)
//  2. Connect to PostgreSQL; run schema migrations
//  3. Register Prometheus metrics
//  4. Start gRPC server (ResolveManually + ListMismatches)
//  5. Start HTTP server (metrics + health)
//  6. Block until SIGTERM / SIGINT → graceful shutdown
package main

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	_ "github.com/lib/pq"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	resolutionpb "github.com/reconx/proto/gen/go/resolution"
	"github.com/reconx/services/resolution/internal/config"
	"github.com/reconx/services/resolution/internal/db"
	"github.com/reconx/services/resolution/internal/metrics"
	"github.com/reconx/services/resolution/internal/server"
)

func main() {
	// ── Logger ───────────────────────────────────────────────────────────────
	log, _ := zap.NewProduction()
	defer log.Sync()

	log.Info("ReconX Resolution Service starting")

	// ── Configuration ─────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config", zap.Error(err))
	}
	log.Info("configuration loaded",
		zap.Int("grpc_port", cfg.GRPC.Port),
		zap.Int("metrics_port", cfg.Metrics.Port),
		zap.String("engine_address", cfg.Engine.Address),
	)

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	database, err := sql.Open("postgres", cfg.Database.DSN)
	if err != nil {
		log.Fatal("failed to open database", zap.Error(err))
	}
	database.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	database.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	database.SetConnMaxLifetime(cfg.Database.ConnMaxLifetime)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := database.PingContext(pingCtx); err != nil {
		log.Warn("database not reachable at startup", zap.Error(err))
	}
	pingCancel()

	// ── Migrations ────────────────────────────────────────────────────────────
	migCtx, migCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := db.RunMigrations(migCtx, database); err != nil {
		log.Fatal("migrations failed", zap.Error(err))
	}
	migCancel()
	log.Info("database migrations applied")

	// ── Prometheus metrics ────────────────────────────────────────────────────
	reg := prometheus.NewRegistry()
	metrics.Register(reg)
	log.Info("metrics registered")

	// ── gRPC Server ───────────────────────────────────────────────────────────
	resolutionSrv := server.New(database, log)
	grpcServer := grpc.NewServer()
	resolutionpb.RegisterResolutionServiceServer(grpcServer, resolutionSrv)
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPC.Port))
	if err != nil {
		log.Fatal("failed to listen on gRPC port", zap.Int("port", cfg.GRPC.Port), zap.Error(err))
	}

	// ── Metrics + Health HTTP Server ──────────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle(cfg.Metrics.Path, promhttp.HandlerFor(reg, promhttp.HandlerOpts{}))
	metricsMux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"reconx-resolution"}`))
	})
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Metrics.Port),
		Handler: metricsMux,
	}

	// ── Start servers ─────────────────────────────────────────────────────────
	go func() {
		log.Info("gRPC server listening", zap.Int("port", cfg.GRPC.Port))
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Error("gRPC server error", zap.Error(err))
		}
	}()

	go func() {
		log.Info("metrics/health server listening", zap.Int("port", cfg.Metrics.Port))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutdown signal received — draining connections")

	grpcServer.GracefulStop()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()
	_ = metricsServer.Shutdown(shutCtx)
	_ = database.Close()

	log.Info("ReconX Resolution Service shut down cleanly")
}
