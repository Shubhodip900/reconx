// ReconX Resolution Service — main entrypoint.
//
// Startup sequence:
//  1. Load configuration (env vars / config.yaml)
//  2. Connect to PostgreSQL; run schema migrations
//  3. Register Prometheus metrics
//  4. Dial Reconciliation Engine gRPC (non-blocking; failure disables retry worker)
//  5. Start gRPC server   (ResolveManually + ListMismatches)  on :50053
//  6. Start HTTP REST API server                              on :8082
//  7. Start Prometheus metrics + health server                on :9092
//  8. Start background retry worker (if engine is reachable)
//  9. Block until SIGTERM / SIGINT → graceful shutdown of all components
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
	"github.com/reconx/services/resolution/internal/api"
	"github.com/reconx/services/resolution/internal/config"
	"github.com/reconx/services/resolution/internal/db"
	engineclient "github.com/reconx/services/resolution/internal/engine"
	"github.com/reconx/services/resolution/internal/metrics"
	"github.com/reconx/services/resolution/internal/resolver"
	retrypkg "github.com/reconx/services/resolution/internal/retry"
	"github.com/reconx/services/resolution/internal/server"
)

func main() {
	// ── Logger ───────────────────────────────────────────────────────────────
	log, _ := zap.NewProduction()
	defer log.Sync() //nolint:errcheck

	log.Info("ReconX Resolution Service starting")

	// ── Configuration ─────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config", zap.Error(err))
	}
	log.Info("configuration loaded",
		zap.Int("grpc_port", cfg.GRPC.Port),
		zap.Int("http_port", cfg.HTTP.Port),
		zap.Int("metrics_port", cfg.Metrics.Port),
		zap.String("engine_address", cfg.Engine.Address),
		zap.Bool("retry_enabled", cfg.Retry.Enabled),
		zap.String("auto_resolve_strategy", cfg.AutoResolve.DefaultStrategy),
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
		log.Warn("database not reachable at startup — will retry on first query", zap.Error(err))
	}
	pingCancel()

	// ── Schema migrations ─────────────────────────────────────────────────────
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

	// ── Context for background workers ────────────────────────────────────────
	// Cancelled on shutdown to stop the retry worker cleanly.
	workerCtx, workerCancel := context.WithCancel(context.Background())
	defer workerCancel()

	// ── Engine gRPC client ────────────────────────────────────────────────────
	// Non-fatal: if the engine is down at startup the retry worker is skipped,
	// but gRPC and HTTP servers continue to serve their requests normally.
	var engClient *engineclient.Client
	{
		cli, err := engineclient.NewClient(
			cfg.Engine.Address,
			cfg.Engine.DialTimeout,
			cfg.Engine.RequestTimeout,
			log,
		)
		if err != nil {
			log.Warn("cannot connect to reconciliation engine — retry worker will not start",
				zap.String("address", cfg.Engine.Address),
				zap.Error(err),
			)
		} else {
			engClient = cli
		}
	}

	// ── Conflict resolver ─────────────────────────────────────────────────────
	res := resolver.New(database, log)

	// ── gRPC Server ───────────────────────────────────────────────────────────
	resolutionSrv := server.New(database, log)
	grpcServer := grpc.NewServer()
	resolutionpb.RegisterResolutionServiceServer(grpcServer, resolutionSrv)
	reflection.Register(grpcServer)

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPC.Port))
	if err != nil {
		log.Fatal("failed to listen on gRPC port",
			zap.Int("port", cfg.GRPC.Port), zap.Error(err))
	}

	// ── HTTP REST API server ──────────────────────────────────────────────────
	apiHandler := api.New(database, res, cfg, log)
	apiMux := http.NewServeMux()
	apiHandler.RegisterRoutes(apiMux)
	apiServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      apiMux,
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	// ── Prometheus metrics + liveness server ─────────────────────────────────
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
		log.Info("HTTP REST API server listening", zap.Int("port", cfg.HTTP.Port))
		if err := apiServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP REST API server error", zap.Error(err))
		}
	}()

	go func() {
		log.Info("metrics/health server listening", zap.Int("port", cfg.Metrics.Port))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()

	// ── Background retry worker ───────────────────────────────────────────────
	// Only started when the engine client is available and retry is enabled.
	switch {
	case !cfg.Retry.Enabled:
		log.Info("retry worker disabled by configuration")
	case engClient == nil:
		log.Warn("retry worker not started — engine client unavailable")
	default:
		worker := retrypkg.New(database, engClient, res, cfg.Retry, cfg.AutoResolve, log)
		go worker.Start(workerCtx)
	}

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutdown signal received — draining connections")

	// Stop background workers first.
	workerCancel()

	// Stop accepting new gRPC requests and wait for in-flight ones to finish.
	grpcServer.GracefulStop()

	// Give HTTP servers 30 s to drain in-flight requests.
	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()
	_ = apiServer.Shutdown(shutCtx)
	_ = metricsServer.Shutdown(shutCtx)

	// Close downstream connections.
	if engClient != nil {
		_ = engClient.Close()
	}
	_ = database.Close()

	log.Info("ReconX Resolution Service shut down cleanly")
}
