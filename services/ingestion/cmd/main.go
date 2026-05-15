// ReconX Ingestion Service — main entrypoint.
//
// This service is the data ingestion layer for the ReconX distributed
// reconciliation engine. It accepts records from multiple sources,
// normalizes them into a canonical format, and stores them for the
// Reconciliation Engine to process.
//
// Startup sequence:
//  1. Load configuration (env vars / config.yaml)
//  2. Connect to PostgreSQL; run schema migrations
//  3. Initialize pipeline (validate → normalize → enrich stages)
//  4. Start gRPC server (SubmitRecord + BulkStreamIngest)
//  5. Start HTTP server (webhook receiver + file upload + metrics)
//  6. Start background adapters (Kafka consumer if enabled)
//  7. Start DLQ metrics updater
//  8. Block until SIGTERM / SIGINT → graceful shutdown
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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	commonpb    "github.com/reconx/proto/gen/go/common"
	ingestionpb "github.com/reconx/proto/gen/go/ingestion"
	"github.com/reconx/services/ingestion/internal/adapters"
	"github.com/reconx/services/ingestion/internal/config"
	"github.com/reconx/services/ingestion/internal/dlq"
	"github.com/reconx/services/ingestion/internal/idempotency"
	"github.com/reconx/services/ingestion/internal/metrics"
	"github.com/reconx/services/ingestion/internal/pipeline"
	"github.com/reconx/services/ingestion/internal/ratelimit"
	"github.com/reconx/services/ingestion/internal/server"
	"github.com/reconx/services/ingestion/internal/storage"
)

func main() {
	// ── Logger ──────────────────────────────────────────────────────────────
	log, _ := zap.NewProduction()
	defer log.Sync()

	log.Info("ReconX Ingestion Service starting")

	// ── Configuration ────────────────────────────────────────────────────────
	cfg, err := config.Load()
	if err != nil {
		log.Fatal("failed to load config", zap.Error(err))
	}
	log.Info("configuration loaded",
		zap.Int("grpc_port", cfg.GRPC.Port),
		zap.Int("http_port", cfg.HTTP.Port),
		zap.Int("metrics_port", cfg.Metrics.Port))

	// ── PostgreSQL ────────────────────────────────────────────────────────────
	db, err := sql.Open("postgres", cfg.Database.DSN)
	if err != nil {
		log.Fatal("failed to open database", zap.Error(err))
	}
	db.SetMaxOpenConns(cfg.Database.MaxOpenConns)
	db.SetMaxIdleConns(cfg.Database.MaxIdleConns)
	db.SetConnMaxLifetime(cfg.Database.ConnMaxLifetime)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	if err := db.PingContext(ctx); err != nil {
		log.Warn("database not reachable at startup (will retry on first request)",
			zap.Error(err), zap.String("dsn", cfg.Database.DSN))
	}
	cancel()

	// ── Storage ───────────────────────────────────────────────────────────────
	store, err := storage.New(db)
	if err != nil {
		log.Fatal("storage init failed", zap.Error(err))
	}
	log.Info("storage initialized")

	// ── Idempotency ───────────────────────────────────────────────────────────
	idempStore, err := idempotency.New(db, cfg.Idempotency.TTL)
	if err != nil {
		log.Fatal("idempotency store init failed", zap.Error(err))
	}
	log.Info("idempotency store initialized", zap.Duration("ttl", cfg.Idempotency.TTL))

	// ── Dead Letter Queue ─────────────────────────────────────────────────────
	dlqQueue, err := dlq.New(db, cfg.DLQ.TableName, cfg.DLQ.MaxRetries)
	if err != nil {
		log.Fatal("DLQ init failed", zap.Error(err))
	}
	log.Info("DLQ initialized", zap.String("table", cfg.DLQ.TableName))

	// ── Rate Limiter ──────────────────────────────────────────────────────────
	rateLimiter := ratelimit.New(cfg.RateLimit.DefaultRPS, cfg.RateLimit.Overrides)
	log.Info("rate limiter initialized", zap.Float64("default_rps", cfg.RateLimit.DefaultRPS))

	// ── Pipeline ──────────────────────────────────────────────────────────────
	// Stage order: Enrich (stamps server time) → Validate → Normalize
	// Enrichment comes first so ServerReceivedAt is always set, even if validation fails.
	pl := pipeline.New(
		pipeline.Enrich(),
		pipeline.Validate(),
		pipeline.Normalize(),
	)
	log.Info("pipeline initialized with stages: Enrich → Validate → Normalize")

	// ── gRPC Server ───────────────────────────────────────────────────────────
	grpcSrv := server.New(pl, idempStore, rateLimiter, store, dlqQueue, log)

	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(cfg.GRPC.MaxRecvMsgSize*1024*1024),
	)
	ingestionpb.RegisterIngestionServiceServer(grpcServer, grpcSrv)
	reflection.Register(grpcServer) // enables grpcurl / grpcui tooling

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPC.Port))
	if err != nil {
		log.Fatal("failed to listen on gRPC port", zap.Error(err))
	}

	// ── HTTP Server (Webhook + File Upload) ───────────────────────────────────
	// Shared channel: all adapters push into this bounded queue.
	// The bounded size provides natural backpressure.
	adapterQueue := make(chan *pipeline.NormalizedRecord, 10_000)

	mux := http.NewServeMux()

	// Webhook endpoint: POST /ingest/{source_system}
	webhookHandler := adapters.NewWebhookHandler(
		adapters.WebhookConfig{MaxBodyBytes: cfg.HTTP.MaxBodySizeMB * 1024 * 1024},
		adapterQueue,
		log,
	)
	mux.Handle("/ingest/{source_system}", webhookHandler)

	// File upload endpoint: POST /ingest/file
	fileHandler := adapters.NewFileHandler(
		adapters.FileConfig{
			MaxFileSizeBytes:  100 * 1024 * 1024,
			AllowedExtensions: []string{".json", ".ndjson", ".csv"},
		},
		adapterQueue,
		log,
	)
	mux.Handle("/ingest/file", fileHandler)

	// Health check.
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok","service":"reconx-ingestion"}`))
	})

	httpServer := &http.Server{
		Addr:         fmt.Sprintf(":%d", cfg.HTTP.Port),
		Handler:      mux,
		ReadTimeout:  cfg.HTTP.ReadTimeout,
		WriteTimeout: cfg.HTTP.WriteTimeout,
	}

	// ── Metrics Server ────────────────────────────────────────────────────────
	metricsMux := http.NewServeMux()
	metricsMux.Handle(cfg.Metrics.Path, promhttp.Handler())
	metricsServer := &http.Server{
		Addr:    fmt.Sprintf(":%d", cfg.Metrics.Port),
		Handler: metricsMux,
	}

	// ── Background: adapter queue consumer ───────────────────────────────────
	// Records pushed by HTTP adapters (webhook/file) are processed here.
	mainCtx, mainCancel := context.WithCancel(context.Background())
	defer mainCancel()

	go consumeAdapterQueue(mainCtx, adapterQueue, grpcSrv, log)

	// ── Background: Kafka consumer (optional) ─────────────────────────────────
	if cfg.Kafka.Enabled {
		kafkaCfg := adapters.KafkaConsumerConfig{
			ID:             "kafka-main",
			SourceSystem:   cfg.Kafka.GroupID,
			Brokers:        cfg.Kafka.Brokers,
			Topic:          cfg.Kafka.Topic,
			GroupID:        cfg.Kafka.GroupID,
			MinBytes:       cfg.Kafka.MinBytes,
			MaxBytes:       cfg.Kafka.MaxBytes,
			CommitInterval: cfg.Kafka.CommitInterval,
		}
		kafkaConsumer := adapters.NewKafkaConsumer(kafkaCfg, log)
		go func() {
			if err := kafkaConsumer.Start(mainCtx, adapterQueue); err != nil && mainCtx.Err() == nil {
				log.Error("Kafka consumer stopped unexpectedly", zap.Error(err))
			}
		}()
		log.Info("Kafka consumer started",
			zap.Strings("brokers", cfg.Kafka.Brokers),
			zap.String("topic", cfg.Kafka.Topic))
	}

	// ── Background: DLQ depth metrics updater ────────────────────────────────
	go updateDLQMetrics(mainCtx, dlqQueue, log)

	// ── Background: idempotency key purge ────────────────────────────────────
	go purgeExpiredIdempotencyKeys(mainCtx, idempStore, log)

	// ── Start servers ─────────────────────────────────────────────────────────
	go func() {
		log.Info("gRPC server listening", zap.Int("port", cfg.GRPC.Port))
		if err := grpcServer.Serve(grpcListener); err != nil {
			log.Error("gRPC server error", zap.Error(err))
		}
	}()

	go func() {
		log.Info("HTTP server listening", zap.Int("port", cfg.HTTP.Port))
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("HTTP server error", zap.Error(err))
		}
	}()

	go func() {
		log.Info("metrics server listening",
			zap.Int("port", cfg.Metrics.Port),
			zap.String("path", cfg.Metrics.Path))
		if err := metricsServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Error("metrics server error", zap.Error(err))
		}
	}()

	// ── Graceful shutdown ─────────────────────────────────────────────────────
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGTERM, syscall.SIGINT)
	<-quit

	log.Info("shutdown signal received — draining connections")
	mainCancel()

	shutCtx, shutCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutCancel()

	grpcServer.GracefulStop()
	_ = httpServer.Shutdown(shutCtx)
	_ = metricsServer.Shutdown(shutCtx)
	_ = db.Close()

	log.Info("ReconX Ingestion Service shut down cleanly")
}

// consumeAdapterQueue processes records pushed by HTTP adapters via SubmitRecord.
func consumeAdapterQueue(
	ctx context.Context,
	queue <-chan *pipeline.NormalizedRecord,
	srv *server.IngestionServer,
	log *zap.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case rec := <-queue:
			if rec == nil {
				continue
			}
			// Convert NormalizedRecord skeleton → IngestRequest for the server handler.
			// The server will re-run idempotency + pipeline.
			req := normalizedToProto(rec)
			if _, err := srv.SubmitRecord(ctx, req); err != nil {
				log.Warn("adapter queue submit failed", zap.Error(err),
					zap.String("idem_key", rec.IdempotencyKey))
			}
		}
	}
}

// normalizedToProto converts a NormalizedRecord skeleton (from HTTP adapters)
// back to an IngestRequest proto for the server handler.
func normalizedToProto(rec *pipeline.NormalizedRecord) *ingestionpb.IngestRequest {
	metaTags := make(map[string]string)
	for k, v := range rec.Tags {
		metaTags[k] = v
	}
	metaTags["adapter_type"] = string(rec.AdapterType)

	return &ingestionpb.IngestRequest{
		IdempotencyKey: rec.IdempotencyKey,
		TransactionRef: rec.TransactionRef,
		Payload:        rec.RawPayload,
		Metadata: &commonpb.Metadata{
			SourceSystem: rec.SourceSystem,
			TraceId:      rec.TraceID,
			Tags:         metaTags,
			IngestedAt:   time.Now().UnixMilli(),
		},
	}
}

// updateDLQMetrics periodically refreshes the DLQ depth Prometheus gauge.
func updateDLQMetrics(ctx context.Context, q *dlq.Queue, log *zap.Logger) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			depths, err := q.Depth(ctx)
			if err != nil {
				log.Warn("DLQ depth query failed", zap.Error(err))
				continue
			}
			for src, depth := range depths {
				metrics.DLQDepth.WithLabelValues(src).Set(float64(depth))
			}
		}
	}
}

// purgeExpiredIdempotencyKeys cleans up stale idempotency records every hour.
func purgeExpiredIdempotencyKeys(ctx context.Context, store *idempotency.Store, log *zap.Logger) {
	ticker := time.NewTicker(1 * time.Hour)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			n, err := store.Purge(ctx)
			if err != nil {
				log.Warn("idempotency purge failed", zap.Error(err))
			} else if n > 0 {
				log.Info("purged expired idempotency keys", zap.Int64("count", n))
			}
		}
	}
}
