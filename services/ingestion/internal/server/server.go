// Package server implements the gRPC IngestionService defined in ingestion.proto.
// It is the primary entry point for structured, typed ingestion from other services.
//
// gRPC methods:
//   - SubmitRecord       — single record, synchronous, with idempotency check
//   - BulkStreamIngest   — client-streaming for high-throughput batch loads
//
// Each received record flows through:
//  1. Rate limit check
//  2. Idempotency check
//  3. Validation pipeline
//  4. Normalization pipeline
//  5. Storage (PostgreSQL)
//  6. Metrics update
//  7. DLQ on failure
package server

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb "github.com/reconx/proto/gen/go/common"
	ingestionpb "github.com/reconx/proto/gen/go/ingestion"
	"github.com/reconx/services/ingestion/internal/dlq"
	"github.com/reconx/services/ingestion/internal/idempotency"
	"github.com/reconx/services/ingestion/internal/metrics"
	"github.com/reconx/services/ingestion/internal/pipeline"
	"github.com/reconx/services/ingestion/internal/ratelimit"
	"github.com/reconx/services/ingestion/internal/storage"
)

// IngestionServer implements the gRPC IngestionService.
type IngestionServer struct {
	ingestionpb.UnimplementedIngestionServiceServer

	pipeline    *pipeline.Pipeline
	idempStore  *idempotency.Store
	rateLimiter *ratelimit.Limiter
	store       *storage.Store
	dlq         *dlq.Queue
	log         *zap.Logger
}

// New creates an IngestionServer wired with all dependencies.
func New(
	pl *pipeline.Pipeline,
	idem *idempotency.Store,
	rl *ratelimit.Limiter,
	store *storage.Store,
	dlqQueue *dlq.Queue,
	log *zap.Logger,
) *IngestionServer {
	return &IngestionServer{
		pipeline:    pl,
		idempStore:  idem,
		rateLimiter: rl,
		store:       store,
		dlq:         dlqQueue,
		log:         log,
	}
}

// SubmitRecord handles a single synchronous record submission.
// Returns the original response if the idempotency_key was already processed.
func (s *IngestionServer) SubmitRecord(
	ctx context.Context,
	req *ingestionpb.IngestRequest,
) (*ingestionpb.IngestResponse, error) {

	src := ""
	if req.Metadata != nil {
		src = req.Metadata.SourceSystem
	}

	start := time.Now()
	defer func() {
		metrics.IngestionDuration.WithLabelValues(src, string(pipeline.AdapterGRPC)).
			Observe(time.Since(start).Seconds())
	}()

	// ── Rate limit ──────────────────────────────────────────────────────────
	if !s.rateLimiter.Allow(src) {
		metrics.RateLimitedRequests.WithLabelValues(src).Inc()
		return nil, status.Errorf(codes.ResourceExhausted, "rate limit exceeded for source %q", src)
	}

	// ── Idempotency check ───────────────────────────────────────────────────
	cached, err := s.idempStore.Get(ctx, req.IdempotencyKey)
	if err == nil {
		// Already processed — return the cached response without re-processing.
		metrics.IdempotencyHits.WithLabelValues(src).Inc()
		metrics.RecordsIngested.WithLabelValues(src, string(pipeline.AdapterGRPC), metrics.StatusDuplicate).Inc()
		s.log.Debug("idempotency hit",
			zap.String("key", req.IdempotencyKey),
			zap.String("internal_id", cached.InternalID))
		return cachedToResponse(cached), nil
	}
	if !errors.Is(err, idempotency.ErrNotFound) {
		s.log.Warn("idempotency store read failed", zap.Error(err))
		// Non-fatal: proceed without idempotency protection.
	}

	// ── Build NormalizedRecord ───────────────────────────────────────────────
	rec := s.protoToRecord(req)

	// ── Run pipeline ─────────────────────────────────────────────────────────
	if err := s.pipeline.Run(ctx, rec); err != nil {
		s.log.Warn("pipeline failed", zap.Error(err),
			zap.String("idem_key", req.IdempotencyKey),
			zap.String("source", src))

		// Route to DLQ.
		var stageErr *pipeline.StageError
		stage, reason := "unknown", "pipeline_error"
		if errors.As(err, &stageErr) {
			stage = stageErr.Stage
			reason = stageErr.Reason
		}
		_ = s.dlq.Enqueue(ctx, &dlq.Entry{
			IdempotencyKey: rec.IdempotencyKey,
			TransactionRef: rec.TransactionRef,
			SourceSystem:   rec.SourceSystem,
			AdapterType:    string(rec.AdapterType),
			RawPayload:     rec.RawPayload,
			ErrorStage:     stage,
			ErrorReason:    reason,
			ErrorMessage:   err.Error(),
		})

		metrics.RecordsIngested.WithLabelValues(src, string(pipeline.AdapterGRPC), metrics.StatusFailed).Inc()
		metrics.ValidationFailures.WithLabelValues(src, reason).Inc()

		errResp := &ingestionpb.IngestResponse{
			Success: false,
			Error: &commonpb.ErrorResponse{
				Code:    reason,
				Message: err.Error(),
			},
		}
		return errResp, nil
	}

	// ── Persist to storage ────────────────────────────────────────────────────
	if err := s.storeRecord(ctx, rec); err != nil {
		s.log.Error("storage failed", zap.Error(err), zap.String("internal_id", rec.InternalID))
		// Storage failure → DLQ.
		_ = s.dlq.Enqueue(ctx, &dlq.Entry{
			IdempotencyKey: rec.IdempotencyKey,
			TransactionRef: rec.TransactionRef,
			SourceSystem:   rec.SourceSystem,
			AdapterType:    string(rec.AdapterType),
			RawPayload:     rec.RawPayload,
			ErrorStage:     "storage",
			ErrorReason:    "db_error",
			ErrorMessage:   err.Error(),
		})
		metrics.RecordsIngested.WithLabelValues(src, string(pipeline.AdapterGRPC), metrics.StatusDLQ).Inc()
		return nil, status.Errorf(codes.Internal, "storage error: %s", err.Error())
	}

	// ── Cache response for idempotency ───────────────────────────────────────
	resp := &ingestionpb.IngestResponse{InternalId: rec.InternalID, Success: true}
	_ = s.idempStore.Set(ctx, req.IdempotencyKey, src, &idempotency.CachedResponse{
		InternalID: rec.InternalID,
		Success:    true,
		StoredAt:   time.Now(),
	})

	metrics.RecordsIngested.WithLabelValues(src, string(pipeline.AdapterGRPC), metrics.StatusSuccess).Inc()
	metrics.PayloadSizeBytes.WithLabelValues(src).Observe(float64(len(req.Payload)))

	s.log.Info("record ingested",
		zap.String("internal_id", rec.InternalID),
		zap.String("tx_ref", rec.TransactionRef),
		zap.String("source", src))

	return resp, nil
}

// BulkStreamIngest handles client-streaming of multiple records.
// Clients stream IngestRequests; server responds once with BulkSummary.
// Natural backpressure: blocking send to bounded internal channel pauses the stream.
func (s *IngestionServer) BulkStreamIngest(
	stream ingestionpb.IngestionService_BulkStreamIngestServer,
) error {
	metrics.ActiveConnections.Inc()
	defer metrics.ActiveConnections.Dec()

	ctx := stream.Context()
	start := time.Now()

	var processed, failed int32

	for {
		req, err := stream.Recv()
		if err != nil {
			// io.EOF = client finished streaming normally.
			break
		}

		src := ""
		if req.Metadata != nil {
			src = req.Metadata.SourceSystem
		}

		// Process via the same SubmitRecord logic (reuse idempotency + pipeline).
		resp, grpcErr := s.SubmitRecord(ctx, req)
		if grpcErr != nil {
			failed++
			metrics.BulkStreamRecords.WithLabelValues(src, metrics.StatusFailed).Inc()
			continue
		}
		if resp != nil && !resp.Success {
			failed++
			metrics.BulkStreamRecords.WithLabelValues(src, metrics.StatusFailed).Inc()
			continue
		}
		processed++
		metrics.BulkStreamRecords.WithLabelValues(src, metrics.StatusSuccess).Inc()
	}

	durationMs := time.Since(start).Milliseconds()
	s.log.Info("bulk stream completed",
		zap.Int32("processed", processed),
		zap.Int32("failed", failed),
		zap.Int64("duration_ms", durationMs))

	return stream.SendAndClose(&ingestionpb.BulkSummary{
		TotalProcessed: processed,
		TotalFailed:    failed,
		DurationMs:     durationMs,
	})
}

// protoToRecord converts an IngestRequest proto to a NormalizedRecord skeleton.
func (s *IngestionServer) protoToRecord(req *ingestionpb.IngestRequest) *pipeline.NormalizedRecord {
	rec := &pipeline.NormalizedRecord{
		InternalID:     uuid.New().String(),
		IdempotencyKey: req.IdempotencyKey,
		TransactionRef: req.TransactionRef,
		RawPayload:     req.Payload,
		AdapterType:    pipeline.AdapterGRPC,
		Tags:           make(map[string]string),
	}
	if req.Metadata != nil {
		rec.SourceSystem = req.Metadata.SourceSystem
		rec.TraceID = req.Metadata.TraceId
		for k, v := range req.Metadata.Tags {
			rec.Tags[k] = v
		}
	}
	return rec
}

// storeRecord persists a successfully processed NormalizedRecord.
func (s *IngestionServer) storeRecord(ctx context.Context, rec *pipeline.NormalizedRecord) error {
	return s.store.Insert(ctx, &storage.Record{
		InternalID:       rec.InternalID,
		IdempotencyKey:   rec.IdempotencyKey,
		TransactionRef:   rec.TransactionRef,
		SourceSystem:     rec.SourceSystem,
		AdapterType:      string(rec.AdapterType),
		Amount:           rec.Amount.String(),
		Currency:         rec.Currency,
		RecordTimestamp:  rec.RecordTimestamp,
		ServerReceivedAt: rec.ServerReceivedAt,
		RawPayload:       rec.RawPayload,
		PayloadSchema:    rec.PayloadSchema,
		Tags:             rec.Tags,
		TraceID:          rec.TraceID,
		Status:           "PENDING",
	})
}

// cachedToResponse converts a stored idempotency response to the proto type.
func cachedToResponse(c *idempotency.CachedResponse) *ingestionpb.IngestResponse {
	resp := &ingestionpb.IngestResponse{
		InternalId: c.InternalID,
		Success:    c.Success,
	}
	if c.ErrorCode != "" {
		resp.Error = &commonpb.ErrorResponse{
			Code:    c.ErrorCode,
			Message: c.ErrorMsg,
		}
	}
	return resp
}
