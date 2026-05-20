// Package server implements the ResolutionService gRPC server.
//
// RPCs:
//   - ResolveManually  — human picks the winning source for a MISMATCHED transaction
//   - ListMismatches   — server-side streaming of all MISMATCHED transactions
package server

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb     "github.com/reconx/proto/gen/go/common"
	enginepb     "github.com/reconx/proto/gen/go/engine"
	resolutionpb "github.com/reconx/proto/gen/go/resolution"
	"github.com/reconx/services/resolution/internal/db"
	"github.com/reconx/services/resolution/internal/metrics"
)

// ResolutionServer implements resolutionpb.ResolutionServiceServer.
type ResolutionServer struct {
	resolutionpb.UnimplementedResolutionServiceServer
	db  *sql.DB
	log *zap.Logger
}

// New creates a ResolutionServer ready to be registered with a gRPC server.
func New(database *sql.DB, log *zap.Logger) *ResolutionServer {
	return &ResolutionServer{db: database, log: log}
}

// ResolveManually handles a human override for a MISMATCHED transaction.
//
// Steps:
//  1. Validate input fields
//  2. Confirm the transaction exists and is MISMATCHED
//  3. Insert/update resolution_records
//  4. Update recon_state → RESOLVED
//  5. Append audit log entry
//  6. Return success + new status
func (s *ResolutionServer) ResolveManually(
	ctx context.Context,
	req *resolutionpb.ResolutionRequest,
) (*resolutionpb.ResolutionResponse, error) {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("ResolveManually").Observe(time.Since(start).Seconds())
	}()

	// ── Validation ───────────────────────────────────────────────────────────
	if req.TransactionRef == "" {
		return nil, status.Error(codes.InvalidArgument, "transaction_ref is required")
	}
	if req.ChosenSource == "" {
		return nil, status.Error(codes.InvalidArgument, "chosen_source is required")
	}
	if req.ResolverId == "" {
		return nil, status.Error(codes.InvalidArgument, "resolver_id is required")
	}

	s.log.Info("ResolveManually called",
		zap.String("transaction_ref", req.TransactionRef),
		zap.String("chosen_source", req.ChosenSource),
		zap.String("resolver_id", req.ResolverId),
	)

	// ── Fetch current state ───────────────────────────────────────────────────
	reconState, err := db.GetReconState(ctx, s.db, req.TransactionRef)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, status.Errorf(codes.NotFound, "transaction %q not found", req.TransactionRef)
		}
		s.log.Error("GetReconState failed", zap.Error(err))
		metrics.ResolutionErrorsTotal.WithLabelValues("db_error").Inc()
		return nil, status.Error(codes.Internal, "database error")
	}

	// Guard: only MISMATCHED (or already RESOLVED for idempotency) transactions
	// can be resolved. Reject PENDING or MATCHED.
	if reconState.Status == "PENDING" || reconState.Status == "MATCHED" {
		return nil, status.Errorf(codes.FailedPrecondition,
			"transaction %q is %s and cannot be manually resolved",
			req.TransactionRef, reconState.Status,
		)
	}

	oldStatus := reconState.Status

	// ── Write resolution record ────────────────────────────────────────────────
	if err := db.InsertResolutionRecord(ctx, s.db,
		req.TransactionRef,
		"MANUAL",
		req.ChosenSource,
		req.ResolutionReason,
		req.ResolverId,
		"", // no auto-resolve strategy for manual resolutions
	); err != nil {
		s.log.Error("InsertResolutionRecord failed", zap.Error(err))
		metrics.ResolutionErrorsTotal.WithLabelValues("db_error").Inc()
		return nil, status.Error(codes.Internal, "failed to persist resolution")
	}

	// ── Update recon_state to RESOLVED ─────────────────────────────────────────
	if err := db.UpdateReconStateToResolved(ctx, s.db, req.TransactionRef); err != nil {
		s.log.Error("UpdateReconStateToResolved failed", zap.Error(err))
		metrics.ResolutionErrorsTotal.WithLabelValues("db_error").Inc()
		return nil, status.Error(codes.Internal, "failed to update recon state")
	}

	// ── Stop any pending retry attempts ────────────────────────────────────────
	// If the transaction was in the retry queue (PENDING or EXHAUSTED), mark it
	// RESOLVED so the worker does not attempt further re-matching. Fire-and-forget.
	_ = db.MarkRetryResolved(ctx, s.db, req.TransactionRef)

	// ── Audit log ───────────────────────────────────────────────────────────────
	_ = db.InsertAuditLog(ctx, s.db,
		req.TransactionRef,
		"MANUAL_RESOLUTION",
		oldStatus,
		"RESOLVED",
		map[string]string{
			"chosen_source":     req.ChosenSource,
			"resolver_id":       req.ResolverId,
			"resolution_reason": req.ResolutionReason,
		},
	)

	s.log.Info("transaction resolved",
		zap.String("transaction_ref", req.TransactionRef),
		zap.String("old_status", oldStatus),
		zap.String("resolver_id", req.ResolverId),
	)
	metrics.ResolutionsTotal.WithLabelValues(req.ResolverId).Inc()

	return &resolutionpb.ResolutionResponse{
		Success:   true,
		NewStatus: commonpb.ReconStatus_RESOLVED,
	}, nil
}

// ListMismatches streams all MISMATCHED transactions to the caller.
// Supports cursor-based pagination via FilterRequest.page_token (the
// transaction_ref of the last item already received).
func (s *ResolutionServer) ListMismatches(
	req *resolutionpb.FilterRequest,
	stream resolutionpb.ResolutionService_ListMismatchesServer,
) error {
	start := time.Now()
	defer func() {
		metrics.RPCDuration.WithLabelValues("ListMismatches").Observe(time.Since(start).Seconds())
	}()

	s.log.Info("ListMismatches called",
		zap.Int32("page_size", req.PageSize),
		zap.String("page_token", req.PageToken),
		zap.String("source_filter", req.SourceFilter),
	)

	rows, err := db.ListMismatched(
		stream.Context(),
		s.db,
		req.PageSize,
		req.PageToken,
		req.SourceFilter,
	)
	if err != nil {
		s.log.Error("ListMismatched query failed", zap.Error(err))
		return status.Error(codes.Internal, "database error")
	}

	for _, row := range rows {
		details := make([]*enginepb.MatchDetail, 0, len(row.Details))
		for _, d := range row.Details {
			details = append(details, &enginepb.MatchDetail{
				SystemName:       d.SystemName,
				DiscrepancyFound: d.DiscrepancyFound,
				// data_captured not stored in recon_match_details; left empty here.
			})
		}

		resp := &enginepb.StateResponse{
			TransactionRef: row.TransactionRef,
			Status:         commonpb.ReconStatus_MISMATCHED,
			Details:        details,
			LastUpdated:    row.LastUpdated.UnixMilli(),
		}

		if err := stream.Send(resp); err != nil {
			return fmt.Errorf("stream send: %w", err)
		}
		metrics.ListMismatchesStreamed.Inc()
	}

	s.log.Info("ListMismatches complete", zap.Int("sent", len(rows)))
	return nil
}
