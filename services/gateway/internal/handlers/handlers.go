// Package handlers implements the HTTP request handlers for the ReconX API Gateway.
//
// REST API surface:
//
//	POST   /v1/ingest                          → Ingestion.SubmitRecord
//	GET    /v1/recon/{transaction_ref}          → Engine.GetReconState
//	POST   /v1/recon/{transaction_ref}/retrigger→ Engine.ReTriggerMatch
//	POST   /v1/resolution/{transaction_ref}     → Resolution.ResolveManually
//	GET    /v1/resolution/mismatches            → Resolution.ListMismatches (stream → JSON array)
//	GET    /health                              → aggregate health
package handlers

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb     "github.com/reconx/proto/gen/go/common"
	ingestionpb  "github.com/reconx/proto/gen/go/ingestion"
	resolutionpb "github.com/reconx/proto/gen/go/resolution"
	enginepb     "github.com/reconx/proto/gen/go/engine"
	"github.com/reconx/services/gateway/internal/clients"
	"github.com/reconx/services/gateway/internal/metrics"
)

// Handler bundles the HTTP mux and upstream clients.
type Handler struct {
	clients *clients.Clients
	log     *zap.Logger
}

// New returns an HTTP mux pre-wired with all gateway routes.
func New(c *clients.Clients, log *zap.Logger) http.Handler {
	h := &Handler{clients: c, log: log}
	mux := http.NewServeMux()

	// Ingestion
	mux.HandleFunc("POST /v1/ingest", h.submitRecord)

	// Engine
	mux.HandleFunc("GET /v1/recon/{transaction_ref}", h.getReconState)
	mux.HandleFunc("POST /v1/recon/{transaction_ref}/retrigger", h.retriggerMatch)

	// Resolution
	mux.HandleFunc("POST /v1/resolution/{transaction_ref}", h.resolveManually)
	mux.HandleFunc("GET /v1/resolution/mismatches", h.listMismatches)

	// Health
	mux.HandleFunc("GET /health", h.health)

	return instrument(mux)
}

// ── Instrumentation middleware ────────────────────────────────────────────────

type responseWriter struct {
	http.ResponseWriter
	statusCode int
}

func (rw *responseWriter) WriteHeader(code int) {
	rw.statusCode = code
	rw.ResponseWriter.WriteHeader(code)
}

func instrument(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rw := &responseWriter{ResponseWriter: w, statusCode: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rw, r)
		dur := time.Since(start).Seconds()
		path := r.URL.Path
		code := strconv.Itoa(rw.statusCode)
		metrics.HTTPRequestsTotal.WithLabelValues(r.Method, path, code).Inc()
		metrics.HTTPDuration.WithLabelValues(r.Method, path).Observe(dur)
	})
}

// ── Helper utilities ──────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, code int, msg string) {
	writeJSON(w, code, map[string]string{"error": msg})
}

// grpcToHTTP converts a gRPC status code to an appropriate HTTP status code.
func grpcToHTTP(err error) int {
	s, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError
	}
	switch s.Code() {
	case codes.NotFound:
		return http.StatusNotFound
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.FailedPrecondition:
		return http.StatusConflict
	case codes.AlreadyExists:
		return http.StatusConflict
	case codes.Unauthenticated:
		return http.StatusUnauthorized
	case codes.PermissionDenied:
		return http.StatusForbidden
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable:
		return http.StatusServiceUnavailable
	default:
		return http.StatusInternalServerError
	}
}

func grpcMessage(err error) string {
	if s, ok := status.FromError(err); ok {
		return s.Message()
	}
	return err.Error()
}

// ── POST /v1/ingest ───────────────────────────────────────────────────────────

type ingestRequestBody struct {
	IdempotencyKey string            `json:"idempotency_key"`
	TransactionRef string            `json:"transaction_ref"`
	Payload        []byte            `json:"payload"`
	SourceSystem   string            `json:"source_system"`
	TraceID        string            `json:"trace_id"`
	Tags           map[string]string `json:"tags"`
}

func (h *Handler) submitRecord(w http.ResponseWriter, r *http.Request) {
	var body ingestRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	resp, err := h.clients.Ingestion.SubmitRecord(r.Context(), &ingestionpb.IngestRequest{
		IdempotencyKey: body.IdempotencyKey,
		TransactionRef: body.TransactionRef,
		Payload:        body.Payload,
		Metadata: &commonpb.Metadata{
			SourceSystem: body.SourceSystem,
			TraceId:      body.TraceID,
			Tags:         body.Tags,
			IngestedAt:   time.Now().UnixMilli(),
		},
	})
	if err != nil {
		h.log.Warn("submitRecord upstream error", zap.Error(err))
		metrics.UpstreamErrors.WithLabelValues("ingestion").Inc()
		writeError(w, grpcToHTTP(err), grpcMessage(err))
		return
	}

	writeJSON(w, http.StatusAccepted, map[string]any{
		"internal_id": resp.InternalId,
		"success":     resp.Success,
	})
}

// ── GET /v1/recon/{transaction_ref} ──────────────────────────────────────────

func (h *Handler) getReconState(w http.ResponseWriter, r *http.Request) {
	txRef := r.PathValue("transaction_ref")
	if txRef == "" {
		writeError(w, http.StatusBadRequest, "transaction_ref is required")
		return
	}

	resp, err := h.clients.Engine.GetReconState(r.Context(),
		&enginepb.StateRequest{TransactionRef: txRef})
	if err != nil {
		h.log.Warn("getReconState upstream error", zap.Error(err), zap.String("tx_ref", txRef))
		metrics.UpstreamErrors.WithLabelValues("engine").Inc()
		writeError(w, grpcToHTTP(err), grpcMessage(err))
		return
	}

	writeJSON(w, http.StatusOK, stateResponseJSON(resp))
}

// ── POST /v1/recon/{transaction_ref}/retrigger ────────────────────────────────

func (h *Handler) retriggerMatch(w http.ResponseWriter, r *http.Request) {
	txRef := r.PathValue("transaction_ref")
	if txRef == "" {
		writeError(w, http.StatusBadRequest, "transaction_ref is required")
		return
	}

	resp, err := h.clients.Engine.ReTriggerMatch(r.Context(),
		&enginepb.StateRequest{TransactionRef: txRef})
	if err != nil {
		h.log.Warn("retriggerMatch upstream error", zap.Error(err), zap.String("tx_ref", txRef))
		metrics.UpstreamErrors.WithLabelValues("engine").Inc()
		writeError(w, grpcToHTTP(err), grpcMessage(err))
		return
	}

	writeJSON(w, http.StatusOK, stateResponseJSON(resp))
}

// ── POST /v1/resolution/{transaction_ref} ─────────────────────────────────────

type resolveRequestBody struct {
	ChosenSource     string `json:"chosen_source"`
	ResolutionReason string `json:"resolution_reason"`
	ResolverID       string `json:"resolver_id"`
}

func (h *Handler) resolveManually(w http.ResponseWriter, r *http.Request) {
	txRef := r.PathValue("transaction_ref")
	if txRef == "" {
		writeError(w, http.StatusBadRequest, "transaction_ref is required")
		return
	}

	var body resolveRequestBody
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeError(w, http.StatusBadRequest, "invalid JSON body")
		return
	}

	resp, err := h.clients.Resolution.ResolveManually(r.Context(), &resolutionpb.ResolutionRequest{
		TransactionRef:   txRef,
		ChosenSource:     body.ChosenSource,
		ResolutionReason: body.ResolutionReason,
		ResolverId:       body.ResolverID,
	})
	if err != nil {
		h.log.Warn("resolveManually upstream error", zap.Error(err), zap.String("tx_ref", txRef))
		metrics.UpstreamErrors.WithLabelValues("resolution").Inc()
		writeError(w, grpcToHTTP(err), grpcMessage(err))
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"success":    resp.Success,
		"new_status": resp.NewStatus.String(),
	})
}

// ── GET /v1/resolution/mismatches ─────────────────────────────────────────────

func (h *Handler) listMismatches(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	pageSize, _ := strconv.Atoi(q.Get("page_size"))
	pageToken := q.Get("page_token")
	sourceFilter := q.Get("source_filter")

	stream, err := h.clients.Resolution.ListMismatches(r.Context(), &resolutionpb.FilterRequest{
		PageSize:     int32(pageSize),
		PageToken:    pageToken,
		SourceFilter: sourceFilter,
	})
	if err != nil {
		h.log.Warn("listMismatches upstream error", zap.Error(err))
		metrics.UpstreamErrors.WithLabelValues("resolution").Inc()
		writeError(w, grpcToHTTP(err), grpcMessage(err))
		return
	}

	// Buffer the stream into a JSON array.
	var items []any
	for {
		msg, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			h.log.Warn("listMismatches stream recv error", zap.Error(err))
			if len(items) == 0 {
				writeError(w, http.StatusInternalServerError, "stream error")
				return
			}
			break // partial result — still write what we have
		}
		items = append(items, stateResponseJSON(msg))
	}

	if items == nil {
		items = []any{} // never return null
	}

	// Determine next page token (last transaction_ref in the batch).
	nextToken := ""
	if len(items) > 0 {
		if last, ok := items[len(items)-1].(map[string]any); ok {
			nextToken, _ = last["transaction_ref"].(string)
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"items":           items,
		"next_page_token": nextToken,
	})
}

// ── GET /health ───────────────────────────────────────────────────────────────

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"status":  "ok",
		"service": "reconx-gateway",
	})
}

// ── Shared serialisation helpers ──────────────────────────────────────────────

func stateResponseJSON(resp *enginepb.StateResponse) map[string]any {
	details := make([]map[string]any, 0, len(resp.Details))
	for _, d := range resp.Details {
		details = append(details, map[string]any{
			"system_name":       d.SystemName,
			"discrepancy_found": d.DiscrepancyFound,
		})
	}

	statusName := strings.TrimPrefix(resp.Status.String(), "STATUS_")

	return map[string]any{
		"transaction_ref": resp.TransactionRef,
		"status":          statusName,
		"details":         details,
		"last_updated_ms": resp.LastUpdated,
	}
}
