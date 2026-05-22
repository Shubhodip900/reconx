// Package handlers implements the HTTP request handlers for the ReconX API Gateway.
//
// REST API surface (all /v1/* routes require X-API-Key header):
//
//	POST   /v1/ingest                              → Ingestion.SubmitRecord (gRPC)
//	GET    /v1/recon/{transaction_ref}             → Engine.GetReconState (gRPC)
//	POST   /v1/recon/{transaction_ref}/retrigger   → Engine.ReTriggerMatch (gRPC)
//	POST   /v1/resolution/{transaction_ref}        → Resolution.ResolveManually (gRPC)
//	GET    /v1/resolution/mismatches               → Resolution.ListMismatches (gRPC stream → JSON)
//	POST   /v1/resolution/{ref}/auto               → Resolution HTTP /v1/resolve/auto/{ref}
//	POST   /v1/resolution/{ref}/retry              → Resolution HTTP /v1/resolve/retry/{ref}
//	GET    /v1/resolution/{ref}/audit              → Resolution HTTP /v1/resolve/audit/{ref}
//	GET    /v1/resolution/retry-queue              → Resolution HTTP /v1/resolve/retry-queue
//	GET    /health                                 → aggregate health (no auth)
package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	commonpb     "github.com/reconx/proto/gen/go/common"
	enginepb     "github.com/reconx/proto/gen/go/engine"
	ingestionpb  "github.com/reconx/proto/gen/go/ingestion"
	resolutionpb "github.com/reconx/proto/gen/go/resolution"
	"github.com/reconx/services/gateway/internal/clients"
	"github.com/reconx/services/gateway/internal/config"
	"github.com/reconx/services/gateway/internal/metrics"
)

// Handler bundles the HTTP mux and upstream clients.
type Handler struct {
	clients *clients.Clients
	cfg     *config.Config
	log     *zap.Logger
}

// New returns an HTTP mux pre-wired with all gateway routes.
func New(c *clients.Clients, cfg *config.Config, log *zap.Logger) http.Handler {
	h := &Handler{clients: c, cfg: cfg, log: log}
	mux := http.NewServeMux()

	// Ingestion
	mux.HandleFunc("POST /v1/ingest", h.submitRecord)

	// Engine
	mux.HandleFunc("GET /v1/recon/{transaction_ref}", h.getReconState)
	mux.HandleFunc("POST /v1/recon/{transaction_ref}/retrigger", h.retriggerMatch)

	// Resolution — gRPC-backed
	mux.HandleFunc("POST /v1/resolution/{transaction_ref}", h.resolveManually)
	mux.HandleFunc("GET /v1/resolution/mismatches", h.listMismatches)

	// Resolution — HTTP proxy routes (forwarded to resolution service REST API)
	mux.HandleFunc("GET /v1/resolution/retry-queue", h.retryQueueProxy)
	mux.HandleFunc("POST /v1/resolution/{ref}/auto", h.autoResolveProxy)
	mux.HandleFunc("POST /v1/resolution/{ref}/retry", h.retryProxy)
	mux.HandleFunc("GET /v1/resolution/{ref}/audit", h.auditProxy)

	// Health (auth-exempt — handled by middleware before reaching here)
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

// ── Resolution HTTP proxy routes ──────────────────────────────────────────────
//
// These four routes are not available over gRPC — they forward to the
// Resolution Service's HTTP REST API (default :8082).
//
// Gateway path                     → Upstream path
// POST /v1/resolution/{ref}/auto   → POST /v1/resolve/auto/{ref}
// POST /v1/resolution/{ref}/retry  → POST /v1/resolve/retry/{ref}
// GET  /v1/resolution/{ref}/audit  → GET  /v1/resolve/audit/{ref}
// GET  /v1/resolution/retry-queue  → GET  /v1/resolve/retry-queue

// autoResolveProxy handles POST /v1/resolution/{ref}/auto.
func (h *Handler) autoResolveProxy(w http.ResponseWriter, r *http.Request) {
	ref := r.PathValue("ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "ref is required")
		return
	}
	h.proxyToResolution(w, r, "/v1/resolve/auto/"+ref)
}

// retryProxy handles POST /v1/resolution/{ref}/retry.
func (h *Handler) retryProxy(w http.ResponseWriter, r *http.Request) {
	ref := r.PathValue("ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "ref is required")
		return
	}
	h.proxyToResolution(w, r, "/v1/resolve/retry/"+ref)
}

// auditProxy handles GET /v1/resolution/{ref}/audit.
func (h *Handler) auditProxy(w http.ResponseWriter, r *http.Request) {
	ref := r.PathValue("ref")
	if ref == "" {
		writeError(w, http.StatusBadRequest, "ref is required")
		return
	}
	h.proxyToResolution(w, r, "/v1/resolve/audit/"+ref)
}

// retryQueueProxy handles GET /v1/resolution/retry-queue.
func (h *Handler) retryQueueProxy(w http.ResponseWriter, r *http.Request) {
	h.proxyToResolution(w, r, "/v1/resolve/retry-queue")
}

// proxyToResolution forwards the current request to the Resolution Service
// HTTP REST API, rewriting the path to the upstream equivalent and piping the
// response (status code + body) back to the caller unchanged.
func (h *Handler) proxyToResolution(w http.ResponseWriter, r *http.Request, upstreamPath string) {
	target := h.clients.ResolutionHTTP.BaseURL() + upstreamPath
	if r.URL.RawQuery != "" {
		target += "?" + r.URL.RawQuery
	}

	req, err := http.NewRequestWithContext(r.Context(), r.Method, target, r.Body)
	if err != nil {
		h.log.Error("proxyToResolution: build request failed",
			zap.String("path", upstreamPath), zap.Error(err))
		writeError(w, http.StatusInternalServerError, "proxy error")
		return
	}
	if ct := r.Header.Get("Content-Type"); ct != "" {
		req.Header.Set("Content-Type", ct)
	}

	resp, err := h.clients.ResolutionHTTP.Do(req)
	if err != nil {
		h.log.Warn("proxyToResolution: upstream unreachable",
			zap.String("path", upstreamPath), zap.Error(err))
		metrics.UpstreamErrors.WithLabelValues("resolution_http").Inc()
		writeError(w, http.StatusBadGateway, "resolution service unavailable")
		return
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	}
	w.WriteHeader(resp.StatusCode)
	_, _ = io.Copy(w, resp.Body)
}

// ── GET /health ───────────────────────────────────────────────────────────────

// healthHTTP is a shared client for upstream health pings.
// Short timeout — we don't want the gateway health check to block callers.
var healthHTTP = &http.Client{Timeout: 5 * time.Second}

// health handles GET /health.
//
// Pings each upstream's /health endpoint concurrently and returns an aggregate
// status. Returns 200 if all upstreams are healthy, 503 otherwise.
//
//	{
//	  "status": "ok",               // "ok" | "degraded"
//	  "services": {
//	    "ingestion":  "ok",
//	    "engine":     "ok",
//	    "resolution": "ok"
//	  }
//	}
func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	type probe struct {
		name string
		url  string
	}
	probes := []probe{
		{"ingestion", h.cfg.Ingestion.HealthURL},
		{"engine", h.cfg.Engine.HealthURL},
		{"resolution", h.cfg.Resolution.HealthURL},
	}

	type result struct {
		name string
		ok   bool
	}
	results := make([]result, len(probes))

	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for i, p := range probes {
		wg.Add(1)
		go func(i int, name, url string) {
			defer wg.Done()
			results[i] = result{name: name, ok: pingUpstream(ctx, url)}
		}(i, p.name, p.url)
	}
	wg.Wait()

	overall := "ok"
	services := make(map[string]string, len(results))
	for _, res := range results {
		if res.ok {
			services[res.name] = "ok"
		} else {
			services[res.name] = "unhealthy"
			overall = "degraded"
		}
	}

	httpCode := http.StatusOK
	if overall != "ok" {
		httpCode = http.StatusServiceUnavailable
	}

	writeJSON(w, httpCode, map[string]any{
		"status":   overall,
		"services": services,
	})
}

// pingUpstream performs a GET request to url and returns true if it responds
// with HTTP 200. Any error or non-200 response is treated as unhealthy.
func pingUpstream(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := healthHTTP.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// Drain the body so the connection can be reused.
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode == http.StatusOK
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
