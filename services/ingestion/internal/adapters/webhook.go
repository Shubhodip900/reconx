// Webhook Adapter — receives HTTP POST payloads from upstream systems.
// This is the most common integration pattern for modern SaaS platforms
// (e.g., Stripe webhooks, vendor portal callbacks).
//
// The adapter exposes an HTTP handler at /ingest/{source_system} that
// accepts individual records or JSON arrays. Records are pushed directly
// into the shared pipeline channel.
package adapters

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/reconx/services/ingestion/internal/pipeline"
)

// WebhookConfig configures the webhook HTTP receiver.
type WebhookConfig struct {
	// MaxBodyBytes is the maximum accepted request body size.
	MaxBodyBytes int64
}

// WebhookHandler is an http.Handler that receives and queues ingestion records.
type WebhookHandler struct {
	cfg WebhookConfig
	out chan<- *pipeline.NormalizedRecord
	log *zap.Logger
}

// NewWebhookHandler creates a WebhookHandler that pushes records to out.
func NewWebhookHandler(cfg WebhookConfig, out chan<- *pipeline.NormalizedRecord, log *zap.Logger) *WebhookHandler {
	if cfg.MaxBodyBytes == 0 {
		cfg.MaxBodyBytes = 10 * 1024 * 1024 // 10 MB default
	}
	return &WebhookHandler{cfg: cfg, out: out, log: log}
}

// ServeHTTP handles POST /ingest/{source_system}
// Accepts:
//   - Single JSON object: {"idempotency_key": "...", "transaction_ref": "...", ...}
//   - JSON array: [{...}, {...}]
//   - NDJSON: one JSON object per line
func (h *WebhookHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// Extract source_system from URL path: /ingest/{source_system}
	sourceSystem := r.PathValue("source_system")
	if sourceSystem == "" {
		sourceSystem = r.Header.Get("X-Source-System")
	}
	if sourceSystem == "" {
		http.Error(w, "source_system required in path or X-Source-System header", http.StatusBadRequest)
		return
	}

	traceID := r.Header.Get("X-Trace-Id")
	if traceID == "" {
		traceID = uuid.New().String()
	}

	body := io.LimitReader(r.Body, h.cfg.MaxBodyBytes)
	defer r.Body.Close()

	count, err := h.parseAndQueue(r.Context(), sourceSystem, traceID, body)
	if err != nil {
		h.log.Warn("webhook parse error", zap.Error(err), zap.String("source", sourceSystem))
		http.Error(w, fmt.Sprintf("parse error: %s", err.Error()), http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"queued":    count,
		"trace_id":  traceID,
	})
}

// parseAndQueue reads records from the body and sends them to the pipeline channel.
// Supports: JSON object, JSON array, NDJSON.
func (h *WebhookHandler) parseAndQueue(ctx context.Context, sourceSystem, traceID string, body io.Reader) (int, error) {
	data, err := io.ReadAll(body)
	if err != nil {
		return 0, fmt.Errorf("read body: %w", err)
	}
	if len(data) == 0 {
		return 0, nil
	}

	var count int

	// Try JSON array first.
	if data[0] == '[' {
		var arr []json.RawMessage
		if err := json.Unmarshal(data, &arr); err != nil {
			return 0, fmt.Errorf("json array: %w", err)
		}
		for _, item := range arr {
			if err := h.enqueue(ctx, sourceSystem, traceID, item); err != nil {
				return count, err
			}
			count++
		}
		return count, nil
	}

	// Try single JSON object.
	if data[0] == '{' {
		if err := h.enqueue(ctx, sourceSystem, traceID, data); err != nil {
			return 0, err
		}
		return 1, nil
	}

	// Fall back to NDJSON.
	scanner := bufio.NewScanner(io.MultiReader())
	scanner = bufio.NewScanner(newBytesReader(data))
	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		raw := make([]byte, len(line))
		copy(raw, line)
		if err := h.enqueue(ctx, sourceSystem, traceID, raw); err != nil {
			return count, err
		}
		count++
	}
	return count, scanner.Err()
}

func (h *WebhookHandler) enqueue(ctx context.Context, sourceSystem, traceID string, raw json.RawMessage) error {
	var partial struct {
		IdempotencyKey string `json:"idempotency_key"`
		TransactionRef string `json:"transaction_ref"`
	}
	_ = json.Unmarshal(raw, &partial)

	idempKey := partial.IdempotencyKey
	if idempKey == "" {
		idempKey = uuid.New().String()
	}

	payload := make([]byte, len(raw))
	copy(payload, raw)

	rec := &pipeline.NormalizedRecord{
		IdempotencyKey: idempKey,
		TransactionRef: partial.TransactionRef,
		SourceSystem:   sourceSystem,
		AdapterType:    pipeline.AdapterWebhook,
		TraceID:        traceID,
		RawPayload:     payload,
	}

	select {
	case h.out <- rec:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// newBytesReader is a simple wrapper to avoid importing bytes in the loop.
func newBytesReader(b []byte) io.Reader {
	return &bytesReader{data: b}
}

type bytesReader struct {
	data []byte
	pos  int
}

func (b *bytesReader) Read(p []byte) (n int, err error) {
	if b.pos >= len(b.data) {
		return 0, io.EOF
	}
	n = copy(p, b.data[b.pos:])
	b.pos += n
	return n, nil
}
