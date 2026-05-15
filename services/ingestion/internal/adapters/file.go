// File Adapter — parses uploaded NDJSON or CSV files.
// Used for batch ingestion from legacy systems that export flat files:
//   - ERP exports (SAP, Oracle)
//   - Bank statement files
//   - Vendor data dumps
//
// Files are delivered via the HTTP multipart upload endpoint at POST /ingest/file.
// Each record in the file becomes a NormalizedRecord in the pipeline.
package adapters

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/reconx/services/ingestion/internal/pipeline"
)

// FileConfig configures the file upload adapter.
type FileConfig struct {
	// MaxFileSizeBytes is the maximum allowed file upload size.
	MaxFileSizeBytes int64

	// AllowedExtensions restricts accepted file types.
	// Empty = allow all. Example: []string{".ndjson", ".json", ".csv"}.
	AllowedExtensions []string
}

// FileHandler is an http.Handler that accepts multipart file uploads.
type FileHandler struct {
	cfg FileConfig
	out chan<- *pipeline.NormalizedRecord
	log *zap.Logger
}

// NewFileHandler creates a FileHandler that pushes records to out.
func NewFileHandler(cfg FileConfig, out chan<- *pipeline.NormalizedRecord, log *zap.Logger) *FileHandler {
	if cfg.MaxFileSizeBytes == 0 {
		cfg.MaxFileSizeBytes = 100 * 1024 * 1024 // 100 MB default
	}
	return &FileHandler{cfg: cfg, out: out, log: log}
}

// ServeHTTP handles POST /ingest/file
// Form fields:
//   - file       (required) – the file to ingest
//   - source_system (required) – the originating system label
func (h *FileHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, h.cfg.MaxFileSizeBytes)
	if err := r.ParseMultipartForm(32 * 1024 * 1024); err != nil {
		http.Error(w, "request too large or not multipart", http.StatusBadRequest)
		return
	}

	sourceSystem := r.FormValue("source_system")
	if sourceSystem == "" {
		http.Error(w, "source_system form field required", http.StatusBadRequest)
		return
	}

	file, header, err := r.FormFile("file")
	if err != nil {
		http.Error(w, "file field required", http.StatusBadRequest)
		return
	}
	defer file.Close()

	ext := strings.ToLower(filepath.Ext(header.Filename))
	if len(h.cfg.AllowedExtensions) > 0 && !h.allowedExt(ext) {
		http.Error(w, fmt.Sprintf("file extension %q not allowed", ext), http.StatusBadRequest)
		return
	}

	start := time.Now()
	count, err := h.parseFile(r.Context(), ext, sourceSystem, file)
	if err != nil {
		h.log.Warn("file parse error", zap.Error(err), zap.String("file", header.Filename))
		http.Error(w, fmt.Sprintf("parse error: %s", err.Error()), http.StatusUnprocessableEntity)
		return
	}

	h.log.Info("file ingested",
		zap.String("file", header.Filename),
		zap.String("source", sourceSystem),
		zap.Int("records", count),
		zap.Duration("duration", time.Since(start)))

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusAccepted)
	_ = json.NewEncoder(w).Encode(map[string]interface{}{
		"file":    header.Filename,
		"queued":  count,
		"source":  sourceSystem,
	})
}

// parseFile routes to the correct parser based on file extension.
func (h *FileHandler) parseFile(ctx context.Context, ext, sourceSystem string, r io.Reader) (int, error) {
	switch ext {
	case ".csv":
		return h.parseCSV(ctx, sourceSystem, r)
	default:
		// Treat everything else as NDJSON / JSON.
		return h.parseNDJSON(ctx, sourceSystem, r)
	}
}

// parseNDJSON processes newline-delimited JSON (one object per line).
func (h *FileHandler) parseNDJSON(ctx context.Context, sourceSystem string, r io.Reader) (int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 1*1024*1024), 1*1024*1024)
	count := 0

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var partial struct {
			IdempotencyKey string `json:"idempotency_key"`
			TransactionRef string `json:"transaction_ref"`
		}
		_ = json.Unmarshal(line, &partial)

		idempKey := partial.IdempotencyKey
		if idempKey == "" {
			idempKey = uuid.New().String()
		}

		payload := make([]byte, len(line))
		copy(payload, line)

		rec := &pipeline.NormalizedRecord{
			IdempotencyKey: idempKey,
			TransactionRef: partial.TransactionRef,
			SourceSystem:   sourceSystem,
			AdapterType:    pipeline.AdapterFile,
			RawPayload:     payload,
		}

		select {
		case h.out <- rec:
			count++
		case <-ctx.Done():
			return count, ctx.Err()
		}
	}
	return count, scanner.Err()
}

// parseCSV processes a CSV file where the header row names the fields.
// Required columns: idempotency_key, transaction_ref
// Optional columns: amount, currency, event_time, source_system (overrides arg)
func (h *FileHandler) parseCSV(ctx context.Context, sourceSystem string, r io.Reader) (int, error) {
	cr := csv.NewReader(r)
	cr.TrimLeadingSpace = true

	headers, err := cr.Read()
	if err != nil {
		return 0, fmt.Errorf("read CSV header: %w", err)
	}
	colIndex := make(map[string]int, len(headers))
	for i, h := range headers {
		colIndex[strings.ToLower(strings.TrimSpace(h))] = i
	}

	count := 0
	for {
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return count, fmt.Errorf("csv read row: %w", err)
		}

		// Build a JSON object from columns for uniform downstream processing.
		obj := make(map[string]string, len(headers))
		for col, idx := range colIndex {
			if idx < len(row) {
				obj[col] = row[idx]
			}
		}

		idempKey := obj["idempotency_key"]
		if idempKey == "" {
			idempKey = uuid.New().String()
		}
		txRef := obj["transaction_ref"]

		// Encode row as JSON for the pipeline.
		payload, _ := json.Marshal(obj)

		rec := &pipeline.NormalizedRecord{
			IdempotencyKey: idempKey,
			TransactionRef: txRef,
			SourceSystem:   sourceSystem,
			AdapterType:    pipeline.AdapterFile,
			RawPayload:     payload,
			PayloadSchema:  "csv.v1",
		}

		select {
		case h.out <- rec:
			count++
		case <-ctx.Done():
			return count, ctx.Err()
		}
	}
	return count, nil
}

// allowedExt checks if the extension is in the allowed list.
func (h *FileHandler) allowedExt(ext string) bool {
	for _, a := range h.cfg.AllowedExtensions {
		if a == ext {
			return true
		}
	}
	return false
}

// multipartFileWrapper wraps multipart.File to satisfy the interface cleanly.
type multipartFileWrapper struct {
	multipart.File
}
