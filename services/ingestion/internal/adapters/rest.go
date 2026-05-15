// REST Poller Adapter — periodically pulls records from an HTTP endpoint.
// This pattern is used for legacy systems that do not support webhooks or Kafka.
// The endpoint must return NDJSON (newline-delimited JSON) or a JSON array.
package adapters

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
	"go.uber.org/zap"

	"github.com/reconx/services/ingestion/internal/pipeline"
)

// RESTPollerConfig configures a REST polling adapter.
type RESTPollerConfig struct {
	// ID uniquely identifies this adapter instance.
	ID string

	// SourceSystem is the reconx source_system label for records from this endpoint.
	SourceSystem string

	// URL is the endpoint to poll.
	URL string

	// PollInterval determines how frequently the endpoint is polled.
	PollInterval time.Duration

	// Headers are additional HTTP headers to send (e.g., Authorization).
	Headers map[string]string

	// TimeoutPerRequest is the HTTP client timeout per poll cycle.
	TimeoutPerRequest time.Duration
}

// RESTPoller polls an HTTP endpoint on a fixed interval and emits records.
type RESTPoller struct {
	cfg    RESTPollerConfig
	client *http.Client
	log    *zap.Logger
}

// NewRESTPoller creates a RESTPoller with the given configuration.
func NewRESTPoller(cfg RESTPollerConfig, log *zap.Logger) *RESTPoller {
	timeout := cfg.TimeoutPerRequest
	if timeout == 0 {
		timeout = 30 * time.Second
	}
	return &RESTPoller{
		cfg: cfg,
		client: &http.Client{
			Timeout: timeout,
		},
		log: log.With(zap.String("adapter", cfg.ID)),
	}
}

func (r *RESTPoller) ID() string                     { return r.cfg.ID }
func (r *RESTPoller) AdapterType() pipeline.AdapterType { return pipeline.AdapterREST }

// Start polls the configured URL every PollInterval until ctx is cancelled.
func (r *RESTPoller) Start(ctx context.Context, out chan<- *pipeline.NormalizedRecord) error {
	ticker := time.NewTicker(r.cfg.PollInterval)
	defer ticker.Stop()

	r.log.Info("REST poller started", zap.String("url", r.cfg.URL))
	for {
		select {
		case <-ctx.Done():
			r.log.Info("REST poller stopped")
			return ctx.Err()
		case <-ticker.C:
			if err := r.poll(ctx, out); err != nil {
				r.log.Warn("poll cycle failed", zap.Error(err))
				// Continue — do not crash; transient failures are normal.
			}
		}
	}
}

// poll performs a single HTTP GET and emits any records found.
func (r *RESTPoller) poll(ctx context.Context, out chan<- *pipeline.NormalizedRecord) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.cfg.URL, nil)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	for k, v := range r.cfg.Headers {
		req.Header.Set(k, v)
	}

	resp, err := r.client.Do(req)
	if err != nil {
		return fmt.Errorf("http get: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("upstream returned HTTP %d", resp.StatusCode)
	}

	return r.parseNDJSON(ctx, resp.Body, out)
}

// parseNDJSON reads newline-delimited JSON and emits one record per line.
func (r *RESTPoller) parseNDJSON(ctx context.Context, body io.Reader, out chan<- *pipeline.NormalizedRecord) error {
	scanner := bufio.NewScanner(body)
	scanner.Buffer(make([]byte, 1*1024*1024), 1*1024*1024)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		// Decode just enough to extract idempotency_key and transaction_ref.
		var partial struct {
			IdempotencyKey string `json:"idempotency_key"`
			TransactionRef string `json:"transaction_ref"`
		}
		_ = json.Unmarshal(line, &partial)

		idempKey := partial.IdempotencyKey
		if idempKey == "" {
			idempKey = uuid.New().String() // assign if missing
		}

		raw := make([]byte, len(line))
		copy(raw, line)

		rec := &pipeline.NormalizedRecord{
			IdempotencyKey: idempKey,
			TransactionRef: partial.TransactionRef,
			SourceSystem:   r.cfg.SourceSystem,
			AdapterType:    pipeline.AdapterREST,
			RawPayload:     raw,
		}

		select {
		case out <- rec:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return scanner.Err()
}
