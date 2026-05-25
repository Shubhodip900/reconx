package integration

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"
)

// ── Request / Response types ──────────────────────────────────────────────────

type ingestRequest struct {
	IdempotencyKey string            `json:"idempotency_key"`
	TransactionRef string            `json:"transaction_ref"`
	Payload        []byte            `json:"payload"`
	SourceSystem   string            `json:"source_system"`
	TraceID        string            `json:"trace_id,omitempty"`
	Tags           map[string]string `json:"tags,omitempty"`
}

type ingestResponse struct {
	InternalID string `json:"internal_id"`
	Success    bool   `json:"success"`
}

type reconDetail struct {
	SystemName       string `json:"system_name"`
	DiscrepancyFound bool   `json:"discrepancy_found"`
}

type reconState struct {
	TransactionRef string        `json:"transaction_ref"`
	Status         string        `json:"status"`
	Details        []reconDetail `json:"details"`
	LastUpdatedMs  int64         `json:"last_updated_ms"`
}

type resolveManualRequest struct {
	ChosenSource     string `json:"chosen_source"`
	ResolutionReason string `json:"resolution_reason"`
	ResolverID       string `json:"resolver_id"`
}

type resolveManualResponse struct {
	Success   bool   `json:"success"`
	NewStatus string `json:"new_status"`
}

type autoResolveRequest struct {
	Strategy       string `json:"strategy,omitempty"`
	SourcePriority string `json:"source_priority,omitempty"`
	ResolverID     string `json:"resolver_id,omitempty"`
}

type autoResolveResponse struct {
	TransactionRef string `json:"transaction_ref"`
	ChosenSource   string `json:"chosen_source"`
	Strategy       string `json:"strategy"`
	Reason         string `json:"reason"`
	ResolvedAt     string `json:"resolved_at"`
}

type enqueueRetryRequest struct {
	RequestedBy string `json:"requested_by,omitempty"`
	MaxAttempts int    `json:"max_attempts,omitempty"`
}

type enqueueRetryResponse struct {
	TransactionRef string `json:"transaction_ref"`
	MaxAttempts    int    `json:"max_attempts"`
	Message        string `json:"message"`
}

type auditEntry struct {
	ID             int64   `json:"id"`
	TransactionRef string  `json:"transaction_ref"`
	EventType      string  `json:"event_type"`
	OldStatus      *string `json:"old_status,omitempty"`
	NewStatus      *string `json:"new_status,omitempty"`
	Detail         string  `json:"detail"`
	CreatedAt      string  `json:"created_at"`
}

type auditTrailResponse struct {
	TransactionRef string       `json:"transaction_ref"`
	Entries        []auditEntry `json:"entries"`
	Count          int          `json:"count"`
}

type retryQueueEntry struct {
	TransactionRef  string  `json:"transaction_ref"`
	AttemptCount    int     `json:"attempt_count"`
	MaxAttempts     int     `json:"max_attempts"`
	LastAttemptedAt *string `json:"last_attempted_at,omitempty"`
	NextRetryAt     string  `json:"next_retry_at"`
	Status          string  `json:"status"`
	RequestedBy     string  `json:"requested_by,omitempty"`
	CreatedAt       string  `json:"created_at"`
}

type retryQueueResponse struct {
	Entries       []retryQueueEntry `json:"entries"`
	Count         int               `json:"count"`
	NextPageToken string            `json:"next_page_token,omitempty"`
}

// ── HTTP helpers ──────────────────────────────────────────────────────────────

var httpClient = &http.Client{Timeout: 15 * time.Second}

// doRequest performs an HTTP request to the gateway, attaching the API key
// header when configured, and returns the status code and raw response body.
func doRequest(t *testing.T, method, path string, body any) (int, []byte) {
	t.Helper()

	var reqBody io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			t.Fatalf("doRequest: marshal body: %v", err)
		}
		reqBody = bytes.NewReader(b)
	}

	url := cfg.GatewayURL + path
	req, err := http.NewRequest(method, url, reqBody)
	if err != nil {
		t.Fatalf("doRequest: new request: %v", err)
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if cfg.APIKey != "" {
		req.Header.Set("X-API-Key", cfg.APIKey)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("doRequest %s %s: %v", method, url, err)
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("doRequest: read body: %v", err)
	}
	return resp.StatusCode, raw
}

// ingest submits a single ingestion record and returns the parsed response.
func ingest(t *testing.T, txRef, idemKey, source, amount, currency string) ingestResponse {
	t.Helper()
	code, raw := doRequest(t, http.MethodPost, "/v1/ingest", ingestRequest{
		IdempotencyKey: idemKey,
		TransactionRef: txRef,
		Payload:        buildPayload(amount, currency),
		SourceSystem:   source,
	})
	if code != http.StatusAccepted {
		t.Fatalf("ingest: unexpected status %d: %s", code, raw)
	}
	var resp ingestResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("ingest: unmarshal response: %v", err)
	}
	return resp
}

// getReconState fetches the current recon state for a transaction.
// Returns a reconState with Status == "NOT_FOUND" when the gateway returns 404.
func getReconState(t *testing.T, txRef string) reconState {
	t.Helper()
	code, raw := doRequest(t, http.MethodGet, "/v1/recon/"+txRef, nil)
	if code == http.StatusNotFound {
		return reconState{TransactionRef: txRef, Status: "NOT_FOUND"}
	}
	if code != http.StatusOK {
		t.Fatalf("getReconState: unexpected status %d: %s", code, raw)
	}
	var s reconState
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("getReconState: unmarshal: %v", err)
	}
	return s
}

// retriggerMatch calls POST /v1/recon/{ref}/retrigger and returns the resulting state.
func retriggerMatch(t *testing.T, txRef string) reconState {
	t.Helper()
	code, raw := doRequest(t, http.MethodPost, "/v1/recon/"+txRef+"/retrigger", nil)
	if code != http.StatusOK {
		t.Fatalf("retriggerMatch: unexpected status %d: %s", code, raw)
	}
	var s reconState
	if err := json.Unmarshal(raw, &s); err != nil {
		t.Fatalf("retriggerMatch: unmarshal: %v", err)
	}
	return s
}

// resolveManually calls POST /v1/resolution/{ref}.
func resolveManually(t *testing.T, txRef, chosenSource, reason, resolverID string) resolveManualResponse {
	t.Helper()
	code, raw := doRequest(t, http.MethodPost, "/v1/resolution/"+txRef, resolveManualRequest{
		ChosenSource:     chosenSource,
		ResolutionReason: reason,
		ResolverID:       resolverID,
	})
	if code != http.StatusOK {
		t.Fatalf("resolveManually: unexpected status %d: %s", code, raw)
	}
	var resp resolveManualResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("resolveManually: unmarshal: %v", err)
	}
	return resp
}

// autoResolve calls POST /v1/resolution/{ref}/auto.
func autoResolve(t *testing.T, txRef, strategy, sourcePriority string) autoResolveResponse {
	t.Helper()
	code, raw := doRequest(t, http.MethodPost, "/v1/resolution/"+txRef+"/auto", autoResolveRequest{
		Strategy:       strategy,
		SourcePriority: sourcePriority,
		ResolverID:     "integration-test",
	})
	if code != http.StatusOK {
		t.Fatalf("autoResolve: unexpected status %d: %s", code, raw)
	}
	var resp autoResolveResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("autoResolve: unmarshal: %v", err)
	}
	return resp
}

// enqueueRetry calls POST /v1/resolution/{ref}/retry.
func enqueueRetry(t *testing.T, txRef string) enqueueRetryResponse {
	t.Helper()
	code, raw := doRequest(t, http.MethodPost, "/v1/resolution/"+txRef+"/retry", enqueueRetryRequest{
		RequestedBy: "integration-test",
	})
	if code != http.StatusOK {
		t.Fatalf("enqueueRetry: unexpected status %d: %s", code, raw)
	}
	var resp enqueueRetryResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("enqueueRetry: unmarshal: %v", err)
	}
	return resp
}

// getAuditTrail calls GET /v1/resolution/{ref}/audit.
func getAuditTrail(t *testing.T, txRef string) auditTrailResponse {
	t.Helper()
	code, raw := doRequest(t, http.MethodGet, "/v1/resolution/"+txRef+"/audit", nil)
	if code != http.StatusOK {
		t.Fatalf("getAuditTrail: unexpected status %d: %s", code, raw)
	}
	var resp auditTrailResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("getAuditTrail: unmarshal: %v", err)
	}
	return resp
}

// getRetryQueue calls GET /v1/resolution/retry-queue with an optional status filter.
func getRetryQueue(t *testing.T, statusFilter string) retryQueueResponse {
	t.Helper()
	path := "/v1/resolution/retry-queue"
	if statusFilter != "" {
		path += "?status=" + statusFilter
	}
	code, raw := doRequest(t, http.MethodGet, path, nil)
	if code != http.StatusOK {
		t.Fatalf("getRetryQueue: unexpected status %d: %s", code, raw)
	}
	var resp retryQueueResponse
	if err := json.Unmarshal(raw, &resp); err != nil {
		t.Fatalf("getRetryQueue: unmarshal: %v", err)
	}
	return resp
}

// pollForStatus polls getReconState until txRef reaches the desired status or
// the configured poll timeout elapses.
func pollForStatus(t *testing.T, txRef, want string) reconState {
	t.Helper()
	deadline := time.Now().Add(cfg.PollTimeout)
	for time.Now().Before(deadline) {
		s := getReconState(t, txRef)
		if s.Status == want {
			return s
		}
		time.Sleep(cfg.PollInterval)
	}
	last := getReconState(t, txRef)
	t.Fatalf("pollForStatus: timed out after %s waiting for status %q (last: %q) for ref %q",
		cfg.PollTimeout, want, last.Status, txRef)
	return reconState{}
}

// ── Utility helpers ───────────────────────────────────────────────────────────

// uniqueTxRef returns a unique transaction reference with the given prefix.
func uniqueTxRef(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano())
}

// buildPayload marshals amount and currency into the JSON payload bytes that
// the ingestion service expects ({"amount":"...", "currency":"..."}).
func buildPayload(amount, currency string) []byte {
	b, err := json.Marshal(map[string]string{
		"amount":   amount,
		"currency": currency,
	})
	if err != nil {
		panic("buildPayload: " + err.Error())
	}
	return b
}

// auditEventTypes extracts the EventType field from each audit entry.
// Useful for asserting that specific events appear in the trail.
func auditEventTypes(entries []auditEntry) []string {
	types := make([]string, len(entries))
	for i, e := range entries {
		types[i] = e.EventType
	}
	return types
}

// containsString reports whether slice s contains elem.
func containsString(s []string, elem string) bool {
	for _, v := range s {
		if v == elem {
			return true
		}
	}
	return false
}

// joinStrings joins a string slice with sep (for human-readable error messages).
func joinStrings(s []string, sep string) string {
	return strings.Join(s, sep)
}
