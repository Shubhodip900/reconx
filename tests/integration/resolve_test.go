package integration

import (
	"testing"
)

// mustIngestMismatch creates two ingestion records with differing amounts for
// the same transaction reference, triggers matching, and polls until the status
// is MISMATCHED. Returns the transaction reference.
//
// It is a shared setup helper used by resolve_test.go, retry_test.go, and
// audit_test.go.
func mustIngestMismatch(t *testing.T, prefix string) string {
	t.Helper()
	txRef := uniqueTxRef(prefix)

	ingest(t, txRef, txRef+"-src-a", "source_a", "100.00", "USD")
	ingest(t, txRef, txRef+"-src-b", "source_b", "200.00", "USD")
	retriggerMatch(t, txRef)
	pollForStatus(t, txRef, "MISMATCHED")

	return txRef
}

// TestAutoResolve_LatestRecord verifies that the latest_record strategy
// successfully auto-resolves a MISMATCHED transaction to RESOLVED.
func TestAutoResolve_LatestRecord(t *testing.T) {
	txRef := mustIngestMismatch(t, "auto-latest")

	resp := autoResolve(t, txRef, "latest_record", "")

	if resp.TransactionRef != txRef {
		t.Errorf("transaction_ref: got %q, want %q", resp.TransactionRef, txRef)
	}
	if resp.Strategy != "latest_record" {
		t.Errorf("strategy: got %q, want %q", resp.Strategy, "latest_record")
	}
	if resp.ChosenSource == "" {
		t.Error("chosen_source should not be empty")
	}

	pollForStatus(t, txRef, "RESOLVED")
}

// TestAutoResolve_SourcePriority verifies that the source_priority strategy
// picks source_a (highest priority) and resolves the transaction.
func TestAutoResolve_SourcePriority(t *testing.T) {
	txRef := mustIngestMismatch(t, "auto-priority")

	resp := autoResolve(t, txRef, "source_priority", "source_a,source_b")

	if resp.TransactionRef != txRef {
		t.Errorf("transaction_ref: got %q, want %q", resp.TransactionRef, txRef)
	}
	if resp.ChosenSource != "source_a" {
		t.Errorf("chosen_source: got %q, want %q", resp.ChosenSource, "source_a")
	}

	pollForStatus(t, txRef, "RESOLVED")
}

// TestManualResolve verifies that a human-initiated manual resolution
// transitions a MISMATCHED transaction to RESOLVED.
func TestManualResolve(t *testing.T) {
	txRef := mustIngestMismatch(t, "manual-resolve")

	resp := resolveManually(t, txRef, "source_a", "integration test resolution", "tester")
	if !resp.Success {
		t.Fatalf("resolveManually: success=false, new_status=%q", resp.NewStatus)
	}

	pollForStatus(t, txRef, "RESOLVED")
}
