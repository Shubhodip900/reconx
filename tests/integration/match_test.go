package integration

import (
	"testing"
)

// TestIngestAndMatch_Exact verifies that two records with identical amounts
// from different source systems result in a MATCHED recon state.
func TestIngestAndMatch_Exact(t *testing.T) {
	txRef := uniqueTxRef("match-exact")

	ingest(t, txRef, txRef+"-src-a", "source_a", "100.00", "USD")
	ingest(t, txRef, txRef+"-src-b", "source_b", "100.00", "USD")
	retriggerMatch(t, txRef)

	state := pollForStatus(t, txRef, "MATCHED")
	if state.TransactionRef != txRef {
		t.Errorf("transaction_ref: got %q, want %q", state.TransactionRef, txRef)
	}
}

// TestIngestAndMatch_Mismatch verifies that two records with different amounts
// from different source systems result in a MISMATCHED recon state.
func TestIngestAndMatch_Mismatch(t *testing.T) {
	txRef := uniqueTxRef("match-mismatch")

	ingest(t, txRef, txRef+"-src-a", "source_a", "100.00", "USD")
	ingest(t, txRef, txRef+"-src-b", "source_b", "200.00", "USD")
	retriggerMatch(t, txRef)

	state := pollForStatus(t, txRef, "MISMATCHED")
	if state.TransactionRef != txRef {
		t.Errorf("transaction_ref: got %q, want %q", state.TransactionRef, txRef)
	}
}
