package integration

import (
	"testing"
)

// TestIdempotency verifies that re-ingesting a record with the same
// idempotency key returns the same internal_id without creating a duplicate.
func TestIdempotency(t *testing.T) {
	txRef := uniqueTxRef("idem")
	idemKey := txRef + "-key"

	first := ingest(t, txRef, idemKey, "source_a", "100.00", "USD")
	if !first.Success {
		t.Fatalf("first ingest: success=false")
	}
	if first.InternalID == "" {
		t.Fatal("first ingest: internal_id is empty")
	}

	second := ingest(t, txRef, idemKey, "source_a", "100.00", "USD")
	if second.InternalID != first.InternalID {
		t.Errorf("idempotency violated: different internal_id on second ingest: %q vs %q",
			second.InternalID, first.InternalID)
	}
}
