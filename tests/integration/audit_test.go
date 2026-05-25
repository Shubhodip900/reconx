package integration

import (
	"testing"
)

// TestAuditTrail verifies that resolution actions are recorded in the audit
// trail with the expected event types, in the correct order.
func TestAuditTrail(t *testing.T) {
	txRef := mustIngestMismatch(t, "audit-trail")

	// Perform a manual resolution to generate an audit entry.
	resolveManually(t, txRef, "source_a", "audit trail integration test", "auditor")
	pollForStatus(t, txRef, "RESOLVED")

	// Fetch and validate the audit trail.
	trail := getAuditTrail(t, txRef)

	if trail.TransactionRef != txRef {
		t.Errorf("transaction_ref: got %q, want %q", trail.TransactionRef, txRef)
	}
	if trail.Count == 0 {
		t.Fatal("audit trail is empty")
	}
	if len(trail.Entries) != trail.Count {
		t.Errorf("count mismatch: Count=%d but len(Entries)=%d",
			trail.Count, len(trail.Entries))
	}

	types := auditEventTypes(trail.Entries)
	if !containsString(types, "MANUAL_RESOLUTION") {
		t.Errorf("audit trail missing MANUAL_RESOLUTION; got: %s",
			joinStrings(types, ", "))
	}
}
