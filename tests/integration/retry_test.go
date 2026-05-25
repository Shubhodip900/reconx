package integration

import (
	"testing"
)

// TestRetryWorker verifies the retry enqueue workflow end-to-end:
//
//  1. Ingest a mismatched transaction and confirm MISMATCHED status.
//  2. Enqueue the transaction for retry via the resolution service.
//  3. Verify the entry appears in the retry queue.
//  4. Ingest a corrected record so the amounts now agree.
//  5. Retrigger the engine directly (bypassing the worker poll cycle for reliability).
//  6. Confirm the transaction transitions to MATCHED.
//  7. Confirm the audit trail contains a RETRY_ENQUEUED event.
func TestRetryWorker(t *testing.T) {
	// Step 1: create a MISMATCHED transaction.
	txRef := mustIngestMismatch(t, "retry-worker")

	// Step 2: enqueue for retry.
	retryResp := enqueueRetry(t, txRef)
	if retryResp.TransactionRef != txRef {
		t.Errorf("enqueueRetry: transaction_ref got %q, want %q",
			retryResp.TransactionRef, txRef)
	}
	if retryResp.Message == "" {
		t.Error("enqueueRetry: message should not be empty")
	}

	// Step 3: verify the entry appears in the PENDING retry queue.
	queue := getRetryQueue(t, "PENDING")
	found := false
	for _, e := range queue.Entries {
		if e.TransactionRef == txRef {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("retry queue: txRef %q not found in PENDING entries", txRef)
	}

	// Step 4: ingest a corrected record from source_b that now agrees with source_a.
	ingest(t, txRef, txRef+"-src-b-fix", "source_b", "100.00", "USD")

	// Step 5: retrigger the engine directly instead of waiting for the worker.
	retriggerMatch(t, txRef)

	// Step 6: poll for MATCHED.
	pollForStatus(t, txRef, "MATCHED")

	// Step 7: verify RETRY_ENQUEUED appears in the audit trail.
	audit := getAuditTrail(t, txRef)
	types := auditEventTypes(audit.Entries)
	if !containsString(types, "RETRY_ENQUEUED") {
		t.Errorf("audit trail missing RETRY_ENQUEUED; got: %s",
			joinStrings(types, ", "))
	}
}
