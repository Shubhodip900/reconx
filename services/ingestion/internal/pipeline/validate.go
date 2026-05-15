// Package pipeline – validation stage.
// Validates NormalizedRecord fields before further processing.
// Validation failures are surfaced as StageErrors and routed to the DLQ.
package pipeline

import (
	"context"
	"fmt"
	"regexp"
	"time"
)

const (
	// MaxPayloadSize is the hard upper limit on raw payload bytes (32 MB).
	MaxPayloadSize = 32 * 1024 * 1024

	// ClockSkewTolerance is the maximum allowed future-dated timestamp offset.
	ClockSkewTolerance = 5 * time.Minute

	// validTxRefPattern enforces non-empty, printable ASCII transaction refs.
	validTxRefPattern = `^[\x20-\x7E]+$`
)

var txRefRegex = regexp.MustCompile(validTxRefPattern)

// ValidISO4217 is the set of currency codes ReconX recognises.
// Extend this list as required by business rules.
var ValidISO4217 = map[string]bool{
	"INR": true, "USD": true, "EUR": true, "GBP": true,
	"JPY": true, "AUD": true, "CAD": true, "CHF": true,
	"CNY": true, "SGD": true, "AED": true, "MYR": true,
}

// Validate returns a Stage that runs all validation checks in sequence.
// The rules mirror production financial ingestion systems (e.g., moov-io/ach):
//  1. Required fields must be present
//  2. transaction_ref must be printable ASCII
//  3. payload must not exceed MaxPayloadSize
//  4. currency must be a recognised ISO 4217 code (if present)
//  5. record timestamp must not be too far in the future (clock skew guard)
func Validate() Stage {
	return func(ctx context.Context, rec *NormalizedRecord) error {
		// 1. Required fields
		if rec.IdempotencyKey == "" {
			return NewStageError("validate", "missing_field",
				fmt.Errorf("idempotency_key is required"))
		}
		if rec.TransactionRef == "" {
			return NewStageError("validate", "missing_field",
				fmt.Errorf("transaction_ref is required"))
		}
		if rec.SourceSystem == "" {
			return NewStageError("validate", "missing_field",
				fmt.Errorf("source_system is required"))
		}

		// 2. transaction_ref format
		if !txRefRegex.MatchString(rec.TransactionRef) {
			return NewStageError("validate", "invalid_format",
				fmt.Errorf("transaction_ref contains non-printable characters"))
		}

		// 3. Payload size
		if len(rec.RawPayload) > MaxPayloadSize {
			return NewStageError("validate", "payload_too_large",
				fmt.Errorf("payload size %d bytes exceeds limit %d", len(rec.RawPayload), MaxPayloadSize))
		}

		// 4. Currency (only validated when present)
		if rec.Currency != "" && !ValidISO4217[rec.Currency] {
			return NewStageError("validate", "invalid_currency",
				fmt.Errorf("currency %q is not a recognised ISO 4217 code", rec.Currency))
		}

		// 5. Clock skew: reject records timestamped > ClockSkewTolerance in the future
		if !rec.RecordTimestamp.IsZero() {
			maxAllowed := time.Now().UTC().Add(ClockSkewTolerance)
			if rec.RecordTimestamp.After(maxAllowed) {
				return NewStageError("validate", "future_timestamp",
					fmt.Errorf("record timestamp %s is more than %s in the future",
						rec.RecordTimestamp.Format(time.RFC3339), ClockSkewTolerance))
			}
		}

		// 6. Amount: must be non-negative when present
		if !rec.Amount.IsZero() && rec.Amount.IsNegative() {
			return NewStageError("validate", "negative_amount",
				fmt.Errorf("amount must be non-negative, got %s", rec.Amount.String()))
		}

		return nil
	}
}
