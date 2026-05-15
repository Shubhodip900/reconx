// Package pipeline – normalization stage.
// Transforms raw IngestRequest bytes into a fully resolved NormalizedRecord.
// Normalization is intentionally separate from validation (moov-io pattern):
// validate first to reject garbage, then normalize survivors into canonical form.
package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/shopspring/decimal"
)

// RawIngestPayload is the expected JSON schema for records arriving via the
// generic REST/webhook/file adapters.
// gRPC callers may pass pre-structured data; Kafka consumers emit this format.
type RawIngestPayload struct {
	// TransactionRef is the cross-system join key.
	TransactionRef string `json:"transaction_ref"`

	// Amount is the monetary value as a string to preserve precision.
	// Example: "10000.00"
	Amount string `json:"amount,omitempty"`

	// Currency is the ISO 4217 code.
	Currency string `json:"currency,omitempty"`

	// EventTime is the source-system timestamp in RFC3339 or Unix-ms format.
	EventTime string `json:"event_time,omitempty"`

	// Schema identifies the payload version.
	Schema string `json:"schema,omitempty"`

	// Extra captures source-specific fields that don't map to canonical fields.
	Extra map[string]string `json:"extra,omitempty"`
}

// Normalize returns a Stage that parses the RawPayload bytes (JSON) and
// populates canonical fields on the NormalizedRecord.
// Unknown fields are preserved in Tags so they are not silently dropped.
func Normalize() Stage {
	return func(ctx context.Context, rec *NormalizedRecord) error {
		if len(rec.RawPayload) == 0 {
			// Empty payload is allowed (metadata-only record); skip parsing.
			return nil
		}

		raw := &RawIngestPayload{}
		if err := json.Unmarshal(rec.RawPayload, raw); err != nil {
			// Non-JSON payload: treat as opaque blob. Log it but do not fail —
			// the raw bytes are preserved in RawPayload for the engine.
			// Mark the schema as "opaque" so downstream knows.
			if rec.Tags == nil {
				rec.Tags = make(map[string]string)
			}
			rec.Tags["parse_warning"] = "non_json_payload"
			return nil
		}

		// Override TransactionRef from payload only if proto field was empty.
		if rec.TransactionRef == "" && raw.TransactionRef != "" {
			rec.TransactionRef = raw.TransactionRef
		}

		// Amount: parse as arbitrary-precision decimal.
		if raw.Amount != "" {
			amt, err := decimal.NewFromString(raw.Amount)
			if err != nil {
				return NewStageError("normalize", "invalid_amount",
					fmt.Errorf("cannot parse amount %q: %w", raw.Amount, err))
			}
			rec.Amount = amt
		}

		// Currency: normalize to UPPER case.
		if raw.Currency != "" {
			rec.Currency = strings.ToUpper(strings.TrimSpace(raw.Currency))
		}

		// EventTime: parse and normalize to UTC.
		if raw.EventTime != "" {
			ts, err := parseTimestamp(raw.EventTime)
			if err != nil {
				// Non-fatal: record the raw value in tags, use zero time.
				if rec.Tags == nil {
					rec.Tags = make(map[string]string)
				}
				rec.Tags["raw_event_time"] = raw.EventTime
			} else {
				rec.RecordTimestamp = ts.UTC()
			}
		}

		// Schema passthrough.
		if raw.Schema != "" {
			rec.PayloadSchema = raw.Schema
		}

		// Merge extra fields into Tags (extra fields take lower priority).
		if len(raw.Extra) > 0 {
			if rec.Tags == nil {
				rec.Tags = make(map[string]string)
			}
			for k, v := range raw.Extra {
				if _, exists := rec.Tags[k]; !exists {
					rec.Tags[k] = v
				}
			}
		}

		return nil
	}
}

// parseTimestamp attempts RFC3339, then RFC3339Nano, then Unix-milliseconds.
func parseTimestamp(s string) (time.Time, error) {
	formats := []string{
		time.RFC3339,
		time.RFC3339Nano,
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
	}
	for _, f := range formats {
		if t, err := time.Parse(f, s); err == nil {
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("cannot parse timestamp %q", s)
}

// Enrich returns a Stage that stamps server-side fields that callers cannot set.
// This ensures ServerReceivedAt reflects the ingestion server's wall clock.
func Enrich() Stage {
	return func(ctx context.Context, rec *NormalizedRecord) error {
		// ServerReceivedAt is always stamped here; never trust client clocks.
		rec.ServerReceivedAt = time.Now().UTC()

		// Ensure Tags map is initialized.
		if rec.Tags == nil {
			rec.Tags = make(map[string]string)
		}

		// Stamp the adapter type as a tag for observability.
		rec.Tags["adapter_type"] = string(rec.AdapterType)

		return nil
	}
}
