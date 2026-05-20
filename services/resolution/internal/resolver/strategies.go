// Package resolver implements conflict resolution strategies for the ReconX
// Resolution Service.
//
// When a transaction is MISMATCHED, the resolver picks a "winning" source system
// whose record is deemed authoritative. The choice of strategy is configurable
// per request and globally via configuration.
//
// Strategies:
//
//   SOURCE_PRIORITY  — Pick the highest-priority source from a pre-configured
//                      ordered list. The first source in the list that submitted
//                      a record wins. Ideal when one system (e.g. the payment
//                      gateway) is always considered ground truth.
//
//   LATEST_RECORD    — Pick the source that submitted its record most recently
//                      (highest server_received_at). Useful when the latest
//                      data is always the most accurate (e.g. correction feeds).
//
//   HIGHEST_AMOUNT   — Pick the source reporting the highest monetary amount.
//                      Common for conservative reconciliation where the higher
//                      value protects against under-payment.
//
//   LOWEST_AMOUNT    — Pick the source reporting the lowest monetary amount.
//                      Common for conservative reconciliation where the lower
//                      value protects against over-payment.
//
//   FIRST_SUBMITTED  — Pick the source that submitted first (lowest
//                      server_received_at). Useful when the first record is
//                      considered the original and later records are corrections.
package resolver

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/reconx/services/resolution/internal/db"
)

// Strategy identifies the algorithm used to pick the winning source.
type Strategy string

const (
	// StrategySourcePriority picks the first source from a priority list.
	StrategySourcePriority Strategy = "source_priority"
	// StrategyLatestRecord picks the source that submitted most recently.
	StrategyLatestRecord Strategy = "latest_record"
	// StrategyHighestAmount picks the source reporting the highest amount.
	StrategyHighestAmount Strategy = "highest_amount"
	// StrategyLowestAmount picks the source reporting the lowest amount.
	StrategyLowestAmount Strategy = "lowest_amount"
	// StrategyFirstSubmitted picks the source that submitted first.
	StrategyFirstSubmitted Strategy = "first_submitted"
)

// ParseStrategy converts a string to a Strategy, case-insensitively.
// Returns an error if the strategy is unknown.
func ParseStrategy(s string) (Strategy, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "source_priority":
		return StrategySourcePriority, nil
	case "latest_record", "latest", "":
		return StrategyLatestRecord, nil
	case "highest_amount", "highest":
		return StrategyHighestAmount, nil
	case "lowest_amount", "lowest":
		return StrategyLowestAmount, nil
	case "first_submitted", "first":
		return StrategyFirstSubmitted, nil
	default:
		return "", fmt.Errorf("unknown resolution strategy %q; valid: source_priority, latest_record, highest_amount, lowest_amount, first_submitted", s)
	}
}

// Result is the output of a resolver strategy run.
type Result struct {
	// ChosenSource is the system_name of the winning record.
	ChosenSource string
	// Strategy is the strategy that was applied.
	Strategy Strategy
	// Reason is a human-readable explanation of why this source was chosen.
	Reason string
}

// Resolver applies resolution strategies using the database as the source of truth.
type Resolver struct {
	db  *sql.DB
	log *zap.Logger
}

// New creates a new Resolver backed by the given database connection.
func New(database *sql.DB, log *zap.Logger) *Resolver {
	return &Resolver{db: database, log: log}
}

// Resolve applies the given strategy to pick the winning source for a transaction.
//
// strategyConfig is a free-form map of strategy-specific parameters:
//   - For StrategySourcePriority: "source_priority" key = comma-separated ordered list
//
// Returns ErrNoSources if no records are found for the transaction.
// Returns ErrStrategyFailed if the strategy cannot produce a winner
// (e.g. no amount data for amount-based strategies).
func (r *Resolver) Resolve(
	ctx context.Context,
	transactionRef string,
	strategy Strategy,
	strategyConfig map[string]string,
) (*Result, error) {
	r.log.Info("applying resolution strategy",
		zap.String("transaction_ref", transactionRef),
		zap.String("strategy", string(strategy)),
	)

	switch strategy {
	case StrategySourcePriority:
		return r.resolveBySourcePriority(ctx, transactionRef, strategyConfig)
	case StrategyLatestRecord:
		return r.resolveByLatest(ctx, transactionRef)
	case StrategyHighestAmount:
		return r.resolveByAmount(ctx, transactionRef, true)
	case StrategyLowestAmount:
		return r.resolveByAmount(ctx, transactionRef, false)
	case StrategyFirstSubmitted:
		return r.resolveByFirst(ctx, transactionRef)
	default:
		return nil, fmt.Errorf("unsupported strategy: %s", strategy)
	}
}

// ─────────────────────────────────────────────────────────────────────────────
// Strategy implementations
// ─────────────────────────────────────────────────────────────────────────────

// resolveBySourcePriority picks the first source from a comma-separated priority
// list that has actually submitted a record for this transaction.
func (r *Resolver) resolveBySourcePriority(
	ctx context.Context,
	transactionRef string,
	strategyConfig map[string]string,
) (*Result, error) {
	priorityStr, ok := strategyConfig["source_priority"]
	if !ok || strings.TrimSpace(priorityStr) == "" {
		return nil, errors.New("source_priority strategy requires 'source_priority' config key (comma-separated list)")
	}

	priorityList := splitAndTrim(priorityStr, ",")
	if len(priorityList) == 0 {
		return nil, errors.New("source_priority list is empty")
	}

	presentSources, err := db.GetPresentSources(ctx, r.db, transactionRef)
	if err != nil {
		return nil, fmt.Errorf("get present sources: %w", err)
	}
	if len(presentSources) == 0 {
		return nil, ErrNoSources
	}

	presentSet := make(map[string]struct{}, len(presentSources))
	for _, s := range presentSources {
		presentSet[s] = struct{}{}
	}

	for _, candidate := range priorityList {
		if _, present := presentSet[candidate]; present {
			return &Result{
				ChosenSource: candidate,
				Strategy:     StrategySourcePriority,
				Reason: fmt.Sprintf(
					"source_priority: %q is highest-priority source present (priority list: [%s])",
					candidate, strings.Join(priorityList, ", "),
				),
			}, nil
		}
	}

	return nil, fmt.Errorf("%w: none of the priority sources %v have submitted a record for %q (present: %v)",
		ErrStrategyFailed, priorityList, transactionRef, presentSources)
}

// resolveByLatest picks the source that submitted its record most recently.
func (r *Resolver) resolveByLatest(ctx context.Context, transactionRef string) (*Result, error) {
	source, err := db.GetSourcesByLatest(ctx, r.db, transactionRef)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoSources
		}
		return nil, fmt.Errorf("get latest source: %w", err)
	}
	return &Result{
		ChosenSource: source,
		Strategy:     StrategyLatestRecord,
		Reason:       fmt.Sprintf("latest_record: %q submitted most recently", source),
	}, nil
}

// resolveByAmount picks the source with the highest (highest=true) or lowest amount.
func (r *Resolver) resolveByAmount(
	ctx context.Context,
	transactionRef string,
	highest bool,
) (*Result, error) {
	var (
		source   string
		err      error
		strategy Strategy
		label    string
	)
	if highest {
		source, err = db.GetSourceByHighestAmount(ctx, r.db, transactionRef)
		strategy = StrategyHighestAmount
		label = "highest"
	} else {
		source, err = db.GetSourceByLowestAmount(ctx, r.db, transactionRef)
		strategy = StrategyLowestAmount
		label = "lowest"
	}

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, fmt.Errorf("%w: no amount data found for transaction %q", ErrStrategyFailed, transactionRef)
		}
		return nil, fmt.Errorf("get %s amount source: %w", label, err)
	}

	return &Result{
		ChosenSource: source,
		Strategy:     strategy,
		Reason:       fmt.Sprintf("%s_amount: %q reports the %s amount", label, source, label),
	}, nil
}

// resolveByFirst picks the source that submitted its record first.
func (r *Resolver) resolveByFirst(ctx context.Context, transactionRef string) (*Result, error) {
	source, err := db.GetSourcesByFirst(ctx, r.db, transactionRef)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNoSources
		}
		return nil, fmt.Errorf("get first source: %w", err)
	}
	return &Result{
		ChosenSource: source,
		Strategy:     StrategyFirstSubmitted,
		Reason:       fmt.Sprintf("first_submitted: %q submitted earliest", source),
	}, nil
}

// ─────────────────────────────────────────────────────────────────────────────
// Sentinel errors
// ─────────────────────────────────────────────────────────────────────────────

// ErrNoSources is returned when no ingestion records exist for a transaction.
var ErrNoSources = errors.New("no source records found for transaction")

// ErrStrategyFailed is returned when the strategy cannot produce a winner.
var ErrStrategyFailed = errors.New("resolution strategy failed to determine a winner")

// ─────────────────────────────────────────────────────────────────────────────
// Helpers
// ─────────────────────────────────────────────────────────────────────────────

func splitAndTrim(s, sep string) []string {
	parts := strings.Split(s, sep)
	result := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			result = append(result, p)
		}
	}
	return result
}
