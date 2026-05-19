/// engine/matcher.rs — core reconciliation matching logic.
///
/// The matcher takes a slice of `RecordView` (one per source system) and
/// produces a `MatchOutcome` that describes whether the transaction is
/// MATCHED, MISMATCHED, or still PENDING.
///
/// Design decisions:
/// - Amounts are compared as `rust_decimal::Decimal` (no f64 ever).
/// - Currency mismatch is detected separately from amount mismatch.
/// - Each source gets an individual `SourceResult` used to populate
///   `recon_match_details` and the gRPC response.
/// - The majority strategy groups sources by "amount bucket" and considers
///   the largest group the winner; all others are flagged.
use chrono::{DateTime, Utc};
use rust_decimal::Decimal;
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::str::FromStr;

use crate::db::models::RecordView;
use crate::engine::rules::{MatchStrategy, RuleSet};
use crate::error::{EngineError, Result};

// ─────────────────────────────────────────────────────────────────────────────
// Public output types
// ─────────────────────────────────────────────────────────────────────────────

/// Per-source matching verdict.
#[derive(Debug, Clone)]
pub struct SourceResult {
    pub source_system: String,
    pub internal_id: String,
    pub amount: Option<Decimal>,
    pub currency: Option<String>,
    pub data_captured: Option<Vec<u8>>,
    pub discrepancy_found: bool,
    /// JSON blob describing the specific discrepancy (included in audit log and DB).
    pub discrepancy_details: Option<JsonValue>,
}

/// Overall outcome returned by `match_records`.
#[derive(Debug, Clone)]
pub enum MatchOutcome {
    /// All sources agree within the configured tolerance.
    Matched {
        source_results: Vec<SourceResult>,
        reference_amount: Option<Decimal>,
        currency: Option<String>,
    },
    /// One or more sources deviate beyond tolerance, or there is a currency mismatch.
    Mismatched {
        source_results: Vec<SourceResult>,
        reason: String,
    },
    /// Not enough sources have arrived yet to make a determination.
    Pending {
        reason: String,
        record_count: usize,
    },
}

impl MatchOutcome {
    /// Canonical status string matching the proto `ReconStatus` enum labels.
    pub fn status_str(&self) -> &'static str {
        match self {
            MatchOutcome::Matched { .. } => "MATCHED",
            MatchOutcome::Mismatched { .. } => "MISMATCHED",
            MatchOutcome::Pending { .. } => "PENDING",
        }
    }

    /// Collect names of matched sources (empty for PENDING/MISMATCHED).
    pub fn matched_sources(&self) -> Vec<String> {
        match self {
            MatchOutcome::Matched { source_results, .. } => {
                source_results.iter().map(|r| r.source_system.clone()).collect()
            }
            _ => vec![],
        }
    }

    /// Collect names of mismatched sources (empty for MATCHED/PENDING).
    pub fn mismatched_sources(&self) -> Vec<String> {
        match self {
            MatchOutcome::Mismatched { source_results, .. } => source_results
                .iter()
                .filter(|r| r.discrepancy_found)
                .map(|r| r.source_system.clone())
                .collect(),
            _ => vec![],
        }
    }

    /// All source results regardless of outcome (empty for PENDING).
    pub fn source_results(&self) -> &[SourceResult] {
        match self {
            MatchOutcome::Matched { source_results, .. } => source_results,
            MatchOutcome::Mismatched { source_results, .. } => source_results,
            MatchOutcome::Pending { .. } => &[],
        }
    }

    /// Structured audit detail payload.
    pub fn audit_details(&self) -> JsonValue {
        match self {
            MatchOutcome::Matched {
                source_results,
                reference_amount,
                currency,
            } => json!({
                "outcome": "MATCHED",
                "reference_amount": reference_amount.map(|d| d.to_string()),
                "currency": currency,
                "sources": source_results.iter().map(|r| json!({
                    "system": r.source_system,
                    "amount": r.amount.map(|d| d.to_string()),
                })).collect::<Vec<_>>(),
            }),
            MatchOutcome::Mismatched { source_results, reason } => json!({
                "outcome": "MISMATCHED",
                "reason": reason,
                "sources": source_results.iter().map(|r| json!({
                    "system": r.source_system,
                    "amount": r.amount.map(|d| d.to_string()),
                    "discrepancy": r.discrepancy_found,
                    "details": r.discrepancy_details,
                })).collect::<Vec<_>>(),
            }),
            MatchOutcome::Pending { reason, record_count } => json!({
                "outcome": "PENDING",
                "reason": reason,
                "record_count": record_count,
            }),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Entry point
// ─────────────────────────────────────────────────────────────────────────────

/// Match a set of ingested records for a single `transaction_ref`.
///
/// # Arguments
/// - `records`       – All `RecordView`s for the same `transaction_ref`.
/// - `rules`         – Pre-compiled rule set (strategy, tolerances, expected sources).
/// - `first_seen_at` – When the earliest record for this transaction arrived.
///                     Used to enforce the pending timeout.
///
/// # Errors
/// Returns `EngineError` only for unrecoverable conditions (e.g. corrupt decimal).
/// Ordinary "need more data" scenarios are represented as `MatchOutcome::Pending`.
pub fn match_records(
    records: &[RecordView],
    rules: &RuleSet,
    first_seen_at: DateTime<Utc>,
) -> Result<MatchOutcome> {
    // ── 0. Basic sanity ───────────────────────────────────────────────────
    if records.is_empty() {
        return Ok(MatchOutcome::Pending {
            reason: "no records ingested yet".into(),
            record_count: 0,
        });
    }

    // Deduplicate by source system (take the *latest* record per source if
    // multiple were ingested, relying on ORDER BY server_received_at ASC in query)
    let mut deduped: HashMap<String, RecordView> = HashMap::new();
    for r in records {
        deduped.insert(r.source_system.clone(), r.clone());
    }
    let unique: Vec<RecordView> = deduped.into_values().collect();
    let present_sources: Vec<String> = unique.iter().map(|r| r.source_system.clone()).collect();

    // ── 1. Check expected sources ─────────────────────────────────────────
    if !rules.all_sources_present(&present_sources) {
        // Check pending timeout
        let age_secs = (Utc::now() - first_seen_at).num_seconds().max(0) as u64;
        if age_secs >= rules.pending_timeout_secs {
            let missing: Vec<String> = rules
                .expected_sources
                .iter()
                .filter(|e| !present_sources.contains(e))
                .cloned()
                .collect();
            let source_results = build_source_results_no_comparison(&unique);
            return Ok(MatchOutcome::Mismatched {
                source_results,
                reason: format!(
                    "pending timeout after {age_secs}s — missing sources: {}",
                    missing.join(", ")
                ),
            });
        }
        return Ok(MatchOutcome::Pending {
            reason: format!(
                "waiting for sources: {:?}; present: {:?}",
                rules.expected_sources, present_sources
            ),
            record_count: unique.len(),
        });
    }

    // ── 2. Need at least 2 distinct sources to compare ────────────────────
    if unique.len() < 2 {
        let age_secs = (Utc::now() - first_seen_at).num_seconds().max(0) as u64;
        if age_secs >= rules.pending_timeout_secs {
            let source_results = build_source_results_no_comparison(&unique);
            return Ok(MatchOutcome::Mismatched {
                source_results,
                reason: format!(
                    "only 1 source present after {age_secs}s timeout (minimum 2 required)"
                ),
            });
        }
        return Ok(MatchOutcome::Pending {
            reason: "only 1 source present; waiting for at least one more".into(),
            record_count: 1,
        });
    }

    // ── 3. Parse amounts ──────────────────────────────────────────────────
    let parsed = parse_amounts(&unique)?;

    // ── 4. Currency consistency check ─────────────────────────────────────
    if let Some(mismatch_detail) = check_currency_consistency(&parsed) {
        let source_results = build_source_results_currency_mismatch(&parsed, &mismatch_detail);
        return Ok(MatchOutcome::Mismatched {
            source_results,
            reason: format!("currency mismatch: {mismatch_detail}"),
        });
    }

    // ── 5. Amount comparison ──────────────────────────────────────────────
    let outcome = match rules.strategy {
        MatchStrategy::Exact => compare_exact(&parsed, rules),
        MatchStrategy::Tolerance => compare_with_tolerance(&parsed, rules),
        MatchStrategy::Majority => compare_majority(&parsed, rules),
    };

    Ok(outcome)
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

/// A `RecordView` with its amount parsed to `Decimal`.
#[derive(Debug, Clone)]
struct ParsedRecord {
    view: RecordView,
    amount: Option<Decimal>,
}

fn parse_amounts(records: &[RecordView]) -> Result<Vec<ParsedRecord>> {
    records
        .iter()
        .map(|r| {
            let amount = r
                .amount_str
                .as_deref()
                .map(|s| {
                    Decimal::from_str(s).map_err(|e| EngineError::InvalidAmount {
                        source: r.source_system.clone(),
                        value: s.to_string(),
                        reason: e.to_string(),
                    })
                })
                .transpose()?;
            Ok(ParsedRecord {
                view: r.clone(),
                amount,
            })
        })
        .collect()
}

/// Returns `Some(detail_string)` if currencies differ across sources.
fn check_currency_consistency(records: &[ParsedRecord]) -> Option<String> {
    let currencies: Vec<&str> = records
        .iter()
        .filter_map(|r| r.view.currency.as_deref())
        .collect();

    if currencies.is_empty() {
        return None; // Nothing to compare
    }

    let first = currencies[0];
    let mismatches: Vec<String> = records
        .iter()
        .filter(|r| {
            r.view
                .currency
                .as_deref()
                .map(|c| c != first)
                .unwrap_or(false)
        })
        .map(|r| {
            format!(
                "{}={}",
                r.view.source_system,
                r.view.currency.as_deref().unwrap_or("?")
            )
        })
        .collect();

    if mismatches.is_empty() {
        None
    } else {
        Some(format!(
            "expected '{}' (from first source), got: {}",
            first,
            mismatches.join(", ")
        ))
    }
}

/// Strategy: all amounts must be exactly equal.
fn compare_exact(records: &[ParsedRecord], _rules: &RuleSet) -> MatchOutcome {
    let amounts_with_source: Vec<_> = records.iter().filter_map(|r| r.amount.map(|a| (&r.view.source_system, a))).collect();
    if amounts_with_source.is_empty() {
        // No amounts → match on presence only
        let source_results = records
            .iter()
            .map(|r| to_source_result(r, false, None))
            .collect();
        return MatchOutcome::Matched {
            source_results,
            reference_amount: None,
            currency: None,
        };
    }

    let reference = amounts_with_source[0].1;
    let currency = records
        .iter()
        .find_map(|r| r.view.currency.clone());

    let mut any_mismatch = false;
    let source_results: Vec<SourceResult> = records
        .iter()
        .map(|r| {
            let discrepancy = r.amount.map(|a| a != reference).unwrap_or(false);
            if discrepancy {
                any_mismatch = true;
            }
            let details = if discrepancy {
                Some(json!({
                    "strategy": "exact",
                    "reference": reference.to_string(),
                    "actual": r.amount.map(|d| d.to_string()),
                    "diff": r.amount.map(|a| (a - reference).abs().to_string()),
                }))
            } else {
                None
            };
            to_source_result(r, discrepancy, details)
        })
        .collect();

    if any_mismatch {
        MatchOutcome::Mismatched {
            source_results,
            reason: "exact match strategy: amounts differ".into(),
        }
    } else {
        MatchOutcome::Matched {
            source_results,
            reference_amount: Some(reference),
            currency,
        }
    }
}

/// Strategy: amounts within the configured tolerance are considered matched.
fn compare_with_tolerance(records: &[ParsedRecord], rules: &RuleSet) -> MatchOutcome {
    let amounts_with_data: Vec<_> = records.iter().filter_map(|r| r.amount.map(|a| (r, a))).collect();

    if amounts_with_data.is_empty() {
        let source_results = records.iter().map(|r| to_source_result(r, false, None)).collect();
        return MatchOutcome::Matched {
            source_results,
            reference_amount: None,
            currency: None,
        };
    }

    // Reference = first record's amount
    let reference = amounts_with_data[0].1;
    let currency = records.iter().find_map(|r| r.view.currency.clone());

    let mut any_mismatch = false;
    let source_results: Vec<SourceResult> = records
        .iter()
        .map(|r| {
            let discrepancy = match r.amount {
                None => false,
                Some(a) => !rules.tolerance.within(a, reference),
            };
            if discrepancy {
                any_mismatch = true;
            }
            let details = if discrepancy {
                Some(json!({
                    "strategy": "tolerance",
                    "reference": reference.to_string(),
                    "actual": r.amount.map(|d| d.to_string()),
                    "diff": r.amount.map(|a| (a - reference).abs().to_string()),
                    "absolute_tolerance": rules.tolerance.absolute.to_string(),
                    "pct_tolerance": rules.tolerance.percentage,
                }))
            } else {
                None
            };
            to_source_result(r, discrepancy, details)
        })
        .collect();

    if any_mismatch {
        MatchOutcome::Mismatched {
            source_results,
            reason: format!(
                "tolerance strategy: amounts deviate beyond abs={} / pct={}%",
                rules.tolerance.absolute, rules.tolerance.percentage
            ),
        }
    } else {
        MatchOutcome::Matched {
            source_results,
            reference_amount: Some(reference),
            currency,
        }
    }
}

/// Strategy: majority rules — the largest group wins; outliers are flagged.
///
/// Groups are formed by bucketing amounts: two amounts belong to the same
/// bucket if they are within tolerance of each other. The largest bucket
/// is declared the winner.
fn compare_majority(records: &[ParsedRecord], rules: &RuleSet) -> MatchOutcome {
    let amounts: Vec<Option<Decimal>> = records.iter().map(|r| r.amount).collect();

    // Build buckets: for each record, find an existing bucket it fits into
    let mut buckets: Vec<Vec<usize>> = Vec::new(); // each bucket = list of record indices

    'outer: for (i, &amt) in amounts.iter().enumerate() {
        let Some(a) = amt else {
            // Records without amounts go in their own bucket
            buckets.push(vec![i]);
            continue;
        };
        for bucket in &mut buckets {
            let rep_idx = bucket[0];
            let Some(rep_amt) = amounts[rep_idx] else { continue };
            if rules.tolerance.within(a, rep_amt) {
                bucket.push(i);
                continue 'outer;
            }
        }
        buckets.push(vec![i]);
    }

    // Winning bucket = largest
    let max_size = buckets.iter().map(|b| b.len()).max().unwrap_or(0);
    let winner_indices: std::collections::HashSet<usize> = buckets
        .iter()
        .filter(|b| b.len() == max_size)
        .flat_map(|b| b.iter().copied())
        .collect();

    let currency = records.iter().find_map(|r| r.view.currency.clone());
    let reference_amount = records
        .get(*winner_indices.iter().next().unwrap_or(&0))
        .and_then(|r| r.amount);

    let mut any_loser = false;
    let source_results: Vec<SourceResult> = records
        .iter()
        .enumerate()
        .map(|(i, r)| {
            let is_winner = winner_indices.contains(&i);
            let discrepancy = !is_winner;
            if discrepancy {
                any_loser = true;
            }
            let details = if discrepancy {
                Some(json!({
                    "strategy": "majority",
                    "majority_amount": reference_amount.map(|d| d.to_string()),
                    "actual": r.amount.map(|d| d.to_string()),
                    "bucket_size": buckets.iter().find(|b| b.contains(&i)).map(|b| b.len()),
                    "winning_bucket_size": max_size,
                }))
            } else {
                None
            };
            to_source_result(r, discrepancy, details)
        })
        .collect();

    if any_loser {
        MatchOutcome::Mismatched {
            source_results,
            reason: format!(
                "majority strategy: {max_size}/{} sources agree; outliers flagged",
                records.len()
            ),
        }
    } else {
        MatchOutcome::Matched {
            source_results,
            reference_amount,
            currency,
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Constructors for SourceResult
// ─────────────────────────────────────────────────────────────────────────────

fn to_source_result(r: &ParsedRecord, discrepancy: bool, details: Option<JsonValue>) -> SourceResult {
    SourceResult {
        source_system: r.view.source_system.clone(),
        internal_id: r.view.internal_id.clone(),
        amount: r.amount,
        currency: r.view.currency.clone(),
        data_captured: r.view.raw_payload.clone(),
        discrepancy_found: discrepancy,
        discrepancy_details: details,
    }
}

fn build_source_results_no_comparison(records: &[RecordView]) -> Vec<SourceResult> {
    records
        .iter()
        .map(|r| SourceResult {
            source_system: r.source_system.clone(),
            internal_id: r.internal_id.clone(),
            amount: r.amount_str.as_deref().and_then(|s| Decimal::from_str(s).ok()),
            currency: r.currency.clone(),
            data_captured: r.raw_payload.clone(),
            discrepancy_found: false,
            discrepancy_details: None,
        })
        .collect()
}

fn build_source_results_currency_mismatch(
    records: &[ParsedRecord],
    detail: &str,
) -> Vec<SourceResult> {
    records
        .iter()
        .map(|r| SourceResult {
            source_system: r.view.source_system.clone(),
            internal_id: r.view.internal_id.clone(),
            amount: r.amount,
            currency: r.view.currency.clone(),
            data_captured: r.view.raw_payload.clone(),
            discrepancy_found: true,
            discrepancy_details: Some(json!({ "currency_mismatch": detail })),
        })
        .collect()
}

// ─────────────────────────────────────────────────────────────────────────────
// Unit tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engine::rules::{MatchStrategy, Tolerance};
    use rust_decimal_macros::dec;

    fn make_view(source: &str, amount: &str) -> RecordView {
        RecordView {
            internal_id: format!("{source}-id"),
            source_system: source.to_string(),
            amount_str: Some(amount.to_string()),
            currency: Some("INR".to_string()),
            raw_payload: None,
            server_received_at: Utc::now(),
        }
    }

    fn make_rules(strategy: MatchStrategy) -> RuleSet {
        RuleSet {
            strategy,
            tolerance: Tolerance {
                absolute: dec!(0.05),
                percentage: 0.0,
            },
            expected_sources: vec![],
            max_retries: 3,
            pending_timeout_secs: 300,
        }
    }

    #[test]
    fn exact_match_succeeds() {
        let records = vec![make_view("vendor", "10000.00"), make_view("erp", "10000.00")];
        let views: Vec<RecordView> = records;
        let rules = make_rules(MatchStrategy::Tolerance);
        let outcome = match_records(&views, &rules, Utc::now()).unwrap();
        assert_eq!(outcome.status_str(), "MATCHED");
    }

    #[test]
    fn tolerance_match_within_threshold() {
        let records = vec![make_view("vendor", "10000.00"), make_view("erp", "9999.98")];
        let rules = make_rules(MatchStrategy::Tolerance); // abs tolerance 0.05
        let outcome = match_records(&records, &rules, Utc::now()).unwrap();
        assert_eq!(outcome.status_str(), "MATCHED");
    }

    #[test]
    fn tolerance_mismatch_outside_threshold() {
        let records = vec![make_view("vendor", "10000.00"), make_view("erp", "9800.00")];
        let rules = make_rules(MatchStrategy::Tolerance);
        let outcome = match_records(&records, &rules, Utc::now()).unwrap();
        assert_eq!(outcome.status_str(), "MISMATCHED");
        assert_eq!(outcome.mismatched_sources().len(), 1);
    }

    #[test]
    fn pending_with_only_one_source() {
        let records = vec![make_view("vendor", "10000.00")];
        let rules = make_rules(MatchStrategy::Tolerance);
        let outcome = match_records(&records, &rules, Utc::now()).unwrap();
        assert_eq!(outcome.status_str(), "PENDING");
    }

    #[test]
    fn majority_two_vs_one() {
        let records = vec![
            make_view("vendor", "10000.00"),
            make_view("erp", "10000.00"),
            make_view("payment_gw", "9800.00"),
        ];
        let rules = make_rules(MatchStrategy::Majority);
        let outcome = match_records(&records, &rules, Utc::now()).unwrap();
        // 2 sources agree → MATCHED (majority wins)
        assert_eq!(outcome.status_str(), "MATCHED");
    }

    #[test]
    fn empty_records_returns_pending() {
        let rules = make_rules(MatchStrategy::Tolerance);
        let outcome = match_records(&[], &rules, Utc::now()).unwrap();
        assert_eq!(outcome.status_str(), "PENDING");
    }
}
