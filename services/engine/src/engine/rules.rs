/// engine/rules.rs — configurable reconciliation rules.
///
/// Rules define *when* and *how* two records are considered a match.
/// They are loaded from `EngineConfig` at startup and passed to the matcher.
use rust_decimal::Decimal;
use std::str::FromStr;

use crate::config::EngineConfig;
use crate::error::{EngineError, Result};

// ─────────────────────────────────────────────────────────────────────────────
// Tolerance
// ─────────────────────────────────────────────────────────────────────────────

/// Computed tolerances used during amount comparison.
/// Both values are derived from `EngineConfig` at startup.
#[derive(Debug, Clone)]
pub struct Tolerance {
    /// Absolute amount tolerance, e.g. Decimal::new(1, 2) = 0.01
    pub absolute: Decimal,
    /// Fractional percentage tolerance, e.g. 0.01 = 1%
    pub percentage: f64,
}

impl Tolerance {
    /// Build a `Tolerance` from the engine config, validating both fields.
    pub fn from_config(cfg: &EngineConfig) -> Result<Self> {
        let absolute = Decimal::from_str(&cfg.amount_tolerance_abs).map_err(|e| {
            EngineError::InvalidTolerance(
                cfg.amount_tolerance_abs.clone(),
                e.to_string(),
            )
        })?;

        if cfg.amount_tolerance_pct < 0.0 {
            return Err(EngineError::InvalidTolerance(
                cfg.amount_tolerance_pct.to_string(),
                "percentage tolerance must be non-negative".into(),
            ));
        }

        Ok(Self {
            absolute,
            percentage: cfg.amount_tolerance_pct,
        })
    }

    /// Return true if `|a - b| ≤ max(absolute_tolerance, pct% of reference)`.
    ///
    /// The reference is taken as the larger of the two values to avoid
    /// asymmetric comparison (avoids the "which side is the reference" problem).
    pub fn within(&self, a: Decimal, b: Decimal) -> bool {
        let diff = (a - b).abs();
        let reference = a.abs().max(b.abs());

        // Percentage component: pct% of the larger value
        let pct_tol = if reference.is_zero() {
            Decimal::ZERO
        } else {
            // Decimal doesn't have native f64 multiply; use string conversion
            let pct = Decimal::from_str(&format!("{:.10}", self.percentage / 100.0))
                .unwrap_or(Decimal::ZERO);
            reference * pct
        };

        // Use the more permissive of the two thresholds
        let effective_tolerance = self.absolute.max(pct_tol);
        diff <= effective_tolerance
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Match strategy
// ─────────────────────────────────────────────────────────────────────────────

/// How the engine compares amounts across source systems.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MatchStrategy {
    /// All amounts must be exactly equal (zero tolerance).
    Exact,
    /// Amounts within the configured tolerance are considered matched.
    Tolerance,
    /// The majority bucket wins; sources outside that bucket are flagged.
    /// Useful when you have 3+ sources and expect one outlier.
    Majority,
}

impl MatchStrategy {
    pub fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "exact" => Ok(Self::Exact),
            "tolerance" => Ok(Self::Tolerance),
            "majority" => Ok(Self::Majority),
            other => Err(EngineError::InvalidMatchStrategy(other.to_string())),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Compiled rule set
// ─────────────────────────────────────────────────────────────────────────────

/// Everything the matcher needs, pre-compiled from `EngineConfig`.
#[derive(Debug, Clone)]
pub struct RuleSet {
    pub strategy: MatchStrategy,
    pub tolerance: Tolerance,
    /// If non-empty, the engine waits until ALL listed sources have submitted
    /// a record before attempting to reconcile (otherwise stays PENDING).
    pub expected_sources: Vec<String>,
    pub max_retries: i32,
    /// Duration (seconds) after which a PENDING transaction is declared timed-out
    /// and escalated to MISMATCHED even if not all sources have arrived.
    pub pending_timeout_secs: u64,
}

impl RuleSet {
    pub fn from_config(cfg: &EngineConfig) -> Result<Self> {
        Ok(Self {
            strategy: MatchStrategy::from_str(&cfg.match_strategy)?,
            tolerance: Tolerance::from_config(cfg)?,
            expected_sources: cfg.expected_sources.clone(),
            max_retries: cfg.max_retries,
            pending_timeout_secs: cfg.pending_timeout_secs,
        })
    }

    /// True if all expected source systems are represented in the given set.
    pub fn all_sources_present(&self, present: &[String]) -> bool {
        if self.expected_sources.is_empty() {
            return true; // No constraint — any 2+ sources can reconcile
        }
        self.expected_sources
            .iter()
            .all(|e| present.iter().any(|p| p == e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn tolerance_exact_match() {
        let t = Tolerance {
            absolute: dec!(0.01),
            percentage: 0.0,
        };
        assert!(t.within(dec!(100.00), dec!(100.00)));
    }

    #[test]
    fn tolerance_within_absolute() {
        let t = Tolerance {
            absolute: dec!(0.05),
            percentage: 0.0,
        };
        assert!(t.within(dec!(100.00), dec!(100.04)));
        assert!(!t.within(dec!(100.00), dec!(100.06)));
    }

    #[test]
    fn tolerance_within_percentage() {
        let t = Tolerance {
            absolute: dec!(0.00),
            percentage: 1.0, // 1%
        };
        // 1% of 10000 = 100 → diff of 99 is ok, 101 is not
        assert!(t.within(dec!(10000.00), dec!(9901.00)));
        assert!(!t.within(dec!(10000.00), dec!(9899.00)));
    }

    #[test]
    fn strategy_parse_roundtrip() {
        assert_eq!(MatchStrategy::from_str("exact").unwrap(), MatchStrategy::Exact);
        assert_eq!(MatchStrategy::from_str("TOLERANCE").unwrap(), MatchStrategy::Tolerance);
        assert_eq!(MatchStrategy::from_str("majority").unwrap(), MatchStrategy::Majority);
        assert!(MatchStrategy::from_str("unknown").is_err());
    }
}
