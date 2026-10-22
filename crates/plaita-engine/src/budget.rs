//! Load-time budget caps (port of plaita-go `pkg/workflow/budget.go`,
//! plaita#267 / #35): the shapes whose retry/timeout budgets could
//! monopolize a shared single-consumer worker must not be expressible in a
//! deployed flow. The IR loader rejects them (ir/validate.rs
//! `check_budget_caps`); the flow-level timeout is deliberately NOT capped —
//! an explicit flow timeout is an operator's budget decision, and at runtime
//! it composes with the worker execution deadline as the stricter of the two
//! (the worker layer itself is out of this repo's scope).
//!
//! Besides the load-time caps the engine also carries the execution-level
//! DEFAULT budget (plaita#267, Python `core/deadline.py`): a flow with no
//! explicit timeout is not unbounded — it runs under 1800s
//! ([`DEFAULT_EXECUTION_DEADLINE`]), overridable through
//! [`EXECUTION_DEADLINE_ENV`] (plaita strategies.py:144
//! `execution_deadline_ms`). An invalid env value falls back to the default
//! with a warning — never silently to "unlimited" (降级必须可观测).

use std::time::Duration;

/// Caps `errorHandler.retryTimes` at the NODE level (B4 r7 narrowed: the
/// flow-level cap is gone with the flow-level handler itself — plaita has no
/// flow-level errorHandler and the whole-flow re-run is not implemented, so
/// the retryTimes budget hazard exists only where retryTimes actually
/// multiplies node attempts).
pub const MAX_RETRY_TIMES: u64 = 10;

/// Caps a single node's timeout budget (plaita#267 MAX_NODE_TIMEOUT_MS) —
/// a node that can pin a worker for a year must not be expressible; longer
/// budgets belong in an explicit flow-level timeout.
pub const MAX_NODE_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

/// How the load errors cite [`MAX_NODE_TIMEOUT`] (Go prints `24h0m0s`;
/// plaita#267 states the limit as 24h).
pub const MAX_NODE_TIMEOUT_TEXT: &str = "24h";

/// Execution-level default budget for flows without an explicit timeout
/// (plaita#267, `core/deadline.py::DEFAULT_EXECUTION_DEADLINE_SECONDS` —
/// same 1800s as the ops runbook's "node stalled >1800s is dead" verdict).
/// An explicit flow timeout (including `PT0S`, the strictest budget,
/// plaita#138) always wins; this only fills the all-defaults case.
pub const DEFAULT_EXECUTION_DEADLINE: Duration = Duration::from_secs(1800);

/// Operator knob overriding [`DEFAULT_EXECUTION_DEADLINE`], in seconds —
/// same name and semantics as Python `deadline.EXECUTION_DEADLINE_ENV`
/// (float seconds accepted).
pub const EXECUTION_DEADLINE_ENV: &str = "PLAITA_EXECUTION_DEADLINE_SECONDS";

/// Parses the env value into the effective default deadline. `None`/empty →
/// the built-in default (silently, Python returns early on empty). Anything
/// else that is not a positive finite number → `Err` with the warning text:
/// the caller prints it and falls back to the default — never silently to
/// unlimited (Python logs a WARNING with the same meaning).
pub fn parse_deadline_seconds(raw: Option<&str>) -> Result<Duration, String> {
    let Some(raw) = raw.map(str::trim).filter(|s| !s.is_empty()) else {
        return Ok(DEFAULT_EXECUTION_DEADLINE);
    };
    let seconds: f64 = raw
        .parse()
        .map_err(|_| invalid_env(raw, "is not a number"))?;
    if !seconds.is_finite() || seconds <= 0.0 {
        return Err(invalid_env(
            raw,
            "must be a positive finite number of seconds",
        ));
    }
    Duration::try_from_secs_f64(seconds).map_err(|_| {
        invalid_env(
            raw,
            "exceeds the representable duration range on this platform",
        )
    })
}

fn invalid_env(raw: &str, why: &str) -> String {
    format!(
        "{EXECUTION_DEADLINE_ENV}={raw:?} {why}; falling back to the default execution \
         deadline of {}s (NOT silently unlimited)",
        DEFAULT_EXECUTION_DEADLINE.as_secs()
    )
}

/// Reads [`EXECUTION_DEADLINE_ENV`] and resolves the effective default
/// budget, warning on stderr when the value is invalid (the Rust dialect of
/// Python's `logger.warning` in `default_execution_deadline_ms`).
pub fn default_execution_deadline() -> Duration {
    let raw = std::env::var(EXECUTION_DEADLINE_ENV).ok();
    match parse_deadline_seconds(raw.as_deref()) {
        Ok(d) => d,
        Err(warning) => {
            eprintln!("[budget] {warning}");
            DEFAULT_EXECUTION_DEADLINE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Python `deadline.default_execution_deadline_ms` 的值域口径：缺省/空串
    /// 静默回默认；非数字/非有限/<=0 回默认但带警告（Err=警告文本）。
    #[test]
    fn deadline_env_parsing() {
        assert_eq!(parse_deadline_seconds(None), Ok(DEFAULT_EXECUTION_DEADLINE));
        assert_eq!(
            parse_deadline_seconds(Some("")),
            Ok(DEFAULT_EXECUTION_DEADLINE)
        );
        assert_eq!(
            parse_deadline_seconds(Some("  ")),
            Ok(DEFAULT_EXECUTION_DEADLINE)
        );
        assert_eq!(
            parse_deadline_seconds(Some("2")),
            Ok(Duration::from_secs(2))
        );
        assert_eq!(
            parse_deadline_seconds(Some("0.5")),
            Ok(Duration::from_millis(500))
        );
        assert_eq!(
            parse_deadline_seconds(Some(" 1800 ")),
            Ok(DEFAULT_EXECUTION_DEADLINE)
        );
        for bad in ["abc", "-1", "0", "inf", "nan", "1e999"] {
            let got = parse_deadline_seconds(Some(bad));
            assert!(got.is_err(), "{bad:?} must be rejected with a warning");
            assert!(
                got.unwrap_err().contains("falling back to the default"),
                "{bad:?}"
            );
        }
    }
}
