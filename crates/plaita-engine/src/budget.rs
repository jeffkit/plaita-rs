//! Load-time budget caps (port of plaita-go `pkg/workflow/budget.go`,
//! plaita#267 / #35): the shapes whose retry/timeout budgets could
//! monopolize a shared single-consumer worker must not be expressible in a
//! deployed flow. The IR loader rejects them (ir/validate.rs
//! `check_budget_caps`); the flow-level timeout is deliberately NOT capped —
//! an explicit flow timeout is an operator's budget decision, and at runtime
//! it composes with the worker execution deadline as the stricter of the two
//! (the worker layer itself is out of this repo's scope).

use std::time::Duration;

/// Caps `errorHandler.retryTimes` at every level (flow and node) —
/// retryTimes multiplies flow re-runs / node attempts, so an unbounded value
/// lets a legal flow occupy a worker for days (retryTimes=1e9 at the 10ms
/// flow-retry cadence ≈ 115 days of continuous occupancy).
pub const MAX_RETRY_TIMES: u64 = 10;

/// Caps a single node's timeout budget (plaita#267 MAX_NODE_TIMEOUT_MS) —
/// a node that can pin a worker for a year must not be expressible; longer
/// budgets belong in an explicit flow-level timeout.
pub const MAX_NODE_TIMEOUT: Duration = Duration::from_secs(24 * 3600);

/// How the load errors cite [`MAX_NODE_TIMEOUT`] (Go prints `24h0m0s`;
/// plaita#267 states the limit as 24h).
pub const MAX_NODE_TIMEOUT_TEXT: &str = "24h";
