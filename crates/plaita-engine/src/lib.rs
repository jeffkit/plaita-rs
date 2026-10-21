//! plaita flow engine: the deployment-IR loader (`ir`), the runtime context
//! (`context`), the execution loop with retry/timeout/error policies
//! (`exec`) and the builtin nodes (`nodes`).
//!
//! Scope (mirrors plaita-go, minus the worker layer): this crate parses
//! plaita deployment-form flow JSON and runs it in-process. Queue/lease/
//! checkpoint/server responsibilities live with the worker runtime and are
//! deliberately out of scope here.

pub mod context;
pub mod duration;
pub mod error;
pub mod exec;
pub mod ir;
pub mod model;
pub mod nodes;
pub mod property;

pub use context::RuntimeCtx;
pub use error::FlowError;
pub use exec::{Event, Host};
pub use model::{Flow, Node, NodeKind};
pub use property::Property;

/// Convenience: load a flow from bytes and run it with the given input.
pub fn run_flow_json(
    data: &[u8],
    input: &serde_json::Value,
    host: &Host,
) -> Result<serde_json::Value, FlowError> {
    let flow = ir::load(data).map_err(FlowError::Other)?;
    flow.execute(input, host)
}
