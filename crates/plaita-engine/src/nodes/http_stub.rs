//! Stub used when the engine is built without the `http` feature: the node
//! fails closed at execute time (the loader still admits the type, matching
//! plaita-go's accept list; a pure-computation deployment simply cannot
//! send requests).

use crate::error::NodeError;
use crate::nodes::StepOutcome;

pub(crate) fn unavailable() -> Result<StepOutcome, NodeError> {
    Err(NodeError::deterministic(
        "http node: this plaita-engine build was compiled without the \"http\" feature",
    ))
}
