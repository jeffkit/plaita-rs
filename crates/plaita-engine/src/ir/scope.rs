//! Loader scope: the root flow vs embedded flows (loop/child bodies,
//! parallel branches). Embedded flows must additionally reach an end node,
//! mirroring plaita's check_childflow_reachable_end.

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    Root,
    Child,
}
