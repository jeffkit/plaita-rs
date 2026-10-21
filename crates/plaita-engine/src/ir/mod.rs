//! `ir::load` — the deployment-JSON contract between the plaita Python
//! frontend (@flow compile / console publish) and this engine (port of
//! plaita-go `pkg/ir`).
//!
//! Pipeline: normalize → validate → engine deserialization. The loader is a
//! strict accept-list by design (fail-closed): node types this engine has
//! not ported are rejected at load with an explicit message instead of being
//! silently skipped. The authoritative contract is plaita's codeflow
//! compiler output (top-level `flow_id`/`inputType`/`nodes`/`runtime`,
//! camelCase with tolerated snake residues).
//!
//! Scope notes versus plaita-go (documented divergences, both fail-closed):
//! - `javascript` code nodes are rejected (no in-process JS interpreter);
//! - agentrun + the plaita-nodes node families (gate/notify/…) are rejected
//!   with per-type hints until ported.

mod normalize;
mod scope;
mod support;
mod validate;

pub use scope::Scope;

use serde_json::Value;

use crate::duration;
use crate::model::{Collection, EndError, Flow, Node, NodeKind, ParallelBranchDef, UpstreamValue};
use crate::property::Property;

/// Parses plaita deployment-form flow JSON and builds the engine flow.
/// Structural problems (missing start, dangling refs, unreachable nodes,
/// unsupported node types) are reported as errors before execution.
pub fn load(data: &[u8]) -> Result<Flow, String> {
    let mut doc: Value =
        serde_json::from_slice(data).map_err(|e| format!("ir: parse flow json: {e}"))?;
    normalize::normalize_doc(&mut doc, Scope::Root)?;
    let flow: Flow = serde_json::from_value(doc).map_err(|e| format!("ir: build flow: {e}"))?;
    flow.validate()
        .map_err(|e| format!("ir: build flow: {e}"))?;
    Ok(flow)
}

impl Flow {
    /// Post-deserialization sanity: non-empty id, known runtime, a start and
    /// an end node (the structural rules already ran in validate_doc; these
    /// catch directly-constructed flows too).
    pub fn validate(&self) -> Result<(), String> {
        if self.id.is_empty() {
            return Err("workflow id is required".to_string());
        }
        if !matches!(
            self.runtime.as_str(),
            "" | "go" | "gengine" | "typescript" | "python" | "rust"
        ) {
            return Err(format!(
                "runtime must be one of [go gengine typescript python], got {:?}",
                self.runtime
            ));
        }
        if self.nodes.is_empty() {
            return Err("nodes is required".to_string());
        }
        if self.find_start().is_none() {
            return Err("start node not found".to_string());
        }
        if self.find_end().is_none() {
            return Err("end node not found".to_string());
        }
        Ok(())
    }
}

/// Builds a `Node` from a normalized node document (the type-tagged
/// deserialization `serde` cannot express with mixed spellings).
pub(crate) fn node_from_value(v: &Value) -> Result<Node, String> {
    let Some(obj) = v.as_object() else {
        return Err("node entry is not an object".to_string());
    };
    let get = |k: &str| obj.get(k);
    let get_str = |k: &str| get(k).and_then(|x| x.as_str()).unwrap_or("").to_string();
    let node_type = get_str("type");
    if node_type.is_empty() {
        return Err("missing type".to_string());
    }

    let base = BaseFields {
        id: get_str("id"),
        node_type: node_type.clone(),
        name: get_str("name"),
        desc: get_str("desc"),
        output: get("output").cloned().filter(|v| !v.is_null()),
        timeout: match get("timeout") {
            Some(Value::String(s)) if !s.is_empty() => Some(
                duration::parse_iso8601(s).map_err(|e| format!("node {:?}: {e}", get_str("id")))?,
            ),
            _ => None,
        },
        error_policy: match get("errorHandler") {
            Some(h) => serde_json::from_value(h.clone()).map_err(|e| e.to_string())?,
            None => Default::default(),
        },
        timeout_policy: match get("timeoutHandler") {
            Some(h) => serde_json::from_value(h.clone()).map_err(|e| e.to_string())?,
            None => Default::default(),
        },
        next: get("next")
            .and_then(|x| x.as_str())
            .map(|s| s.to_string())
            .filter(|s| !s.is_empty()),
        output_type: match get("outputType") {
            Some(p) if p.is_object() && !p.as_object().unwrap().is_empty() => {
                Some(serde_json::from_value::<Property>(p.clone()).map_err(|e| e.to_string())?)
            }
            _ => None,
        },
        metadata: get("metadata")
            .and_then(|m| m.as_object().cloned())
            .unwrap_or_default(),
    };

    let kind = match node_type.as_str() {
        "start" => NodeKind::Start,
        "end" => NodeKind::End {
            result_type: get_str("resultType"),
            error: match get("error") {
                Some(e) if e.is_object() => {
                    Some(serde_json::from_value::<EndError>(e.clone()).map_err(|e| e.to_string())?)
                }
                _ => None,
            },
        },
        "assignment" => NodeKind::Assignment {
            upstream_output: match get("upstreamOutput").and_then(|u| u.as_array()) {
                Some(u) => u
                    .iter()
                    .map(|x| {
                        serde_json::from_value::<UpstreamValue>(x.clone())
                            .map_err(|e| e.to_string())
                    })
                    .collect::<Result<Vec<_>, _>>()?,
                _ => Vec::new(),
            },
        },
        "switch" => NodeKind::Switch {
            branches: get("branches")
                .and_then(|b| b.as_array())
                .map(|a| {
                    a.iter()
                        .map(|x| {
                            serde_json::from_value::<BranchDefShim>(x.clone())
                                .map_err(|e| e.to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .map(|v: Vec<BranchDefShim>| v.into_iter().map(|s| s.inner).collect())
                .unwrap_or_default(),
        },
        "if" => NodeKind::If {
            condition: get("condition").cloned().filter(|c| !c.is_null()),
            else_next: get_str("else_next"),
        },
        "loop" => NodeKind::Loop {
            collection: match get("collection") {
                Some(Value::String(s)) => Collection::Expr(s.clone()),
                Some(Value::Array(a)) => Collection::Array(a.clone()),
                _ => Collection::Expr(String::new()),
            },
            condition: get("condition").cloned().filter(|c| !c.is_null()),
            item_type: match get("itemType") {
                Some(p) if p.is_object() && !p.as_object().unwrap().is_empty() => {
                    Some(serde_json::from_value::<Property>(p.clone()).map_err(|e| e.to_string())?)
                }
                _ => None,
            },
            child_flow: child_flow_of(obj)?,
        },
        "parallel" => NodeKind::Parallel {
            branches: get("branches")
                .and_then(|b| b.as_array())
                .map(|a| {
                    a.iter()
                        .map(|x| {
                            serde_json::from_value::<ParallelBranchDef>(x.clone())
                                .map_err(|e| e.to_string())
                        })
                        .collect::<Result<Vec<_>, _>>()
                })
                .transpose()?
                .unwrap_or_default(),
            is_conditional: get("isConditional")
                .and_then(|b| b.as_bool())
                .unwrap_or(false),
            mode: get_str("mode"),
            join_branches: get("joinBranches")
                .and_then(|j| j.as_array())
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(String::from))
                        .collect()
                })
                .unwrap_or_default(),
        },
        "child" => NodeKind::Child {
            child_flow: child_flow_of(obj)?,
            input: get("input").cloned().filter(|v| !v.is_null()),
        },
        "code" => NodeKind::Code {
            language: get_str("language"),
            code: get_str("code"),
            input: get("input").cloned().filter(|v| !v.is_null()),
            sandbox_backend: get_str("sandboxBackend"),
        },
        "http" => NodeKind::Http {
            method: get_str("method"),
            content_type: get_str("contentType"),
            url: get_str("url"),
            query: get("query").cloned().filter(|v| !v.is_null()),
            headers: get("headers").cloned().filter(|v| !v.is_null()),
            body: get("body").cloned().filter(|v| !v.is_null()),
        },
        other => {
            return Err(format!(
                "node type {other:?} is not supported by this engine"
            ));
        }
    };

    Ok(Node {
        id: base.id,
        node_type: base.node_type,
        name: base.name,
        desc: base.desc,
        output: base.output,
        timeout: base.timeout,
        error_policy: base.error_policy,
        timeout_policy: base.timeout_policy,
        next: base.next,
        output_type: base.output_type,
        metadata: base.metadata,
        kind,
    })
}

#[derive(serde::Deserialize)]
struct BranchDefShim {
    #[serde(flatten)]
    inner: crate::model::BranchDef,
}

fn child_flow_of(obj: &serde_json::Map<String, Value>) -> Result<Box<Flow>, String> {
    match obj.get("childFlow") {
        Some(cf) if cf.is_object() => {
            let flow: Flow = serde_json::from_value(cf.clone()).map_err(|e| e.to_string())?;
            flow.validate().map_err(|e| e.to_string())?;
            Ok(Box::new(flow))
        }
        _ => Err("child node requires an inline childFlow document".to_string()),
    }
}

impl<'de> serde::Deserialize<'de> for Node {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let v = Value::deserialize(deserializer)?;
        node_from_value(&v).map_err(serde::de::Error::custom)
    }
}

struct BaseFields {
    id: String,
    node_type: String,
    name: String,
    desc: String,
    output: Option<Value>,
    timeout: Option<std::time::Duration>,
    error_policy: crate::model::ErrorPolicy,
    timeout_policy: crate::model::TimeoutPolicy,
    next: Option<String>,
    output_type: Option<Property>,
    metadata: serde_json::Map<String, Value>,
}
