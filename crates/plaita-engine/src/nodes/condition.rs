//! Condition evaluation for switch/if/loop/parallel (port of plaita-go
//! `pkg/workflow/nodes/branch.go`, which ports plaita decide.py).
//!
//! Comparison semantics are the exported plaitaexpr ones (`equal` /
//! `compare` / `contains` — the same primitives `$F.eq`/`gt`/`in` use), plus
//! Python truthiness for truthy/falsy. plaita's None short-circuit: when
//! either operand is None, only eq/ne do a None identity comparison and
//! every other operator is False.

use serde::Deserialize;
use serde_json::json;
use serde_json::Value;

use crate::context::SharedCtx;
use crate::error::NodeError;
use plaita_expression::{contains as py_contains, equal, py_compare, truthy};

/// A single `field op value` condition (all three are expressions).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct LogicCondition {
    #[serde(default)]
    pub field: Value,
    #[serde(default)]
    pub operator: String,
    #[serde(default)]
    pub value: Value,
}

/// A recursive condition group (`relation: and|or`, `conditions: [...]`).
#[derive(Debug, Clone, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct LogicConditionGroup {
    #[serde(default)]
    pub relation: String,
    #[serde(default)]
    pub conditions: Vec<Value>,
}

/// The parsed condition of a node (plaita-go `Condition`).
#[derive(Debug, Clone)]
pub enum Condition {
    /// zero-value condition (e.g. a plaita loop without one): always match
    Zero,
    Single(LogicCondition),
    Group(LogicConditionGroup),
    Expression(String),
}

impl Condition {
    /// Mirrors plaita-go's `Condition.UnmarshalJSON` probe order:
    /// `{field: ...}` → single, `{relation: ...}` → group, string →
    /// expression.
    pub fn from_value(v: &Value) -> Condition {
        match v {
            Value::Null => Condition::Zero,
            Value::String(s) => Condition::Expression(s.clone()),
            Value::Object(o) => {
                if o.contains_key("field") {
                    serde_json::from_value::<LogicCondition>(v.clone())
                        .map(Condition::Single)
                        .unwrap_or(Condition::Zero)
                } else if o.contains_key("relation") {
                    serde_json::from_value::<LogicConditionGroup>(v.clone())
                        .map(Condition::Group)
                        .unwrap_or(Condition::Zero)
                } else {
                    Condition::Zero
                }
            }
            _ => Condition::Zero,
        }
    }

    /// Evaluates against the context. Returns `(matched, match_info)`.
    pub fn match_context(&self, rc: &SharedCtx) -> Result<(bool, Option<Value>), NodeError> {
        match self {
            Condition::Zero => Ok((true, Some(json!({"matched": true})))),
            Condition::Single(c) => match_single(c, rc).map(|(m, info)| (m, Some(info))),
            Condition::Group(g) => match_group(g, rc),
            Condition::Expression(expr) => {
                let result = eval_or_err(rc, &Value::String(expr.clone()), "condition")?;
                Ok((truthy(&result), None))
            }
        }
    }
}

fn eval_or_err(rc: &SharedCtx, expr: &Value, what: &str) -> Result<Value, NodeError> {
    plaita_expression::eval_value(expr, rc.as_ref())
        .map_err(|e| NodeError::transient(format!("failed to eval {what} expression: {e}")))
}

fn eval_field(c: &LogicCondition, rc: &SharedCtx) -> Result<Value, NodeError> {
    eval_or_err(rc, &c.field, "condition field")
}

fn eval_value_expr(c: &LogicCondition, rc: &SharedCtx) -> Result<Value, NodeError> {
    eval_or_err(rc, &c.value, "condition value")
}

fn match_single(c: &LogicCondition, rc: &SharedCtx) -> Result<(bool, Value), NodeError> {
    let mut info = json!({
        "field": c.field,
        "operator": c.operator,
    });
    let lhs = eval_field(c, rc)?;
    info["actual"] = lhs.clone();

    // truthy/falsy never consult the value (plaita semantics)
    if c.operator == "truthy" || c.operator == "falsy" {
        let mut matched = truthy(&lhs);
        if c.operator == "falsy" {
            matched = !matched;
        }
        info["matched"] = json!(matched);
        return Ok((matched, info));
    }

    let rhs = eval_value_expr(c, rc)?;
    info["expect"] = rhs.clone();

    // Python Condition.match 的 None 短路：任一操作数为 None 时，仅 eq/ne 做
    // None 身份比较，其余算子一律 False（Python 的 TypeError 场景统一落为
    // 不匹配）。
    if lhs.is_null() || rhs.is_null() {
        let both = lhs.is_null() && rhs.is_null();
        let matched = match c.operator.as_str() {
            "eq" => both,
            "ne" => !both,
            _ => false,
        };
        info["matched"] = json!(matched);
        return Ok((matched, info));
    }

    let matched = match c.operator.as_str() {
        "gte" => py_compare_op("gte", &lhs, &rhs, &mut info, |o| {
            o != std::cmp::Ordering::Less
        })?,
        "gt" => py_compare_op("gt", &lhs, &rhs, &mut info, |o| {
            o == std::cmp::Ordering::Greater
        })?,
        "lt" => py_compare_op("lt", &lhs, &rhs, &mut info, |o| {
            o == std::cmp::Ordering::Less
        })?,
        "lte" => py_compare_op("lte", &lhs, &rhs, &mut info, |o| {
            o != std::cmp::Ordering::Greater
        })?,
        "eq" => {
            let m = equal(&lhs, &rhs);
            info["matched"] = json!(m);
            m
        }
        "ne" => {
            let m = !equal(&lhs, &rhs);
            info["matched"] = json!(m);
            m
        }
        // plaita-go extension: numeric bounds only, value is [low, high]
        "between" | "notBetween" => {
            let bounds = rhs.as_array().cloned().unwrap_or_default();
            if bounds.len() != 2 {
                let e = NodeError::transient(format!(
                    "value in between/notBetween condition should have two bound, but got len: {}",
                    bounds.len()
                ));
                info["error"] = json!(e.to_string());
                return Err(e);
            }
            let (Some(l), Some(hv)) = (
                plaita_expression::eval_value(&bounds[0], rc.as_ref())
                    .ok()
                    .as_ref()
                    .and_then(numeric),
                numeric(&bounds[1].clone()),
            ) else {
                let e = NodeError::transient("between bounds must be numbers");
                info["error"] = json!(e.to_string());
                return Err(e);
            };
            let v = numeric(&lhs)
                .ok_or_else(|| NodeError::transient("between requires a numeric field value"))?;
            let m = v >= l && v <= hv;
            let m = if c.operator == "notBetween" { !m } else { m };
            info["matched"] = json!(m);
            m
        }
        // Python `left in right`: right may be a string (substring), a list
        // (membership via ==) or a dict (key test)
        "in" | "notIn" => match py_contains("in", &rhs, &lhs) {
            Ok(m) => {
                let m = if c.operator == "notIn" { !m } else { m };
                info["matched"] = json!(m);
                m
            }
            Err(e) => {
                info["error"] = json!(e.to_string());
                return Err(NodeError::transient(e.to_string()));
            }
        },
        // plaita-go extension: SQL-style substring match on strings
        "like" => {
            let (Some(l), Some(r)) = (as_string(&lhs), as_string(&rhs)) else {
                let e = NodeError::transient("like requires string operands");
                info["error"] = json!(e.to_string());
                return Err(e);
            };
            let m = l.contains(&r);
            info["matched"] = json!(m);
            m
        }
        // Python `right in left`
        "contains" | "notContains" => match py_contains("contains", &lhs, &rhs) {
            Ok(m) => {
                let m = if c.operator == "notContains" { !m } else { m };
                info["matched"] = json!(m);
                m
            }
            Err(e) => {
                info["error"] = json!(e.to_string());
                return Err(NodeError::transient(e.to_string()));
            }
        },
        other => {
            let e = NodeError::transient(format!("unsupported operator: {other:?}"));
            info["error"] = json!(e.to_string());
            return Err(e);
        }
    };
    Ok((matched, info))
}

fn numeric(v: &Value) -> Option<f64> {
    match v {
        Value::Number(n) => n.as_f64(),
        _ => None,
    }
}

fn as_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

fn py_compare_op(
    op: &str,
    lhs: &Value,
    rhs: &Value,
    info: &mut Value,
    want: impl Fn(std::cmp::Ordering) -> bool,
) -> Result<bool, NodeError> {
    match py_compare(op, lhs, rhs) {
        Ok(ord) => {
            let m = want(ord);
            info["matched"] = json!(m);
            Ok(m)
        }
        Err(e) => {
            let ne = NodeError::transient(e.to_string());
            info["error"] = json!(ne.to_string());
            Err(ne)
        }
    }
}

/// Evaluates a condition group with short-circuit `and`/`or` (plaita's
/// default; the `switch:eagerMode` global is a plaita-go extension and is
/// not honoured here).
fn match_group(
    g: &LogicConditionGroup,
    rc: &SharedCtx,
) -> Result<(bool, Option<Value>), NodeError> {
    let relation = g.relation.as_str();
    if !matches!(relation, "and" | "or") {
        return Err(NodeError::transient(format!(
            "unsupported logic operator: {relation}"
        )));
    }
    let mut children: Vec<Value> = Vec::new();
    for raw in &g.conditions {
        let cond = Condition::from_value(raw);
        let (matched, info) = cond.match_context(rc)?;
        if let Some(info) = info {
            children.push(info);
        }
        let short_circuit = match relation {
            "and" => !matched,
            _ => matched,
        };
        if short_circuit {
            // Go returns nil matchInfo on short-circuit; the partial tree is
            // dropped with the verdict.
            return Ok((matched, None));
        }
    }
    Ok((
        relation == "and",
        Some(json!({ "childRelation": relation, "children": children })),
    ))
}
