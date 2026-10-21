//! The 45 plaita golden expression cases, run against this implementation.
//!
//! `testdata/expression_golden.json` is exported from
//! `plaita/tests/unit/test_expression_golden.py` by plaita-go's
//! `scripts/export_golden.py` and vendored verbatim — the same file the Go
//! engine's `pkg/expression/golden_test.go` runs. Context roots carry the
//! "$INPUT"-spelled keys of the export; they map onto evaluator roots here.

use plaita_expression::{eval_value, MapRoots};
use serde_json::{json, Value};

#[derive(serde::Deserialize)]
struct GoldenCase {
    id: String,
    expr: Value,
    #[serde(default)]
    context: serde_json::Map<String, Value>,
    expected: Value,
    #[serde(default)]
    note: String,
}

fn golden_cases() -> Vec<GoldenCase> {
    let data = include_str!("../testdata/expression_golden.json");
    serde_json::from_str(data).expect("parse golden cases")
}

/// Pushes both sides through a JSON domain where every number is a float, so
/// evaluator ints (i64) and JSON-decoded expected ints compare equal while
/// `3` and `3.0` still stay distinct from each other's bools/strings.
fn json_normalize(v: &Value) -> Value {
    match v {
        Value::Number(n) => {
            let f = n.as_f64().unwrap_or_else(|| n.to_string().parse().unwrap());
            json!(f)
        }
        Value::Array(a) => Value::Array(a.iter().map(json_normalize).collect()),
        Value::Object(o) => {
            let mut out = serde_json::Map::new();
            for (k, v) in o {
                out.insert(k.clone(), json_normalize(v));
            }
            Value::Object(out)
        }
        other => other.clone(),
    }
}

fn want_error(expected: &Value) -> bool {
    expected.get("$error").is_some()
}

fn golden_roots(context: &serde_json::Map<String, Value>) -> MapRoots<'static> {
    let mut r = MapRoots::new();
    for (k, v) in context {
        r.set(k.trim_start_matches('$'), v.clone());
    }
    r
}

#[test]
fn plaita_expression_golden() {
    let cases = golden_cases();
    assert!(!cases.is_empty(), "no golden cases");
    let mut ran = 0usize;
    for tc in &cases {
        if tc.id.contains("parse_function") {
            // tests plaita's parse_function API, not the evaluate facade
            continue;
        }
        let roots = golden_roots(&tc.context);
        let got = eval_value(&tc.expr, &roots);
        if want_error(&tc.expected) {
            let err = got.expect_err(&format!("{}: expected an error", tc.id));
            let want_kind = tc.expected["$error"].as_str().unwrap_or("?");
            let got_kind = format!("{:?}", err.kind);
            assert!(
                got_kind.starts_with(want_kind),
                "{id}: expected {want_kind}, got {err}",
                id = tc.id
            );
        } else {
            let got = got.unwrap_or_else(|e| panic!("{}: eval failed: {e}", tc.id));
            assert_eq!(
                json_normalize(&got),
                json_normalize(&tc.expected),
                "{}{}",
                tc.id,
                if tc.note.is_empty() {
                    String::new()
                } else {
                    format!(" ({})", tc.note)
                }
            );
        }
        ran += 1;
    }
    assert_eq!(
        ran, 43,
        "the export carries 45 cases, 2 parse_function API cases skipped"
    );
}
