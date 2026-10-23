//! 指纹 golden（M1 验收 10，rs-worker-design §4.6）：rs 的
//! `flow_definition_hash` 必须与 `testdata/flow_hash_golden.json`（自
//! plaita Python 引擎现场导出、plaita-go 逐字复用的 fixture）全量一致——
//! canonical 文本逐字节相等 + sha256 相等；`go_rejects` 家族必须被拒绝。

use plaita_worker::flowsource::{canonical_flow_definition_json, flow_definition_hash};
use serde_json::Value;

fn golden_cases() -> Vec<Value> {
    let data = include_str!("../testdata/flow_hash_golden.json");
    let fixture: Value = serde_json::from_str(data).expect("decode golden fixture");
    // Matrix-size guard: the fixture must always carry the full A1 r120
    // ten-family matrix plus the extended families (≥20 non-a1 accepted).
    let cases = fixture["cases"].as_array().cloned().expect("cases array");
    let a1 = cases
        .iter()
        .filter(|c| c["name"].as_str().unwrap_or("").starts_with("a1_"))
        .count();
    let extended = cases
        .iter()
        .filter(|c| {
            !c["go_rejects"].as_bool().unwrap_or(false)
                && !c["name"].as_str().unwrap_or("").starts_with("a1_")
        })
        .count();
    assert!(
        a1 >= 10 && extended >= 20,
        "golden matrix shrunk: a1={a1} (want ≥10) extended={extended} (want ≥20)"
    );
    cases
}

#[test]
fn flow_hash_golden_matches_python() {
    for case in golden_cases() {
        let name = case["name"].as_str().unwrap_or("");
        let go_rejects = case["go_rejects"].as_bool().unwrap_or(false);
        if go_rejects || name.starts_with("err_") {
            continue;
        }
        let raw = case["raw"].as_str().expect("raw");
        let want_canonical = case["python_canonical"].as_str().expect("canonical");
        let want_hash = case["python_hash"].as_str().expect("hash");
        let canonical = canonical_flow_definition_json(raw.as_bytes())
            .unwrap_or_else(|e| panic!("{name}: canonicalize: {e}"));
        assert_eq!(
            String::from_utf8(canonical).unwrap(),
            want_canonical,
            "{name}: canonical text diverges from Python"
        );
        let got =
            flow_definition_hash(raw.as_bytes()).unwrap_or_else(|e| panic!("{name}: hash: {e}"));
        assert_eq!(got, want_hash, "{name}: hash diverges from Python");
    }
}

#[test]
fn flow_hash_golden_rejections_stay_rejected() {
    for case in golden_cases() {
        let name = case["name"].as_str().unwrap_or("");
        let go_rejects = case["go_rejects"].as_bool().unwrap_or(false);
        if !go_rejects && !name.starts_with("err_") {
            continue;
        }
        let raw = case["raw"].as_str().expect("raw");
        assert!(
            flow_definition_hash(raw.as_bytes()).is_err(),
            "{name}: accepted {raw:?} — must stay rejected"
        );
    }
}

#[test]
fn rs_algo_marker_is_family_stamped_and_distinct_from_python() {
    // The #60 lesson: a shared label on a separately-maintained
    // implementation is a "same name, different algorithm" blind spot — rs
    // carries its own family stamp.
    assert_eq!(
        plaita_worker::flowsource::FLOW_HASH_ALGO,
        "flow-definition-json-sortkeys-v2-rs"
    );
    assert_ne!(
        plaita_worker::flowsource::FLOW_HASH_ALGO,
        "flow-definition-json-sortkeys-v2",
        "rs algo marker must differ from Python's pre-#60 marker"
    );
}
