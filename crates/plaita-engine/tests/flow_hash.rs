//! 流程定义指纹门（plaita#60 语义，B4 r11）：规范化编码器逐字节对拍
//! Python 权威（62 案 golden，fixture 自 plaita-go
//! `pkg/worker/testdata/flow_hash_golden.json` 逐字复用——该 fixture 由
//! plaita-go `scripts/export_flow_hash_golden.py` 从 plaita Python 真跑
//! 导出，禁止手改）；内嵌 `definitionHash` 门的四格负控：
//! 匹配装载 / 不匹配具名拒 / 无 hash 兼容 / 未知算法具名拒。

use plaita_engine::ir;

fn golden() -> Vec<serde_json::Value> {
    let data = std::fs::read(format!(
        "{}/testdata/flow_hash_golden.json",
        env!("CARGO_MANIFEST_DIR")
    ))
    .expect("read flow_hash_golden.json");
    let v: serde_json::Value = serde_json::from_slice(&data).expect("decode golden fixture");
    v["cases"].as_array().cloned().expect("cases array")
}

/// 跨引擎钉死（#60）：每一案 canonical 文本与 sha256 必须字节等于 Python
/// 权威输出；`go_rejects`/`err_*` 案必须具名拒（fail-closed——不给 Python
/// 拒绝的定义算指纹）。矩阵守卫同 Go：a1_* ≥10、非拒扩展案 ≥20。
#[test]
fn flow_hash_golden_matches_python() {
    let cases = golden();
    let mut a1 = 0;
    let mut hashed = 0;
    for c in &cases {
        let name = c["name"].as_str().unwrap();
        let raw = c["raw"].as_str().unwrap();
        let rejects =
            c["go_rejects"].as_bool().unwrap() || c["python_hash"].as_str().unwrap().is_empty();
        if rejects {
            let err = ir::flow_definition_hash(raw.as_bytes()).unwrap_err();
            assert!(!err.is_empty(), "{name}: expected a named rejection");
            continue;
        }
        let canonical = ir::canonical_definition_json(raw.as_bytes())
            .unwrap_or_else(|e| panic!("{name}: canonicalize: {e}"));
        assert_eq!(
            canonical,
            c["python_canonical"].as_str().unwrap(),
            "{name}: canonical text diverges from Python"
        );
        let hash = ir::flow_definition_hash(raw.as_bytes())
            .unwrap_or_else(|e| panic!("{name}: hash: {e}"));
        assert_eq!(
            hash,
            c["python_hash"].as_str().unwrap(),
            "{name}: hash diverges from Python"
        );
        if name.starts_with("a1_") {
            a1 += 1;
        }
        hashed += 1;
    }
    assert!(
        a1 >= 10 && hashed >= 50,
        "golden matrix shrunk: a1={a1} hashed={hashed} total={}",
        cases.len()
    );
}

fn base_flow() -> &'static str {
    r#"{"flow_id":"gated","nodes":[{"type":"start","id":"s","next":"e"},{"type":"end","id":"e","resultType":"success","output":"ok"}]}"#
}

/// 格1（hash 匹配装载成功）：正确的 `definitionHash`（+`hashAlgo`）→
/// 装载通过，保留键剥离不进引擎管道（装载语义与无 hash 文档一致）。
#[test]
fn matching_hash_loads() {
    let h = ir::flow_definition_hash(base_flow().as_bytes()).unwrap();
    let doc = format!(
        r#"{{"definitionHash":"{h}","hashAlgo":"{}","nodes":[{{"type":"start","id":"s","next":"e"}},{{"type":"end","id":"e","resultType":"success","output":"ok"}}],"flow_id":"gated"}}"#,
        plaita_engine::ir::FLOW_HASH_ALGO
    );
    let flow = ir::load(doc.as_bytes()).unwrap_or_else(|e| panic!("matching hash must load: {e}"));
    assert_eq!(flow.id, "gated");
}

/// 格2（不匹配具名拒）：定义内容被篡改而 `definitionHash` 未刷新 → 装载
/// 拒绝，错误含 `flow_hash_mismatch` 具名令牌与双侧摘要（镜像 #60 语义：
/// mismatch → 具名终态可判，不静默；装载前拒绝，不给带病定义建 Flow）。
#[test]
fn mismatching_hash_is_a_named_rejection() {
    let h = ir::flow_definition_hash(base_flow().as_bytes()).unwrap();
    let doc = format!(
        r#"{{"definitionHash":"{h}","flow_id":"gated","nodes":[{{"type":"start","id":"s","next":"e"}},{{"type":"end","id":"e","resultType":"success","output":"tampered"}}]}}"#
    );
    let err = ir::load(doc.as_bytes()).unwrap_err();
    assert!(
        err.contains("flow_hash_mismatch"),
        "expected the named mismatch rejection, got: {err}"
    );
    assert!(
        err.contains(&h) && err.contains("current="),
        "rejection must carry both digests, got: {err}"
    );
}

/// 格3（无 hash 兼容）：不带 `definitionHash` 的旧定义照常装载——
/// 向后兼容存量（Python classify 的 no_guard 同语义）。
#[test]
fn no_hash_field_stays_compatible() {
    let flow = ir::load(base_flow().as_bytes()).expect("legacy doc must load unchanged");
    assert_eq!(flow.id, "gated");
}

/// 附加格（未知算法具名拒）：`hashAlgo` 在场但非本引擎算法 → 具名拒——
/// 拒绝跨算法比摘要（拿 v1 老口径摘要对 v2 规范化比只会读出假 mismatch）。
#[test]
fn unsupported_algo_is_named_rejected() {
    let doc = r#"{"definitionHash":"deadbeef","hashAlgo":"flow-dump-json-sortkeys-v1","flow_id":"gated","nodes":[]}"#;
    let err = ir::load(doc.as_bytes()).unwrap_err();
    assert!(
        err.contains("unsupported") && err.contains("hashAlgo"),
        "expected the named unsupported-algo rejection, got: {err}"
    );
}

/// 附加格（hashAlgo 单独在场、无 definitionHash）：跳过门（无摘要可校），
/// 装载照常。
#[test]
fn algo_field_alone_skips_the_gate() {
    let doc = r#"{"hashAlgo":"flow-definition-json-sortkeys-v2-rust","flow_id":"gated","nodes":[{"type":"start","id":"s","next":"e"},{"type":"end","id":"e","resultType":"success","output":"ok"}]}"#;
    let flow = ir::load(doc.as_bytes()).expect("algo-only doc must load (no digest to verify)");
    assert_eq!(flow.id, "gated");
}

/// 附加格（规范化不敏感面）：键序与空白不同、语义相同的两份文档 → 同一
/// 指纹（sort_keys + 解析后规范化），且各自嵌入对方的 hash 都能装载。
#[test]
fn fingerprint_is_canonicalization_invariant() {
    let a = r#"{"flow_id":"k","nodes":[{"type":"start","id":"s","next":"e"},{"type":"end","id":"e","resultType":"success","output":"ok"}]}"#;
    let b = r#"{ "nodes" : [ {"type":"start","id":"s","next":"e"} , {"type":"end","id":"e","resultType":"success","output":"ok"} ], "flow_id":"k" }"#;
    let ha = ir::flow_definition_hash(a.as_bytes()).unwrap();
    let hb = ir::flow_definition_hash(b.as_bytes()).unwrap();
    assert_eq!(
        ha, hb,
        "same definition, different layout: hashes must agree"
    );
    let doc = format!(
        r#"{{"definitionHash":"{hb}","flow_id":"k","nodes":[{{"type":"start","id":"s","next":"e"}},{{"type":"end","id":"e","resultType":"success","output":"ok"}}]}}"#
    );
    ir::load(doc.as_bytes())
        .unwrap_or_else(|e| panic!("cross-layout embedded hash must load: {e}"));
}
