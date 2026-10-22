//! parallel 分支并发上限（B4 r7 对齐 Python 后台池，A5）：plaita
//! `parallel_executor.py` 的模块级共享池 `ThreadPoolExecutor(max_workers=
//! PLAITA_BG_THREAD_WORKERS||8)` 把**所有** parallel 节点（join+后台、嵌套
//! 与否）的并发分支流压在同一预算内——16 分支默认峰值 8（env=2 实测峰值
//! 2，排队语义）。Rust 先前每分支一线程无上限（60 分支=61 并发 sidecar，
//! r5 实测）。

use plaita_engine::{ir, Host};
use serde_json::{json, Value};

const BRANCH_CODE: &str = "import time\ndef run(input):\n    s=time.time()\n    time.sleep(0.8)\n    return {'start': s, 'end': time.time()}\n";

fn cap_flow(n: usize) -> String {
    let branches: Vec<String> = (0..n)
        .map(|i| {
            format!(
                r#"{{"name": "b{i}", "flow": {{
                    "flow_id": "br{i}",
                    "nodes": [
                        {{"type": "start", "id": "s", "next": "c"}},
                        {{"type": "code", "id": "c", "language": "python",
                          "sandbox_backend": "subprocess", "code": {code:?}, "next": "e"}},
                        {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.c"}}
                    ]}}}}"#,
                i = i,
                code = BRANCH_CODE
            )
        })
        .collect();
    let joins: Vec<String> = (0..n).map(|i| format!("\"b{i}\"")).collect();
    format!(
        r#"{{
        "flow_id": "cap-probe",
        "nodes": [
            {{"type": "start", "id": "s", "next": "p"}},
            {{"type": "parallel", "id": "p", "next": "e",
              "branches": [{}],
              "joinBranches": [{}]}},
            {{"type": "end", "id": "e", "resultType": "success", "output": "$NODE.p"}}
        ]}}"#,
        branches.join(","),
        joins.join(",")
    )
}

/// 区间重叠峰值（各分支 code sidecar 上报 start/end 墙钟）。
fn peak_overlap(result: &Value) -> usize {
    let mut iv: Vec<(f64, f64)> = result
        .as_object()
        .expect("join results object")
        .values()
        .map(|v| (v["start"].as_f64().unwrap(), v["end"].as_f64().unwrap()))
        .collect();
    iv.sort_by(|a, b| a.0.total_cmp(&b.0));
    iv.iter()
        .map(|&(s, _)| iv.iter().filter(|&&(s2, e2)| s2 <= s && s < e2).count())
        .max()
        .unwrap_or(0)
}

/// 默认上限 = 8（Python 后台池缺省 worker 数）：16 分支的峰值重叠必须 ≤8，
/// 且全部 16 个分支结果齐全（排队语义：不丢分支、不失败）。
/// 负控 RED：无上限实现峰值=16，断言失败。
#[test]
fn parallel_branches_peak_at_the_default_pool_size() {
    let data = cap_flow(16);
    let flow = ir::load(data.as_bytes()).unwrap();
    let got = flow.execute(&json!({}), &Host::new()).unwrap();
    let obj = got.as_object().unwrap();
    assert_eq!(obj.len(), 16, "every join branch reports a result");
    let peak = peak_overlap(&got);
    assert!(
        peak <= 8,
        "peak concurrent branch flows must respect the default pool size of 8, got {peak}"
    );
}
