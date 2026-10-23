//! 亲和让渡端到端（rs#17 WK-M3；go handleForeignTarget worker.go:2232-2334
//! + terminalizeHandoverExhausted :2340-2443 对拍）：
//!
//! 1. 目标不在 → 剥 target 广播（A46-02：死目标不保留）+ 台账 DECISION_BROADCAST；
//! 2. 目标活着 → 定向转投 + 计数 INCR + 台账 DECISION_TARGETED；
//! 3. 让渡界收敛：计数超 AffinityHandoverMax(3) → 有界终态
//!    AffinityHandoverExhausted（无租约时 fenced 写）；
//! 4. 挂起执行永不终态化（决策载体保持可投递）；
//! 5. 计数不可用 = 界失效 = 原条留 pending（fail-closed，不 DLQ）。

use std::sync::Arc;

use plaita_worker::fake::FakeRedis;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::{keys, state, Config, Metrics, RedisBackend, Worker};
use serde_json::json;

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: Config,
    worker: Arc<Worker>,
}

fn harness_with(name: &str, instance: &str, extra: &dyn Fn(&str) -> String) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("af-{name}"),
        _ => extra(k),
    });
    let cfg = plaita_worker::Config {
        instance_id: instance.to_string(),
        ..cfg
    };
    let backend = Arc::new(FakeRedis::new());
    let metrics = Arc::new(RecordingMetrics::default());
    let worker = Arc::new(Worker::new(
        backend.clone() as Arc<dyn RedisBackend>,
        cfg.clone(),
        metrics as Arc<dyn Metrics>,
    ));
    Harness {
        backend,
        cfg,
        worker,
    }
}

fn harness(name: &str, instance: &str) -> Harness {
    harness_with(name, instance, &|_| String::new())
}

impl Harness {
    /// 让目标实例"活着"：以目标 instance_id 写注册键（status=running）——
    /// Registry::new + flush 即可（LookupStatus 读注册键状态字段）。
    fn make_target_alive(&self, target: &str) {
        let target_cfg = plaita_worker::Config {
            instance_id: target.to_string(),
            ..self.cfg.clone()
        };
        let registry = plaita_worker::registry::Registry::new(
            self.backend.clone() as Arc<dyn RedisBackend>,
            target_cfg,
            Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        );
        registry.set_status(plaita_worker::registry::REG_STATUS_RUNNING);
    }

    /// Seeds a capacity-routed start carrier addressed to `target`.
    fn seed_targeted(&self, execution_id: &str, target: &str) {
        self.backend
            .xadd(
                &keys::queue_key(&self.cfg),
                &[
                    (
                        "payload".to_string(),
                        json!({
                            "type": "start", "flow_id": "afwf",
                            "execution_id": execution_id,
                            "target_instance_id": target,
                            "params": {}, "timestamp": "2026-10-24T00:00:00Z"
                        })
                        .to_string(),
                    ),
                    ("schema_version".to_string(), "1".to_string()),
                ],
            )
            .unwrap();
    }

    fn seed_suspended(&self, execution_id: &str) {
        let row = state::ExecutionState {
            execution_id: execution_id.to_string(),
            status: state::STATUS_SUSPENDED.to_string(),
            ..Default::default()
        };
        state::save(
            self.backend.as_ref(),
            &self.cfg,
            "",
            &row,
            0, // fence_gen=0: bootstrap write (no lease yet)
        )
        .unwrap();
    }

    fn row(&self, execution_id: &str) -> Option<state::ExecutionState> {
        state::load(self.backend.as_ref(), &self.cfg, "", execution_id).unwrap()
    }
}

#[test]
fn target_absent_broadcasts_without_target() {
    let h = harness("absent", "worker-a");
    h.seed_targeted("exec-absent", "worker-gone");
    h.worker.process_one().unwrap();

    // 剥 target 广播：队列流尾出现一条无 target_instance_id 的同型载体。
    let entries = h
        .backend
        .xrange(&keys::queue_key(&h.cfg), "-", "+", None)
        .unwrap();
    let broadcast = entries
        .iter()
        .filter_map(|e| {
            e.fields
                .iter()
                .find(|(k, _)| k == "payload")
                .and_then(|(_, v)| serde_json::from_str::<serde_json::Value>(v).ok())
        })
        .find(|body| body.get("target_instance_id").is_none())
        .expect("a broadcast carrier must be re-enqueued");
    assert!(
        broadcast.get("target_instance_id").is_none(),
        "dead target must be stripped (A46-02): {broadcast}"
    );
    // 原条 ack（PEL 清空）。
    let pel = h
        .backend
        .xpending_ext(
            &keys::queue_key(&h.cfg),
            &h.cfg.consumer_group,
            "-",
            "+",
            10,
        )
        .unwrap();
    assert!(
        pel.is_empty(),
        "original must be acked after the broadcast enqueue, got {} entries",
        pel.len()
    );
    // 台账：DECISION_BROADCAST 可读。
    let ledger = h
        .backend
        .xrange(&keys::dispatch_ledger_key(&h.cfg), "-", "+", None)
        .unwrap();
    assert!(
        ledger.iter().any(|e| {
            e.fields
                .iter()
                .any(|(k, v)| k == "payload" && v.contains("\"decision\":\"broadcast\""))
        }),
        "broadcast decision must be ledgered"
    );
}

#[test]
fn target_alive_forwards_and_counts() {
    let h = harness("forward", "worker-a");
    h.make_target_alive("worker-b");
    h.seed_targeted("exec-fwd", "worker-b");
    h.worker.process_one().unwrap();

    // 计数键 +1。
    let key = keys::affinity_handover_execution_key(&h.cfg, "", "exec-fwd");
    assert_eq!(h.backend.get(&key).unwrap(), Some("1".to_string()));
    // 台账：DECISION_TARGETED。
    let ledger = h
        .backend
        .xrange(&keys::dispatch_ledger_key(&h.cfg), "-", "+", None)
        .unwrap();
    assert!(
        ledger.iter().any(|e| {
            e.fields.iter().any(|(k, v)| {
                k == "payload"
                    && v.contains("\"decision\":\"targeted\"")
                    && v.contains("TaskNotForThisWorker: handover")
            })
        }),
        "handover decision must be ledgered"
    );
}

#[test]
fn handover_budget_converges_to_named_terminal() {
    let h = harness("converge", "worker-a");
    h.make_target_alive("worker-b");
    let execution_id = "exec-conv";
    // 先落一行 running 执行行（终态化需要一个存在行可翻；#51 同款终态写面）。
    let row = state::ExecutionState {
        execution_id: execution_id.to_string(),
        status: state::STATUS_RUNNING.to_string(),
        ..Default::default()
    };
    state::save(h.backend.as_ref(), &h.cfg, "", &row, 0).unwrap();
    // 无活租约、无挂起：反复让渡直至越界——第 4 次（count>3）走有界终态。
    // 目标恒活（广播分支不触发；回投载体仍带 target_instance_id，本实例
    // process_one 再走亲和闸）。
    for _ in 0..4 {
        h.seed_targeted(execution_id, "worker-b");
        h.worker.process_one().unwrap();
    }
    let row = h.row(execution_id).expect("terminal row must exist");
    assert_eq!(row.status, state::STATUS_ERROR);
    let err = row.error.expect("error payload");
    assert_eq!(err["error_type"], "AffinityHandoverExhausted");
    assert_eq!(err["details"]["affinity_handovers"], 4);
}

#[test]
fn handover_exhausted_on_suspended_never_terminalizes() {
    let h = harness("susp", "worker-a");
    h.make_target_alive("worker-b");
    let execution_id = "exec-susp";
    h.seed_suspended(execution_id);
    for _ in 0..4 {
        h.seed_targeted(execution_id, "worker-b");
        let _ = h.worker.process_one();
    }
    let row = h.row(execution_id).unwrap();
    assert_eq!(
        row.status,
        state::STATUS_SUSPENDED,
        "a suspended execution waits for its decision carrier; never terminalized"
    );
}

#[test]
fn counter_unavailable_leaves_pending_fail_closed() {
    let h = harness("nofail", "worker-a");
    h.make_target_alive("worker-b");
    let execution_id = "exec-nofail";
    // 计数不可用（Python WRONGTYPE 键污染事故面）：fake 的 INCR 故障注入
    // 与真 Redis WRONGTYPE 同构——INCR 报错 = 界失效 = 原条留 pending
    // （不 DLQ、不广播、不终态）。
    h.backend.fail_next_incr(1);
    h.seed_targeted(execution_id, "worker-b");
    let err = h.worker.process_one().unwrap_err();
    assert!(
        err.to_string().contains("counter unavailable"),
        "named fail-closed error expected, got: {err}"
    );
}
