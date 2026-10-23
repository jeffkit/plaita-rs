//! M2 验收行为测试 · §5.3 租约冲突处置（M2 registry 查询面启用 ack 分支；
//! go worker.go:1725-1753 leaseConflictDisposition 对拍）：决议载体永不
//! ack；其余载体仅当持有者注册项证明存活 running worker 时 ack（phase2_
//! resume_test.go:191-213 行族）；探针失败保守留 pending。

use std::sync::Arc;

use plaita_worker::config::now_iso;
use plaita_worker::fake::FakeRedis;
use plaita_worker::keys;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::{error::ErrorKind, lease, RedisBackend, Worker};
use serde_json::json;

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    metrics: Arc<RecordingMetrics>,
}

fn harness(name: &str) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("conf-{name}"),
        "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
        // this test's worker must NOT be the holder: a distinct instance id
        "PLAITA_INSTANCE_ID" => format!("worker-{name}-b"),
        _ => String::new(),
    });
    let backend = Arc::new(FakeRedis::new());
    let metrics = Arc::new(RecordingMetrics::default());
    let worker = Arc::new(Worker::new(
        backend.clone() as Arc<dyn plaita_worker::RedisBackend>,
        cfg.clone(),
        metrics.clone() as Arc<dyn plaita_worker::Metrics>,
    ));
    Harness {
        backend,
        cfg,
        worker,
        metrics,
    }
}

impl Harness {
    /// A live foreign holder owns the lease of `execution_id`.
    fn hold_externally(&self, execution_id: &str, holder_instance: &str) {
        lease::acquire(
            self.backend.as_ref(),
            &self.cfg,
            "",
            execution_id,
            &format!("resume:{holder_instance}:aaaa1111bbbb2222"),
            120,
        )
        .unwrap();
    }

    fn set_registry_status(&self, instance: &str, status: &str) {
        self.backend
            .set(
                &keys::registry_worker_key_of(&self.cfg, instance),
                &json!({
                    "instance_id": instance,
                    "service_type": "flow_worker",
                    "status": status,
                    "last_heartbeat": now_iso(),
                })
                .to_string(),
                Some(30),
            )
            .unwrap();
    }

    fn enqueue_resume(&self, execution_id: &str, resume_type: &str) {
        self.worker
            .queue()
            .enqueue(
                &json!({
                    "type": "resume", "flow_id": "wf", "execution_id": execution_id,
                    "resume_type": resume_type, "timestamp": now_iso()
                }),
                1,
            )
            .unwrap();
    }

    fn pel_len(&self) -> usize {
        self.backend
            .xpending_ext(
                &keys::queue_key(&self.cfg),
                &self.cfg.consumer_group,
                "-",
                "+",
                10,
            )
            .unwrap()
            .len()
    }
}

/// 验收 8：活持有者时普通载体 ack + 具名 Warn（#53 resume 无效果口径）。
#[test]
fn live_holder_proofs_a_non_decision_carrier_ack() {
    let h = harness("live");
    h.hold_externally("exec-live", "worker-live-a");
    h.set_registry_status("worker-live-a", "running");
    h.enqueue_resume("exec-live", "continue");
    let handled = h.worker.process_one().unwrap();
    assert!(handled, "acked under live-holder proof");
    assert_eq!(h.pel_len(), 0, "the carrier was acked");
}

/// 验收 8：决议载体（event/timeout/cancel）在冲突窗口内永不 ack——即使
/// 持有者活证在场（A47-F2 语义）。
#[test]
fn decision_carriers_never_ack_even_with_live_holder() {
    for rt in ["event", "timeout", "cancel"] {
        let h = harness("decision");
        h.hold_externally("exec-dec", "worker-dec-a");
        h.set_registry_status("worker-dec-a", "running");
        h.enqueue_resume("exec-dec", rt);
        let err = h.worker.process_one().unwrap_err();
        assert_eq!(err.kind(), ErrorKind::LeaseConflict, "resume_type={rt}");
        assert_eq!(h.pel_len(), 1, "resume_type={rt}: stays pending");
    }
}

/// 持有者非 running（draining/缺失/损坏）→ 保守留 pending。
#[test]
fn non_running_holder_parks() {
    // draining holder
    let h = harness("draining");
    h.hold_externally("exec-d", "worker-drain-a");
    h.set_registry_status("worker-drain-a", "draining");
    h.enqueue_resume("exec-d", "continue");
    assert_eq!(
        h.worker.process_one().unwrap_err().kind(),
        ErrorKind::LeaseConflict
    );
    assert_eq!(h.pel_len(), 1);
    h.worker
        .queue()
        .ack(&{
            let ids: Vec<String> = h
                .backend
                .xpending_ext(
                    &keys::queue_key(&h.cfg),
                    &h.cfg.consumer_group,
                    "-",
                    "+",
                    10,
                )
                .unwrap()
                .into_iter()
                .map(|p| p.id)
                .collect();
            ids[0].clone()
        })
        .unwrap();

    // registry entry missing (expired with the TTL)
    let h = harness("missing");
    h.hold_externally("exec-m", "worker-gone-a");
    h.enqueue_resume("exec-m", "continue");
    assert_eq!(
        h.worker.process_one().unwrap_err().kind(),
        ErrorKind::LeaseConflict
    );
    assert_eq!(h.pel_len(), 1);

    // corrupt registry entry = probe failure → conservative park (the error
    // surfaces as the probe failure itself; the message stays pending either
    // way — §5.3 探针失败按最保守分支)
    let h = harness("corrupt");
    h.hold_externally("exec-c", "worker-corrupt-a");
    h.backend
        .set(
            &keys::registry_worker_key_of(&h.cfg, "worker-corrupt-a"),
            "{not json",
            Some(30),
        )
        .unwrap();
    h.enqueue_resume("exec-c", "continue");
    let err = h.worker.process_one().unwrap_err();
    assert!(
        matches!(err.kind(), ErrorKind::LeaseConflict | ErrorKind::Transient),
        "probe failure parks: {err}"
    );
    assert_eq!(h.pel_len(), 1);
}

/// 持有者反解失败（legacy 数值形态）→ 不可解析=存活未知 → 保守留 pending。
#[test]
fn unparsable_holder_parks() {
    let h = harness("unparsable");
    // a holder value that fails holder_instance_id (legacy numeric form)
    let handle = {
        let backend = h.backend.as_ref();
        lease::acquire(backend, &h.cfg, "", "exec-u", "resume:12345", 120).unwrap()
    };
    let _ = handle; // legacy value "resume:12345:1" — holder unparsable
    h.enqueue_resume("exec-u", "continue");
    assert_eq!(
        h.worker.process_one().unwrap_err().kind(),
        ErrorKind::LeaseConflict
    );
    assert_eq!(h.pel_len(), 1);
}
