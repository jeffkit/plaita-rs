//! 回扫器验收行为测试（rs#17 WK-M3；rs-worker-design §8.2 桥「订阅确认前
//! 丢事件」窗口收口）：确认窗口内到达（未经理 Pub/Sub 推送）的事件经事件
//! 存储对账仍投递；回放幂等；入队失败整轮作废游标不动（#153）；载荷缺失
//! 惰性 ZREM；开关旁路。真 Redis 同构腿在 tests/redis_integration.rs。

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use plaita_worker::config::{float_epoch, now_iso};
use plaita_worker::fake::FakeRedis;
use plaita_worker::keys;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::queue::TaskQueue;
use plaita_worker::reconcile::{self, reconcile_loop, scan_once};
use plaita_worker::state;
use plaita_worker::subscriptions::RedisSubscriptions;
use plaita_worker::{checkpoint, flowsource, RedisBackend, Worker};
use serde_json::{json, Value};

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    queue: Arc<TaskQueue>,
    subs: Arc<RedisSubscriptions>,
}

fn harness_with(name: &str, extra: &dyn Fn(&str) -> String) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("rc-e2e-{name}"),
        "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
        _ => extra(k),
    });
    let backend = Arc::new(FakeRedis::new());
    let metrics = Arc::new(RecordingMetrics::default());
    let worker = Arc::new(Worker::new(
        backend.clone() as Arc<dyn RedisBackend>,
        cfg.clone(),
        metrics.clone() as Arc<dyn plaita_worker::Metrics>,
    ));
    let queue = Arc::new(TaskQueue::new(
        backend.clone() as Arc<dyn RedisBackend>,
        cfg.clone(),
        metrics.clone() as Arc<dyn plaita_worker::Metrics>,
    ));
    let subs = Arc::new(RedisSubscriptions::new(
        backend.clone() as Arc<dyn RedisBackend>,
        cfg.clone(),
    ));
    Harness {
        backend,
        cfg,
        worker,
        queue,
        subs,
    }
}

fn harness(name: &str) -> Harness {
    harness_with(name, &|_| String::new())
}

const EVENT_FLOW: &str = r#"{"flow_id":"wfr","nodes":[
    {"type":"start","id":"s","next":"e1"},
    {"type":"event","id":"e1","event_type":"tick","next":"mid"},
    {"type":"assignment","id":"mid","output":{"seen":"$NODE.e1.status"},"next":"end"},
    {"type":"end","id":"end","output":"$NODE.mid","resultType":"success"}
]}"#;

impl Harness {
    /// 挂起一个执行（订阅/类型索引随挂起登记），返回 (execution_id, sub_id)。
    fn suspend_execution(&self, execution_id: &str) -> String {
        flowsource::preset_flow_definition(
            self.backend.as_ref(),
            &self.cfg,
            "",
            "wfr",
            "v1",
            EVENT_FLOW.as_bytes(),
        )
        .unwrap();
        self.queue
            .enqueue(
                &json!({
                    "type": "start",
                    "flow_id": "wfr",
                    "execution_id": execution_id,
                    "params": {"who": "rc"},
                    "timestamp": now_iso(),
                }),
                1,
            )
            .unwrap();
        self.worker.process_one().unwrap();
        let row = state::load(self.backend.as_ref(), &self.cfg, "", execution_id)
            .unwrap()
            .unwrap();
        assert_eq!(row.status, state::STATUS_SUSPENDED);
        let cp = checkpoint::load(self.backend.as_ref(), &self.cfg, "", execution_id)
            .unwrap()
            .expect("checkpoint");
        cp.context["nodes"]["e1"]["subscription_id"]
            .as_str()
            .expect("subscription id")
            .to_string()
    }

    /// 写事件存储两键（镜像 Python store_event_sync：记录 + 类型索引）。
    fn store_event(&self, event_id: &str, timestamp: f64, correlation_id: &str) {
        let record = json!({
            "event_id": event_id,
            "event_type": "tick",
            "data": {"status": "completed"},
            "timestamp": timestamp,
            "source": "producer",
            "correlation_id": correlation_id,
            "tenant_id": null,
        });
        self.backend
            .set(
                &keys::event_storage_record_key(&self.cfg, event_id),
                &record.to_string(),
                Some(7 * 86400),
            )
            .unwrap();
        self.backend
            .zadd(
                &keys::event_storage_type_index_key(&self.cfg, "tick"),
                timestamp,
                event_id,
            )
            .unwrap();
    }

    fn bucket_members(&self) -> Vec<String> {
        self.backend
            .zrangebyscore(
                &keys::event_storage_type_index_key(&self.cfg, "tick"),
                0.0,
                f64::MAX,
                1000,
            )
            .unwrap()
    }

    fn cursor(&self) -> Option<String> {
        self.backend
            .get(&keys::event_reconcile_cursor_key(&self.cfg))
            .unwrap()
    }

    fn queue_len(&self) -> i64 {
        self.backend.xlen(&keys::queue_key(&self.cfg)).unwrap()
    }
}

/// 验收主钉：确认窗口内到达的事件（存储在案、Pub/Sub 通知从未发出）经
/// 回扫仍投递——恰一条 event 载体，resume 跑到 completed；游标推进到窗口
/// 上界（排空确认）。
#[test]
fn event_missed_by_the_bridge_is_delivered_via_reconcile() {
    let h = harness("window");
    let exec = "exec-rc-1";
    let _sub = h.suspend_execution(exec);

    // 事件发布只落存储（生产者的 store_event_sync 半边），Pub/Sub 通知在
    // 桥确认窗口内丢失——worker 侧从未收到。
    let ts = float_epoch() - 60.0;
    h.store_event("evt-rc-1", ts, exec);
    assert_eq!(
        h.queue_len(),
        0,
        "no Pub/Sub notification reached the worker"
    );

    let outcome = scan_once(
        &(h.backend.clone() as Arc<dyn RedisBackend>),
        &h.cfg,
        &h.queue,
        &h.subs,
    )
    .unwrap();
    assert_eq!(outcome.scanned, 1);
    assert_eq!(outcome.replayed, 1);
    assert!(outcome.drained, "short pages only: window drained");
    assert_eq!(h.queue_len(), 1, "exactly one resume carrier");

    // 载体形状（§8.2 第 6 步：data.subscription_id 直喂引擎绑定校验）。
    let entries = h
        .backend
        .xrange(&keys::queue_key(&h.cfg), "-", "+", Some(10))
        .unwrap();
    let payload = entries[0].field("payload").unwrap();
    let body: Value = serde_json::from_str(payload).unwrap();
    assert_eq!(body["type"], "resume");
    assert_eq!(body["resume_type"], "event");
    assert_eq!(body["execution_id"], exec);
    assert_eq!(body["data"]["event_id"], "evt-rc-1");
    assert!(
        body["data"]["subscription_id"].as_str().is_some(),
        "subscription binding rides in the carrier: {body}"
    );

    // resume 跑到完成（真唤醒链与推送路径共用）。
    h.worker.process_one().unwrap();
    let row = state::load(h.backend.as_ref(), &h.cfg, "", exec)
        .unwrap()
        .unwrap();
    assert_eq!(row.status, state::STATUS_COMPLETED);
    assert_eq!(row.context["nodes"]["mid"]["seen"], "completed");

    // 游标 = 窗口上界；下一轮窗口不再覆盖旧事件（scanned 0，无重复载体）。
    let cursor = h.cursor().expect("cursor written");
    let (cur_ts, _) = cursor.split_once('|').unwrap();
    let cur_ts: f64 = cur_ts.parse().unwrap();
    assert!(cur_ts >= ts, "cursor at/after the window end: {cursor}");
    let outcome2 = scan_once(
        &(h.backend.clone() as Arc<dyn RedisBackend>),
        &h.cfg,
        &h.queue,
        &h.subs,
    )
    .unwrap();
    assert_eq!(outcome2.scanned, 0, "drained window not re-scanned");
    assert_eq!(
        h.queue_len(),
        0,
        "the sole carrier was consumed by the resume; no duplicate landed"
    );
}

/// 回放幂等：游标丢失（回填重扫同窗）时去重键压住重复 resume——多扫不
/// 重投（at-least-once 兜底安全的根据）。
#[test]
fn rescan_after_cursor_loss_is_dedup_suppressed() {
    let h = harness("dedup");
    let exec = "exec-rc-2";
    let _sub = h.suspend_execution(exec);
    h.store_event("evt-rc-2", float_epoch() - 30.0, exec);
    let b = h.backend.clone() as Arc<dyn RedisBackend>;
    scan_once(&b, &h.cfg, &h.queue, &h.subs).unwrap();
    assert_eq!(h.queue_len(), 1);
    // 游标丢失：同窗重扫，事件重取但去重键（1h）拒绝二次载体。
    h.backend
        .del(&[keys::event_reconcile_cursor_key(&h.cfg).as_str()])
        .unwrap();
    let outcome = scan_once(&b, &h.cfg, &h.queue, &h.subs).unwrap();
    assert_eq!(
        outcome.scanned, 1,
        "event re-fetched from the backfill window"
    );
    assert_eq!(
        outcome.replayed, 1,
        "replay runs (handle_event dedups inside)"
    );
    assert_eq!(h.queue_len(), 1, "but NO duplicate carrier is enqueued");
}

/// 入队失败 → 整轮作废：失败事件不计 replayed、游标不动（#153）；恢复后
/// 同一事件被真正重放，不被「失败也算成功」的游标永久跳过。
#[test]
fn enqueue_failure_voids_the_round_and_holds_the_cursor() {
    let h = harness("void");
    let exec = "exec-rc-3";
    let _sub = h.suspend_execution(exec);
    let now = float_epoch();
    h.store_event("evt-old", now - 20.0, exec);
    h.store_event("evt-new", now - 10.0, exec);
    let b = h.backend.clone() as Arc<dyn RedisBackend>;

    h.backend.fail_next_xadd(1);
    let err = scan_once(&b, &h.cfg, &h.queue, &h.subs).unwrap_err();
    assert!(
        err.to_string().contains("reconcile"),
        "the replay failure surfaces: {err}"
    );
    assert!(
        h.cursor().is_none(),
        "cursor untouched after a voided round"
    );
    assert_eq!(h.queue_len(), 0, "no carrier landed for the failed event");

    // 故障恢复：同窗重扫，两个事件（含失败的 evt-old）都被真正重放。
    let outcome = scan_once(&b, &h.cfg, &h.queue, &h.subs).unwrap();
    assert_eq!(
        outcome.replayed, 2,
        "the failed event is retried, not skipped"
    );
    assert!(h.cursor().is_some(), "drained round advances the cursor");
    assert_eq!(h.queue_len(), 2);
}

/// 载荷缺失的索引成员：惰性 ZREM（读一次即收敛），不产载体、不算扫描。
#[test]
fn missing_record_member_is_lazily_dropped() {
    let h = harness("lazy");
    let exec = "exec-rc-4";
    let _sub = h.suspend_execution(exec);
    // 只写索引成员，不写记录（记录 TTL 先于索引到期的形状）。
    h.backend
        .zadd(
            &keys::event_storage_type_index_key(&h.cfg, "tick"),
            float_epoch() - 5.0,
            "evt-ghost",
        )
        .unwrap();
    let outcome = scan_once(
        &(h.backend.clone() as Arc<dyn RedisBackend>),
        &h.cfg,
        &h.queue,
        &h.subs,
    )
    .unwrap();
    assert_eq!(outcome.scanned, 0);
    assert!(outcome.drained);
    assert_eq!(h.queue_len(), 0);
    assert!(
        h.bucket_members().is_empty(),
        "the ghost member is dropped from the bucket"
    );
    assert!(h.cursor().is_some(), "drained despite the dropped ghost");
}

/// 游标复合键防同时间戳活锁：同一时间戳的已回放事件在下一轮被严格大于
/// 过滤跳过（事件 id 参与序）。
#[test]
fn composite_cursor_advances_past_same_timestamp_prefix() {
    let h = harness("same-ts");
    let exec = "exec-rc-5";
    let _sub = h.suspend_execution(exec);
    let ts = float_epoch() - 15.0;
    h.store_event("evt-ts-a", ts, exec);
    h.store_event("evt-ts-b", ts, exec);
    let b = h.backend.clone() as Arc<dyn RedisBackend>;
    let outcome = scan_once(&b, &h.cfg, &h.queue, &h.subs).unwrap();
    assert_eq!(outcome.replayed, 2);
    // 游标推到窗口上界（含 ts），重扫不重投（窗口不再覆盖 + 去重双保险）。
    let outcome2 = scan_once(&b, &h.cfg, &h.queue, &h.subs).unwrap();
    assert_eq!(outcome2.scanned, 0);
    assert_eq!(h.queue_len(), 2);
}

/// 回滚开关：PLAITA_DISABLE_EVENT_RECONCILE=1 时回扫环整面旁路（worker
/// 不挂线程时行为等价——环函数直接返回，存储事件不回放）。
#[test]
fn disable_switch_bypasses_the_whole_face() {
    let h = harness_with("disabled", &|k| {
        if k == "PLAITA_DISABLE_EVENT_RECONCILE" {
            "1".to_string()
        } else {
            String::new()
        }
    });
    let exec = "exec-rc-6";
    let _sub = h.suspend_execution(exec);
    h.store_event("evt-rc-6", float_epoch() - 5.0, exec);
    // 环函数同步调用：禁用 = 立即返回（不扫不回放）。
    reconcile_loop(
        h.backend.clone() as Arc<dyn RedisBackend>,
        h.cfg.clone(),
        h.queue.clone(),
        h.subs.clone(),
        Arc::new(AtomicBool::new(false)),
    );
    assert_eq!(h.queue_len(), 0, "no replay under the kill switch");
    assert!(h.cursor().is_none());
}

/// scan_once 在禁用配置下仍然可用（环级开关不钳函数面——嵌入宿主可自选
/// 调用策略），此处只钉函数面与环面的分工边界。
#[test]
fn scan_once_remains_available_regardless_of_the_switch() {
    let h = harness_with("scanfn", &|k| {
        if k == "PLAITA_DISABLE_EVENT_RECONCILE" {
            "1".to_string()
        } else {
            String::new()
        }
    });
    let exec = "exec-rc-7";
    let _sub = h.suspend_execution(exec);
    h.store_event("evt-rc-7", float_epoch() - 5.0, exec);
    let outcome = reconcile::scan_once(
        &(h.backend.clone() as Arc<dyn RedisBackend>),
        &h.cfg,
        &h.queue,
        &h.subs,
    )
    .unwrap();
    assert_eq!(outcome.replayed, 1);
}
