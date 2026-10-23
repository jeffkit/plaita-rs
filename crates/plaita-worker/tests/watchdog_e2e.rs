//! M2 验收行为测试 · §5.3/§11.3 watchdog 面（go worker.go:768-779, 846-901,
//! 1025-1062 同款）：deadline watchdog 到点协作取消 + `execution_deadline_
//! exceeded` 终态、取消标志轮询中止在途执行（软中断 → cancelled 非 error）、
//! 失租 → 无终态/消息留 pending/孤儿心跳、running 行保鲜（进度心跳）。
//! 「重流程」用 2 万轮 loop×childFlow（实测 ~4s）制造足够长的在途执行。

use std::sync::Arc;
use std::time::Duration;

use plaita_worker::config::now_iso;
use plaita_worker::fake::FakeRedis;
use plaita_worker::keys;
use plaita_worker::metrics::RecordingMetrics;
use plaita_worker::state;
use plaita_worker::{error::ErrorKind, lease, RedisBackend, Worker};
use serde_json::{json, Value};

struct Harness {
    backend: Arc<FakeRedis>,
    cfg: plaita_worker::Config,
    worker: Arc<Worker>,
    metrics: Arc<RecordingMetrics>,
}

fn harness_with(name: &str, extra: &dyn Fn(&str) -> String) -> Harness {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => format!("wd-{name}"),
        "PLAITA_CLAIM_MIN_IDLE_MS" => "0".to_string(),
        _ => extra(k),
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
    fn preset_heavy_flow(&self) {
        let flow = json!({
            "flow_id": "heavy",
            "nodes": [
                {"type":"start","id":"s","next":"lp"},
                {"type":"loop","id":"lp","collection":"$INPUT.items","next":"end",
                 "childFlow":{"nodes":[
                    {"type":"start","id":"s","next":"a"},
                    {"type":"assignment","id":"a","output":{"v":"$INPUT.item"},"next":"end"},
                    {"type":"end","id":"end","output":"$NODE.a","resultType":"success"}]}},
                {"type":"end","id":"end","output":"ok","resultType":"success"}
            ]
        });
        plaita_worker::flowsource::preset_flow_definition(
            self.backend.as_ref(),
            &self.cfg,
            "",
            "heavy",
            "v1",
            flow.to_string().as_bytes(),
        )
        .unwrap();
    }

    fn enqueue_start(&self, execution_id: &str, items: usize) {
        let items: Vec<Value> = (0..items).map(|i| json!(i)).collect();
        self.backend
            .xadd(
                &keys::queue_key(&self.cfg),
                &[
                    (
                        "payload".to_string(),
                        json!({
                            "type": "start", "flow_id": "heavy",
                            "execution_id": execution_id,
                            "params": {"items": items},
                            "timestamp": now_iso(),
                        })
                        .to_string(),
                    ),
                    ("schema_version".to_string(), "1".to_string()),
                ],
            )
            .unwrap();
    }

    fn row(&self, execution_id: &str) -> state::ExecutionState {
        state::load(self.backend.as_ref(), &self.cfg, "", execution_id)
            .unwrap()
            .unwrap_or_else(|| panic!("state row for {execution_id} missing"))
    }
}

/// 验收 7：短预算执行被终态化为 execution_deadline_exceeded（非重试、无
/// DLQ）；deadline 触发的引擎取消不被误判成 cancelled。
#[test]
fn deadline_watchdog_terminalizes_execution_deadline_exceeded() {
    let h = harness_with("deadline", &|k| {
        if k == "PLAITA_EXECUTION_DEADLINE_SECONDS" {
            "0.3".to_string()
        } else {
            String::new()
        }
    });
    h.preset_heavy_flow();
    h.enqueue_start("exec-deadline", 20000);
    h.worker.process_one().unwrap();
    let row = h.row("exec-deadline");
    assert_eq!(row.status, state::STATUS_ERROR, "deadline → error terminal");
    let err = row.error.unwrap();
    assert_eq!(err["error_type"], "execution_deadline_exceeded");
    assert_eq!(err["code"], -500);
    assert!(
        err["details"]["deadline_seconds"].as_f64().unwrap() > 0.0,
        "deadline details recorded: {err}"
    );
    assert!(
        err["details"]["occupied_seconds"].as_f64().unwrap() >= 0.3,
        "occupied time recorded: {err}"
    );
    // the timeline carries the named event + the live channel got it
    let tl = row.timeline.unwrap();
    assert!(
        tl["events"]
            .as_array()
            .unwrap()
            .iter()
            .any(|e| e["type"] == "execution_deadline_exceeded"),
        "deadline event on the timeline"
    );
    // non-retryable: no carrier re-enqueue; the original acked
    assert_eq!(h.backend.xlen(&keys::queue_key(&h.cfg)).unwrap(), 0);
    assert_eq!(
        h.metrics.counter_of(
            plaita_worker::metrics::METRIC_EXEC_DEADLINE_TOTAL,
            &[("tenant", "")]
        ),
        1
    );
    // the budget forbids the retry path: no node-retry counter moved
    assert!(h
        .backend
        .get(&keys::noderefetch_key(&h.cfg, "", "exec-deadline"))
        .unwrap()
        .is_none());
}

/// 验收 6：取消标志轮询中止在途执行（软中断）且终态为 cancelled 非 error。
#[test]
fn cancel_poll_aborts_in_flight_execution_to_cancelled() {
    let h = harness_with("cancel-poll", &|k| {
        if k == "PLAITA_CANCEL_POLL_MS" {
            "100".to_string()
        } else {
            String::new()
        }
    });
    h.preset_heavy_flow();
    h.enqueue_start("exec-cancel", 20000);
    // a console-style cancel lands 400ms into the run
    {
        let backend = h.backend.clone();
        let cfg = h.cfg.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(400));
            backend
                .set(&keys::cancel_key(&cfg, "", "exec-cancel"), &now_iso(), None)
                .unwrap();
        });
    }
    h.worker.process_one().unwrap();
    let row = h.row("exec-cancel");
    assert_eq!(row.status, state::STATUS_CANCELLED, "cancelled, not error");
    assert!(row.error.is_none(), "取消永不落 error 行");
    assert!(!row.end_time.is_empty());
}

/// 验收 4（lease_test.go:279 行族）：失租——renew 假 → 孤儿心跳发布、步中
/// 止、消息留 pending、无终态；新持有者 fence 世代使旧写 CAS 失败。
/// （注入方式：中途 DEL 租约键 = ttl 到期契约的确定性等价物——真实世界里
/// 正是「renew 无法按时跑」导致键过期；renew 返回 false 即失租信号。）
#[test]
fn lease_loss_parks_without_terminal_and_orphan_keeps_heartbeat() {
    let h = harness_with("lease-lost", &|k| {
        if k == "PLAITA_LEASE_TTL_SECONDS" {
            "2".to_string() // watchdog interval = max(2/3, 1) = 1s
        } else {
            String::new()
        }
    });
    h.preset_heavy_flow();
    h.enqueue_start("exec-lost", 20000);
    // the lease vanishes half a second into the run (expiry contract); the
    // watchdog's first renew (~1s) then returns false
    {
        let backend = h.backend.clone();
        let cfg = h.cfg.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(500));
            backend
                .del(&[&keys::lease_key(&cfg, "", "exec-lost")])
                .unwrap();
        });
    }
    let err = h.worker.process_one().unwrap_err();
    assert_eq!(err.kind(), ErrorKind::LeaseLost, "lease lost parks: {err}");
    // NO terminal write; the row stays running; the lease is gone
    let row = h.row("exec-lost");
    assert_eq!(row.status, state::STATUS_RUNNING);
    assert!(row.error.is_none());
    assert!(row.end_time.is_empty());
    assert!(
        lease::get_holder(h.backend.as_ref(), &h.cfg, "", "exec-lost")
            .unwrap()
            .is_none()
    );
    assert_eq!(
        h.metrics
            .counter_of(plaita_worker::metrics::METRIC_LEASE_LOST, &[("tenant", "")]),
        1
    );
    // the orphan watch keeps the row fresh (progress heartbeat, no renew)
    let mut fresh = false;
    for _ in 0..30 {
        std::thread::sleep(Duration::from_millis(100));
        if let Ok(Some(r)) = state::load(h.backend.as_ref(), &h.cfg, "", "exec-lost") {
            if r.status == state::STATUS_RUNNING && r.last_update_time != r.start_time {
                fresh = true;
                break;
            }
        }
    }
    assert!(fresh, "orphan watch refreshed last_update_time mid-run");
    // a new holder takes over (fencegen bumps to 2): our stale fence-1 write
    // must be refused — the CAS is the lost-write-authority proof
    let thief = lease::acquire(
        h.backend.as_ref(),
        &h.cfg,
        "",
        "exec-lost",
        "start:thief:cccc1111dddd2222",
        120,
    )
    .unwrap();
    assert_eq!(thief.fence_token, 2);
    let mut stale_row = h.row("exec-lost");
    stale_row.status = state::STATUS_COMPLETED.to_string();
    let err = state::save(h.backend.as_ref(), &h.cfg, "", &stale_row, 1).unwrap_err();
    assert!(
        matches!(err.kind(), ErrorKind::LeaseLost),
        "stale fence write refused: {err}"
    );
}

/// 验收（进度心跳 §8.1 layer 3）：renew 成功拍合并 node_timings 进 running
/// 行并刷新 last_update_time——行停跳但进程还活的假象不会出现。
#[test]
fn progress_heartbeat_keeps_running_row_fresh() {
    let h = harness_with("heartbeat", &|k| {
        if k == "PLAITA_LEASE_TTL_SECONDS" {
            "3".to_string() // watchdog interval = 1s, lease lives 3s
        } else {
            String::new()
        }
    });
    h.preset_heavy_flow();
    h.enqueue_start("exec-hb", 20000);
    let sampler = {
        let backend = h.backend.clone();
        let cfg = h.cfg.clone();
        std::thread::spawn(move || {
            // the first renew/heartbeat lands ~1s into the run (the run
            // itself takes ~4s): wait for a refreshed last_update_time on a
            // still-RUNNING row (the terminal write would also refresh it,
            // but only AFTER completion — a refreshed RUNNING row mid-run is
            // the heartbeat's signature)
            // The heartbeat's signature: a still-RUNNING row that already
            // carries merged node timings with an in-run marker (the initial
            // running row has node_timings = None; the terminal row is not
            // RUNNING and has all markers closed).
            for _ in 0..60 {
                std::thread::sleep(Duration::from_millis(100));
                if let Ok(Some(r)) = state::load(backend.as_ref(), &cfg, "", "exec-hb") {
                    if r.status == state::STATUS_RUNNING && r.last_update_time != r.start_time {
                        let has_open = r
                            .node_timings
                            .as_ref()
                            .and_then(|t| t.as_object())
                            .map(|m| {
                                m.values()
                                    .any(|v| v.get("ended_at").and_then(|e| e.as_str()) == Some(""))
                            })
                            .unwrap_or(false);
                        if has_open {
                            return Some(r);
                        }
                    }
                }
            }
            None
        })
    };
    h.worker.process_one().unwrap();
    let mid = sampler
        .join()
        .unwrap()
        .expect("heartbeat refreshed the running row mid-run");
    // the merged timings carry the in-run marker (ended_at == "") — the
    // keeper liveness evidence ① — and last_update_time moved off start_time
    let timings = mid
        .node_timings
        .expect("heartbeat row carries merged timings");
    assert!(
        timings
            .as_object()
            .unwrap()
            .values()
            .any(|v| v["ended_at"] == ""),
        "in-run marker present in the heartbeat row"
    );
    assert!(mid.last_update_time != mid.start_time);
    // the run still completed normally afterwards
    let row = h.row("exec-hb");
    assert_eq!(row.status, state::STATUS_COMPLETED);
}

/// env 预算键的降级口径：非法/≤0 回默认 + WARNING，绝不静默无界。
#[test]
fn invalid_deadline_env_falls_back_to_default_with_warning() {
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_EXECUTION_DEADLINE_SECONDS" => "not-a-number".to_string(),
        _ => String::new(),
    });
    assert_eq!(
        cfg.execution_deadline_secs,
        plaita_worker::config::DEFAULT_EXECUTION_DEADLINE_SECS
    );
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_EXECUTION_DEADLINE_SECONDS" => "-5".to_string(),
        _ => String::new(),
    });
    assert_eq!(
        cfg.execution_deadline_secs,
        plaita_worker::config::DEFAULT_EXECUTION_DEADLINE_SECS,
        "env can never disable the budget"
    );
    // the Config-struct off switch (deployment decision only)
    let mut cfg = plaita_worker::Config::from_getenv(|_| String::new());
    cfg.execution_deadline_secs = -1.0;
    cfg.normalize();
    assert_eq!(cfg.execution_deadline_secs, -1.0);
    // a valid override passes through
    let cfg = plaita_worker::Config::from_getenv(|k| match k {
        "PLAITA_EXECUTION_DEADLINE_SECONDS" => "0.5".to_string(),
        _ => String::new(),
    });
    assert_eq!(cfg.execution_deadline_secs, 0.5);
}
