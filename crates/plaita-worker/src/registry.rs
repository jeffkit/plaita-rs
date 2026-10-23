//! Worker registry heartbeat (§8.1; plaita-go `pkg/worker/registry.go`
//! 同形移植，含 #82 的 capability manifest 写入面——B4 第十五轮接线)。
//!
//! 三层：
//! 1. registry 心跳 — `SETEX {prefix}:registry:flow_worker:{instance_id}`
//!    每 10s（TTL 30s），整份 ServiceInfo JSON 重写（无独立续期面）；
//! 2. capacity 块 — `metadata.worker_capacity.active_tasks` 随每次 flush
//!    从共享在途计数取值（单消费者，M1 无磁盘守卫：free_disk_gib=null，
//!    capacity schema 允许）；
//! 3. 执行进度心跳（长节点 node_timings 合并）在 worker.go 的 fenced 状态
//!    路径，不在本模块。
//!
//! **#82 manifest 写入面**（对齐 plaita-go f13819a）：metadata 挂
//! `engine="plaita-rs"`（console ENGINE_TO_RUNTIME 映射 rust 池）+
//! `capability_manifest` = 引擎 `ir::capability_manifest()` 整结构体嵌套
//! 序列化（console manifest_from_metadata 要求 dict，字符串形态会被判无
//! 清单退回 fail-open）。降级口径与 go 一致：清单构造是 accept-list 纯
//! 函数、序列化实际不可失败；若真失败则整体省略两键（console 无清单走
//! fail-open 兜底）+ 具名 WARNING `PLAITA_CAPABILITY_MANIFEST_UNAVAILABLE`，
//! 刻意不写「在但为空」的清单（过不了五键形状闸却对粗看者显得健康），也
//! 不因不可达错误拒启 worker。

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde_json::{json, Map, Value};

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config};
use crate::keys;

/// Registry 生命周期状态（§8.1/§9.3；plaita-go RegStatus* 同值）。
pub const REG_STATUS_STARTING: &str = "starting";
pub const REG_STATUS_RUNNING: &str = "running";
pub const REG_STATUS_DRAINING: &str = "draining";
pub const REG_STATUS_STOPPING: &str = "stopping";
pub const REG_STATUS_STOPPED: &str = "stopped";

/// 停止墓碑的短 TTL（§9.3：stopped SETEX 5s → DEL；消费面据此区分
/// 「刚停」与「键已过期」）。
const STOPPED_TOMBSTONE_TTL_SECS: i64 = 5;

struct RegistryInner {
    status: &'static str,
    start_time: String,
    metadata_base: Map<String, Value>,
}

/// Registry 写 `{prefix}:registry:flow_worker:{instance_id}`。
pub struct Registry {
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
    /// 在途任务计数（worker 执行面维护；flush 时随 capacity 块写出）。
    active_tasks: Arc<AtomicUsize>,
    inner: Mutex<RegistryInner>,
}

impl Registry {
    pub fn new(
        backend: Arc<dyn RedisBackend>,
        cfg: Config,
        active_tasks: Arc<AtomicUsize>,
    ) -> Registry {
        Registry {
            backend,
            cfg,
            active_tasks,
            inner: Mutex::new(RegistryInner {
                status: REG_STATUS_STARTING,
                start_time: now_iso(),
                metadata_base: Map::new(),
            }),
        }
    }

    /// 注册键。
    pub fn key(&self) -> String {
        keys::registry_worker_key(&self.cfg)
    }

    /// Start：构造初始 ServiceInfo（host/port/db 自 Redis URL 解析，密码
    /// 打码镜像 Python metadata 规则）并立即写入。
    pub fn start(&self) {
        let (masked, host, port, db) = split_redis_url(&self.cfg.redis_url);
        let meta = json!({
            "queue_name": self.cfg.queue_name,
            "cache_size": self.cfg.flow_cache_size,
            "cache_ttl": self.cfg.flow_cache_ttl_secs,
            "concurrency": 1, // M1: single consumer loop
            "redis_url": masked,
            "host": host,
            "port": port,
            "db": db,
            "sampled_at": now_iso(),
        });
        let mut meta = meta.as_object().cloned().unwrap_or_default();
        append_capability_manifest(&mut meta);
        {
            let mut inner = self.inner.lock().unwrap();
            inner.status = REG_STATUS_STARTING;
            inner.start_time = now_iso();
            inner.metadata_base = meta;
        }
        if let Err(e) = self.flush() {
            eprintln!("[registry] initial registration write failed (heartbeat will retry): {e}");
        }
    }

    /// 状态翻转 + 立即落盘。`attempts` 次短重试——draining 转换必须立即可
    /// 见，否则 dispatcher 在窗口期继续向本实例派发（registry.py 3 次重试
    /// 同口径）。
    fn set_status_flush(&self, status: &'static str, attempts: usize) {
        {
            let mut inner = self.inner.lock().unwrap();
            inner.status = status;
        }
        let mut last = String::new();
        for attempt in 0..attempts.max(1) {
            if attempt > 0 {
                std::thread::sleep(Duration::from_millis(50));
            }
            match self.flush() {
                Ok(()) => return,
                Err(e) => last = e,
            }
        }
        eprintln!("[registry] status flush failed (status={status}): {last}");
    }

    /// 单次尝试的状态翻转（§9.3：状态转换立即持久化，不等下一个心跳拍）。
    pub fn set_status(&self, status: &'static str) {
        self.set_status_flush(status, 1);
    }

    /// draining 转换（plaita-go drain.go:70 口径）：3 次短重试——窗口期
    /// dispatcher 继续向本实例派发的代价高于一切。
    pub fn set_status_flush_draining(&self) {
        self.set_status_flush(REG_STATUS_DRAINING, 3);
    }

    /// 停止序列（plaita-go `Unregister`）：stopped 墓碑 SETEX 5s → DEL。
    pub fn shutdown(&self) {
        self.set_status_flush(REG_STATUS_STOPPING, 1);
        let tombstone = json!({
            "instance_id": self.cfg.instance_id,
            "status": REG_STATUS_STOPPED,
        });
        let key = self.key();
        if let Err(e) = self.backend.set(
            &key,
            &tombstone.to_string(),
            Some(STOPPED_TOMBSTONE_TTL_SECS),
        ) {
            eprintln!("[registry] stopped tombstone write failed: {e}");
        }
        if let Err(e) = self.backend.del(&[key.as_str()]) {
            eprintln!("[registry] registry key delete failed: {e}");
        }
    }

    /// 整份重写当前 ServiceInfo（registry TTL；last_heartbeat/active_tasks
    /// 随写刷新）。整份重写 = 心跳即续期，无独立续期面（go Flush 同构）。
    pub fn flush(&self) -> Result<(), String> {
        let (status, start_time, metadata_base) = {
            let inner = self.inner.lock().unwrap();
            (
                inner.status,
                inner.start_time.clone(),
                inner.metadata_base.clone(),
            )
        };
        let active = self.active_tasks.load(Ordering::SeqCst) as i64;
        let now = now_iso();
        let mut metadata = metadata_base;
        metadata.insert(
            "worker_capacity".to_string(),
            json!({
                "schema_version": 1,
                "free_disk_gib": Value::Null, // disk guard not implemented (phase 2)
                "min_free_disk_gib": 0,
                "disk_blocked": false,
                "active_tasks": active,
                "sampled_at": now,
            }),
        );
        let info = json!({
            "instance_id": self.cfg.instance_id,
            "service_type": "flow_worker",
            "host": metadata.get("host").cloned().unwrap_or(Value::Null),
            "status": status,
            "start_time": start_time,
            "metadata": Value::Object(metadata),
            "active_tasks": active,
            "last_heartbeat": now,
        });
        self.backend
            .set(
                &self.key(),
                &info.to_string(),
                Some(self.cfg.registry_ttl_secs),
            )
            .map_err(|e| format!("registry write: {e}"))
    }

    /// 心跳环：每 heartbeat_interval 整份重写，直到 stop 置位（§8.1 层 1）。
    pub fn heartbeat_loop(&self, stop: Arc<std::sync::atomic::AtomicBool>) {
        let interval = Duration::from_secs(self.cfg.heartbeat_interval_secs.max(1) as u64);
        loop {
            if crate::events::sleep_interruptible(&stop, interval) {
                return;
            }
            if let Err(e) = self.flush() {
                eprintln!("[registry] heartbeat failed: {e}");
            }
        }
    }
}

/// #82：metadata 挂 engine + capability_manifest（见模块文档降级口径）。
fn append_capability_manifest(meta: &mut Map<String, Value>) {
    let manifest = plaita_engine::ir::capability_manifest();
    if let Err(e) = serde_json::to_string(&manifest) {
        eprintln!(
            "[registry] capability manifest unavailable; registry entry will omit \
             capability_manifest (console capability gate fails open for this pool) \
             marker=PLAITA_CAPABILITY_MANIFEST_UNAVAILABLE error={e}"
        );
        return;
    }
    // engine 名取自清单自身（同源，绝不两处硬编码漂移）。
    let engine = manifest
        .get("engine")
        .cloned()
        .unwrap_or_else(|| json!("plaita-rs"));
    meta.insert("engine".to_string(), engine);
    meta.insert("capability_manifest".to_string(), manifest);
}

/// Redis URL → (打码 URL, host, port, db)：用户名保留、密码 `******`
/// （flow_worker.py:416-449 打码规则）。解析失败退 `unknown`/`0`。
fn split_redis_url(redis_url: &str) -> (String, String, i64, i64) {
    match url::Url::parse(redis_url) {
        Ok(u) => {
            let masked = match u.password() {
                Some(_) => {
                    let mut m = u.clone();
                    let _ = m.set_password(Some("******"));
                    m.to_string()
                }
                None => u.to_string(),
            };
            let port = u.port().map(i64::from).unwrap_or(0);
            let db = u.path().trim_start_matches('/').parse::<i64>().unwrap_or(0);
            (
                masked,
                u.host_str().unwrap_or("unknown").to_string(),
                port,
                db,
            )
        }
        Err(_) => (redis_url.to_string(), "unknown".to_string(), 0, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;
    use std::sync::atomic::AtomicBool;

    fn reg(cfg: &Config, backend: &Arc<FakeRedis>) -> Registry {
        Registry::new(
            backend.clone() as Arc<dyn RedisBackend>,
            cfg.clone(),
            Arc::new(AtomicUsize::new(0)),
        )
    }

    fn test_cfg(name: &str) -> Config {
        Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                format!("reg-{name}")
            } else {
                String::new()
            }
        })
    }

    /// 注册写入格：ServiceInfo 顶层字段 + metadata 五键清单在册
    /// （manifest_from_metadata 要求 dict 形态——字符串形态会被 console
    /// 判无清单 fail-open）。
    #[test]
    fn registry_entry_carries_manifest_five_keys() {
        let cfg = test_cfg("shape");
        let backend = Arc::new(FakeRedis::new());
        let r = reg(&cfg, &backend);
        r.start();
        r.set_status(REG_STATUS_RUNNING);

        let raw = backend.get(&r.key()).unwrap().expect("registry entry");
        let info: Value = serde_json::from_str(&raw).unwrap();
        assert_eq!(info["instance_id"], cfg.instance_id);
        assert_eq!(info["service_type"], "flow_worker");
        assert_eq!(info["status"], "running");
        assert!(!info["start_time"].as_str().unwrap().is_empty());
        assert!(!info["last_heartbeat"].as_str().unwrap().is_empty());
        assert_eq!(info["active_tasks"], 0);

        let meta = &info["metadata"];
        assert_eq!(meta["engine"], "plaita-rs");
        assert_eq!(meta["queue_name"], cfg.queue_name);
        assert_eq!(meta["concurrency"], 1);
        // manifest：整 dict 嵌套 + 五键逐键在册（console MANIFEST_KEYS 键面）
        let m = &meta["capability_manifest"];
        assert!(m.is_object(), "manifest must ride as a nested object");
        for k in [
            "engine",
            "ir_contract_version",
            "supported_node_types",
            "unsupported_node_types",
            "features",
        ] {
            assert!(m.get(k).is_some(), "manifest key {k} missing");
        }
        assert_eq!(
            m["ir_contract_version"],
            plaita_engine::ir::CONTRACT_VERSION
        );
        assert_eq!(m["engine"], "plaita-rs");
        assert!(
            m["supported_node_types"].as_array().unwrap().len() >= 25,
            "supported list mirrors the engine accept-list"
        );
        assert_eq!(m["features"], json!([]));
        // worker_capacity 随行且 active_tasks 取共享计数
        assert_eq!(meta["worker_capacity"]["schema_version"], 1);
        assert_eq!(meta["worker_capacity"]["active_tasks"], 0);
    }

    /// 心跳 = 整份重写：last_heartbeat 前进 + active_tasks 取当前在途数。
    #[test]
    fn heartbeat_rewrites_entry_with_fresh_fields() {
        let cfg = test_cfg("beat");
        let backend = Arc::new(FakeRedis::new());
        let active = Arc::new(AtomicUsize::new(0));
        let r = Registry::new(
            backend.clone() as Arc<dyn RedisBackend>,
            cfg.clone(),
            active.clone(),
        );
        r.start();
        let first: Value = serde_json::from_str(&backend.get(&r.key()).unwrap().unwrap()).unwrap();
        active.store(2, Ordering::SeqCst);
        let _ = r.flush();
        let second: Value = serde_json::from_str(&backend.get(&r.key()).unwrap().unwrap()).unwrap();
        assert_eq!(
            second["last_heartbeat"],
            second["metadata"]["worker_capacity"]["sampled_at"]
        );
        assert_eq!(second["active_tasks"], 2);
        assert_eq!(second["metadata"]["worker_capacity"]["active_tasks"], 2);
        assert_eq!(
            second["start_time"], first["start_time"],
            "start_time is sticky"
        );
    }

    /// 停止序列：draining 状态在场 + 墓碑后键删除。
    #[test]
    fn drain_status_and_stopped_tombstone() {
        let cfg = test_cfg("stop");
        let backend = Arc::new(FakeRedis::new());
        let r = reg(&cfg, &backend);
        r.start();
        r.set_status(REG_STATUS_DRAINING);
        let info: Value = serde_json::from_str(&backend.get(&r.key()).unwrap().unwrap()).unwrap();
        assert_eq!(info["status"], "draining");
        r.shutdown();
        assert!(
            backend.get(&r.key()).unwrap().is_none(),
            "shutdown deletes the registry key after the stopped tombstone"
        );
    }

    /// redis_url 打码：密码星号、host/port/db 解析；坏 URL 退 unknown。
    #[test]
    fn redis_url_is_masked_and_parsed() {
        let (masked, host, port, db) = split_redis_url("redis://:s3cret@redis-host:6380/2");
        assert!(
            !masked.contains("s3cret"),
            "password must be masked: {masked}"
        );
        assert!(masked.contains("******"));
        assert_eq!((host.as_str(), port, db), ("redis-host", 6380, 2));
        let (masked2, host2, port2, db2) = split_redis_url("not a url");
        assert_eq!(masked2, "not a url");
        assert_eq!((host2.as_str(), port2, db2), ("unknown", 0, 0));
    }

    /// 心跳环在 stop 置位后有界退出（≤interval+1s）。
    #[test]
    fn heartbeat_loop_stops_on_flag() {
        let cfg = test_cfg("loop");
        let backend = Arc::new(FakeRedis::new());
        let r = Arc::new(reg(&cfg, &backend));
        r.start();
        let stop = Arc::new(AtomicBool::new(false));
        let r2 = r.clone();
        let stop2 = stop.clone();
        let h = std::thread::spawn(move || r2.heartbeat_loop(stop2));
        stop.store(true, Ordering::SeqCst);
        h.join().unwrap();
    }
}
