//! 事件存储兜底回扫器（§8.2 M3 收口；Python `server/event_reconcile.py`
//! 对应物）。Pub/Sub 不持久：桥的 PSubscribe→确认窗口由 backend 内闭
//! （确认后才返回流），但 worker 启动/重连窗口内发布的事件通知错过即永失
//! ——事件在存储里明明躺 7 天，推送链路却没有任何补偿（go 同款窗口风险
//! 在 eventbus.go:369-375 记录在案：「an event published in that window
//! would otherwise be missed with no reconciler in this build」）。
//!
//! ## 设计取舍（周期性对账事件存储 vs 挂起执行扫描——本实现拍板前者）
//!
//! - **选「周期性对账事件存储」**：Python 发布端（console/RedisEventBus，
//!   `store_event_sync`）在发 Pub/Sub 通知**之前**已把每条业务事件落
//!   `{esp}events:{id}`（记录）+ `{esp}types:{type}`（ZSET 索引，
//!   score=事件时间戳，TTL 7d）。回扫器周期性 + 启动回填地扫该存储时间窗，
//!   对每条事件调 `handle_event` 回放——与推送链路共用的 SET NX 去重键
//!   （`{prefix}:event_filter:dedup:{event_id}:{sub_id}`，1h）保证已推送
//!   处理的事件不重复 resume（幂等，at-least-once 兜底安全）。
//! - **否决「挂起执行扫描」**：挂起行的订阅记录不携带事件历史，只扫订阅
//!   侧无法知道「哪些事件通知错过了」——没有事件存储就没有对账基准；
//!   挂起的超时半边已由 §8.3 超时检查器覆盖。在 go 兼容协议键面下，事件
//!   存储对账是唯一真正闭合窗口的机制（存储键由发布方写入，消费者侧
//!   只读，互操作面不新增写键）。
//!
//! ## 与 Python 对应物的有意分歧（均记录在案）
//!
//! 1. **游标键从 worker 前缀派生**（`{prefix}:event_reconcile:cursor`）：
//!    默认前缀下与 Python 固定键 `plaita:event_reconcile:cursor` 逐字同键；
//!    Python 多对账进程共享一个全局游标，rs 每命名空间私有书签（影子
//!    隔离；游标只是断点，丢失/分叉只多扫不漏扫——去重键兜幂等）。
//! 2. **逐桶翻页**（Python `list_events` 跨类型全局归并取最早 limit 条，
//!    #79）：rs 对每个类型索引各自 `ZRANGEBYSCORE` 取一页后按
//!    (timestamp, event_id) 归并——单一游标语义不变，全局归并的「已消费
//!    前缀」断言在逐桶翻页下不成立，故游标推进比 Python 更保守（见下）。
//! 3. **损坏记录就地惰性 ZREM**（Python 只对「载荷缺失」惰性 ZREM，
//!    A69-03；损坏 JSON 每轮报错重扫）：rs 对缺失与损坏同一处置——记录
//!    是回放的唯一载荷源，不可解码的成员永远不可回放，留着只会钉住翻页。
//! 4. **失败轮整轮作废**与 Python #153 同款：任一回放失败 → 本轮不计
//!    replayed、游标不动、具名上抛，下一轮重试；绝不影响推送主链路。
//!
//! ## 游标推进规则（#85「窗口是否排空」不变式）
//!
//! 游标 = 复合 `<ts>|<event_id>`，严格前进；**只有确认窗口已排空**（每桶
//! 翻页都未取满，且本轮所有取回事件全部回放成功）才推到窗口上界 `now`；
//! 任一桶取满（截断）则**游标原地不动**——逐桶翻页存在跨桶全局序损失，
//! 截断轮内「已回放前缀」不等于「窗口已消费前缀」，推进会在漏扫；原地
//! 不动只多扫（下一轮从原窗口重扫，去重键压住重复 resume）。发布方时钟
//! 超前的事件按其记录时间戳参与排序，但游标永不超过窗口上界（否则后续
//! 窗口接不上，Python 同款钳制）。
//!
//! 回滚开关：`PLAITA_DISABLE_EVENT_RECONCILE=1`（worker 不挂回扫线程）；
//! 周期/回填窗口经 `PLAITA_EVENT_RECONCILE_INTERVAL` / `_BACKFILL` 覆盖。

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use serde_json::Value;

use crate::backend::RedisBackend;
use crate::config::{float_epoch, Config};
use crate::error::WorkerError;
use crate::events::{handle_event, sleep_interruptible};
use crate::keys;
use crate::queue::TaskQueue;
use crate::subscriptions::RedisSubscriptions;

/// 每轮回放的事件数上界（Python DEFAULT_BATCH_SIZE 同值——限速而非窗口
/// 边界；一轮取满即说明窗口未排空，游标不动下一轮续扫）。
const RECONCILE_BATCH: usize = 200;

/// 游标键 TTL（7 天自清理；游标丢失只导致从回填窗口重扫——去重键保幂等）。
const CURSOR_TTL_SECS: i64 = 7 * 86400;

/// 一轮扫描的结局（可观测面：回放数 + 窗口是否排空）。
#[derive(Debug, Default, Clone, PartialEq)]
pub struct ScanOutcome {
    pub scanned: usize,
    pub replayed: usize,
    /// 本轮跳过的已游标前缀事件数（(ts,id) ≤ cursor——推送链路已处理）。
    pub skipped: usize,
    pub drained: bool,
}

/// 一条待回放事件（存储记录解码后）。
struct StoredEvent {
    timestamp: f64,
    event: crate::events::Event,
}

/// 读游标：复合 `<ts>|<event_id>`；纯时间戳旧格式容忍（解析成 (ts, "")，
/// Python 兼容面）；缺失 = None（从回填窗口起扫）。
fn load_cursor(backend: &Arc<dyn RedisBackend>, cfg: &Config) -> Option<(f64, String)> {
    let raw = backend.get(&keys::event_reconcile_cursor_key(cfg)).ok()??;
    let mut parts = raw.splitn(2, '|');
    let ts = parts.next()?.parse::<f64>().ok()?;
    let id = parts.next().unwrap_or("").to_string();
    Some((ts, id))
}

/// 写游标（TTL 7d；写失败只多扫不漏扫，具名告警）。
fn save_cursor(backend: &Arc<dyn RedisBackend>, cfg: &Config, ts: f64, event_id: &str) {
    let key = keys::event_reconcile_cursor_key(cfg);
    if let Err(e) = backend.set(&key, &format!("{ts}|{event_id}"), Some(CURSOR_TTL_SECS)) {
        eprintln!(
            "[reconcile] cursor save failed (next round re-scans from the old position, \
             dedup keys keep replays idempotent): {e}"
        );
    }
}

/// 解码一条存储记录（Python Event JSON：event_id/event_type/data/timestamp/
/// correlation_id?/tenant_id?）为引擎事件。损坏 = None（调用方惰性 ZREM）。
fn decode_record(raw: &str) -> Option<StoredEvent> {
    let v: Value = serde_json::from_str(raw).ok()?;
    let event_id = v.get("event_id")?.as_str()?.to_string();
    let event_type = v.get("event_type")?.as_str()?.to_string();
    let timestamp = v.get("timestamp")?.as_f64()?;
    let correlation_id = v
        .get("correlation_id")
        .and_then(|c| c.as_str())
        .unwrap_or("")
        .to_string();
    let tenant_id = v
        .get("tenant_id")
        .and_then(|t| t.as_str())
        .unwrap_or("")
        .to_string();
    let data = v.get("data").cloned().unwrap_or(Value::Null);
    Some(StoredEvent {
        timestamp,
        event: crate::events::Event {
            event_id,
            event_type,
            correlation_id,
            tenant_id,
            data,
        },
    })
}

/// 单轮扫描：窗口 (max(cursor_ts, now-backfill), now]，逐桶翻页归并回放。
/// 返回本轮结局；回放失败时整轮作废（游标不动）并上抛。
pub fn scan_once(
    backend: &Arc<dyn RedisBackend>,
    cfg: &Config,
    queue: &TaskQueue,
    subs: &RedisSubscriptions,
) -> Result<ScanOutcome, WorkerError> {
    let now = float_epoch();
    let cursor = load_cursor(backend, cfg);
    let window_start = match &cursor {
        Some((ts, _)) => ts.max(now - cfg.event_reconcile_backfill_secs),
        None => now - cfg.event_reconcile_backfill_secs,
    };
    let window_end = now;
    let index_glob = format!("{}types:*", cfg.event_storage_prefix);
    let buckets = backend
        .scan_match(&index_glob)
        .map_err(|e| WorkerError::transient(format!("event reconcile scan: {e}")))?;

    let mut fetched: Vec<(f64, String, String, StoredEvent)> = Vec::new();
    let mut truncated = false;
    for bucket in &buckets {
        let members = backend
            .zrangebyscore(bucket, window_start, window_end, RECONCILE_BATCH as i64)
            .map_err(|e| WorkerError::transient(format!("event reconcile page: {e}")))?;
        if members.len() >= RECONCILE_BATCH {
            // 页取满 = 桶可能还有窗口内事件——窗口未确认排空。
            truncated = true;
        }
        for event_id in members {
            let record_key = keys::event_storage_record_key(cfg, &event_id);
            let raw = backend
                .get(&record_key)
                .map_err(|e| WorkerError::transient(format!("event reconcile record: {e}")))?;
            let Some(raw) = raw else {
                // 载荷缺失（记录 TTL 先于索引到期）：惰性 ZREM——该成员永远
                // 不可回放，留着只会钉住翻页（Python A69-03 同款，读一次即收敛）。
                let _ = backend.zrem(bucket, &event_id);
                eprintln!(
                    "[reconcile] event record missing, index member dropped (event_id=\
                     {event_id}, bucket={bucket})"
                );
                continue;
            };
            let Some(stored) = decode_record(&raw) else {
                // 损坏记录：同惰性 ZREM（分歧点 3——Python 每轮报错重扫）。
                let _ = backend.zrem(bucket, &event_id);
                eprintln!(
                    "[reconcile] event record corrupt, index member dropped (event_id=\
                     {event_id}, bucket={bucket})"
                );
                continue;
            };
            fetched.push((stored.timestamp, event_id, bucket.clone(), stored));
        }
    }

    // 跨桶归并：全局 (timestamp, event_id) 升序；同 id 保留最早分（同一
    // event_id 理论上只属一个桶，防御性去重）。
    fetched.sort_by(|a, b| {
        a.0.partial_cmp(&b.0)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.1.cmp(&b.1))
    });
    let mut seen_ids: std::collections::HashSet<String> = std::collections::HashSet::new();
    fetched.retain(|(_, id, _, _)| seen_ids.insert(id.clone()));

    let mut outcome = ScanOutcome::default();
    let mut last_replayed: Option<(f64, String)> = None;
    for (ts, event_id, _, stored) in &fetched {
        if let Some((cur_ts, cur_id)) = &cursor {
            // 已消费前缀：严格大于游标才回放（复合游标防同时间戳活锁）。
            if (*ts, event_id.as_str()) <= (*cur_ts, cur_id.as_str()) {
                outcome.skipped += 1;
                continue;
            }
        }
        if outcome.replayed >= RECONCILE_BATCH {
            // 限速到顶：窗口未排空，游标不动下一轮续扫（不漏扫）。
            truncated = true;
            break;
        }
        if let Err(e) = handle_event(backend, cfg, queue, subs, &stored.event) {
            // #153：失败不计 replayed、游标不动（本轮整轮作废）——失败事件
            // 留在下一轮窗口内，Redis 恢复后被真正重放，绝不被「失败也算
            // 成功」的游标永久跳过。
            eprintln!(
                "[reconcile] replay failed, round voided with the cursor held \
                 (event_id={event_id}, replayed={}): {e}",
                outcome.replayed
            );
            return Err(WorkerError::transient(format!(
                "event reconcile replay: {e}"
            )));
        }
        outcome.replayed += 1;
        last_replayed = Some((*ts, event_id.clone()));
    }
    outcome.scanned = fetched.len();
    // 排空判据：无任何桶翻页取满、也未触限速（循环走满 fetched）。
    outcome.drained = !truncated;

    // 游标推进：排空 → 窗口上界（时钟超前的已回放事件也不越过 window_end，
    // 否则后续窗口接不上）；未排空 → 原地不动（保守不漏扫，分歧点 2）。
    if outcome.drained {
        save_cursor(backend, cfg, window_end, "");
    } else if let Some((ts, id)) = &last_replayed {
        if *ts <= window_end {
            save_cursor(backend, cfg, *ts, id);
        } else {
            save_cursor(backend, cfg, window_end, "");
        }
    }
    if outcome.scanned > 0 || outcome.replayed > 0 {
        eprintln!(
            "[reconcile] window ({window_start:.3}, {window_end:.3}] scanned {} events, \
             replayed {}, skipped {} (already-consumed prefix), cursor {}",
            outcome.scanned,
            outcome.replayed,
            outcome.skipped,
            if outcome.drained {
                "advanced to the window end"
            } else {
                "held (window not drained)"
            }
        );
    }
    Ok(outcome)
}

/// 回扫环：启动即先扫一轮（回填窗口），此后每 interval 一轮，直到 stop。
/// `PLAITA_DISABLE_EVENT_RECONCILE=1` 时整面旁路（worker 不挂本线程——
/// worker.run 的挂载点据此跳过）。扫描失败只记日志放弃本轮（游标不动，
/// 不漏扫），绝不影响推送主链路。
pub fn reconcile_loop(
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
    queue: Arc<TaskQueue>,
    subs: Arc<RedisSubscriptions>,
    stop: Arc<std::sync::atomic::AtomicBool>,
) {
    if cfg.disable_event_reconcile {
        eprintln!(
            "[reconcile] disabled by PLAITA_DISABLE_EVENT_RECONCILE; the Pub/Sub bridge \
             runs without a storage backstop (window-loss risk is on the deployment)"
        );
        return;
    }
    let interval = Duration::from_secs_f64(cfg.event_reconcile_interval_secs.max(0.001));
    loop {
        if stop.load(Ordering::SeqCst) {
            return;
        }
        match scan_once(&backend, &cfg, &queue, &subs) {
            Ok(_) => {}
            Err(e) => eprintln!("[reconcile] scan round failed (will retry next interval): {e}"),
        }
        if sleep_interruptible(&stop, interval) {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake::FakeRedis;

    fn test_cfg(name: &str) -> Config {
        Config::from_getenv(|k| {
            if k == "PLAITA_KEY_PREFIX" {
                format!("rc-{name}")
            } else {
                String::new()
            }
        })
    }

    /// 游标复合键的严格前进语义：纯时间戳旧格式可读。
    #[test]
    fn cursor_parses_composite_and_legacy_formats() {
        let cfg = test_cfg("cursor");
        let backend: Arc<dyn RedisBackend> = Arc::new(FakeRedis::new());
        assert_eq!(load_cursor(&backend, &cfg), None);
        backend
            .set(&keys::event_reconcile_cursor_key(&cfg), "100.5|evt-9", None)
            .unwrap();
        assert_eq!(
            load_cursor(&backend, &cfg),
            Some((100.5, "evt-9".to_string()))
        );
        backend
            .set(&keys::event_reconcile_cursor_key(&cfg), "42.0", None)
            .unwrap();
        assert_eq!(load_cursor(&backend, &cfg), Some((42.0, String::new())));
    }

    /// 记录解码：Python Event JSON 全字段 + Optional 空值容忍。
    #[test]
    fn record_decode_maps_python_event_json() {
        let ok = decode_record(
            &serde_json::json!({
                "event_id": "e1", "event_type": "tick", "data": {"a": 1},
                "timestamp": 12.5, "source": "s",
                "correlation_id": "exec-1", "tenant_id": "acme",
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(ok.timestamp, 12.5);
        assert_eq!(ok.event.event_type, "tick");
        assert_eq!(ok.event.correlation_id, "exec-1");
        assert_eq!(ok.event.tenant_id, "acme");
        let legacy = decode_record(
            &serde_json::json!({
                "event_id": "e2", "event_type": "tick", "data": {},
                "timestamp": 1.0, "source": "", "correlation_id": null, "tenant_id": null,
            })
            .to_string(),
        )
        .unwrap();
        assert_eq!(legacy.event.correlation_id, "");
        assert_eq!(legacy.event.tenant_id, "");
        assert!(decode_record("not-json").is_none());
    }

    /// env 口径：interval 非法/≤0 回默认（0 是紧环不是关闸）；backfill 负值
    /// 钳 0；开关注入直读。
    #[test]
    fn env_parsing_falls_back_with_named_discipline() {
        let cfg = Config::from_getenv(|k| match k {
            "PLAITA_EVENT_RECONCILE_INTERVAL" => "not-a-number".to_string(),
            "PLAITA_EVENT_RECONCILE_BACKFILL" => "-5".to_string(),
            _ => String::new(),
        });
        assert_eq!(cfg.event_reconcile_interval_secs, 300.0);
        assert_eq!(cfg.event_reconcile_backfill_secs, 0.0);
        let zero = Config::from_getenv(|k| match k {
            "PLAITA_EVENT_RECONCILE_INTERVAL" => "0".to_string(),
            _ => String::new(),
        });
        assert_eq!(
            zero.event_reconcile_interval_secs, 300.0,
            "0 is a tight loop, not off — the default stands"
        );
        let off = Config::from_getenv(|k| {
            if k == "PLAITA_DISABLE_EVENT_RECONCILE" {
                "1".to_string()
            } else {
                String::new()
            }
        });
        assert!(off.disable_event_reconcile);
    }

    /// 影子改写：默认事件存储前缀随 apply_shadow 移入影子命名空间（影子
    /// 部署绝不回扫生产事件存储）；显式覆盖保持调用方意图。
    #[test]
    fn shadow_rewrite_moves_the_default_storage_prefix() {
        let mut cfg = Config::from_getenv(|_| String::new());
        assert_eq!(cfg.event_storage_prefix, "plaita:event:");
        cfg.apply_shadow();
        assert_eq!(cfg.event_storage_prefix, "plaita-rs-shadow:event:");
        let mut explicit = Config::from_getenv(|k| {
            if k == "PLAITA_EVENT_STORAGE_PREFIX" {
                "tenant-events:".to_string()
            } else {
                String::new()
            }
        });
        explicit.apply_shadow();
        assert_eq!(
            explicit.event_storage_prefix, "tenant-events:",
            "an explicit prefix is the deployer's decision"
        );
    }
}
