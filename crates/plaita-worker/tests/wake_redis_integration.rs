//! Real-Redis integration tests for the delay wake source (rs#19 first
//! installment). Run:
//!
//! ```text
//! PLAITA_RS_REDIS_TESTS=1 \
//! PLAITA_RS_REDIS_URL=redis://127.0.0.1:6379/0 \
//! cargo test -p plaita-worker --test wake_redis_integration
//! ```
//!
//! All keys land under a per-PID one-shot prefix (`plaita-rs-wake-it-<pid>`),
//! cleaned up at the end — same discipline as redis_integration.rs.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::{Duration, Instant};

use plaita_worker::wake::{DelayWake, RedisEventSink};
use plaita_worker::{redis_impl::RedisConnection, Config, RedisBackend};
use serde_json::json;

fn integration_backend(test_name: &str) -> Option<(Arc<RedisConnection>, Config)> {
    if std::env::var("PLAITA_RS_REDIS_TESTS").as_deref() != Ok("1") {
        return None;
    }
    let url = std::env::var("PLAITA_RS_REDIS_URL")
        .ok()
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "redis://127.0.0.1:6379/0".to_string());
    let prefix = format!("plaita-rs-wake-it-{}-{test_name}", std::process::id());
    let mut cfg = Config::from_getenv(|k| match k {
        "PLAITA_KEY_PREFIX" => prefix.clone(),
        _ => String::new(),
    });
    cfg.redis_url = url;
    // Isolate the event-storage namespace the same way (the default
    // `plaita:event:` is shared across deployments).
    cfg.event_storage_prefix = format!("{prefix}:event:");
    let backend = Arc::new(RedisConnection::connect(&cfg.redis_url).unwrap());
    Some((backend, cfg))
}

fn cleanup(backend: &Arc<RedisConnection>, cfg: &Config) {
    let _ = backend.delete_pattern(&format!("{}*", cfg.key_prefix));
    let _ = backend.delete_pattern(&format!("{}*", cfg.event_storage_prefix));
}

#[test]
fn real_redis_delay_wake_full_chain() {
    let Some((backend, cfg)) = integration_backend("delay_full_chain") else {
        eprintln!("skipping: set PLAITA_RS_REDIS_TESTS=1 (+ PLAITA_RS_REDIS_URL)");
        return;
    };
    // Fast poll so the chain lands within the wait window.
    std::env::set_var("PLAITA_DELAY_POLL_INTERVAL_SECONDS", "0.05");
    let backend_dyn: Arc<dyn RedisBackend> = backend.clone();
    let stop = Arc::new(AtomicBool::new(false));
    let wake = Arc::new(DelayWake::new(cfg.clone(), backend_dyn, Arc::clone(&stop)));

    // Subscribe BEFORE dispatching so the pub/sub notification is observed.
    let pattern = plaita_worker::keys::event_bridge_pattern(&cfg);
    let mut stream = backend.psubscribe(&pattern).unwrap();

    // The worker-side dispatch shape: RPUSH {prefix}:delay:queue with the
    // service task body (service_config + execution_id + tenant_id).
    let queue_key = plaita_worker::keys::service_queue_key(&cfg, "delay");
    let task = json!({
        "type": "delay",
        "node_id": "wait",
        "event_type": "delay_trigger",
        "execution_id": "exec-it-1",
        "flow_id": "flow-it",
        "tenant_id": "default",
        "delay_ms": 1000i64,
        "trigger_timestamp": now_ms() - 100i64,
    })
    .to_string();
    backend.rpush(&queue_key, &task).unwrap();

    let run_wake = Arc::clone(&wake);
    let runner = std::thread::spawn(move || run_wake.run());

    // 1) The resume event lands on the bridge channel with the §6.2 shape.
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut event = None;
    while Instant::now() < deadline {
        match stream.next_message(Duration::from_millis(300)) {
            Some((channel, payload)) => {
                assert!(
                    channel.ends_with(":events:delay_trigger"),
                    "channel={channel}"
                );
                let v: serde_json::Value = serde_json::from_str(&payload).unwrap();
                assert_eq!(v["correlation_id"], "exec-it-1");
                assert_eq!(v["tenant_id"], "default");
                assert_eq!(v["data"]["trigger_type"], "delay_completed");
                event = Some(v);
                break;
            }
            None => continue,
        }
    }
    let event = event.expect("no resume event within 10s");

    // 2) Double-write storage: record present, type index score in seconds.
    let record_key =
        plaita_worker::keys::event_storage_record_key(&cfg, event["event_id"].as_str().unwrap());
    let stored = backend.get(&record_key).unwrap().expect("storage record");
    let stored: serde_json::Value = serde_json::from_str(&stored).unwrap();
    assert_eq!(stored["data"]["success"], json!(true));
    let ts = event["timestamp"].as_f64().unwrap();
    assert!(
        (ts - now_ms() as f64 / 1000.0).abs() < 30.0,
        "wire timestamp must be epoch seconds: {ts}"
    );

    // 3) Confirmed trigger ⇒ schedule drained, no attempts, entry acked.
    let scheduled = format!("{queue_key}:scheduled");
    assert!(
        backend
            .zrangebyscore(&scheduled, -1e300, 1e300, 10)
            .unwrap()
            .is_empty(),
        "confirmed trigger must leave the schedule"
    );
    assert!(
        backend.lrange(&queue_key, 0, -1).unwrap().is_empty(),
        "dispatch list must be acked"
    );

    // 4) Registry row: the roll-call data source.
    let registry = format!("{}:registry:delay:{}", cfg.key_prefix, cfg.instance_id);
    let row = backend.get(&registry).unwrap().expect("registry row");
    assert!(row.contains("\"service_type\":\"delay\""), "row: {row}");

    // Stop + verify the tombstone path reclaims the key.
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    runner.join().unwrap();
    assert!(
        backend.get(&registry).unwrap().is_none(),
        "registry key must be gone after stop"
    );
    cleanup(&backend, &cfg);
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}
