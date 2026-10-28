//! `plaita-rs-wake` — the wake services entry point (plaita-rs#19): the
//! companion processes that consume the suspend-service queues the worker's
//! §5.2 dispatch writes and publish the resume events that wake suspended
//! flows. The Go counterpart is plaita-go's `plaita-wake`; the Python one is
//! `python -m plaita.server.services wake`. A pure-Rust deployment
//! (plaita-rs-worker + plaita-rs-wake, no Python services) gets the delay
//! wake face.
//!
//! Every key derives from PLAITA_KEY_PREFIX (default "plaita"): the delay
//! queue `{prefix}:delay:queue` (+:scheduled/:attempts/:dead), the resume
//! event channels `{ns}:events:{event_type}` with the §6.2 double-write, and
//! the roll-call registry `{prefix}:registry:delay:{instance_id}` (TTL 30s,
//! 10s heartbeat). Shadow deployments set PLAITA_KEY_PREFIX=plaita-rs-shadow
//! like the worker.
//!
//! Shutdown: SIGTERM/SIGINT set the stop flag — the consume loop exits at a
//! cycle boundary, in-flight triggers land or stay parked in the schedule
//! ZSET (at-least-once), and the registry record is tombstoned.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use plaita_worker::wake::DelayWake;
use plaita_worker::{redis_impl::RedisConnection, Config, RedisBackend};

fn main() {
    // Distinct default instance identity: the wake registry key is
    // {prefix}:registry:{source}:{instance_id} — never collide with a
    // worker's worker-<pid> default. Only the DEFAULT is rewritten; an
    // operator-set PLAITA_INSTANCE_ID wins verbatim (Go parity).
    if std::env::var("PLAITA_INSTANCE_ID")
        .map(|v| v.trim().is_empty())
        .unwrap_or(true)
    {
        std::env::set_var(
            "PLAITA_INSTANCE_ID",
            format!("delay-{}", std::process::id()),
        );
    }
    let cfg = Config::from_env();
    eprintln!(
        "[bin] plaita-rs-wake starting (sources=delay, prefix={}, instance={})",
        cfg.key_prefix, cfg.instance_id
    );
    let backend = match RedisConnection::connect(&cfg.redis_url) {
        Ok(b) => Arc::new(b) as Arc<dyn RedisBackend>,
        Err(e) => {
            eprintln!("[bin] cannot connect to redis at {}: {e}", cfg.redis_url);
            std::process::exit(1);
        }
    };
    let stop = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(sig, Arc::clone(&stop)) {
            eprintln!("[bin] signal registration failed for {sig}: {e}");
        }
    }

    let wake = Arc::new(DelayWake::new(cfg, backend, Arc::clone(&stop)));
    wake.run();
    eprintln!("[bin] plaita-rs-wake stopped");
}
