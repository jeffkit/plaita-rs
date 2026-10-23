//! `plaita-rs-worker` — the long-running worker entry point (rs-worker
//! design §2.1): env-var configuration (§3 key-space table), Redis connect,
//! signal handling (SIGTERM/SIGINT → D1 drain; the drain timer escalates to
//! D2), the consume loop with its background threads, and the exit sequence
//! (consumer loop exits at the task boundary → background join → stop).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use plaita_worker::metrics::NoopMetrics;
use plaita_worker::{redis_impl::RedisConnection, Config, RedisBackend, Worker};

fn main() {
    let cfg = Config::from_env();
    eprintln!(
        "[bin] plaita-rs-worker starting (queue={}, group={}, consumer={}, instance={}, \
         prefix={})",
        cfg.queue_name, cfg.consumer_group, cfg.consumer_name, cfg.instance_id, cfg.key_prefix
    );
    let backend = match RedisConnection::connect(&cfg.redis_url) {
        Ok(b) => Arc::new(b) as Arc<dyn RedisBackend>,
        Err(e) => {
            eprintln!("[bin] cannot connect to redis at {}: {e}", cfg.redis_url);
            std::process::exit(1);
        }
    };
    let worker = Arc::new(Worker::new(backend, cfg, Arc::new(NoopMetrics)));

    // D3: signals only REQUEST a drain (never a hard exit) — the consumer
    // loop stops at the task boundary and the drain timer escalates to D2.
    let term_flag = Arc::new(AtomicBool::new(false));
    for sig in [signal_hook::consts::SIGTERM, signal_hook::consts::SIGINT] {
        if let Err(e) = signal_hook::flag::register(sig, Arc::clone(&term_flag)) {
            eprintln!("[bin] signal registration failed for {sig}: {e}");
        }
    }

    let run_worker = worker.clone();
    let handle = std::thread::spawn(move || {
        if let Err(e) = run_worker.run() {
            eprintln!("[bin] worker run failed: {e}");
        }
    });

    while !term_flag.load(Ordering::SeqCst) {
        std::thread::sleep(std::time::Duration::from_millis(200));
    }
    eprintln!("[bin] signal received, requesting drain");
    worker.request_drain("SIGTERM/SIGINT");
    let _ = handle.join();
    eprintln!("[bin] worker stopped");
}
