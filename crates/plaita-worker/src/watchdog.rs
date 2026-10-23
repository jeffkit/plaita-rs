//! Run-scoped watchdogs (rs-worker-design §5.3 运行期纪律 + §11.3 执行预算 +
//! §11.1 D2；go `worker.go:768-779, 846-901, 1025-1062` 同款):
//!
//! - **deadline watchdog** — the worker-level execution budget (default
//!   1800s, `PLAITA_EXECUTION_DEADLINE_SECONDS`; invalid/≤0 env falls back to
//!   the default with a WARNING, never silent-unbounded — a negative value is
//!   the deployment-level off switch expressible only through the Config
//!   struct). On expiry: set the local flag + `host.cancel()` (cooperative,
//!   no kill -9); the §9.5 classification turns the flag into the named
//!   `execution_deadline_exceeded` terminal. The timer is stopped BEFORE
//!   classification so a late tick never re-judges a boundary completion
//!   (worker.go:939-945).
//! - **cancel poller** — the in-execution cancel-flag probe (worker 只读):
//!   entry/step boundaries are covered by the engine's own gates; this poller
//!   covers the gaps at `PLAITA_CANCEL_POLL_MS` (default 1000).
//! - **lease watchdog** — renew every `ttl/3` (floor 1s); a renew ERROR
//!   (Redis blip) is not a death sentence, only a warning; renew == false =
//!   失租 → one orphan progress heartbeat first (keeper must not kill a
//!   healthy run in the "lease lost but still running" window), then mark
//!   lease lost, request cooperative abort, and hand the run to the orphan
//!   watch (orphan.go:9-19 — heartbeats only: no renew, no re-cancel, no
//!   terminal).
//! - **progress heartbeat** — on every successful renew the current
//!   node_timings merge into a FRESH load of the running row (never overwrite
//!   another writer's snapshot) and last_update_time refreshes — the
//!   `last_update_time` 保鲜 keeper evidence (§11.4). Divergence note: the rs
//!   resume path keeps the row SUSPENDED while the carrier applies (M1
//!   documented posture, worker.rs), so the heartbeat only touches RUNNING
//!   rows — a suspended row mid-resume is skipped.
//!
//! All threads sleep in 200ms shards and check the finished flag — shutdown
//! latency is bounded and the deadline flag is only set while the engine was
//! actually running (never after the classification began).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use plaita_engine::exec::Host;
use serde_json::{Map, Value};

use crate::backend::RedisBackend;
use crate::config::{now_iso, Config};
use crate::error::ErrorKind;
use crate::lease::{self, LeaseHandle};
use crate::state;
use crate::worker::RunHistory;

const SHARD: Duration = Duration::from_millis(200);

/// The classification-visible outcome flags of one engine invocation.
#[derive(Default)]
pub(crate) struct RunFlags {
    pub deadline_hit: AtomicBool,
    pub lease_lost: AtomicBool,
    pub poll_cancel_hit: AtomicBool,
    pub finished: AtomicBool,
}

/// The joinable set of run-scoped threads.
pub(crate) struct RunThreads {
    flags: Arc<RunFlags>,
    handles: Vec<std::thread::JoinHandle<()>>,
}

impl RunThreads {
    /// Stops the deadline timer (and the pollers) BEFORE classification —
    /// only firings that happened while the engine was actually running may
    /// drive the terminal verdict (worker.go:939-945). Returns the final
    /// flag values (read after the join — no thread writes after `finished`).
    pub(crate) fn shutdown(self) -> (bool, bool, bool) {
        self.flags.finished.store(true, Ordering::SeqCst);
        for h in self.handles {
            let _ = h.join();
        }
        (
            self.flags.deadline_hit.load(Ordering::SeqCst),
            self.flags.lease_lost.load(Ordering::SeqCst),
            self.flags.poll_cancel_hit.load(Ordering::SeqCst),
        )
    }
}

/// Sleeps until `deadline` in shards, returning early (true) when the run
/// finished or the worker stopped.
fn sleep_until_sharded(stop: &AtomicBool, finished: &AtomicBool, deadline: Instant) -> bool {
    while Instant::now() < deadline {
        if finished.load(Ordering::SeqCst) || stop.load(Ordering::SeqCst) {
            return true;
        }
        std::thread::sleep(SHARD);
    }
    finished.load(Ordering::SeqCst) || stop.load(Ordering::SeqCst)
}

/// Spawns the three run-scoped watchdogs. `stop` is the worker-level flag
/// (drain/stop cut every wait short); `finished` is the run-level flag.
pub(crate) fn spawn_run_watchdogs(
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
    stop: Arc<AtomicBool>,
    host: Host,
    handle: LeaseHandle,
    history: RunHistory,
    tenant: String,
) -> RunThreads {
    let flags = Arc::new(RunFlags::default());
    let mut handles = Vec::new();

    // -- deadline watchdog (#35 / plaita#267) ------------------------------
    let deadline = cfg.execution_deadline_secs;
    if deadline > 0.0 {
        let flags2 = flags.clone();
        let stop2 = stop.clone();
        let host2 = host.clone();
        handles.push(std::thread::spawn(move || {
            let expires_at = Instant::now() + Duration::from_secs_f64(deadline);
            if sleep_until_sharded(&stop2, &flags2.finished, expires_at) {
                return; // the run (or the worker) ended first — no late tick
            }
            flags2.deadline_hit.store(true, Ordering::SeqCst);
            eprintln!(
                "[watchdog] execution deadline exceeded; cancelling the run cooperatively \
                 (terminal error will be written) deadline_seconds={deadline}"
            );
            host2.cancel();
        }));
    }

    // -- cancel poller (§6.2 三处消费面的在途腿) ----------------------------
    {
        let flags2 = flags.clone();
        let stop2 = stop.clone();
        let host2 = host.clone();
        let backend2 = backend.clone();
        let cfg2 = cfg.clone();
        let tenant2 = tenant.clone();
        let handle2 = handle.clone();
        let poll_ms = cfg.cancel_poll_ms.max(100) as u64;
        handles.push(std::thread::spawn(move || {
            loop {
                let next = Instant::now() + Duration::from_millis(poll_ms);
                if sleep_until_sharded(&stop2, &flags2.finished, next) {
                    return;
                }
                match state::is_cancel_requested(
                    backend2.as_ref(),
                    &cfg2,
                    &tenant2,
                    &handle2.execution_id,
                ) {
                    Err(e) => {
                        // probe failure: not a cancel — keep polling
                        eprintln!("[watchdog] cancel poll failed: {e}");
                    }
                    Ok(Some(_)) => {
                        flags2.poll_cancel_hit.store(true, Ordering::SeqCst);
                        eprintln!(
                            "[watchdog] cancel flag observed mid-run; cancelling cooperatively \
                             (execution_id={})",
                            handle2.execution_id
                        );
                        host2.cancel();
                        return;
                    }
                    Ok(None) => {}
                }
            }
        }));
    }

    // -- lease watchdog + progress heartbeat (§5.3 / §8.1 layer 3) ---------
    {
        let flags2 = flags.clone();
        let stop2 = stop.clone();
        let host2 = host;
        let handle2 = handle;
        let backend2 = backend;
        let cfg2 = cfg.clone();
        let tenant2 = tenant.clone();
        let history2 = history.clone();
        let interval_ms = ((cfg.lease_ttl_seconds / 3).max(1) * 1000) as u64;
        handles.push(std::thread::spawn(move || {
            loop {
                let next = Instant::now() + Duration::from_millis(interval_ms);
                if sleep_until_sharded(&stop2, &flags2.finished, next) {
                    return;
                }
                match lease::renew(backend2.as_ref(), &cfg2, &handle2, cfg2.lease_ttl_seconds) {
                    Err(e) => {
                        // Redis 瞬断不判死 (§5.3)
                        eprintln!("[watchdog] lease renew errored (transient): {e}");
                    }
                    Ok(false) => {
                        // 失租：先发布一次进度（防 keeper 在「租约已丢但仍在
                        // 跑」窗口误杀），再标记 lease_lost、请求协作中止、
                        // 转交孤儿观察（只发心跳，不续租、不再 cancel）。
                        eprintln!(
                            "[watchdog] execution lease lost mid-run, publishing orphan \
                             progress and aborting the step (execution_id={})",
                            handle2.execution_id
                        );
                        let _ = progress_heartbeat(
                            backend2.as_ref(),
                            &cfg2,
                            &tenant2,
                            &handle2,
                            &history2,
                        );
                        flags2.lease_lost.store(true, Ordering::SeqCst);
                        host2.cancel();
                        spawn_orphan_watch(
                            backend2.clone(),
                            cfg2.clone(),
                            stop2.clone(),
                            flags2.clone(),
                            handle2.clone(),
                            history2.clone(),
                            tenant2.clone(),
                            interval_ms,
                        );
                        return;
                    }
                    Ok(true) => {
                        // progress heartbeat (§8.1 layer 3): keep the running
                        // row fresh (last_update_time + merged node_timings).
                        if let Err(e) = progress_heartbeat(
                            backend2.as_ref(),
                            &cfg2,
                            &tenant2,
                            &handle2,
                            &history2,
                        ) {
                            eprintln!(
                                "[watchdog] progress heartbeat failed (observation \
                                       only): {e}"
                            );
                        }
                    }
                }
            }
        }));
    }

    RunThreads { flags, handles }
}

/// The post-lease-loss observation loop (orphan.go): heartbeats only. Exits
/// when the run finishes or a fresh fence generation proves another holder
/// took over.
#[allow(clippy::too_many_arguments)]
fn spawn_orphan_watch(
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
    stop: Arc<AtomicBool>,
    flags: Arc<RunFlags>,
    handle: LeaseHandle,
    history: RunHistory,
    tenant: String,
    interval_ms: u64,
) {
    eprintln!(
        "[watchdog] execution entered orphan watch after lease loss (progress heartbeats \
         only, execution_id={})",
        handle.execution_id
    );
    std::thread::spawn(move || {
        let mut rounds = 0u64;
        loop {
            let next = Instant::now() + Duration::from_millis(interval_ms);
            if sleep_until_sharded(&stop, &flags.finished, next) {
                return;
            }
            if !run_is_advancing(&history) {
                eprintln!(
                    "[watchdog] orphan watch dropped (execution ended / no open node; \
                     execution_id={}, rounds={rounds})",
                    handle.execution_id
                );
                return;
            }
            match progress_heartbeat(backend.as_ref(), &cfg, &tenant, &handle, &history) {
                Err(e) if e.kind() == ErrorKind::LeaseLost => {
                    // A new generation owns the row: this process's heartbeats
                    // are no longer trustworthy.
                    eprintln!(
                        "[watchdog] orphan watch dropped (fence generation taken over; \
                         execution_id={}, rounds={rounds})",
                        handle.execution_id
                    );
                    return;
                }
                Err(e) => {
                    eprintln!(
                        "[watchdog] orphan progress heartbeat failed (observation \
                               only): {e}"
                    );
                }
                Ok(_) => {}
            }
            rounds += 1;
        }
    });
}

/// The keeper liveness evidence ①: any node timing still carrying the in-run
/// `ended_at: ""` marker (§6.3).
fn run_is_advancing(history: &RunHistory) -> bool {
    let timings = history.snapshot_node_timings();
    timings
        .as_object()
        .map(|m| {
            m.values().any(|v| {
                v.get("ended_at")
                    .and_then(|e| e.as_str())
                    .map(|s| s.is_empty())
                    .unwrap_or(false)
            })
        })
        .unwrap_or(false)
}

/// Merges the run's current node_timings into a FRESH load of the state row
/// and refreshes last_update_time (never overwrite another writer's
/// snapshot). Only RUNNING rows are touched (module-doc divergence note).
/// A fence mismatch is the "new holder took over" signal (`Err(LeaseLost)`).
fn progress_heartbeat(
    backend: &dyn RedisBackend,
    cfg: &Config,
    tenant: &str,
    handle: &LeaseHandle,
    history: &RunHistory,
) -> Result<(), crate::error::WorkerError> {
    let row = state::load(backend, cfg, tenant, &handle.execution_id)?;
    let Some(mut row) = row else {
        return Ok(());
    };
    if row.status != state::STATUS_RUNNING {
        return Ok(());
    }
    let current = history.snapshot_node_timings();
    let mut merged: Map<String, Value> = match &row.node_timings {
        Some(Value::Object(m)) => m.clone(),
        _ => Map::new(),
    };
    if let Value::Object(c) = &current {
        for (k, v) in c {
            merged.insert(k.clone(), v.clone());
        }
    }
    row.node_timings = Some(Value::Object(merged));
    row.last_update_time = now_iso();
    match state::save(backend, cfg, tenant, &row, handle.fence_token) {
        Ok(()) => Ok(()),
        Err(e)
            if matches!(
                e.kind(),
                ErrorKind::LeaseLost | ErrorKind::FenceGenerationInconsistent
            ) =>
        {
            Err(e)
        }
        // persist outage: the heartbeat is observation-only — never fatal
        Err(e) => {
            eprintln!("[watchdog] heartbeat persist failed (observation only): {e}");
            Ok(())
        }
    }
}
