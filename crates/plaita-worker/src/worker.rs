//! The worker loop (§9, port of worker.go — M1 cut): single-concurrency
//! consume (reclaim-before-read), start/resume/cancel carrier paths with
//! the §9.1 disposition table, suspend persistence with the dispatch
//! credential (§9.4), ResumeGuard-ack (#68), the §9.2 true wake-up over the
//! engine recovery chain, fresh-run replay for continue carriers (§9.6,
//! observable), and the D1/D2 drain with the post-read gate.
//!
//! M1 boundaries (documented, spec §12): no registry heartbeats (M2/M3) —
//! lease conflicts park conservatively for ALL carriers; no mid-run cancel
//! poller / lease watchdog / deadline flag (M2) — the engine's own
//! execution budget bounds every run; no retry budgets (M1 fails the node
//! straight to the error terminal).

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use plaita_engine::error::FlowError;
use plaita_engine::exec::{Host, ResumeKind};
use serde_json::{json, Value};

use crate::backend::RedisBackend;
use crate::checkpoint::{self, CheckpointRecord};
use crate::config::{now_iso, Config};
use crate::error::{terminal_error_state, ErrorKind, WorkerError, FLOW_ERROR_CODE};
use crate::events;
use crate::flowsource::{self, FlowDefinition};
use crate::keys;
use crate::lease::{self, LeaseHandle};
use crate::metrics::Metrics;
use crate::queue::{Task, TaskQueue};
use crate::state::{self, ExecutionState};
use crate::subscriptions::RedisSubscriptions;

/// Node timings / timeline carry-forward across resume sessions (§6.3): the
/// timeline sequence stays monotonic so the final row reads as ONE
/// execution.
#[derive(Clone)]
pub(crate) struct RunHistory {
    inner: Arc<Mutex<HistoryInner>>,
}

struct HistoryInner {
    node_timings: serde_json::Map<String, Value>,
    /// Executed node results (the $NODE root of the terminal context — the
    /// Go buildCheckpoint analogue; M1 rebuilds it from the event stream
    /// because the engine's final rc is not exposed to the host).
    node_results: serde_json::Map<String, Value>,
    last_node: Option<String>,
    events: Vec<Value>,
    seq: u64,
    dropped: u64,
    enqueued_at: String,
    queue_wait_ms: Option<f64>,
}

const TIMELINE_MAX_EVENTS: usize = 500;

impl RunHistory {
    fn new(enqueued_at: &str) -> RunHistory {
        let queue_wait_ms = queue_wait_millis(enqueued_at);
        RunHistory {
            inner: Arc::new(Mutex::new(HistoryInner {
                node_timings: serde_json::Map::new(),
                node_results: serde_json::Map::new(),
                last_node: None,
                events: Vec::new(),
                seq: 0,
                dropped: 0,
                enqueued_at: enqueued_at.to_string(),
                queue_wait_ms,
            })),
        }
    }

    /// Carries the previous session's node timings + timeline forward
    /// (worker.go seedRunFromState).
    fn seed_from(&self, state: &ExecutionState) {
        let mut h = self.inner.lock().unwrap();
        if let Some(nt) = &state.node_timings {
            if let Some(map) = nt.as_object() {
                for (k, v) in map {
                    h.node_timings.insert(k.clone(), v.clone());
                }
            }
        }
        if let Some(tl) = &state.timeline {
            h.dropped = tl.get("dropped").and_then(|d| d.as_u64()).unwrap_or(0);
            if let Some(events) = tl.get("events").and_then(|e| e.as_array()) {
                for ev in events {
                    h.events.push(ev.clone());
                    if let Some(seq) = ev.get("seq").and_then(|s| s.as_u64()) {
                        h.seq = h.seq.max(seq);
                    }
                }
            }
        }
    }

    fn record_timeline(&self, event_type: &str, extra: Value) -> Value {
        let mut h = self.inner.lock().unwrap();
        h.seq += 1;
        let mut ev = json!({
            "seq": h.seq,
            "type": event_type,
            "at": now_iso(),
            "at_ms": epoch_ms(),
        });
        if let (Value::Object(dst), Value::Object(src)) = (&mut ev, &extra) {
            for (k, v) in src {
                dst.insert(k.clone(), v.clone());
            }
        }
        let ev_clone = ev.clone();
        push_event_locked(&mut h, ev);
        ev_clone
    }

    fn snapshot_node_timings(&self) -> Value {
        Value::Object(self.inner.lock().unwrap().node_timings.clone())
    }

    /// Folds the captured node results + last completed node into the
    /// terminal row's context (the rs snapshot shape; Go buildCheckpoint's
    /// $NODE/$LAST_NODE analogue).
    fn apply_to_context(&self, context: &mut Value) {
        let h = self.inner.lock().unwrap();
        if let Value::Object(ctx) = context {
            if let Some(Value::Object(nodes)) = ctx.get_mut("nodes") {
                for (k, v) in &h.node_results {
                    nodes.insert(k.clone(), v.clone());
                }
            }
            if let Some(last) = &h.last_node {
                ctx.insert("last_node".into(), json!(last));
            }
        }
    }

    fn snapshot_timeline(&self) -> Value {
        let h = self.inner.lock().unwrap();
        let mut tl = json!({
            "events": h.events,
            "dropped": h.dropped,
            "max_events": TIMELINE_MAX_EVENTS,
            "enqueued_at": h.enqueued_at,
        });
        if let Some(w) = h.queue_wait_ms {
            tl["queue_wait_ms"] = json!(w);
        }
        tl
    }

    /// The engine event sink: node start/end bookkeeping (the in-run
    /// `ended_at: ""` marker is the keeper liveness evidence, §6.3).
    fn sink(self) -> plaita_engine::exec::EventSink {
        Arc::new(move |ev: &plaita_engine::exec::Event| match ev {
            plaita_engine::exec::Event::NodeStart {
                node_id, node_type, ..
            } => {
                let mut h = self.inner.lock().unwrap();
                let entry = h.node_timings.entry(node_id.clone()).or_insert_with(|| {
                    json!({
                        "started_at": now_iso(),
                        "started_ms": epoch_ms(),
                        "ended_at": "",
                        "ended_ms": 0,
                        "duration_ms": 0,
                        "attempts": 0,
                        "failed": false,
                    })
                });
                if let Some(o) = entry.as_object_mut() {
                    o.insert(
                        "attempts".into(),
                        json!(o.get("attempts").and_then(|a| a.as_i64()).unwrap_or(0) + 1),
                    );
                    o.insert("ended_at".into(), json!(""));
                }
                push_event_locked(
                    &mut h,
                    json!({
                        "type": "node_start",
                        "node_id": node_id,
                        "node_type": node_type,
                    }),
                );
            }
            plaita_engine::exec::Event::NodeEnd {
                node_id,
                node_type,
                error,
                result,
                ..
            } => {
                let mut h = self.inner.lock().unwrap();
                // Success results feed the terminal row's context ($NODE
                // shape); a failed attempt leaves the last result untouched.
                if error.is_none() {
                    h.node_results
                        .insert(node_id.clone(), result.clone().unwrap_or(Value::Null));
                    h.last_node = Some(node_id.clone());
                }
                let entry = h.node_timings.entry(node_id.clone()).or_insert_with(|| {
                    json!({
                        "started_at": now_iso(),
                        "started_ms": epoch_ms(),
                        "ended_at": "",
                        "ended_ms": 0,
                        "duration_ms": 0,
                        "attempts": 1,
                        "failed": false,
                    })
                });
                let ended_ms = epoch_ms();
                if let Some(o) = entry.as_object_mut() {
                    let started = o
                        .get("started_ms")
                        .and_then(|s| s.as_i64())
                        .unwrap_or(ended_ms);
                    o.insert("ended_at".into(), json!(now_iso()));
                    o.insert("ended_ms".into(), json!(ended_ms));
                    o.insert("duration_ms".into(), json!(ended_ms - started));
                    if error.is_some() {
                        o.insert("failed".into(), json!(true));
                    }
                }
                push_event_locked(
                    &mut h,
                    json!({
                        "type": "node_end",
                        "node_id": node_id,
                        "node_type": node_type,
                        "error": error.clone(),
                    }),
                );
            }
            _ => {}
        })
    }
}

/// Appends one timeline event under the inner lock (seq monotonic, 500-cap
/// with the drop counter — §6.3).
fn push_event_locked(h: &mut HistoryInner, mut ev: Value) {
    h.seq += 1;
    if let Some(o) = ev.as_object_mut() {
        o.insert("seq".into(), json!(h.seq));
        o.insert("at".into(), json!(now_iso()));
        o.insert("at_ms".into(), json!(epoch_ms()));
    }
    h.events.push(ev);
    if h.events.len() > TIMELINE_MAX_EVENTS {
        let overflow = h.events.len() - TIMELINE_MAX_EVENTS;
        h.events.drain(0..overflow);
        h.dropped += overflow as u64;
    }
}

fn epoch_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parses the producer timestamp (naive local wall clock or RFC3339) into a
/// queue wait in milliseconds; unparsable → None.
fn queue_wait_millis(ts: &str) -> Option<f64> {
    if ts.is_empty() {
        return None;
    }
    let naive = chrono::NaiveDateTime::parse_from_str(ts, "%Y-%m-%dT%H:%M:%S%.f").ok();
    let t = match naive {
        Some(n) => n.and_utc(),
        None => chrono::DateTime::parse_from_rfc3339(ts)
            .ok()?
            .with_timezone(&chrono::Utc),
    };
    let produced = t.timestamp_millis();
    let now = epoch_ms();
    Some((now - produced).max(0) as f64)
}

/// The resume payload assembled after the definition gate.
struct ResumeInput {
    checkpoint: CheckpointRecord,
    resume_kind: ResumeKind,
    data: Value,
}

/// The definition-gate outcome (loadContinuationDefinition).
enum Gate {
    Ready(Box<FlowDefinition>),
    /// Carrier flow_id ≠ stored row flow_id: poison ack, row untouched.
    PoisonAck,
    /// A deterministic error terminal was already written.
    TerminalDone,
}

struct ActiveRun {
    host: Host,
}

/// The worker. One consume loop, single in-flight execution (§4.5).
pub struct Worker {
    backend: Arc<dyn RedisBackend>,
    cfg: Config,
    queue: TaskQueue,
    subs: Arc<RedisSubscriptions>,
    metrics: Arc<dyn Metrics>,
    drain: Mutex<DrainState>,
    active: Mutex<Option<ActiveRun>>,
    stop: Arc<AtomicBool>,
}

#[derive(Default)]
struct DrainState {
    draining: bool,
    forced: bool,
    drain_started: bool,
}

impl Worker {
    pub fn new(backend: Arc<dyn RedisBackend>, cfg: Config, metrics: Arc<dyn Metrics>) -> Worker {
        let queue = TaskQueue::new(backend.clone(), cfg.clone(), metrics.clone());
        let subs = Arc::new(RedisSubscriptions::new(backend.clone(), cfg.clone()));
        Worker {
            backend,
            cfg,
            queue,
            subs,
            metrics,
            drain: Mutex::new(DrainState::default()),
            active: Mutex::new(None),
            stop: Arc::new(AtomicBool::new(false)),
        }
    }

    pub fn cfg(&self) -> &Config {
        &self.cfg
    }

    pub fn queue(&self) -> &TaskQueue {
        &self.queue
    }

    // -- drain (§11.1 D1/D2; D3 lives in the bin) --------------------------

    /// D1: stop taking new tasks; the consume loop exits at the task
    /// boundary. Idempotent — a second call never resets the timer. A drain
    /// timer escalates to D2 after cfg.drain_timeout_secs.
    pub fn request_drain(self: &Arc<Self>, reason: &str) {
        {
            let mut drain = self.drain.lock().unwrap();
            if drain.draining {
                return;
            }
            drain.draining = true;
            drain.drain_started = true;
        }
        let timeout = self.cfg.drain_timeout_secs;
        eprintln!(
            "[worker] draining: no new tasks, waiting for in-flight work \
             (reason={reason}, drain_timeout={timeout}s)"
        );
        // D1→D2 escalation: when the bounded wait elapses the drain is
        // upgraded to a force-stop (idempotent; the worker's stop flag cuts
        // the timer short).
        let worker = self.clone();
        let stop = self.stop.clone();
        std::thread::spawn(move || {
            let deadline = std::time::Instant::now() + Duration::from_secs_f64(timeout);
            while std::time::Instant::now() < deadline {
                if stop.load(Ordering::SeqCst) {
                    return;
                }
                std::thread::sleep(Duration::from_millis(100));
            }
            worker.force_stop("drain timeout exceeded");
        });
    }

    /// D2: unconditional stop — cancel every in-flight run cooperatively
    /// (no hard interrupt; "abandoning the step" semantics). Abandoned
    /// steps write NO terminal state; the message stays pending.
    pub fn force_stop(&self, reason: &str) {
        let mut drain = self.drain.lock().unwrap();
        if drain.forced {
            return;
        }
        drain.forced = true;
        drain.draining = true;
        drop(drain);
        let active = self.active.lock().unwrap();
        eprintln!(
            "[worker] force-stop: cancelling in-flight executions (best-effort, no hard \
             interrupt; reason={reason}, inflight={})",
            active.is_some() as u8
        );
        if let Some(run) = active.as_ref() {
            run.host.cancel();
        }
    }

    pub fn draining(&self) -> bool {
        self.drain.lock().unwrap().draining
    }

    pub fn force_stopped(&self) -> bool {
        self.drain.lock().unwrap().forced
    }

    /// Flips the stop flag: background loops wind down within ≤1s.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::SeqCst);
    }

    // -- consume loop -------------------------------------------------------

    /// Runs the consume loop until a drain completes or [`Worker::stop`] is
    /// set. Spawns the residue sweep, the timeout checker and the event
    /// bridge; joins them on exit (each stops within ≤1s).
    pub fn run(&self) -> Result<(), WorkerError> {
        self.queue.ensure_group()?;
        if let Err(e) = self.queue.sweep_acked_residue() {
            eprintln!("[worker] startup acked-residue sweep failed: {e}");
        }
        let mut handles = Vec::new();
        // Residue sweep loop (§4.3).
        {
            let queue =
                TaskQueue::new(self.backend.clone(), self.cfg.clone(), self.metrics.clone());
            let stop = self.stop.clone();
            let interval = Duration::from_secs(self.cfg.sweep_interval_secs.max(1) as u64);
            handles.push(std::thread::spawn(move || loop {
                if events::sleep_interruptible(&stop, interval) {
                    return;
                }
                if let Err(e) = queue.sweep_acked_residue() {
                    eprintln!("[worker] acked-residue sweep failed: {e}");
                }
            }));
        }
        // Subscription timeout checker (§8.3, 10s cadence).
        {
            let backend = self.backend.clone();
            let cfg = self.cfg.clone();
            let queue =
                TaskQueue::new(self.backend.clone(), self.cfg.clone(), self.metrics.clone());
            let subs = self.subs.clone();
            let stop = self.stop.clone();
            handles.push(std::thread::spawn(move || loop {
                if events::sleep_interruptible(&stop, Duration::from_secs(10)) {
                    return;
                }
                if let Err(e) = events::sweep_event_timeouts(&backend, &cfg, &queue, &subs) {
                    eprintln!("[worker] event timeout sweep failed: {e}");
                }
            }));
        }
        // Event bridge (§8.2).
        {
            let backend = self.backend.clone();
            let cfg = self.cfg.clone();
            let queue =
                TaskQueue::new(self.backend.clone(), self.cfg.clone(), self.metrics.clone());
            let subs = self.subs.clone();
            let stop = self.stop.clone();
            handles.push(std::thread::spawn(move || {
                events::event_bridge_loop(backend, cfg, Arc::new(queue), subs, stop)
            }));
        }
        let mut backoff = Duration::ZERO;
        while !self.draining() && !self.stop.load(Ordering::SeqCst) {
            if backoff > Duration::ZERO && events::sleep_interruptible(&self.stop, backoff) {
                break;
            }
            self.metrics.set_gauge(
                crate::metrics::METRIC_QUEUE_DEPTH,
                self.queue.depth() as f64,
                &[],
            );
            match self.process_one() {
                Ok(_handled) => backoff = Duration::ZERO,
                Err(e) => {
                    if self.force_stopped() {
                        eprintln!(
                            "[worker] task abandoned by drain force-stop (message left pending): {e}"
                        );
                        continue;
                    }
                    // Transient family: warn + backoff, never blow up the
                    // loop (§4.2).
                    eprintln!("[worker] task processing left message pending: {e}");
                    backoff = crate::config::RECONNECT_BACKOFF;
                }
            }
        }
        eprintln!("[worker] consume loop stopped");
        self.stop();
        for h in handles {
            let _ = h.join();
        }
        Ok(())
    }

    /// Exactly one consume round: reclaim + read + dispatch. `Ok(true)` = a
    /// message was handled to a terminal disposition (acked, dead-lettered,
    /// poison-acked or carrier-rejected); `Ok(false)` = nothing available.
    /// A non-nil error leaves the message pending for redelivery/reclaim.
    pub fn process_one(&self) -> Result<bool, WorkerError> {
        let Some(delivery) = self.queue.read()? else {
            return Ok(false);
        };
        // §9.3 D1 post-read gate: a message delivered after draining began
        // is neither dispatched nor acked — it stays in the PEL for another
        // worker.
        if self.draining() || self.force_stopped() {
            eprintln!(
                "[worker] message delivered after drain began, leaving pending (id={})",
                delivery.task.message_id
            );
            return Ok(true);
        }
        let task = delivery.task;
        let result = match task.task_type.as_str() {
            "start" => self.handle_start(&task),
            "resume" => self.handle_resume(&task),
            other => {
                // Unknown type → poison ack (§4.5): no execution, no
                // requeue, no DLQ.
                eprintln!(
                    "[worker] unknown task type, poison ack (type={other:?}, id={})",
                    task.message_id
                );
                Ok(())
            }
        };
        match result {
            Ok(()) => {
                self.queue.ack(&task.message_id)?;
                Ok(true)
            }
            Err(e) => {
                if e.kind().is_carrier_rejected() {
                    // #68: the rejection is permanent by construction; the
                    // execution stays suspended and the loop must not exist
                    // structurally.
                    self.metrics.inc_counter(
                        "resume_carrier_rejected_total",
                        &[("resume_type", task.resume_type.as_str())],
                    );
                    eprintln!(
                        "[worker] resume carrier permanently rejected; acked, execution stays \
                         suspended (execution_id={}, resume_type={}, id={}): {e}",
                        task.execution_id, task.resume_type, task.message_id
                    );
                    self.queue.ack(&task.message_id)?;
                    return Ok(true);
                }
                Err(e)
            }
        }
    }

    // -- start path (§9.3) --------------------------------------------------

    fn handle_start(&self, task: &Task) -> Result<(), WorkerError> {
        let mut task = task.clone();
        let tenant = task.tenant_id.clone();
        if task.execution_id.is_empty() {
            // legacy messages: mint in place (§4.1) — the minted id must
            // reach every downstream consumer (row, lease, terminal write).
            task.execution_id = crate::config::new_execution_id();
        }
        let execution_id = task.execution_id.clone();

        // Lease acquire strictly before the running row (§6.2).
        let holder = lease::new_holder_token("start", &self.cfg.instance_id);
        let handle = match lease::acquire(
            self.backend.as_ref(),
            &self.cfg,
            &tenant,
            &execution_id,
            &holder,
            self.cfg.lease_ttl_seconds,
        ) {
            Ok(h) => h,
            Err(e) if e.kind() == ErrorKind::LeaseConflict => {
                return self.lease_conflict_disposition(&task);
            }
            Err(e) => return Err(WorkerError::transient(format!("lease acquire: {e}"))),
        };

        // Idempotency gate (§9.3): a carrier for an execution that already
        // has a state row never re-runs the flow; terminal rows are never
        // overwritten; an id owned by another flow is poison ack.
        let prev = match state::load(self.backend.as_ref(), &self.cfg, &tenant, &execution_id) {
            Ok(p) => p,
            Err(e) => {
                let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
                return Err(e);
            }
        };
        if let Some(prev) = prev {
            if !prev.flow_id.is_empty() && !task.flow_id.is_empty() && prev.flow_id != task.flow_id
            {
                let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
                eprintln!(
                    "[worker] execution_id already belongs to another flow; duplicate start \
                     poison-acked (execution_id={execution_id}, carrier_flow={}, stored_flow={})",
                    task.flow_id, prev.flow_id
                );
                return Ok(());
            }
            match prev.status.as_str() {
                state::STATUS_QUEUED => {
                    // The console's placeholder (acceptance evidence, not
                    // processing evidence): fall through into the normal
                    // start; the takeover write is fenced by our lease.
                }
                state::STATUS_RUNNING => {
                    // Crash-recovery carrier (#47): convert to a continue
                    // carrier (enqueue → ack; §9.6 fresh-run replay).
                    let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
                    return self.requeue_normalized_continue(&task);
                }
                state::STATUS_SUSPENDED => {
                    let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
                    self.compensate_suspend_dispatch(&task, &prev)?;
                    eprintln!(
                        "[worker] start carrier for a suspended execution, idempotent ack \
                         (use a resume carrier to wake it; execution_id={execution_id})"
                    );
                    return Ok(());
                }
                other => {
                    let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
                    eprintln!(
                        "[worker] duplicate start for an existing execution, idempotent ack \
                         (no overwrite; execution_id={execution_id}, status={other})"
                    );
                    return Ok(());
                }
            }
        }

        // A brand-new id must not inherit a stale checkpoint from a previous
        // life of the same id (§7.3; retry counters are M2).
        self.delete_checkpoint(&tenant, &execution_id);

        // Entry cancel checkpoint (§6.2): cancel intent before the running
        // row exists. fence_gen=0 is safe — no live holder can exist yet.
        let entry_cancel_hit =
            state::is_cancel_requested(self.backend.as_ref(), &self.cfg, &tenant, &execution_id)
                .inspect_err(|_| {
                    let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
                })?
                .is_some();
        if entry_cancel_hit {
            let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
            let mut state_row = ExecutionState::new_running(
                &self.cfg,
                &task.flow_id,
                &task.params,
                &execution_id,
                &tenant,
            );
            state_row.status = state::STATUS_CANCELLED.to_string();
            let now = now_iso();
            state_row.end_time = now.clone();
            state_row.last_update_time = now;
            state::save(self.backend.as_ref(), &self.cfg, &tenant, &state_row, 0)
                .map_err(|e| WorkerError::transient(format!("entry-cancel persist: {e}")))?;
            self.delete_checkpoint(&tenant, &execution_id);
            eprintln!("[worker] execution cancelled at entry (execution_id={execution_id})");
            return Ok(());
        }

        // Flow definition (§4.6). Missing/unloadable = deterministic error
        // terminal; transient read failure = leave pending.
        let def = match flowsource::get_flow_definition(
            self.backend.as_ref(),
            &self.cfg,
            &tenant,
            &task.flow_id,
            &task.version,
        ) {
            Ok(d) => d,
            Err(e) => {
                let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
                if e.kind() == ErrorKind::FlowNotFound {
                    eprintln!(
                        "[worker] flow definition not loadable, error terminal (flow_id={}): {e}",
                        task.flow_id
                    );
                    let state_row = ExecutionState::new_running(
                        &self.cfg,
                        &task.flow_id,
                        &task.params,
                        &execution_id,
                        &tenant,
                    );
                    let err_value = terminal_error_state(
                        "flow_not_found",
                        FLOW_ERROR_CODE,
                        &e.to_string(),
                        None,
                        None,
                    );
                    return self.persist_error_terminal(
                        &tenant,
                        handle.fence_token,
                        state_row,
                        None,
                        err_value,
                    );
                }
                return Err(WorkerError::transient(format!("flow load: {e}")));
            }
        };

        let mut state_row = ExecutionState::new_running(
            &self.cfg,
            &task.flow_id,
            &task.params,
            &execution_id,
            &tenant,
        );
        state_row.flow_version = def.version.clone();
        state_row.flow_hash = def.hash.clone();
        state_row.flow_hash_algo = def.hash_algo.clone();
        state_row.flow_name = def.workflow.name.clone();
        state_row.last_update_time = now_iso();
        if let Err(e) = state::save(
            self.backend.as_ref(),
            &self.cfg,
            &tenant,
            &state_row,
            handle.fence_token,
        ) {
            let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
            return Err(WorkerError::transient(format!("running row persist: {e}")));
        }

        self.execute_and_finalize(&task, &def, &handle, state_row, None, false)
    }

    // -- resume path (§9.1 disposition table) --------------------------------

    fn handle_resume(&self, task: &Task) -> Result<(), WorkerError> {
        let tenant = task.tenant_id.clone();
        let execution_id = task.execution_id.clone();
        if execution_id.is_empty() {
            eprintln!(
                "[worker] resume without execution_id, poison ack (resume_type={})",
                task.resume_type
            );
            return Ok(());
        }

        // Pre-lease probes (no lease taken on the short-circuit paths).
        let cancel_hit =
            state::is_cancel_requested(self.backend.as_ref(), &self.cfg, &tenant, &execution_id)?;
        let state_row = state::load(self.backend.as_ref(), &self.cfg, &tenant, &execution_id)?;
        // A missing row is NOT a pre-lease decision: the lease-conflict
        // window must still be observed (§5.3).
        if let Some(row) = &state_row {
            if state::is_terminal(&row.status) {
                eprintln!(
                    "[worker] resume for terminal execution, idempotent ack (execution_id=\
                     {execution_id}, status={}, resume_type={})",
                    row.status, task.resume_type
                );
                return Ok(());
            }
        }
        // Cancellation priority: a cancel flag (or the cancel carrier) is
        // consumed under the lease; it overrides every shortcut.
        if cancel_hit.is_some() || task.resume_type == "cancel" {
            return self.resume_cancel_under_lease(task);
        }
        // suspended + continue/retry carries NO progress semantics (§9.1);
        // no lease is taken unless a dispatch credential needs one.
        if let Some(row) = &state_row {
            if row.status == state::STATUS_SUSPENDED
                && matches!(task.resume_type.as_str(), "continue" | "retry")
            {
                self.compensate_suspend_dispatch(task, row)?;
                eprintln!(
                    "[worker] suspended execution: resume carrier carries no progress \
                     semantics, idempotent ack (execution_id={execution_id}, resume_type={})",
                    task.resume_type
                );
                return Ok(());
            }
        }
        self.resume_under_lease(task)
    }

    /// Acquires the execution lease, re-reads the state (§6.2 resume
    /// TOCTOU), re-runs the preconditions and performs the disposition.
    fn resume_under_lease(&self, task: &Task) -> Result<(), WorkerError> {
        let tenant = task.tenant_id.clone();
        let execution_id = task.execution_id.clone();
        let holder = lease::new_holder_token("resume", &self.cfg.instance_id);
        let handle = match lease::acquire(
            self.backend.as_ref(),
            &self.cfg,
            &tenant,
            &execution_id,
            &holder,
            self.cfg.lease_ttl_seconds,
        ) {
            Ok(h) => h,
            Err(e) if e.kind() == ErrorKind::LeaseConflict => {
                return self.lease_conflict_disposition(task);
            }
            Err(e) => return Err(WorkerError::transient(format!("resume lease acquire: {e}"))),
        };
        let release = |w: &Worker, h: &LeaseHandle| {
            let _ = lease::release_with(w.backend.as_ref(), &w.cfg, h);
        };

        // In-lease re-check (plaita#241): the entry snapshot may be stale.
        if state::is_cancel_requested(self.backend.as_ref(), &self.cfg, &tenant, &execution_id)?
            .is_some()
        {
            let r = self.write_resume_cancelled(&tenant, &execution_id, handle.fence_token);
            release(self, &handle);
            return r;
        }
        let state_row = state::load(self.backend.as_ref(), &self.cfg, &tenant, &execution_id)?;
        let Some(mut row) = state_row else {
            eprintln!(
                "[worker] resume: execution vanished under the lease, poison ack \
                 (execution_id={execution_id})"
            );
            release(self, &handle);
            return Ok(());
        };
        if state::is_terminal(&row.status) {
            eprintln!(
                "[worker] resume: execution terminalized under the lease, idempotent ack \
                 (execution_id={execution_id}, status={})",
                row.status
            );
            release(self, &handle);
            return Ok(());
        }
        if task.resume_type == "cancel" {
            let r = self.write_resume_cancelled(&tenant, &execution_id, handle.fence_token);
            release(self, &handle);
            return r;
        }
        if row.status == state::STATUS_SUSPENDED
            && matches!(task.resume_type.as_str(), "continue" | "retry")
        {
            let r = self.dispatch_pending_service(task, &row, handle.fence_token);
            release(self, &handle);
            eprintln!(
                "[worker] suspended execution (in-lease recheck): idempotent ack \
                 (execution_id={execution_id})"
            );
            return r.map(|_| ());
        }
        if row.status == state::STATUS_RUNNING
            && matches!(task.resume_type.as_str(), "event" | "timeout")
        {
            // #258: the carrier arrived after the suspend point was consumed
            // — normalize into a continue carrier instead of acking the only
            // recovery path.
            release(self, &handle);
            return self.requeue_normalized_continue(task);
        }
        if row.status == state::STATUS_SUSPENDED
            && matches!(task.resume_type.as_str(), "event" | "timeout")
        {
            // The §9.2 true wake-up.
            return self.resume_suspended(task, &mut row, &handle);
        }
        if row.status == state::STATUS_RUNNING
            && matches!(task.resume_type.as_str(), "continue" | "retry")
        {
            return self.continue_running(task, &row, &handle);
        }
        // Any other carrier/state combination has no advance semantics in
        // this build — fail closed with an observable terminal instead of
        // acking the carrier silently (§9.1).
        let message = format!(
            "resume carrier (type={}) cannot advance an execution in status {}",
            task.resume_type, row.status
        );
        let err_value = terminal_error_state(
            "resume_not_supported",
            FLOW_ERROR_CODE,
            &message,
            None,
            None,
        );
        let r = self.persist_error_terminal(&tenant, handle.fence_token, row, None, err_value);
        release(self, &handle);
        eprintln!(
            "[worker] resume carrier has no applicable state transition, deterministic error \
             terminal written (execution_id={execution_id}, resume_type={})",
            task.resume_type
        );
        r
    }

    /// Terminalizes a cancel intent (idempotent). The lease mutual exclusion
    /// keeps two carriers from writing concurrently.
    fn resume_cancel_under_lease(&self, task: &Task) -> Result<(), WorkerError> {
        let tenant = task.tenant_id.clone();
        let execution_id = task.execution_id.clone();
        let holder = lease::new_holder_token("resume", &self.cfg.instance_id);
        let handle = match lease::acquire(
            self.backend.as_ref(),
            &self.cfg,
            &tenant,
            &execution_id,
            &holder,
            self.cfg.lease_ttl_seconds,
        ) {
            Ok(h) => h,
            Err(e) if e.kind() == ErrorKind::LeaseConflict => {
                return self.lease_conflict_disposition(task);
            }
            Err(e) => {
                return Err(WorkerError::transient(format!(
                    "resume cancel lease acquire: {e}"
                )))
            }
        };
        let state_row = state::load(self.backend.as_ref(), &self.cfg, &tenant, &execution_id)?;
        let r = match state_row {
            None => Ok(()),
            Some(row) if state::is_terminal(&row.status) => Ok(()),
            Some(_) => self.write_resume_cancelled(&tenant, &execution_id, handle.fence_token),
        };
        let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
        r
    }

    fn write_resume_cancelled(
        &self,
        tenant: &str,
        execution_id: &str,
        fence_gen: i64,
    ) -> Result<(), WorkerError> {
        let Some(mut row) = state::load(self.backend.as_ref(), &self.cfg, tenant, execution_id)?
        else {
            return Ok(());
        };
        if state::is_terminal(&row.status) {
            return Ok(());
        }
        let now = now_iso();
        row.status = state::STATUS_CANCELLED.to_string();
        row.end_time = now.clone();
        row.last_update_time = now;
        state::save(self.backend.as_ref(), &self.cfg, tenant, &row, fence_gen)
            .map_err(|e| WorkerError::transient(format!("cancelled persist: {e}")))?;
        self.delete_checkpoint(tenant, execution_id);
        eprintln!("[worker] resume terminalized cancelled execution (execution_id={execution_id})");
        Ok(())
    }

    /// #258: replace a missed event/timeout carrier with a continue carrier.
    /// The original is acked only after the enqueue succeeded (ProcessOne's
    /// standard ack).
    fn requeue_normalized_continue(&self, task: &Task) -> Result<(), WorkerError> {
        let mut body = json!({
            "type": "resume",
            "flow_id": task.flow_id,
            "execution_id": task.execution_id,
            "resume_type": "continue",
            "timestamp": now_iso(),
        });
        if !task.tenant_id.is_empty() {
            body["tenant_id"] = json!(task.tenant_id);
        }
        if task.params.is_object() && !task.params.as_object().unwrap().is_empty() {
            body["params"] = task.params.clone();
        }
        if let Err(e) = self.queue.enqueue(&body, task.envelope_schema_ver) {
            eprintln!(
                "[worker] normalized continue re-enqueue failed; leaving the original pending \
                 (execution_id={}): {e}",
                task.execution_id
            );
            return Err(WorkerError::transient(format!(
                "normalized continue enqueue: {e}"
            )));
        }
        eprintln!(
            "[worker] resume carrier missed the suspend point; normalized to a continue \
             carrier (execution_id={}, resume_type={})",
            task.execution_id, task.resume_type
        );
        Ok(())
    }

    /// §5.3 conflict disposition, M1 posture (no registry yet): decision
    /// carriers are NEVER acked in the conflict window; all other carriers
    /// park conservatively too (no live-holder proof source in M1 — the
    /// registry's LookupStatus ack branch is M2).
    fn lease_conflict_disposition(&self, task: &Task) -> Result<(), WorkerError> {
        if task.suspended_resume_chain {
            eprintln!(
                "[worker] lease conflict on a decision resume carrier, leaving pending \
                 (execution_id={}, resume_type={})",
                task.execution_id, task.resume_type
            );
        } else {
            eprintln!(
                "[worker] lease conflict, leaving pending (execution_id={}, type={})",
                task.execution_id, task.task_type
            );
        }
        Err(WorkerError::new(
            ErrorKind::LeaseConflict,
            format!(
                "execution lease held by another worker ({})",
                task.execution_id
            ),
        ))
    }

    /// The §9.2 wake-up: flow gates → checkpoint load → rebuild → resume →
    /// classify (shared execute_and_finalize).
    fn resume_suspended(
        &self,
        task: &Task,
        row: &mut ExecutionState,
        handle: &LeaseHandle,
    ) -> Result<(), WorkerError> {
        let tenant = task.tenant_id.clone();
        let flow_id = if task.flow_id.is_empty() {
            row.flow_id.clone()
        } else {
            task.flow_id.clone()
        };
        let def =
            match self.load_continuation_definition(task, row, &flow_id, handle.fence_token)? {
                Gate::Ready(d) => d,
                Gate::PoisonAck => return Ok(()),
                Gate::TerminalDone => return Ok(()),
            };
        // The dedicated checkpoint key is authoritative (§9.2 step 2).
        let cp = checkpoint::load(
            self.backend.as_ref(),
            &self.cfg,
            &tenant,
            &task.execution_id,
        )?;
        let Some(cp) = cp else {
            eprintln!(
                "[worker] resume: no checkpoint for suspended execution (execution_id={}, \
                 resume_type={})",
                task.execution_id, task.resume_type
            );
            let err_value = terminal_error_state(
                "resume_checkpoint_missing",
                FLOW_ERROR_CODE,
                &format!(
                    "resume carrier (type={}) cannot advance execution {}: no checkpoint found",
                    task.resume_type, task.execution_id
                ),
                None,
                None,
            );
            self.delete_checkpoint(&tenant, &task.execution_id);
            return self.persist_error_terminal(
                &tenant,
                handle.fence_token,
                row.clone(),
                None,
                err_value,
            );
        };
        let resume_kind = ResumeKind::parse(&task.resume_type).ok_or_else(|| {
            WorkerError::transient(format!(
                "resume type {:?} is not a decision carrier",
                task.resume_type
            ))
        })?;
        self.execute_and_finalize(
            task,
            &def,
            handle,
            row.clone(),
            Some(ResumeInput {
                checkpoint: cp,
                resume_kind,
                data: task.data.clone(),
            }),
            false,
        )
    }

    /// continue/retry carrier on a live (running) execution (§9.6): the rs
    /// engine has no mid-flow entry in M1 → the carrier degrades to a
    /// flow-start re-entry; the fallback is recorded in the timeline
    /// (`continue_fallback_start`) and the previous session's node timings +
    /// timeline carry forward.
    fn continue_running(
        &self,
        task: &Task,
        row: &ExecutionState,
        handle: &LeaseHandle,
    ) -> Result<(), WorkerError> {
        let tenant = task.tenant_id.clone();
        let flow_id = if task.flow_id.is_empty() {
            row.flow_id.clone()
        } else {
            task.flow_id.clone()
        };
        let def =
            match self.load_continuation_definition(task, row, &flow_id, handle.fence_token)? {
                Gate::Ready(d) => d,
                Gate::PoisonAck => return Ok(()),
                Gate::TerminalDone => return Ok(()),
            };
        eprintln!(
            "[worker] continue carrier resuming a live execution without a mid-flow checkpoint \
             (fresh-run parity; execution_id={}, resume_type={})",
            task.execution_id, task.resume_type
        );
        let _ = &tenant;
        self.execute_and_finalize(task, &def, handle, row.clone(), None, true)
    }

    /// The §9.2 step-1 gates shared by both resume paths: flow-id mismatch →
    /// poison ack; missing/unloadable → flow_not_found terminal; fingerprint
    /// mismatch → flow_hash_mismatch terminal unless the carrier carries
    /// allow_flow_hash_change.
    fn load_continuation_definition(
        &self,
        task: &Task,
        row: &ExecutionState,
        flow_id: &str,
        fence_gen: i64,
    ) -> Result<Gate, WorkerError> {
        let tenant = task.tenant_id.clone();
        if !row.flow_id.is_empty() && !flow_id.is_empty() && row.flow_id != flow_id {
            eprintln!(
                "[worker] resume carrier flow_id does not match the stored row, poison ack \
                 (execution_id={}, carrier_flow={flow_id}, stored_flow={})",
                task.execution_id, row.flow_id
            );
            return Ok(Gate::PoisonAck);
        }
        let def = match flowsource::get_flow_definition(
            self.backend.as_ref(),
            &self.cfg,
            &tenant,
            flow_id,
            &row.flow_version,
        ) {
            Ok(d) => d,
            Err(e) => {
                if e.kind() == ErrorKind::FlowNotFound {
                    eprintln!(
                        "[worker] resume: flow definition not loadable, error terminal \
                         (flow_id={flow_id}, version={}): {e}",
                        row.flow_version
                    );
                    let err_value = terminal_error_state(
                        "flow_not_found",
                        FLOW_ERROR_CODE,
                        &e.to_string(),
                        None,
                        None,
                    );
                    self.delete_checkpoint(&tenant, &task.execution_id);
                    self.persist_error_terminal(&tenant, fence_gen, row.clone(), None, err_value)?;
                    return Ok(Gate::TerminalDone);
                }
                return Err(WorkerError::transient(format!("resume flow load: {e}")));
            }
        };
        // §9.2 fingerprint hard gate: resuming a checkpoint against a changed
        // flow silently corrupts the run.
        let allow = task
            .data
            .get("allow_flow_hash_change")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        if !allow && !row.flow_hash.is_empty() && !def.hash.is_empty() && row.flow_hash != def.hash
        {
            let upgrade_suspected = !row.flow_hash_algo.is_empty()
                && !def.hash_algo.is_empty()
                && row.flow_hash_algo != def.hash_algo;
            eprintln!(
                "[worker] resume: flow definition changed since the checkpoint was written \
                 (execution_id={}, stored_flow_hash={}, current_flow_hash={})",
                task.execution_id, row.flow_hash, def.hash
            );
            let message = format!(
                "flow definition changed since execution {} started (stored={} current={}); \
                 resume refused (set allow_flow_hash_change=true on the carrier to override)",
                task.execution_id, row.flow_hash, def.hash
            );
            let err_value = terminal_error_state(
                "flow_hash_mismatch",
                FLOW_ERROR_CODE,
                &message,
                Some(json!({
                    "stored_flow_hash": row.flow_hash,
                    "current_flow_hash": def.hash,
                    "stored_flow_hash_algo": row.flow_hash_algo,
                    "current_flow_hash_algo": def.hash_algo,
                    "upgrade_suspected": upgrade_suspected,
                })),
                None,
            );
            self.persist_error_terminal(&tenant, fence_gen, row.clone(), None, err_value)?;
            return Ok(Gate::TerminalDone);
        }
        Ok(Gate::Ready(Box::new(def)))
    }

    // -- execution + classification (§9.5) -----------------------------------

    #[allow(clippy::too_many_arguments)]
    fn execute_and_finalize(
        &self,
        task: &Task,
        def: &FlowDefinition,
        handle: &LeaseHandle,
        mut state_row: ExecutionState,
        resume: Option<ResumeInput>,
        continuation: bool,
    ) -> Result<(), WorkerError> {
        let tenant = task.tenant_id.clone();
        let _exec_started = std::time::Instant::now();
        let history = RunHistory::new(&task.timestamp);
        if resume.is_some() || continuation {
            // A continuation of the same execution: carry the previous
            // session's node timings + timeline forward (§6.3).
            history.seed_from(&state_row);
        }
        let host = Host::with_execution_id(task.execution_id.clone())
            .with_tenant_id(tenant.clone())
            .with_subscriptions(
                self.subs.clone() as Arc<dyn plaita_engine::exec::SubscriptionRegistry>
            )
            .listen(history.clone().sink());

        // Register the in-flight run for the D2 cooperative cancel.
        {
            let mut active = self.active.lock().unwrap();
            *active = Some(ActiveRun { host: host.clone() });
        }

        // §9.6: the fresh-run fallback is observable.
        if continuation {
            let ev = history.record_timeline(
                "continue_fallback_start",
                json!({"reason": "no mid-flow checkpoint; re-entering at flow start"}),
            );
            self.publish_execution_event(&task.execution_id, &ev);
        }

        // Run (single-threaded occupancy of the consumer).
        let result = match &resume {
            Some(ri) => {
                // §9.2 step 3: rebuild via the engine entries ONLY — the
                // worker never forks recovery logic. The resume chain
                // re-issues the fresh session budget.
                let rc = match plaita_engine::RuntimeCtx::from_snapshot_json(&ri.checkpoint.context)
                {
                    Ok(rc) => rc.shared(),
                    Err(e) => {
                        let err_value = terminal_error_state(
                            "StateSnapshotError",
                            FLOW_ERROR_CODE,
                            &format!(
                                "checkpoint context for execution {} cannot be rebuilt: {e}",
                                task.execution_id
                            ),
                            None,
                            None,
                        );
                        let r = self.persist_error_terminal(
                            &tenant,
                            handle.fence_token,
                            state_row.clone(),
                            Some(&history),
                            err_value,
                        );
                        let _ = lease::release_with(self.backend.as_ref(), &self.cfg, handle);
                        self.active.lock().unwrap().take();
                        return r;
                    }
                };
                let suspended = plaita_engine::exec::SuspendedExecution::from_parts(
                    def.workflow.clone(),
                    rc,
                    host.clone(),
                    ri.checkpoint.suspended_node.clone(),
                );
                suspended.resume(ri.resume_kind, ri.data.clone())
            }
            None => def.workflow.execute(&task.params, &host),
        };
        self.active.lock().unwrap().take();

        let emit_result = |status: &str| {
            self.metrics.inc_counter(
                crate::metrics::METRIC_EXEC_RESULT,
                &[
                    ("tenant", tenant.as_str()),
                    ("flow", task.flow_id.as_str()),
                    ("status", status),
                ],
            );
        };

        // Classification order is the §9.5 priority (drain after cancel,
        // before carrier rejection / suspend / errors / completion).
        let cancel_hit = matches!(&result, Err(FlowError::Cancelled { .. }))
            || state::is_cancel_requested(
                self.backend.as_ref(),
                &self.cfg,
                &tenant,
                &task.execution_id,
            )
            .unwrap_or(None)
            .is_some();

        if cancel_hit && self.force_stopped() {
            // §9.3 D2: the step was abandoned by force-stop. Deliberately NO
            // terminal write — the execution stays resumable and the message
            // remains pending for another worker.
            let _ = lease::release_with(self.backend.as_ref(), &self.cfg, handle);
            eprintln!(
                "[worker] execution abandoned by drain force-stop; message left pending \
                 (execution_id={})",
                task.execution_id
            );
            return Err(WorkerError::new(
                ErrorKind::DrainAborted,
                format!(
                    "execution {} abandoned by drain force-stop",
                    task.execution_id
                ),
            ));
        }
        if cancel_hit {
            eprintln!(
                "[worker] execution cancelled (execution_id={})",
                task.execution_id
            );
            emit_result("cancelled");
            history.record_timeline("run_end", json!({"final": true}));
            history.apply_to_context(&mut state_row.context);
            let now = now_iso();
            state_row.status = state::STATUS_CANCELLED.to_string();
            state_row.end_time = now.clone();
            state_row.last_update_time = now;
            state_row.node_timings = Some(history.snapshot_node_timings());
            state_row.timeline = Some(history.snapshot_timeline());
            let r = state::save(
                self.backend.as_ref(),
                &self.cfg,
                &tenant,
                &state_row,
                handle.fence_token,
            )
            .map_err(|e| WorkerError::transient(format!("cancelled persist: {e}")));
            let _ = lease::release_with(self.backend.as_ref(), &self.cfg, handle);
            r?;
            self.delete_checkpoint(&tenant, &task.execution_id);
            return Ok(());
        }
        match result {
            Err(FlowError::ResumeGuard { message }) => {
                // Permanent rejection: the execution stays suspended; the
                // carrier is acked by process_one (#68).
                let _ = lease::release_with(self.backend.as_ref(), &self.cfg, handle);
                eprintln!(
                    "[worker] resume carrier rejected; execution stays suspended \
                     (execution_id={}, resume_type={}): {message}",
                    task.execution_id, task.resume_type
                );
                Err(WorkerError::new(ErrorKind::ResumeGuardRejected, message))
            }
            Err(FlowError::ResumeError { message }) => {
                // Possibly a transient resume-handler failure: the message
                // stays pending (§9.2 step 4 double classification).
                let _ = lease::release_with(self.backend.as_ref(), &self.cfg, handle);
                Err(WorkerError::transient(format!(
                    "resume node handler failed (execution_id={}): {message}",
                    task.execution_id
                )))
            }
            Err(FlowError::Suspended(suspended)) => {
                eprintln!(
                    "[worker] execution suspended (execution_id={}, node={})",
                    task.execution_id, suspended.suspended_node
                );
                emit_result("suspended");
                let r = self.persist_suspended(task, handle, &mut state_row, &history, &suspended);
                let _ = lease::release_with(self.backend.as_ref(), &self.cfg, handle);
                r
            }
            Err(engine_err) => {
                // M1: no node retry budgets — a failing node lands straight
                // in the normalized error terminal (§9.5 M1 posture).
                eprintln!(
                    "[worker] execution failed (execution_id={}): {engine_err}",
                    task.execution_id
                );
                emit_result("failed");
                history.apply_to_context(&mut state_row.context);
                let err_value = normalize_engine_error(&engine_err);
                let r = self.persist_error_terminal(
                    &tenant,
                    handle.fence_token,
                    state_row,
                    Some(&history),
                    err_value,
                );
                let _ = lease::release_with(self.backend.as_ref(), &self.cfg, handle);
                r
            }
            Ok(value) => {
                eprintln!(
                    "[worker] execution completed (execution_id={})",
                    task.execution_id
                );
                emit_result("completed");
                history.record_timeline("run_end", json!({"final": true, "result": value}));
                history.apply_to_context(&mut state_row.context);
                let now = now_iso();
                state_row.status = state::STATUS_COMPLETED.to_string();
                state_row.end_time = now.clone();
                state_row.last_update_time = now;
                state_row.node_timings = Some(history.snapshot_node_timings());
                state_row.timeline = Some(history.snapshot_timeline());
                let r = state::save(
                    self.backend.as_ref(),
                    &self.cfg,
                    &tenant,
                    &state_row,
                    handle.fence_token,
                )
                .map_err(|e| WorkerError::transient(format!("completed persist: {e}")));
                let _ = lease::release_with(self.backend.as_ref(), &self.cfg, handle);
                r?;
                self.delete_checkpoint(&tenant, &task.execution_id);
                Ok(())
            }
        }
    }

    /// The §7.3 suspend persistence order (all inside the lease, before the
    /// message ack): checkpoint save → suspended row write (with the
    /// dispatch credential in the SAME write) → RPUSH service task → clear
    /// credential → re-save. An RPUSH failure keeps the credential and the
    /// message pending (at-least-once dispatch; the compensation paths
    /// re-dispatch).
    fn persist_suspended(
        &self,
        task: &Task,
        handle: &LeaseHandle,
        state_row: &mut ExecutionState,
        history: &RunHistory,
        suspended: &plaita_engine::exec::SuspendedExecution,
    ) -> Result<(), WorkerError> {
        let tenant = task.tenant_id.clone();
        let cp = CheckpointRecord::suspend(
            &task.execution_id,
            &tenant,
            &task.flow_id,
            &suspended.suspended_node,
            suspended.checkpoint_json(),
        );
        state_row.status = state::STATUS_SUSPENDED.to_string();
        state_row.last_update_time = now_iso();
        state_row.end_time = String::new();
        state_row.context = cp.context.clone();
        state_row.error = None;
        state_row.node_timings = Some(history.snapshot_node_timings());
        state_row.timeline = Some(history.snapshot_timeline());
        // §9.4: the dispatch credential rides in the SAME write as the
        // suspended row (non-service suspensions carry none).
        let node_state = suspended.node_state();
        let svc_cfg = node_state
            .get("service_config")
            .cloned()
            .unwrap_or(Value::Null);
        let subtype = svc_cfg
            .get("type")
            .and_then(|t| t.as_str())
            .unwrap_or("")
            .to_string();
        let has_credential = !svc_cfg.is_null() && !subtype.is_empty();
        if has_credential {
            state_row.dispatch_pending = Some(json!({
                "pending": true,
                "service_type": subtype,
                "service_config": svc_cfg,
                "node_id": suspended.suspended_node,
                "recorded_at": now_iso(),
            }));
        } else if let Some(o) = node_state.as_object() {
            if o.contains_key("service_config") {
                // service_config present without a type: named warning, no
                // credential (nothing to dispatch to).
                eprintln!(
                    "[worker] suspend dispatch credential has no service type; no credential \
                     recorded (execution_id={})",
                    task.execution_id
                );
            }
        }
        checkpoint::save(self.backend.as_ref(), &self.cfg, &cp)?;
        state::save(
            self.backend.as_ref(),
            &self.cfg,
            &tenant,
            state_row,
            handle.fence_token,
        )
        .map_err(|e| WorkerError::transient(format!("suspended persist: {e}")))?;
        if !has_credential {
            return Ok(());
        }
        self.push_pending_service_task(&tenant, &task.execution_id, state_row, handle.fence_token)
    }

    /// RPUSH `{prefix}:{subtype}:queue` then clear the credential (§9.4).
    fn push_pending_service_task(
        &self,
        tenant: &str,
        execution_id: &str,
        state_row: &mut ExecutionState,
        fence_gen: i64,
    ) -> Result<(), WorkerError> {
        let Some(marker) = pending_suspend_dispatch(state_row) else {
            return Ok(());
        };
        let svc_cfg = marker.get("service_config").cloned().unwrap_or(Value::Null);
        let mut subtype = marker
            .get("service_type")
            .and_then(|s| s.as_str())
            .unwrap_or("")
            .to_string();
        if subtype.is_empty() {
            subtype = svc_cfg
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("")
                .to_string();
        }
        if subtype.is_empty() {
            eprintln!(
                "[worker] suspend dispatch credential has no service type; clearing it \
                 (execution_id={execution_id})"
            );
        } else {
            // An RPUSH failure must NOT ack upstream: the message is the
            // only carrier that can retry the dispatch; the credential
            // stays pending.
            self.rpush_service_task(tenant, execution_id, &subtype, &svc_cfg)?;
        }
        state_row.dispatch_pending = None;
        if let Err(e) = state::save(
            self.backend.as_ref(),
            &self.cfg,
            tenant,
            state_row,
            fence_gen,
        ) {
            eprintln!(
                "[worker] suspend dispatch succeeded but clearing the credential failed; a \
                 later carrier will re-dispatch (execution_id={execution_id}): {e}"
            );
            return Err(WorkerError::transient(format!("credential clear: {e}")));
        }
        Ok(())
    }

    /// Builds the service task body (service_config + execution_id +
    /// tenant_id, empty tenant normalized to default) and RPUSHes it.
    fn rpush_service_task(
        &self,
        tenant: &str,
        execution_id: &str,
        subtype: &str,
        svc_cfg: &Value,
    ) -> Result<(), WorkerError> {
        let tenant_norm = if tenant.is_empty() {
            self.cfg.default_tenant_id.clone()
        } else {
            tenant.to_string()
        };
        let mut body = match svc_cfg {
            Value::Object(o) => o.clone(),
            _ => serde_json::Map::new(),
        };
        body.entry("execution_id".to_string())
            .or_insert_with(|| json!(execution_id));
        body.entry("tenant_id".to_string())
            .or_insert_with(|| json!(tenant_norm));
        let queue_key = keys::service_queue_key(&self.cfg, subtype);
        if let Err(e) = self
            .backend
            .rpush(&queue_key, &Value::Object(body).to_string())
        {
            eprintln!(
                "[worker] suspend service dispatch failed; message will be left pending \
                 (execution_id={execution_id}, queue={queue_key}): {e}"
            );
            return Err(WorkerError::transient(format!(
                "suspend dispatch rpush: {e}"
            )));
        }
        eprintln!(
            "[worker] suspend service task dispatched (execution_id={execution_id}, queue={queue_key})"
        );
        Ok(())
    }

    /// Re-dispatches a pending credential for the suspended+continue
    /// short-circuit under a short "dispatch" lease (§9.4): the lease
    /// protects the credential-clear write from clobbering a concurrent
    /// advance; a lease held by another worker means IT owns the re-dispatch.
    fn compensate_suspend_dispatch(
        &self,
        task: &Task,
        state_row: &ExecutionState,
    ) -> Result<(), WorkerError> {
        if pending_suspend_dispatch(state_row).is_none() {
            return Ok(());
        }
        let holder = lease::new_holder_token("dispatch", &self.cfg.instance_id);
        let handle = match lease::acquire(
            self.backend.as_ref(),
            &self.cfg,
            &task.tenant_id,
            &task.execution_id,
            &holder,
            self.cfg.lease_ttl_seconds,
        ) {
            Ok(h) => h,
            Err(e) if e.kind() == ErrorKind::LeaseConflict => {
                eprintln!(
                    "[worker] suspend dispatch compensation skipped: execution lease held by \
                     another worker (it will re-dispatch; execution_id={})",
                    task.execution_id
                );
                return Ok(());
            }
            Err(e) => {
                return Err(WorkerError::transient(format!(
                    "suspend dispatch lease: {e}"
                )))
            }
        };
        let fresh = state::load(
            self.backend.as_ref(),
            &self.cfg,
            &task.tenant_id,
            &task.execution_id,
        )?;
        let r = match fresh {
            Some(f) if f.status == state::STATUS_SUSPENDED => self
                .dispatch_pending_service(task, &f, handle.fence_token)
                .map(|_| ()),
            _ => Ok(()),
        };
        let _ = lease::release_with(self.backend.as_ref(), &self.cfg, &handle);
        r
    }

    fn dispatch_pending_service(
        &self,
        task: &Task,
        state_row: &ExecutionState,
        fence_gen: i64,
    ) -> Result<bool, WorkerError> {
        if pending_suspend_dispatch(state_row).is_none() {
            return Ok(false);
        }
        let mut row = state_row.clone();
        self.push_pending_service_task(&task.tenant_id, &task.execution_id, &mut row, fence_gen)?;
        Ok(true)
    }

    fn delete_checkpoint(&self, tenant: &str, execution_id: &str) {
        // The state row is authoritative; a cleanup failure only warns (the
        // terminal row bounds the execution's lifecycle either way).
        if let Err(e) = checkpoint::delete(self.backend.as_ref(), &self.cfg, tenant, execution_id) {
            eprintln!("[worker] checkpoint cleanup failed (execution_id={execution_id}): {e}");
        }
    }

    /// Writes the normalized error terminal (§9.5) + checkpoint cleanup.
    fn persist_error_terminal(
        &self,
        tenant: &str,
        fence_gen: i64,
        mut state_row: ExecutionState,
        history: Option<&RunHistory>,
        error: Value,
    ) -> Result<(), WorkerError> {
        if let Some(h) = history {
            h.record_timeline("run_end", json!({"final": true}));
            state_row.node_timings = Some(h.snapshot_node_timings());
            state_row.timeline = Some(h.snapshot_timeline());
        }
        state_row.status = state::STATUS_ERROR.to_string();
        state_row.end_time = now_iso();
        state_row.last_update_time = state_row.end_time.clone();
        state_row.error = Some(error);
        state::save(
            self.backend.as_ref(),
            &self.cfg,
            tenant,
            &state_row,
            fence_gen,
        )
        .map_err(|e| WorkerError::transient(format!("error terminal persist: {e}")))?;
        self.delete_checkpoint(tenant, &state_row.execution_id);
        Ok(())
    }

    /// Worker is the only publisher of the execution timeline channel
    /// (§6.3).
    fn publish_execution_event(&self, execution_id: &str, ev: &Value) {
        let channel = keys::execution_events_channel(&self.cfg, execution_id);
        let _ = self.backend.publish(&channel, &ev.to_string());
    }
}

fn pending_suspend_dispatch(state_row: &ExecutionState) -> Option<&Value> {
    let marker = state_row.dispatch_pending.as_ref()?;
    if marker.get("pending").and_then(|p| p.as_bool()) != Some(true) {
        return None;
    }
    Some(marker)
}

/// The rs error normalization (§9.5): Handler keeps its code + "Handler"
/// type; Node records the node error code + failed_node; FlowTimeout/Other
/// fall back to -500 with the variant name as error_type.
fn normalize_engine_error(err: &FlowError) -> Value {
    match err {
        FlowError::Handler { code, .. } => {
            terminal_error_state("Handler", *code, &err.to_string(), None, None)
        }
        FlowError::Node { node_id, error, .. } => {
            terminal_error_state("Node", error.code, &err.to_string(), None, Some(node_id))
        }
        FlowError::FlowTimeout { .. } => {
            terminal_error_state("FlowTimeout", FLOW_ERROR_CODE, &err.to_string(), None, None)
        }
        FlowError::Other(_) => {
            terminal_error_state("Other", FLOW_ERROR_CODE, &err.to_string(), None, None)
        }
        FlowError::Cancelled { .. } => terminal_error_state(
            "Cancelled",
            crate::error::CANCELLED_ERROR_CODE,
            &err.to_string(),
            None,
            None,
        ),
        FlowError::Suspended(_) | FlowError::ResumeGuard { .. } | FlowError::ResumeError { .. } => {
            terminal_error_state("Other", FLOW_ERROR_CODE, &err.to_string(), None, None)
        }
    }
}
