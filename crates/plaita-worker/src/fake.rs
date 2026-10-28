//! The in-memory Redis backend: a faithful-enough protocol fake so the
//! worker's behavior tests run with no Redis daemon (rs-worker-design §13.3
//! decision: memory fake + #[ignore] real-Redis integration leg).
//!
//! What is pinned (protocol semantics the worker depends on):
//! - streams: id-ordered entries, `ms-seq` id format, XGROUP CREATE
//!   id=0/MKSTREAM + BUSYGROUP, XREADGROUP `>` advancing last-delivered-id,
//!   per-group PEL with consumer/delivery-count/idle tracking, XCLAIM
//!   min-idle-time + delivery-count increment + ownership transfer, XACK,
//!   XDEL (which also removes PEL rows — Redis semantics), XRANGE bounds,
//!   XTRIM exact, XINFO GROUPS;
//! - strings/sets/zsets/lists/hashes with per-key TTL (lazy expiry);
//! - the four Lua scripts' semantics (fenced acquire / CAS release / CAS
//!   renew / fenced CAS state save) — the production backend runs the
//!   verbatim script texts, this fake implements the identical behavior;
//! - Pub/Sub pattern delivery with `*` wildcards.
//!
//! Deliberately NOT modeled: blocking XREADGROUP (the fake returns
//! immediately — the consume loop's stop-flag check covers shutdown),
//! persistence, clustering, transactions racing other threads (single
//! Mutex; the fake is deterministic under sequential test drivers).

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::backend::{
    BackendError, BackendResult, GroupInfo, PendingEntry, PsubStream, RedisBackend, StreamEntry,
    TxOp,
};

type Result_<T> = BackendResult<T>;

pub fn io_err<T>(msg: impl Into<String>) -> Result_<T> {
    Err(BackendError::Io(msg.into()))
}

/// Wall-clock helpers (epoch ms for stream ids / ms fields).
fn epoch_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn parse_stream_id(id: &str) -> Option<(u64, u64)> {
    let (ms, seq) = match id.split_once('-') {
        Some((a, b)) => (a, Some(b)),
        None => (id, None),
    };
    let ms: u64 = ms.parse().ok()?;
    let seq: u64 = match seq {
        Some(b) => b.parse().ok()?,
        None => 0,
    };
    Some((ms, seq))
}

#[derive(Debug)]
struct Entry<T> {
    value: T,
    expiry: Option<Instant>,
}

impl<T> Entry<T> {
    fn new(value: T) -> Self {
        Entry {
            value,
            expiry: None,
        }
    }
}

#[derive(Debug, Default)]
struct StreamData {
    entries: Vec<StreamEntry>,
    groups: HashMap<String, GroupState>,
    last_ms: u64,
    seq: u64,
}

impl StreamData {
    fn next_id(&mut self) -> String {
        let ms = epoch_ms();
        if ms == self.last_ms {
            self.seq += 1;
        } else {
            self.last_ms = ms;
            self.seq = 0;
        }
        format!("{}-{}", ms, self.seq)
    }
}

#[derive(Debug)]
struct GroupState {
    last_delivered: (u64, u64),
    pel: Vec<PelRow>,
}

#[derive(Debug)]
struct PelRow {
    id: (u64, u64),
    consumer: String,
    delivery_count: i64,
    delivered_at: Instant,
}

impl GroupState {
    fn pel_find(&mut self, id: (u64, u64)) -> Option<&mut PelRow> {
        self.pel.iter_mut().find(|p| p.id == id)
    }
}

#[derive(Default)]
struct Store {
    strings: HashMap<String, Entry<String>>,
    sets: HashMap<String, Entry<HashSet<String>>>,
    zsets: HashMap<String, Entry<HashMap<String, f64>>>,
    lists: HashMap<String, Entry<Vec<String>>>,
    hashes: HashMap<String, Entry<HashMap<String, String>>>,
    streams: HashMap<String, Entry<StreamData>>,
}

/// The in-memory backend. Cheap to construct; share by Arc across threads.
pub struct FakeRedis {
    store: Mutex<Store>,
    subs: Mutex<Vec<Subscription>>,
    /// Fault injection for tests: the next N rpush calls fail (pins the
    /// park-on-dispatch-failure semantics).
    rpush_failures: AtomicUsize,
    /// Fault injection for tests: the next N XADD calls fail (pins the
    /// enqueue-failure branches — the reconcile round-void semantics).
    xadd_failures: AtomicUsize,
    /// Fault injection for tests: the next N INCR calls fail (pins the
    /// handover-counter-unavailable fail-closed branch — the production
    /// analogue is WRONGTYPE key pollution, flow_worker.py._record_affinity_
    /// handover 的部署阻断级洞).
    incr_failures: AtomicUsize,
    /// Fault injection for tests: (key prefix, remaining failures) queue for
    /// GET — pins the probe-failure/fail-closed branches.
    get_failures: Mutex<Vec<(String, usize)>>,
}

/// Shared queue + wake condvar for one fake Pub/Sub subscription.
type PsubQueue = Arc<(Mutex<VecDeque<(String, String)>>, Condvar)>;

struct Subscription {
    pattern: String,
    queue: PsubQueue,
}

impl Default for FakeRedis {
    fn default() -> Self {
        FakeRedis::new()
    }
}

impl FakeRedis {
    pub fn new() -> FakeRedis {
        FakeRedis {
            store: Mutex::new(Store::default()),
            subs: Mutex::new(Vec::new()),
            rpush_failures: AtomicUsize::new(0),
            xadd_failures: AtomicUsize::new(0),
            incr_failures: AtomicUsize::new(0),
            get_failures: Mutex::new(Vec::new()),
        }
    }

    /// Direct inspection hook for tests (raw stream entry lookup).
    pub fn stream_entry(&self, key: &str, id: &str) -> Option<StreamEntry> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        store
            .streams
            .get(key)
            .and_then(|e| e.value.entries.iter().find(|e| e.id == id))
            .cloned()
    }

    /// Test hook: the next `times` RPUSH calls fail (fault injection).
    pub fn fail_next_rpush(&self, times: usize) {
        self.rpush_failures
            .store(times, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test hook: the next `times` XADD calls fail (fault injection).
    pub fn fail_next_xadd(&self, times: usize) {
        self.xadd_failures
            .store(times, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test hook: the next `times` INCR calls fail (fault injection — the
    /// production analogue is a WRONGTYPE-polluted counter key).
    pub fn fail_next_incr(&self, times: usize) {
        self.incr_failures
            .store(times, std::sync::atomic::Ordering::SeqCst);
    }

    /// Test hook: the next `times` GET calls whose key starts with `prefix`
    /// fail (fault injection — pins the probe-failure/fail-closed branches).
    pub fn fail_next_get(&self, prefix: String, times: usize) {
        if times > 0 {
            self.get_failures.lock().unwrap().push((prefix, times));
        }
    }

    /// Test hook: XADD under a fixed id (residue-crafting in tests).
    pub fn xadd_with_id(
        &self,
        key: &str,
        id: &str,
        fields: &[(String, String)],
    ) -> Result_<String> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let e = store
            .streams
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(StreamData::default()));
        e.value.entries.push(StreamEntry {
            id: id.to_string(),
            fields: fields.to_vec(),
        });
        e.value
            .entries
            .sort_by_key(|entry| parse_stream_id(&entry.id).unwrap_or((0, 0)));
        Ok(id.to_string())
    }

    /// Direct TTL probe for tests (None = missing; Some(false) = no TTL).
    pub fn ttl_ms(&self, key: &str) -> Option<Option<i64>> {
        let store = self.store.lock().unwrap();
        Self::find_ttl(&store, key)
    }

    fn find_ttl(store: &Store, key: &str) -> Option<Option<i64>> {
        fn read<T>(e: &Entry<T>) -> Option<i64> {
            e.expiry
                .map(|x| x.duration_since(Instant::now()).as_millis() as i64)
        }
        fn table_ttl<T>(t: &HashMap<String, Entry<T>>, key: &str) -> Option<Option<i64>> {
            t.get(key).map(read)
        }
        table_ttl(&store.strings, key)
            .or_else(|| table_ttl(&store.sets, key))
            .or_else(|| table_ttl(&store.zsets, key))
            .or_else(|| table_ttl(&store.lists, key))
            .or_else(|| table_ttl(&store.hashes, key))
            .or_else(|| table_ttl(&store.streams, key))
    }

    /// Removes the key when its TTL elapsed (lazy expiry, Redis-like).
    fn gc_key(store: &mut Store, key: &str) {
        fn expired<T>(e: &Entry<T>) -> bool {
            e.expiry.is_some_and(|x| Instant::now() >= x)
        }
        if store.strings.get(key).is_some_and(&expired) {
            store.strings.remove(key);
        }
        if store.sets.get(key).is_some_and(&expired) {
            store.sets.remove(key);
        }
        if store.zsets.get(key).is_some_and(&expired) {
            store.zsets.remove(key);
        }
        if store.lists.get(key).is_some_and(&expired) {
            store.lists.remove(key);
        }
        if store.hashes.get(key).is_some_and(&expired) {
            store.hashes.remove(key);
        }
        if store.streams.get(key).is_some_and(&expired) {
            store.streams.remove(key);
        }
    }

    fn string_get(store: &mut Store, key: &str) -> Option<String> {
        Self::gc_key(store, key);
        store.strings.get(key).map(|e| e.value.clone())
    }

    fn apply_tx_op(store: &mut Store, op: &TxOp) {
        match op {
            TxOp::Set {
                key,
                value,
                ttl_secs,
            } => {
                let expiry =
                    ttl_secs.map(|s| Instant::now() + Duration::from_secs(s.max(0) as u64));
                store.strings.insert(
                    key.clone(),
                    Entry {
                        value: value.clone(),
                        expiry,
                    },
                );
            }
            TxOp::Sadd { key, member } => {
                let e = store
                    .sets
                    .entry(key.clone())
                    .or_insert_with(|| Entry::new(HashSet::new()));
                e.value.insert(member.clone());
            }
            TxOp::Srem { key, member } => {
                if let Some(e) = store.sets.get_mut(key) {
                    e.value.remove(member);
                }
            }
            TxOp::Expire { key, ttl_secs } => {
                let deadline = Instant::now() + Duration::from_secs((*ttl_secs).max(0) as u64);
                // Expire applies to whichever table holds the key.
                if let Some(e) = store.strings.get_mut(key) {
                    e.expiry = Some(deadline);
                } else if let Some(e) = store.sets.get_mut(key) {
                    e.expiry = Some(deadline);
                } else if let Some(e) = store.zsets.get_mut(key) {
                    e.expiry = Some(deadline);
                } else if let Some(e) = store.lists.get_mut(key) {
                    e.expiry = Some(deadline);
                } else if let Some(e) = store.hashes.get_mut(key) {
                    e.expiry = Some(deadline);
                } else if let Some(e) = store.streams.get_mut(key) {
                    e.expiry = Some(deadline);
                }
            }
            TxOp::Zadd { key, score, member } => {
                let e = store
                    .zsets
                    .entry(key.clone())
                    .or_insert_with(|| Entry::new(HashMap::new()));
                e.value.insert(member.clone(), *score);
            }
            TxOp::Zrem { key, member } => {
                if let Some(e) = store.zsets.get_mut(key) {
                    e.value.remove(member);
                }
            }
            TxOp::Rpush { key, value } => {
                let e = store
                    .lists
                    .entry(key.clone())
                    .or_insert_with(|| Entry::new(Vec::new()));
                e.value.push(value.clone());
            }
            TxOp::Hdel { key, field } => {
                if let Some(e) = store.hashes.get_mut(key) {
                    e.value.remove(field);
                }
            }
            TxOp::Del { key } => {
                store.strings.remove(key);
                store.sets.remove(key);
                store.zsets.remove(key);
                store.lists.remove(key);
                store.hashes.remove(key);
                store.streams.remove(key);
            }
        }
    }
}

impl RedisBackend for FakeRedis {
    fn get(&self, key: &str) -> Result_<Option<String>> {
        {
            let mut failures = self.get_failures.lock().unwrap();
            if let Some(pos) = failures
                .iter()
                .position(|(prefix, _)| key.starts_with(prefix.as_str()))
            {
                failures[pos].1 -= 1;
                if failures[pos].1 == 0 {
                    failures.remove(pos);
                }
                drop(failures);
                return io_err(format!("injected get failure ({key})"));
            }
        }
        let mut store = self.store.lock().unwrap();
        Ok(Self::string_get(&mut store, key))
    }

    fn scan_match(&self, pattern: &str) -> Result_<Vec<String>> {
        let store = self.store.lock().unwrap();
        let mut keys: Vec<String> = Vec::new();
        keys.extend(
            store
                .strings
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .filter(|k| glob_match(pattern, k)),
        );
        keys.extend(
            store
                .sets
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .filter(|k| glob_match(pattern, k)),
        );
        keys.extend(
            store
                .zsets
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .filter(|k| glob_match(pattern, k)),
        );
        keys.extend(
            store
                .lists
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .filter(|k| glob_match(pattern, k)),
        );
        keys.extend(
            store
                .hashes
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .filter(|k| glob_match(pattern, k)),
        );
        keys.extend(
            store
                .streams
                .keys()
                .cloned()
                .collect::<Vec<_>>()
                .into_iter()
                .filter(|k| glob_match(pattern, k)),
        );
        keys.sort();
        Ok(keys)
    }

    fn set(&self, key: &str, value: &str, ttl_secs: Option<i64>) -> Result_<()> {
        let mut store = self.store.lock().unwrap();
        let expiry = ttl_secs.map(|s| Instant::now() + Duration::from_secs(s.max(0) as u64));
        store.strings.insert(
            key.to_string(),
            Entry {
                value: value.to_string(),
                expiry,
            },
        );
        Ok(())
    }

    fn set_nx(&self, key: &str, value: &str, ttl_secs: i64) -> Result_<bool> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        if store.strings.contains_key(key) {
            return Ok(false);
        }
        let expiry = Some(Instant::now() + Duration::from_secs(ttl_secs.max(0) as u64));
        store.strings.insert(
            key.to_string(),
            Entry {
                value: value.to_string(),
                expiry,
            },
        );
        Ok(true)
    }

    fn del(&self, keys: &[&str]) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        let mut n = 0;
        for key in keys {
            Self::gc_key(&mut store, key);
            if store.strings.remove(*key).is_some()
                || store.sets.remove(*key).is_some()
                || store.zsets.remove(*key).is_some()
                || store.lists.remove(*key).is_some()
                || store.hashes.remove(*key).is_some()
                || store.streams.remove(*key).is_some()
            {
                n += 1;
            }
        }
        Ok(n)
    }

    fn expire(&self, key: &str, ttl_secs: i64) -> Result_<bool> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let deadline = Instant::now() + Duration::from_secs(ttl_secs.max(0) as u64);
        let mut found = false;
        if let Some(e) = store.strings.get_mut(key) {
            e.expiry = Some(deadline);
            found = true;
        } else if let Some(e) = store.sets.get_mut(key) {
            e.expiry = Some(deadline);
            found = true;
        } else if let Some(e) = store.zsets.get_mut(key) {
            e.expiry = Some(deadline);
            found = true;
        } else if let Some(e) = store.lists.get_mut(key) {
            e.expiry = Some(deadline);
            found = true;
        } else if let Some(e) = store.hashes.get_mut(key) {
            e.expiry = Some(deadline);
            found = true;
        } else if let Some(e) = store.streams.get_mut(key) {
            e.expiry = Some(deadline);
            found = true;
        }
        Ok(found)
    }

    fn incr(&self, key: &str) -> Result_<i64> {
        self.incr_by(key, 1)
    }

    fn incr_by(&self, key: &str, delta: i64) -> Result_<i64> {
        if self
            .incr_failures
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| if n > 0 { Some(n - 1) } else { None },
            )
            .is_ok()
        {
            return io_err(format!("injected incr failure ({key})"));
        }
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let current: i64 = store
            .strings
            .get(key)
            .and_then(|e| e.value.parse().ok())
            .unwrap_or(0);
        let next = current + delta;
        let entry = store
            .strings
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(String::new()));
        entry.value = next.to_string();
        Ok(next)
    }

    fn hincr_by(&self, key: &str, field: &str, delta: i64) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let e = store
            .hashes
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(HashMap::new()));
        let current: i64 = e.value.get(field).and_then(|v| v.parse().ok()).unwrap_or(0);
        let next = current + delta;
        e.value.insert(field.to_string(), next.to_string());
        Ok(next)
    }

    fn hdel(&self, key: &str, field: &str) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let mut n = 0;
        if let Some(e) = store.hashes.get_mut(key) {
            if e.value.remove(field).is_some() {
                n = 1;
            }
        }
        Ok(n)
    }

    fn hget(&self, key: &str, field: &str) -> Result_<Option<String>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        Ok(store
            .hashes
            .get(key)
            .and_then(|e| e.value.get(field).cloned()))
    }

    fn hset(&self, key: &str, field: &str, value: &str) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let e = store
            .hashes
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(HashMap::new()));
        let inserted = if e.value.contains_key(field) { 0 } else { 1 };
        e.value.insert(field.to_string(), value.to_string());
        Ok(inserted)
    }

    fn sadd(&self, key: &str, member: &str) -> Result_<()> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let e = store
            .sets
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(HashSet::new()));
        e.value.insert(member.to_string());
        Ok(())
    }

    fn srem(&self, key: &str, member: &str) -> Result_<()> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        if let Some(e) = store.sets.get_mut(key) {
            e.value.remove(member);
        }
        Ok(())
    }

    fn smembers(&self, key: &str) -> Result_<Vec<String>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        Ok(store
            .sets
            .get(key)
            .map(|e| {
                let mut v: Vec<String> = e.value.iter().cloned().collect();
                v.sort();
                v
            })
            .unwrap_or_default())
    }

    fn zadd(&self, key: &str, score: f64, member: &str) -> Result_<()> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let e = store
            .zsets
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(HashMap::new()));
        e.value.insert(member.to_string(), score);
        Ok(())
    }

    fn zrem(&self, key: &str, member: &str) -> Result_<()> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        if let Some(e) = store.zsets.get_mut(key) {
            e.value.remove(member);
        }
        Ok(())
    }

    fn zrangebyscore(&self, key: &str, min: f64, max: f64, count: i64) -> Result_<Vec<String>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let mut rows: Vec<(f64, String)> = store
            .zsets
            .get(key)
            .map(|e| {
                e.value
                    .iter()
                    .filter(|(_, s)| **s >= min && **s <= max)
                    .map(|(m, s)| (*s, m.clone()))
                    .collect()
            })
            .unwrap_or_default();
        rows.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(a.1.cmp(&b.1)));
        Ok(rows
            .into_iter()
            .take(count.max(0) as usize)
            .map(|(_, m)| m)
            .collect())
    }

    fn rpush(&self, key: &str, value: &str) -> Result_<()> {
        if self
            .rpush_failures
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| {
                    if n == 0 {
                        None
                    } else {
                        Some(n - 1)
                    }
                },
            )
            .is_ok()
        {
            return io_err(format!("injected rpush failure ({key})"));
        }
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let e = store
            .lists
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(Vec::new()));
        e.value.push(value.to_string());
        Ok(())
    }

    fn lrange(&self, key: &str, start: i64, stop: i64) -> Result_<Vec<String>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let empty: Vec<String> = Vec::new();
        let list = store
            .lists
            .get(key)
            .map(|e| e.value.clone())
            .unwrap_or(empty);
        let len = list.len() as i64;
        let start = if start < 0 {
            (len + start).max(0)
        } else {
            start.min(len)
        };
        let stop = if stop < 0 {
            (len + stop + 1).max(0)
        } else {
            (stop + 1).min(len)
        };
        if start >= stop {
            return Ok(Vec::new());
        }
        Ok(list[start as usize..stop as usize].to_vec())
    }

    fn hmget(&self, key: &str, fields: &[&str]) -> Result_<Vec<Option<String>>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let empty: HashMap<String, String> = HashMap::new();
        let map = store
            .hashes
            .get(key)
            .map(|e| e.value.clone())
            .unwrap_or(empty);
        Ok(fields.iter().map(|f| map.get(*f).cloned()).collect())
    }

    fn blpop(&self, key: &str, _timeout_secs: i64) -> Result_<Option<(String, String)>> {
        // The fake is non-blocking: pop immediately or report the timeout
        // (tests drive timing through the caller, not the block).
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(entry) = store.lists.get_mut(key) else {
            return Ok(None);
        };
        if entry.value.is_empty() {
            return Ok(None);
        }
        let value = entry.value.remove(0);
        Ok(Some((key.to_string(), value)))
    }

    fn xread(&self, key: &str, last_id: &str, block_ms: i64) -> Result_<Vec<StreamEntry>> {
        let _ = block_ms;
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(entry) = store.streams.get(key) else {
            return Ok(Vec::new());
        };
        // StreamData: BTreeMap<id, fields> in insertion-compatible order.
        let from = if last_id == "$" {
            // "$" = only-new semantics: the fake cannot know "new", so
            // callers under test use explicit ids.
            return Ok(Vec::new());
        } else {
            last_id.to_string()
        };
        Ok(entry
            .value
            .entries
            .iter()
            .filter(|e| e.id > from)
            .map(|e| StreamEntry {
                id: e.id.clone(),
                fields: e.fields.clone(),
            })
            .collect())
    }

    fn lrem(&self, key: &str, count: i64, value: &str) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(entry) = store.lists.get_mut(key) else {
            return Ok(0);
        };
        let mut removed = 0i64;
        if count >= 0 {
            // head→tail; count 0 = all occurrences (Redis semantics)
            let mut i = 0usize;
            while i < entry.value.len() && (count == 0 || removed < count) {
                if entry.value[i] == value {
                    entry.value.remove(i);
                    removed += 1;
                } else {
                    i += 1;
                }
            }
        } else {
            // tail→head
            let mut i = entry.value.len();
            while i > 0 && removed < -count {
                i -= 1;
                if entry.value[i] == value {
                    entry.value.remove(i);
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    fn publish(&self, channel: &str, payload: &str) -> Result_<i64> {
        let subs = self.subs.lock().unwrap();
        let mut delivered = 0i64;
        for sub in subs.iter() {
            if glob_match(&sub.pattern, channel) {
                let (queue, cv) = &*sub.queue;
                queue
                    .lock()
                    .unwrap()
                    .push_back((channel.to_string(), payload.to_string()));
                cv.notify_all();
                delivered += 1;
            }
        }
        Ok(delivered)
    }

    fn psubscribe(&self, pattern: &str) -> Result_<Box<dyn PsubStream>> {
        let sub = Subscription {
            pattern: pattern.to_string(),
            queue: Arc::new((Mutex::new(VecDeque::new()), Condvar::new())),
        };
        let stream = FakePsubStream {
            queue: sub.queue.clone(),
        };
        self.subs.lock().unwrap().push(sub);
        Ok(Box::new(stream))
    }

    fn xadd(&self, key: &str, fields: &[(String, String)]) -> Result_<String> {
        if self
            .xadd_failures
            .fetch_update(
                std::sync::atomic::Ordering::SeqCst,
                std::sync::atomic::Ordering::SeqCst,
                |n| if n > 0 { Some(n - 1) } else { None },
            )
            .is_ok()
        {
            return io_err(format!("injected xadd failure ({key})"));
        }
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let e = store
            .streams
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(StreamData::default()));
        let id = e.value.next_id();
        e.value.entries.push(StreamEntry {
            id: id.clone(),
            fields: fields.to_vec(),
        });
        Ok(id)
    }

    fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<i64>,
    ) -> Result_<Vec<StreamEntry>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(e) = store.streams.get(key) else {
            return Ok(Vec::new());
        };
        let lo = if start == "-" {
            (0, 0)
        } else {
            parse_stream_id(start).unwrap_or((0, 0))
        };
        let hi = if end == "+" {
            (u64::MAX, u64::MAX)
        } else {
            parse_stream_id(end).unwrap_or((u64::MAX, u64::MAX))
        };
        Ok(e.value
            .entries
            .iter()
            .filter(|entry| {
                let id = parse_stream_id(&entry.id).unwrap_or((0, 0));
                id >= lo && id <= hi
            })
            .take(count.map(|c| c.max(0) as usize).unwrap_or(usize::MAX))
            .cloned()
            .collect())
    }

    fn xlen(&self, key: &str) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        Ok(store
            .streams
            .get(key)
            .map(|e| e.value.entries.len() as i64)
            .unwrap_or(0))
    }

    fn xgroup_create_mkstream(&self, key: &str, group: &str, id: &str) -> Result_<()> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let e = store
            .streams
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(StreamData::default()));
        if e.value.groups.contains_key(group) {
            return Err(BackendError::BusyGroup(format!(
                "BUSYGROUP Consumer Group name already exists ({key}/{group})"
            )));
        }
        let last_delivered = parse_stream_id(id).unwrap_or((0, 0));
        e.value.groups.insert(
            group.to_string(),
            GroupState {
                last_delivered,
                pel: Vec::new(),
            },
        );
        Ok(())
    }

    fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        key: &str,
        count: i64,
        _block_ms: Option<i64>,
    ) -> Result_<Vec<StreamEntry>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(e) = store.streams.get_mut(key) else {
            return Err(BackendError::Io(format!("NOGROUP no such key ({key})")));
        };
        let Some(g) = e.value.groups.get_mut(group) else {
            return Err(BackendError::Io(format!(
                "NOGROUP no such consumer group ({key}/{group})"
            )));
        };
        let mut out = Vec::new();
        for entry in e.value.entries.iter() {
            if out.len() >= count.max(0) as usize {
                break;
            }
            let id = parse_stream_id(&entry.id).unwrap_or((0, 0));
            if id <= g.last_delivered {
                continue;
            }
            if g.pel.iter().any(|p| p.id == id) {
                continue; // already delivered inside this group
            }
            g.pel.push(PelRow {
                id,
                consumer: consumer.to_string(),
                delivery_count: 1,
                delivered_at: Instant::now(),
            });
            g.last_delivered = id;
            out.push(entry.clone());
        }
        Ok(out)
    }

    fn xpending_ext(
        &self,
        key: &str,
        group: &str,
        start: &str,
        end: &str,
        count: i64,
    ) -> Result_<Vec<PendingEntry>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(e) = store.streams.get(key) else {
            return Ok(Vec::new());
        };
        let Some(g) = e.value.groups.get(group) else {
            return Ok(Vec::new());
        };
        let lo = if start == "-" {
            (0, 0)
        } else {
            parse_stream_id(start).unwrap_or((0, 0))
        };
        let hi = if end == "+" {
            (u64::MAX, u64::MAX)
        } else {
            parse_stream_id(end).unwrap_or((u64::MAX, u64::MAX))
        };
        let now = Instant::now();
        let mut rows: Vec<PendingEntry> = g
            .pel
            .iter()
            .filter(|p| p.id >= lo && p.id <= hi)
            .map(|p| PendingEntry {
                id: format!("{}-{}", p.id.0, p.id.1),
                consumer: p.consumer.clone(),
                idle_ms: now.duration_since(p.delivered_at).as_millis() as i64,
                delivery_count: p.delivery_count,
            })
            .collect();
        rows.sort_by_key(|r| parse_stream_id(&r.id).unwrap_or((0, 0)));
        rows.truncate(count.max(0) as usize);
        Ok(rows)
    }

    fn xclaim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: i64,
        ids: &[&str],
    ) -> Result_<Vec<StreamEntry>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let now = Instant::now();
        let Some(e) = store.streams.get_mut(key) else {
            return Ok(Vec::new());
        };
        let Some(g) = e.value.groups.get_mut(group) else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        for id in ids {
            let Some(pid) = parse_stream_id(id) else {
                continue;
            };
            let Some(row) = g.pel_find(pid) else {
                continue;
            };
            let idle = now.duration_since(row.delivered_at).as_millis() as i64;
            if idle < min_idle_ms {
                continue;
            }
            // XDEL'd message: the PEL row disappears (lazy cleanup, Redis
            // semantics) and nothing is claimed.
            let exists = e
                .value
                .entries
                .iter()
                .any(|entry| parse_stream_id(&entry.id) == Some(pid));
            if !exists {
                row.delivery_count = -1; // drop marker
                continue;
            }
            row.consumer = consumer.to_string();
            row.delivery_count += 1;
            row.delivered_at = now;
            if let Some(entry) = e
                .value
                .entries
                .iter()
                .find(|entry| parse_stream_id(&entry.id) == Some(pid))
            {
                out.push(entry.clone());
            }
        }
        // Drop the rows marked for removal (XDEL'd messages).
        g.pel.retain(|p| p.delivery_count >= 0);
        Ok(out)
    }

    fn xack(&self, key: &str, group: &str, ids: &[&str]) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(e) = store.streams.get_mut(key) else {
            return Ok(0);
        };
        let Some(g) = e.value.groups.get_mut(group) else {
            return Ok(0);
        };
        let mut n = 0;
        for id in ids {
            if let Some(pid) = parse_stream_id(id) {
                let before = g.pel.len();
                g.pel.retain(|p| p.id != pid);
                if g.pel.len() < before {
                    n += 1;
                }
            }
        }
        Ok(n)
    }

    fn xdel(&self, key: &str, ids: &[&str]) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(e) = store.streams.get_mut(key) else {
            return Ok(0);
        };
        let mut n = 0;
        for id in ids {
            if let Some(pid) = parse_stream_id(id) {
                let before = e.value.entries.len();
                e.value
                    .entries
                    .retain(|entry| parse_stream_id(&entry.id) != Some(pid));
                if e.value.entries.len() < before {
                    n += 1;
                }
                for g in e.value.groups.values_mut() {
                    g.pel.retain(|p| p.id != pid);
                }
            }
        }
        Ok(n)
    }

    fn xtrim_maxlen_exact(&self, key: &str, maxlen: i64) -> Result_<()> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        if let Some(e) = store.streams.get_mut(key) {
            let len = e.value.entries.len() as i64;
            if len > maxlen {
                let drop = (len - maxlen) as usize;
                e.value.entries.drain(0..drop);
            }
        }
        Ok(())
    }

    fn xinfo_groups(&self, key: &str) -> Result_<Vec<GroupInfo>> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, key);
        let Some(e) = store.streams.get(key) else {
            return Ok(Vec::new());
        };
        let mut infos: Vec<GroupInfo> = e
            .value
            .groups
            .iter()
            .map(|(name, g)| GroupInfo {
                name: name.clone(),
                last_delivered_id: format!("{}-{}", g.last_delivered.0, g.last_delivered.1),
            })
            .collect();
        infos.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(infos)
    }

    fn lease_acquire(
        &self,
        lease_key: &str,
        fence_key: &str,
        fencegen_key: &str,
        holder: &str,
        ttl_secs: i64,
        fence_ttl_secs: i64,
    ) -> Result_<i64> {
        // luaLeaseAcquire semantics (execution_lease.py:68-81): the lease
        // existence check, the fencegen INCR with the fence-key floor and
        // the two SETs are one atomic unit on the real backend.
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, lease_key);
        if Self::string_get(&mut store, lease_key).is_some() {
            return Ok(0);
        }
        Self::gc_key(&mut store, fence_key);
        let floor: i64 = Self::string_get(&mut store, fence_key)
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        let mut gen = Self::incr_counter(&mut store, fencegen_key, 1);
        if gen <= floor {
            gen = Self::incr_counter(&mut store, fencegen_key, floor - gen + 1);
        }
        let fence_deadline =
            Some(Instant::now() + Duration::from_secs(fence_ttl_secs.max(0) as u64));
        store.strings.insert(
            fence_key.to_string(),
            Entry {
                value: gen.to_string(),
                expiry: fence_deadline,
            },
        );
        let lease_deadline = Some(Instant::now() + Duration::from_secs(ttl_secs.max(0) as u64));
        store.strings.insert(
            lease_key.to_string(),
            Entry {
                value: format!("{}:{}", holder, gen),
                expiry: lease_deadline,
            },
        );
        Ok(gen)
    }

    fn lease_release(&self, lease_key: &str, lease_value: &str) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, lease_key);
        match Self::string_get(&mut store, lease_key) {
            Some(v) if v == lease_value => {
                store.strings.remove(lease_key);
                Ok(1)
            }
            _ => Ok(0),
        }
    }

    fn lease_renew(&self, lease_key: &str, lease_value: &str, ttl_secs: i64) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, lease_key);
        match Self::string_get(&mut store, lease_key) {
            Some(v) if v == lease_value => {
                let deadline = Instant::now() + Duration::from_secs(ttl_secs.max(0) as u64);
                if let Some(e) = store.strings.get_mut(lease_key) {
                    e.expiry = Some(deadline);
                }
                Ok(1)
            }
            _ => Ok(0),
        }
    }

    fn fenced_state_save(
        &self,
        state_key: &str,
        fence_key: &str,
        index_key: &str,
        fencegen_key: &str,
        state_json: &str,
        gen: i64,
        ttl_secs: i64,
        score: f64,
    ) -> Result_<i64> {
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, state_key);
        Self::gc_key(&mut store, fence_key);
        Self::gc_key(&mut store, fencegen_key);
        let fence_val = Self::string_get(&mut store, fence_key);
        if fence_val.as_deref() != Some(&gen.to_string()) {
            return Ok(0); // fence mismatch (missing fence key counts)
        }
        let fencegen_val = Self::string_get(&mut store, fencegen_key);
        if fencegen_val.as_deref() != Some(&gen.to_string()) {
            return Ok(-1); // partial restore suspected
        }
        let expiry = if ttl_secs > 0 {
            Some(Instant::now() + Duration::from_secs(ttl_secs as u64))
        } else {
            None
        };
        store.strings.insert(
            state_key.to_string(),
            Entry {
                value: state_json.to_string(),
                expiry,
            },
        );
        let e = store
            .zsets
            .entry(index_key.to_string())
            .or_insert_with(|| Entry::new(HashMap::new()));
        e.value.insert(state_key.to_string(), score);
        Ok(1)
    }

    fn tenant_quota_admit(
        &self,
        running_key: &str,
        now: f64,
        ttl_secs: f64,
        cap: i64,
        member: &str,
    ) -> Result_<(i64, i64)> {
        // quota.go luaAdmitTenantStart semantics, natively
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, running_key);
        let zset = store
            .zsets
            .entry(running_key.to_string())
            .or_insert_with(|| Entry::new(HashMap::new()));
        zset.value.retain(|_, score| *score >= now);
        let running = zset.value.len() as i64;
        if zset.value.contains_key(member) {
            // existing member: idempotent refresh (redelivery-safe)
            zset.value.insert(member.to_string(), now + ttl_secs);
            let expiry = (ttl_secs.ceil() as i64).max(1) + 60;
            zset.expiry = Some(Instant::now() + Duration::from_secs(expiry.max(1) as u64));
            return Ok((1, zset.value.len() as i64));
        }
        if running >= cap {
            return Ok((0, running));
        }
        zset.value.insert(member.to_string(), now + ttl_secs);
        let expiry = (ttl_secs.ceil() as i64).max(1) + 60;
        zset.expiry = Some(Instant::now() + Duration::from_secs(expiry.max(1) as u64));
        Ok((1, zset.value.len() as i64))
    }

    fn tx(&self, ops: &[TxOp]) -> Result_<()> {
        let mut store = self.store.lock().unwrap();
        for op in ops {
            Self::apply_tx_op(&mut store, op);
        }
        Ok(())
    }

    fn claim_callback(
        &self,
        record_key: &str,
        claim_key: &str,
        claim_ttl_ms: i64,
    ) -> Result_<crate::backend::ClaimOutcome> {
        // GET record + SET claim NX PX as one step: hold the store lock
        // across both (set_nx would re-lock — std Mutex is not reentrant).
        let mut store = self.store.lock().unwrap();
        Self::gc_key(&mut store, record_key);
        Self::gc_key(&mut store, claim_key);
        let Some(entry) = store.strings.get(record_key) else {
            return Ok(crate::backend::ClaimOutcome::NoRecord);
        };
        let record = entry.value.clone();
        if store.strings.contains_key(claim_key) {
            return Ok(crate::backend::ClaimOutcome::RaceLost);
        }
        let expiry = Some(
            std::time::Instant::now() + Duration::from_secs((claim_ttl_ms / 1000).max(1) as u64),
        );
        store.strings.insert(
            claim_key.to_string(),
            Entry {
                value: "1".to_string(),
                expiry,
            },
        );
        Ok(crate::backend::ClaimOutcome::Claimed(record))
    }
}

impl FakeRedis {
    fn incr_counter(store: &mut Store, key: &str, delta: i64) -> i64 {
        let current: i64 = store
            .strings
            .get(key)
            .and_then(|e| e.value.parse().ok())
            .unwrap_or(0);
        let next = current + delta;
        let entry = store
            .strings
            .entry(key.to_string())
            .or_insert_with(|| Entry::new(String::new()));
        entry.value = next.to_string();
        next
    }
}

/// Wildcard match supporting literal text plus any number of `*` segments
/// (sufficient for `{prefix}*:events:*`).
fn glob_match(pattern: &str, text: &str) -> bool {
    fn match_from(p: &[u8], t: &[u8]) -> bool {
        if p.is_empty() {
            return t.is_empty();
        }
        if p[0] == b'*' {
            // collapse consecutive stars, then try every suffix
            let rest = &p[1..];
            for i in 0..=t.len() {
                if match_from(rest, &t[i..]) {
                    return true;
                }
            }
            return false;
        }
        match t.first() {
            Some(&c) if c == p[0] => match_from(&p[1..], &t[1..]),
            _ => false,
        }
    }
    match_from(pattern.as_bytes(), text.as_bytes())
}

struct FakePsubStream {
    queue: PsubQueue,
}

impl PsubStream for FakePsubStream {
    fn next_message(&mut self, timeout: Duration) -> Option<(String, String)> {
        let (queue, cv) = &*self.queue;
        let guard = queue.lock().unwrap();
        if let Some(m) = guard.front() {
            let m = m.clone();
            drop(guard);
            queue.lock().unwrap().pop_front();
            return Some(m);
        }
        let (mut guard, _timeout) = cv
            .wait_timeout_while(guard, timeout, |q| q.is_empty())
            .unwrap();
        guard.pop_front()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stream_group_semantics() {
        let r = FakeRedis::new();
        // BUSYGROUP on re-create
        r.xgroup_create_mkstream("s", "g", "0").unwrap();
        assert!(matches!(
            r.xgroup_create_mkstream("s", "g", "0"),
            Err(BackendError::BusyGroup(_))
        ));
        // deliver two, read COUNT 1 — last-delivered advances by one
        r.xadd("s", &[("payload".into(), "a".into())]).unwrap();
        r.xadd("s", &[("payload".into(), "b".into())]).unwrap();
        let got = r.xreadgroup("g", "c1", "s", 1, None).unwrap();
        assert_eq!(got.len(), 1);
        let first_id = got[0].id.clone();
        let groups = r.xinfo_groups("s").unwrap();
        assert_eq!(groups[0].last_delivered_id, first_id);
        // second read gets the second message; third read empty
        assert_eq!(r.xreadgroup("g", "c1", "s", 1, None).unwrap().len(), 1);
        assert!(r.xreadgroup("g", "c1", "s", 1, None).unwrap().is_empty());
        // PEL: both entries pending on c1
        let pending = r.xpending_ext("s", "g", "-", "+", 10).unwrap();
        assert_eq!(pending.len(), 2);
        assert_eq!(pending[0].delivery_count, 1);
        // reclaim cannot take a fresh message (idle < min_idle)
        std::thread::sleep(Duration::from_millis(5));
        assert!(r
            .xclaim("s", "g", "c2", 60_000, &[&first_id])
            .unwrap()
            .is_empty());
        // short min-idle claim transfers ownership + bumps delivery count
        let claimed = r.xclaim("s", "g", "c2", 0, &[&first_id]).unwrap();
        assert_eq!(claimed.len(), 1);
        let pending = r.xpending_ext("s", "g", "-", "+", 10).unwrap();
        assert_eq!(pending[0].consumer, "c2");
        assert_eq!(pending[0].delivery_count, 2);
        // ack removes from PEL
        assert_eq!(r.xack("s", "g", &[&first_id]).unwrap(), 1);
        assert_eq!(r.xpending_ext("s", "g", "-", "+", 10).unwrap().len(), 1);
        // xdel removes the entry and its PEL row
        let second = r.xpending_ext("s", "g", "-", "+", 10).unwrap()[0]
            .id
            .clone();
        assert_eq!(r.xdel("s", &[&second]).unwrap(), 1);
        assert_eq!(r.xpending_ext("s", "g", "-", "+", 10).unwrap().len(), 0);
        assert_eq!(r.xlen("s").unwrap(), 1);
        // trim keeps the newest N
        r.xtrim_maxlen_exact("s", 0).unwrap();
        assert_eq!(r.xlen("s").unwrap(), 0);
    }

    #[test]
    fn lua_script_semantics() {
        let r = FakeRedis::new();
        // acquire mints gen 1 and writes holder:gen
        let gen = r
            .lease_acquire("l", "f", "fg", "start:i:aa", 120, 604800)
            .unwrap();
        assert_eq!(gen, 1);
        assert_eq!(r.get("l").unwrap().as_deref(), Some("start:i:aa:1"));
        // second acquire conflicts (0)
        assert_eq!(r.lease_acquire("l", "f", "fg", "x", 1, 1).unwrap(), 0);
        // renew with the right value refreshes; wrong value is 0
        assert_eq!(r.lease_renew("l", "start:i:aa:1", 120).unwrap(), 1);
        assert_eq!(r.lease_renew("l", "wrong", 120).unwrap(), 0);
        // release only on full-value match
        assert_eq!(r.lease_release("l", "wrong").unwrap(), 0);
        assert_eq!(r.lease_release("l", "start:i:aa:1").unwrap(), 1);
        // floor raise: fence key above the counter lifts the next gen past it
        r.set("f", "5", None).unwrap();
        let gen = r.lease_acquire("l", "f", "fg", "h", 1, 1).unwrap();
        assert_eq!(gen, 6);
        assert_eq!(r.get("fg").unwrap().as_deref(), Some("6"));
        // fenced CAS save: mismatched fence → 0, matched → 1, diverged
        // fencegen → -1
        assert_eq!(
            r.fenced_state_save("st", "f", "ix", "fg", "{}", 3, 0, 1.0)
                .unwrap(),
            0
        );
        assert_eq!(
            r.fenced_state_save("st", "f", "ix", "fg", "{}", 6, 30, 1.5)
                .unwrap(),
            1
        );
        assert_eq!(r.get("st").unwrap().as_deref(), Some("{}"));
        assert_eq!(
            r.zrangebyscore("ix", f64::MIN, f64::MAX, 10).unwrap(),
            ["st"]
        );
        r.incr_by("fg", 1).unwrap();
        assert_eq!(
            r.fenced_state_save("st", "f", "ix", "fg", "{}2", 6, 0, 1.0)
                .unwrap(),
            -1
        );
    }

    #[test]
    fn pubsub_pattern_delivery() {
        let r = FakeRedis::new();
        let mut sub = r.psubscribe("plaita*:events:*").unwrap();
        r.publish("plaita:events:tick", "1").unwrap();
        r.publish("plaita:acme:events:t2", "2").unwrap();
        // Redis glob: "plaita*:events:*" DOES match the execution timeline
        // channel ("plaita" + ":execution" via *, then ":events:", then id)
        // — the bridge-level skip is the defense (§8.2). Pin the glob
        // behavior the bridge must defend against.
        r.publish("plaita:execution:events:e1", "3").unwrap();
        r.publish("other:events:x", "4").unwrap();
        assert_eq!(
            sub.next_message(Duration::from_secs(1)).unwrap(),
            ("plaita:events:tick".to_string(), "1".to_string())
        );
        assert_eq!(
            sub.next_message(Duration::from_secs(1)).unwrap(),
            ("plaita:acme:events:t2".to_string(), "2".to_string())
        );
        assert_eq!(
            sub.next_message(Duration::from_secs(1)).unwrap(),
            ("plaita:execution:events:e1".to_string(), "3".to_string())
        );
        assert!(sub.next_message(Duration::from_millis(50)).is_none());
    }

    #[test]
    fn tx_and_expiry() {
        let r = FakeRedis::new();
        r.tx(&[
            TxOp::Set {
                key: "a".into(),
                value: "1".into(),
                ttl_secs: Some(1),
            },
            TxOp::Sadd {
                key: "b".into(),
                member: "m".into(),
            },
            TxOp::Expire {
                key: "b".into(),
                ttl_secs: 1,
            },
            TxOp::Zadd {
                key: "c".into(),
                score: 1.5,
                member: "z".into(),
            },
        ])
        .unwrap();
        assert_eq!(r.get("a").unwrap().as_deref(), Some("1"));
        assert_eq!(r.smembers("b").unwrap(), ["m"]);
        assert!(r.ttl_ms("a").unwrap().is_some(), "TTL was armed");
        assert!(r.ttl_ms("b").unwrap().is_some());
        // expiry elapses → key gone
        std::thread::sleep(Duration::from_millis(1100));
        assert_eq!(r.get("a").unwrap(), None);
        assert!(r.smembers("b").unwrap().is_empty());
        assert_eq!(r.zrangebyscore("c", 0.0, 10.0, 10).unwrap(), ["z"]);
    }
}
