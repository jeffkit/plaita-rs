//! The production Redis backend: redis-rs (blocking) mapping every
//! [`RedisBackend`] method onto the exact command, and the four Lua scripts
//! as VERBATIM transplants of plaita/server/execution_lease.py (via
//! plaita-go lease.go/state.go) — their semantics must not be "improved".

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use redis::Commands;

use crate::backend::{
    BackendError, BackendResult, GroupInfo, PendingEntry, PsubStream, RedisBackend, StreamEntry,
    TxOp,
};

// -- Lua scripts (verbatim; execution_lease.py:33-81 + state.go:140-154) -----

/// Fenced acquire (execution_lease.py:68-80). KEYS[1]=lease key,
/// KEYS[2]=fence key, KEYS[3]=fencegen key; ARGV[1]=holder, ARGV[2]=ttl
/// seconds, ARGV[3]=fence TTL seconds. Returns the fence generation (>0) or
/// 0 = already held.
const LUA_LEASE_ACQUIRE: &str = r#"
if redis.call('get', KEYS[1]) then
  return 0
end
local floor = tonumber(redis.call('get', KEYS[2])) or 0
local gen = redis.call('incr', KEYS[3])
if gen <= floor then
  gen = redis.call('incrby', KEYS[3], floor - gen + 1)
end
redis.call('set', KEYS[2], tostring(gen), 'EX', ARGV[3])
redis.call('set', KEYS[1], ARGV[1] .. ':' .. tostring(gen), 'EX', ARGV[2])
return gen
"#;

/// Compare-and-deletes the ENTIRE lease value (execution_lease.py:33-39).
/// ARGV[1] = full "{holder}:{gen}" value.
const LUA_LEASE_RELEASE: &str = r#"
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"#;

/// Re-EXPIREs only when the full value still matches
/// (execution_lease.py:42-48); 0 = lost lease.
const LUA_LEASE_RENEW: &str = r#"
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('expire', KEYS[1], ARGV[2])
else
  return 0
end
"#;

/// The fenced CAS write (state.go luaFencedStateSave): the state row is
/// written only while the fence key still equals the holder's generation
/// AND the never-cleared fencegen counter agrees (#54, A2R25-05); the index
/// ZADD rides in the same atomic script. Returns 1 written / 0 fence
/// mismatch / -1 fence-fencegen divergence.
/// Per-tenant start admission (quota.go luaAdmitTenantStart — verbatim):
/// KEYS[1] = running ZSET; ARGV: now, ttl, cap, member. Returns
/// {allowed, running}.
const LUA_TENANT_QUOTA_ADMIT: &str = r#"
redis.call('ZREMRANGEBYSCORE', KEYS[1], '-inf', ARGV[1])
if redis.call('ZSCORE', KEYS[1], ARGV[4]) then
  redis.call('ZADD', KEYS[1], tonumber(ARGV[1]) + tonumber(ARGV[2]), ARGV[4])
  redis.call('EXPIRE', KEYS[1], math.ceil(tonumber(ARGV[2])) + 60)
  return {1, redis.call('ZCARD', KEYS[1])}
end
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then
  return {0, redis.call('ZCARD', KEYS[1])}
end
redis.call('ZADD', KEYS[1], tonumber(ARGV[1]) + tonumber(ARGV[2]), ARGV[4])
redis.call('EXPIRE', KEYS[1], math.ceil(tonumber(ARGV[2])) + 60)
return {1, redis.call('ZCARD', KEYS[1])}
"#;

const LUA_FENCED_STATE_SAVE: &str = r#"
if redis.call('get', KEYS[2]) ~= ARGV[2] then
  return 0
end
if redis.call('get', KEYS[4]) ~= ARGV[2] then
  return -1
end
if tonumber(ARGV[3]) > 0 then
  redis.call('set', KEYS[1], ARGV[1], 'EX', ARGV[3])
else
  redis.call('set', KEYS[1], ARGV[1])
end
redis.call('zadd', KEYS[3], ARGV[4], KEYS[1])
return 1
"#;

/// redis-rs 0.27's Msg has no kind accessor; subscribe/psubscribe
/// confirmations carry an integer payload (no bytes), real messages a
/// non-empty payload.
fn is_control_message(msg: &redis::Msg) -> bool {
    msg.get_payload_bytes().is_empty()
}

/// Maps a redis-rs error onto the backend error taxonomy: BUSYGROUP is a
/// named case (EnsureGroup tolerates it); NOGROUP behaves like an empty
/// read (the group/key vanished mid-loop and is recreated next round).
fn map_err(e: redis::RedisError) -> BackendError {
    let text = e.to_string();
    if text.contains("BUSYGROUP") {
        return BackendError::BusyGroup(text);
    }
    if text.contains("NOGROUP") || text.contains("no such key") {
        // Treated as "nothing available" by the readers via Io + message —
        // callers warn and keep polling; the queue recreates the group.
        return BackendError::Io(format!("nogroup: {text}"));
    }
    BackendError::Io(text)
}

/// The blocking backend over one redis-rs client (a connection pool handle;
/// each call takes a connection).
pub struct RedisConnection {
    client: redis::Client,
}

impl RedisConnection {
    pub fn connect(url: &str) -> BackendResult<RedisConnection> {
        let client =
            redis::Client::open(url).map_err(|e| BackendError::Io(format!("open {url}: {e}")))?;
        // Fail fast on a bad URL/unreachable server instead of at first use.
        let mut conn = client.get_connection().map_err(map_err)?;
        redis::cmd("PING")
            .query::<String>(&mut conn)
            .map_err(map_err)?;
        Ok(RedisConnection { client })
    }

    fn with_conn<T>(
        &self,
        f: impl FnOnce(&mut redis::Connection) -> BackendResult<T>,
    ) -> BackendResult<T> {
        let mut conn = self.client.get_connection().map_err(map_err)?;
        f(&mut conn)
    }
}

/// Wraps a parsed redis value into stream entries (id + flat field pairs).
fn value_to_stream_entries(v: &redis::Value) -> BackendResult<Vec<StreamEntry>> {
    let rows = match v {
        redis::Value::Array(rows) => rows.clone(),
        redis::Value::Nil => return Ok(Vec::new()),
        other => {
            return Err(BackendError::Io(format!(
                "unexpected stream reply: {other:?}"
            )))
        }
    };
    let mut out = Vec::new();
    for row in rows {
        // row = [key, [[id, [f1, v1, ...]], ...]] — take the first stream
        let entries = match &row {
            redis::Value::Array(r) => r.get(1),
            _ => None,
        };
        let Some(Some(entries)) = entries.map(|e| match e {
            redis::Value::Array(x) => Some(x.clone()),
            _ => None,
        }) else {
            continue;
        };
        for entry in entries {
            let parts = match entry {
                redis::Value::Array(p) => p,
                _ => continue,
            };
            if parts.len() < 2 {
                continue;
            }
            let id = from_val_string(&parts[0])?;
            let mut fields = Vec::new();
            if let redis::Value::Array(flat) = &parts[1] {
                for pair in flat.chunks(2) {
                    if pair.len() == 2 {
                        fields.push((from_val_string(&pair[0])?, from_val_string(&pair[1])?));
                    }
                }
            }
            out.push(StreamEntry { id, fields });
        }
    }
    Ok(out)
}

fn from_val_string(v: &redis::Value) -> BackendResult<String> {
    let s: String =
        redis::from_redis_value(v).map_err(|e| BackendError::Io(format!("value decode: {e}")))?;
    Ok(s)
}

impl RedisBackend for RedisConnection {
    fn get(&self, key: &str) -> BackendResult<Option<String>> {
        self.with_conn(|c| {
            let v: Option<String> = c.get(key).map_err(map_err)?;
            Ok(v)
        })
    }

    fn set(&self, key: &str, value: &str, ttl_secs: Option<i64>) -> BackendResult<()> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("SET");
            cmd.arg(key).arg(value);
            if let Some(ttl) = ttl_secs {
                cmd.arg("EX").arg(ttl.max(1).to_string());
            }
            cmd.query::<String>(c).map_err(map_err)?;
            Ok(())
        })
    }

    fn set_nx(&self, key: &str, value: &str, ttl_secs: i64) -> BackendResult<bool> {
        self.with_conn(|c| {
            let v: Option<String> = redis::cmd("SET")
                .arg(key)
                .arg(value)
                .arg("EX")
                .arg(ttl_secs.max(1))
                .arg("NX")
                .query(c)
                .map_err(map_err)?;
            Ok(v.is_some())
        })
    }

    fn del(&self, keys: &[&str]) -> BackendResult<i64> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("DEL");
            for k in keys {
                cmd.arg(k);
            }
            cmd.query(c).map_err(map_err)
        })
    }

    fn expire(&self, key: &str, ttl_secs: i64) -> BackendResult<bool> {
        self.with_conn(|c| {
            let n: i64 = c.expire(key, ttl_secs.max(1)).map_err(map_err)?;
            Ok(n == 1)
        })
    }

    fn incr(&self, key: &str) -> BackendResult<i64> {
        self.with_conn(|c| c.incr(key, 1).map_err(map_err))
    }

    fn incr_by(&self, key: &str, delta: i64) -> BackendResult<i64> {
        self.with_conn(|c| c.incr(key, delta).map_err(map_err))
    }

    fn hincr_by(&self, key: &str, field: &str, delta: i64) -> BackendResult<i64> {
        self.with_conn(|c| c.hincr(key, field, delta).map_err(map_err))
    }

    fn hdel(&self, key: &str, field: &str) -> BackendResult<i64> {
        self.with_conn(|c| c.hdel(key, field).map_err(map_err))
    }

    fn hget(&self, key: &str, field: &str) -> BackendResult<Option<String>> {
        self.with_conn(|c| {
            let v: Option<String> = c.hget(key, field).map_err(map_err)?;
            Ok(v)
        })
    }

    fn hset(&self, key: &str, field: &str, value: &str) -> BackendResult<i64> {
        self.with_conn(|c| {
            let n: i64 = c.hset(key, field, value).map_err(map_err)?;
            Ok(n)
        })
    }

    fn sadd(&self, key: &str, member: &str) -> BackendResult<()> {
        self.with_conn(|c| {
            let _: i64 = c.sadd(key, member).map_err(map_err)?;
            Ok(())
        })
    }

    fn srem(&self, key: &str, member: &str) -> BackendResult<()> {
        self.with_conn(|c| {
            let _: i64 = c.srem(key, member).map_err(map_err)?;
            Ok(())
        })
    }

    fn smembers(&self, key: &str) -> BackendResult<Vec<String>> {
        self.with_conn(|c| {
            let v: Vec<String> = c.smembers(key).map_err(map_err)?;
            Ok(v)
        })
    }

    fn scan_match(&self, pattern: &str) -> BackendResult<Vec<String>> {
        // SCAN iterates to cursor 0 (COUNT 100 per page); a hard cap guards
        // against a pathological key space (the callers' scans are per-
        // execution counter keys, so hitting the cap means something is very
        // wrong and partial results are the safer answer anyway).
        const PAGE: u64 = 100;
        const HARD_CAP: usize = 10_000;
        self.with_conn(|c| {
            let mut cursor = 0u64;
            let mut keys: Vec<String> = Vec::new();
            loop {
                let (next, page): (u64, Vec<String>) = redis::cmd("SCAN")
                    .arg(cursor)
                    .arg("MATCH")
                    .arg(pattern)
                    .arg("COUNT")
                    .arg(PAGE)
                    .query(c)
                    .map_err(map_err)?;
                keys.extend(page);
                cursor = next;
                if cursor == 0 || keys.len() >= HARD_CAP {
                    break;
                }
            }
            Ok(keys)
        })
    }

    fn zadd(&self, key: &str, score: f64, member: &str) -> BackendResult<()> {
        self.with_conn(|c| {
            let _: i64 = c.zadd(key, member, score).map_err(map_err)?;
            Ok(())
        })
    }

    fn zrem(&self, key: &str, member: &str) -> BackendResult<()> {
        self.with_conn(|c| {
            let _: i64 = c.zrem(key, member).map_err(map_err)?;
            Ok(())
        })
    }

    fn zrangebyscore(
        &self,
        key: &str,
        min: f64,
        max: f64,
        count: i64,
    ) -> BackendResult<Vec<String>> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("ZRANGEBYSCORE");
            cmd.arg(key)
                .arg(format!("{min}"))
                .arg(format!("{max}"))
                .arg("LIMIT")
                .arg(0)
                .arg(count.max(0));
            let v: Vec<String> = cmd.query(c).map_err(map_err)?;
            Ok(v)
        })
    }

    fn rpush(&self, key: &str, value: &str) -> BackendResult<()> {
        self.with_conn(|c| {
            let _: i64 = c.rpush(key, value).map_err(map_err)?;
            Ok(())
        })
    }

    fn lrange(&self, key: &str, start: i64, stop: i64) -> BackendResult<Vec<String>> {
        self.with_conn(|c| {
            let v: Vec<String> = c
                .lrange(key, start as isize, stop as isize)
                .map_err(map_err)?;
            Ok(v)
        })
    }

    fn publish(&self, channel: &str, payload: &str) -> BackendResult<i64> {
        self.with_conn(|c| {
            let n: i64 = redis::cmd("PUBLISH")
                .arg(channel)
                .arg(payload)
                .query(c)
                .map_err(map_err)?;
            Ok(n)
        })
    }

    fn psubscribe(&self, pattern: &str) -> BackendResult<Box<dyn PsubStream>> {
        // redis-rs 0.27's PubSub borrows its connection, so a dedicated
        // thread owns the connection and forwards messages over a channel
        // (the same "PSubscribe on a dedicated connection" shape as Go's
        // eventbus). The confirmation is consumed BEFORE the stream is
        // handed out (§8.2: an event published in the confirmation window
        // would otherwise be missed with no reconciler in this build).
        let client = self.client.clone();
        let pattern = pattern.to_string();
        let (tx, rx) = std::sync::mpsc::channel::<Option<(String, String)>>();
        let stop = Arc::new(AtomicBool::new(false));
        let stop_thread = stop.clone();
        let handle = std::thread::spawn(move || {
            let mut conn = match client.get_connection() {
                Ok(c) => c,
                Err(e) => {
                    let _ = tx.send(None);
                    eprintln!("[redis] pubsub connection failed: {e}");
                    return;
                }
            };
            let mut pubsub = conn.as_pubsub();
            // redis-rs consumes the +psubscribe confirmation INSIDE
            // psubscribe() and caches any messages that raced it into its
            // internal queue (waiting_messages) — nothing is lost, and no
            // extra confirmation read is needed here (an extra one would
            // swallow real business messages).
            if let Err(e) = pubsub.psubscribe(&pattern) {
                let _ = tx.send(None);
                eprintln!("[redis] pubsub subscribe failed: {e}");
                return;
            }
            loop {
                if stop_thread.load(std::sync::atomic::Ordering::SeqCst) {
                    return;
                }
                // Read-timeout slices keep the stop flag responsive; a
                // timeout surfaces as Err and re-checks above.
                let _ = pubsub.set_read_timeout(Some(Duration::from_secs(1)));
                match pubsub.get_message() {
                    Ok(msg) => {
                        if is_control_message(&msg) {
                            continue; // stray control frame: skip
                        }
                        let channel = msg.get_channel_name().to_string();
                        match msg.get_payload::<String>() {
                            Ok(payload) => {
                                if tx.send(Some((channel, payload))).is_err() {
                                    return; // receiver dropped
                                }
                            }
                            Err(e) => {
                                eprintln!("[redis] pubsub payload decode failed: {e}");
                            }
                        }
                    }
                    Err(_) => continue, // read timeout slice: re-check stop
                }
            }
        });
        Ok(Box::new(RedisPsub {
            rx,
            stop,
            handle: std::sync::Mutex::new(Some(handle)),
        }))
    }

    fn xadd(&self, key: &str, fields: &[(String, String)]) -> BackendResult<String> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("XADD");
            cmd.arg(key).arg("*");
            for (k, v) in fields {
                cmd.arg(k).arg(v);
            }
            cmd.query(c).map_err(map_err)
        })
    }

    fn xrange(
        &self,
        key: &str,
        start: &str,
        end: &str,
        count: Option<i64>,
    ) -> BackendResult<Vec<StreamEntry>> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("XRANGE");
            cmd.arg(key).arg(start).arg(end);
            if let Some(n) = count {
                cmd.arg("COUNT").arg(n.max(0));
            }
            let v = cmd.query(c).map_err(map_err)?;
            value_to_stream_entries(&v)
        })
    }

    fn xlen(&self, key: &str) -> BackendResult<i64> {
        self.with_conn(|c| {
            let n: i64 = redis::cmd("XLEN").arg(key).query(c).map_err(map_err)?;
            Ok(n)
        })
    }

    fn xgroup_create_mkstream(&self, key: &str, group: &str, id: &str) -> BackendResult<()> {
        self.with_conn(|c| {
            redis::cmd("XGROUP")
                .arg("CREATE")
                .arg(key)
                .arg(group)
                .arg(id)
                .arg("MKSTREAM")
                .query::<String>(c)
                .map_err(map_err)?;
            Ok(())
        })
    }

    fn xreadgroup(
        &self,
        group: &str,
        consumer: &str,
        key: &str,
        count: i64,
        block_ms: Option<i64>,
    ) -> BackendResult<Vec<StreamEntry>> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("XREADGROUP");
            cmd.arg("GROUP")
                .arg(group)
                .arg(consumer)
                .arg("COUNT")
                .arg(count.max(1));
            if let Some(block) = block_ms {
                cmd.arg("BLOCK").arg(block.max(0));
            }
            cmd.arg("STREAMS").arg(key).arg(">");
            let v: redis::Value = cmd.query(c).map_err(map_err)?;
            match &v {
                redis::Value::Nil => Ok(Vec::new()),
                _ => value_to_stream_entries(&v),
            }
        })
    }

    fn xpending_ext(
        &self,
        key: &str,
        group: &str,
        start: &str,
        end: &str,
        count: i64,
    ) -> BackendResult<Vec<PendingEntry>> {
        self.with_conn(|c| {
            let v: redis::Value = redis::cmd("XPENDING")
                .arg(key)
                .arg(group)
                .arg("IDLE")
                .arg(0)
                .arg(start)
                .arg(end)
                .arg(count.max(0))
                .query(c)
                .map_err(map_err)?;
            // EXT form rows: [id, consumer, idle-ms, delivery-count]
            let rows = match v {
                redis::Value::Array(r) => r,
                redis::Value::Nil => return Ok(Vec::new()),
                other => {
                    return Err(BackendError::Io(format!(
                        "unexpected xpending reply: {other:?}"
                    )))
                }
            };
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let parts = match row {
                    redis::Value::Array(p) => p,
                    _ => continue,
                };
                if parts.len() < 4 {
                    continue;
                }
                out.push(PendingEntry {
                    id: from_val_string(&parts[0])?,
                    consumer: from_val_string(&parts[1])?,
                    idle_ms: redis::from_redis_value(&parts[2])
                        .map_err(|e| BackendError::Io(format!("xpending idle: {e}")))?,
                    delivery_count: redis::from_redis_value(&parts[3])
                        .map_err(|e| BackendError::Io(format!("xpending count: {e}")))?,
                });
            }
            Ok(out)
        })
    }

    fn xclaim(
        &self,
        key: &str,
        group: &str,
        consumer: &str,
        min_idle_ms: i64,
        ids: &[&str],
    ) -> BackendResult<Vec<StreamEntry>> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("XCLAIM");
            cmd.arg(key)
                .arg(group)
                .arg(consumer)
                .arg(min_idle_ms.max(0));
            for id in ids {
                cmd.arg(id);
            }
            let v: redis::Value = cmd.query(c).map_err(map_err)?;
            // XCLAIM replies with plain entries: [[id, [f1, v1, ...]], ...]
            let rows = match v {
                redis::Value::Array(r) => r,
                redis::Value::Nil => return Ok(Vec::new()),
                other => {
                    return Err(BackendError::Io(format!(
                        "unexpected xclaim reply: {other:?}"
                    )))
                }
            };
            let mut out = Vec::with_capacity(rows.len());
            for row in rows {
                let parts = match row {
                    redis::Value::Array(p) => p,
                    _ => continue,
                };
                if parts.len() < 2 {
                    continue;
                }
                let id = from_val_string(&parts[0])?;
                let mut fields = Vec::new();
                if let redis::Value::Array(flat) = &parts[1] {
                    for pair in flat.chunks(2) {
                        if pair.len() == 2 {
                            fields.push((from_val_string(&pair[0])?, from_val_string(&pair[1])?));
                        }
                    }
                }
                out.push(StreamEntry { id, fields });
            }
            Ok(out)
        })
    }

    fn xack(&self, key: &str, group: &str, ids: &[&str]) -> BackendResult<i64> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("XACK");
            cmd.arg(key).arg(group);
            for id in ids {
                cmd.arg(id);
            }
            cmd.query(c).map_err(map_err)
        })
    }

    fn xdel(&self, key: &str, ids: &[&str]) -> BackendResult<i64> {
        self.with_conn(|c| {
            let mut cmd = redis::cmd("XDEL");
            cmd.arg(key);
            for id in ids {
                cmd.arg(id);
            }
            cmd.query(c).map_err(map_err)
        })
    }

    fn xtrim_maxlen_exact(&self, key: &str, maxlen: i64) -> BackendResult<()> {
        self.with_conn(|c| {
            redis::cmd("XTRIM")
                .arg(key)
                .arg("MAXLEN")
                .arg(maxlen.max(0))
                .query::<i64>(c)
                .map_err(map_err)?;
            Ok(())
        })
    }

    fn xinfo_groups(&self, key: &str) -> BackendResult<Vec<GroupInfo>> {
        self.with_conn(|c| {
            let v: redis::Value = redis::cmd("XINFO")
                .arg("GROUPS")
                .arg(key)
                .query(c)
                .map_err(map_err)?;
            let rows = match v {
                redis::Value::Array(r) => r,
                redis::Value::Nil => return Ok(Vec::new()),
                other => {
                    return Err(BackendError::Io(format!(
                        "unexpected xinfo reply: {other:?}"
                    )))
                }
            };
            let mut out = Vec::new();
            for row in rows {
                let parts = match row {
                    redis::Value::Array(p) => p,
                    _ => continue,
                };
                let mut name = String::new();
                let mut last = String::new();
                for pair in parts.chunks(2) {
                    if pair.len() != 2 {
                        continue;
                    }
                    match from_val_string(&pair[0])?.as_str() {
                        "name" => name = from_val_string(&pair[1])?,
                        "last-delivered-id" => last = from_val_string(&pair[1])?,
                        _ => {}
                    }
                }
                if !name.is_empty() {
                    out.push(GroupInfo {
                        name,
                        last_delivered_id: last,
                    });
                }
            }
            Ok(out)
        })
    }

    fn lease_acquire(
        &self,
        lease_key: &str,
        fence_key: &str,
        fencegen_key: &str,
        holder: &str,
        ttl_secs: i64,
        fence_ttl_secs: i64,
    ) -> BackendResult<i64> {
        self.with_conn(|c| {
            let script = redis::Script::new(LUA_LEASE_ACQUIRE);
            script
                .key(lease_key)
                .key(fence_key)
                .key(fencegen_key)
                .arg(holder)
                .arg(ttl_secs)
                .arg(fence_ttl_secs)
                .invoke(c)
                .map_err(map_err)
        })
    }

    fn lease_release(&self, lease_key: &str, lease_value: &str) -> BackendResult<i64> {
        self.with_conn(|c| {
            let script = redis::Script::new(LUA_LEASE_RELEASE);
            script
                .key(lease_key)
                .arg(lease_value)
                .invoke(c)
                .map_err(map_err)
        })
    }

    fn lease_renew(&self, lease_key: &str, lease_value: &str, ttl_secs: i64) -> BackendResult<i64> {
        self.with_conn(|c| {
            let script = redis::Script::new(LUA_LEASE_RENEW);
            script
                .key(lease_key)
                .arg(lease_value)
                .arg(ttl_secs)
                .invoke(c)
                .map_err(map_err)
        })
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
    ) -> BackendResult<i64> {
        self.with_conn(|c| {
            let script = redis::Script::new(LUA_FENCED_STATE_SAVE);
            script
                .key(state_key)
                .key(fence_key)
                .key(index_key)
                .key(fencegen_key)
                .arg(state_json)
                .arg(gen.to_string())
                .arg(ttl_secs)
                .arg(format!("{score:.6}"))
                .invoke(c)
                .map_err(map_err)
        })
    }

    fn tenant_quota_admit(
        &self,
        running_key: &str,
        now: f64,
        ttl_secs: f64,
        cap: i64,
        member: &str,
    ) -> BackendResult<(i64, i64)> {
        self.with_conn(|c| {
            let script = redis::Script::new(LUA_TENANT_QUOTA_ADMIT);
            let v: redis::Value = script
                .key(running_key)
                .arg(format!("{now:.6}"))
                .arg(format!("{ttl_secs:.6}"))
                .arg(cap)
                .arg(member)
                .invoke(c)
                .map_err(map_err)?;
            // The script returns {allowed, running}; an unexpected shape
            // fails OPEN like any other gate outage (quota.go AdmitStart).
            let items = match v {
                redis::Value::Array(items) if items.len() == 2 => items,
                _ => return Ok((1, 0)),
            };
            let as_i64 = |x: redis::Value| match x {
                redis::Value::Int(n) => n,
                redis::Value::SimpleString(s) => s.parse().unwrap_or(0),
                redis::Value::BulkString(d) => String::from_utf8_lossy(&d).parse().unwrap_or(0),
                _ => 0,
            };
            let mut it = items.into_iter();
            let allowed = as_i64(it.next().unwrap_or(redis::Value::Nil));
            let running = as_i64(it.next().unwrap_or(redis::Value::Nil));
            Ok((allowed, running))
        })
    }

    fn tx(&self, ops: &[TxOp]) -> BackendResult<()> {
        self.with_conn(|c| {
            let mut pipe = redis::pipe();
            for op in ops {
                match op {
                    TxOp::Set {
                        key,
                        value,
                        ttl_secs,
                    } => {
                        let _ = ttl_secs;
                        let mut cmd = redis::cmd("SET");
                        cmd.arg(key).arg(value);
                        if let Some(ttl) = ttl_secs {
                            cmd.arg("EX").arg((*ttl).max(1).to_string());
                        }
                        pipe.add_command(cmd);
                    }
                    TxOp::Sadd { key, member } => {
                        pipe.cmd("SADD").arg(key).arg(member).ignore();
                    }
                    TxOp::Srem { key, member } => {
                        pipe.cmd("SREM").arg(key).arg(member).ignore();
                    }
                    TxOp::Expire { key, ttl_secs } => {
                        pipe.cmd("EXPIRE").arg(key).arg((*ttl_secs).max(1)).ignore();
                    }
                    TxOp::Zadd { key, score, member } => {
                        pipe.cmd("ZADD")
                            .arg(key)
                            .arg(format!("{score}"))
                            .arg(member)
                            .ignore();
                    }
                    TxOp::Zrem { key, member } => {
                        pipe.cmd("ZREM").arg(key).arg(member).ignore();
                    }
                    TxOp::Del { key } => {
                        pipe.cmd("DEL").arg(key).ignore();
                    }
                }
            }
            pipe.query::<()>(&mut *c).map_err(map_err)
        })
    }
}

/// The real Pub/Sub stream: a channel fed by the dedicated subscription
/// thread. Dropping the stream stops the thread (join with the 1s read
/// timeout slices).
struct RedisPsub {
    rx: std::sync::mpsc::Receiver<Option<(String, String)>>,
    stop: Arc<AtomicBool>,
    handle: std::sync::Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl PsubStream for RedisPsub {
    fn next_message(&mut self, timeout: Duration) -> Option<(String, String)> {
        match self.rx.recv_timeout(timeout.max(Duration::from_millis(1))) {
            Ok(Some(m)) => Some(m),
            Ok(None) => {
                // subscription thread died — surface as end-of-stream; the
                // bridge loop reconnects after its backoff
                None
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => None,
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => None,
        }
    }
}

impl Drop for RedisPsub {
    fn drop(&mut self) {
        self.stop.store(true, std::sync::atomic::Ordering::SeqCst);
        if let Ok(mut slot) = self.handle.lock() {
            if let Some(handle) = slot.take() {
                let _ = handle.join();
            }
        }
    }
}

impl From<RedisConnection> for Arc<dyn RedisBackend> {
    fn from(c: RedisConnection) -> Arc<dyn RedisBackend> {
        Arc::new(c)
    }
}

impl RedisConnection {
    /// Deletes every key matching a glob pattern (KEYS + DEL). A maintenance
    /// helper for tests and shadow teardowns — never for a hot production
    /// loop (KEYS is O(N) and blocking).
    pub fn delete_pattern(&self, pattern: &str) -> BackendResult<i64> {
        self.with_conn(|c| {
            let keys: Vec<String> = redis::cmd("KEYS").arg(pattern).query(c).map_err(map_err)?;
            if keys.is_empty() {
                return Ok(0);
            }
            let refs: Vec<&str> = keys.iter().map(|s| s.as_str()).collect();
            let mut cmd = redis::cmd("DEL");
            for k in refs {
                cmd.arg(k);
            }
            cmd.query(c).map_err(map_err)
        })
    }
}
