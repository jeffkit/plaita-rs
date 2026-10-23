//! 定义装载入口的 console 读穿透（#299 §2，plaita-go
//! `pkg/worker/flowsource.go` + `console_flowsource.go` 的引擎侧对应形态）。
//!
//! 跨 Redis 部署（console 与 worker 不同 Redis）存在最终一致窗口：console
//! 的 durable publish outbox（plaita 2f1be94）把定义投递到目标 Redis 之前，
//! engine storage 里没有 `{ns}:flow:{fid}:{ver}` 键，装载方只能拿到
//! not-found。本模块让定义装载在本地 miss 时回源 console 管理面：
//!
//! ```text
//! GET /api/flows/{flow_id}/versions/{version}   → VersionView（definition 原文 + status）
//! GET /api/flows/{flow_id}                      → FlowDetailResponse（versions 清单，latest 哨兵解析用）
//! ```
//!
//! 鉴权走管理面 API-key（`X-Admin-API-Key` =
//! PLAITA_CONSOLE_ADMIN_API_KEY），租户用 `X-Tenant-ID` 头声明（API-key 是
//! 平台身份，console 按头选租户并校验租户存在性）——契约端点
//! `/api/flowVersion/semver/detail` 的租户归属由 HMAC 密钥决定、没有租户头，
//! 不适合按消息租户回源。
//!
//! 错误语义（对齐 Python #304 与 plaita-go 分类）：
//!   - console 404 / 无 published 版本 / 版本未发布 →
//!     [`FlowSourceError::NotFound`]（确定性：终态错误，进负缓存，不重投
//!     ——console 也没有，重投不可能成功）；
//!   - 网络错误 / 超时 / 5xx / 其余 4xx → [`FlowSourceError::Transient`]
//!     （瞬态：不 ack 等重投，经投递上限→DLQ 收敛）——console 不可达时
//!     装载方照常运转，**绝不把「不可达」伪造成「不存在」**；
//!   - 拉到的定义 unparsable / `ir::load` 拒载（含 hash 门
//!     `flow_hash_mismatch`）→ `NotFound`（与本地直读路径的 not-loadable
//!     口径一致：该引擎构建下永久失败，不烧重试预算）。
//!
//! 本轮落点（B4 r13 全量入位第三步）：**定义装载入口层**——嵌入式宿主经
//! 引擎装载定义的场景。本地腿由 [`LocalFlowStore`] 抽象（内置
//! [`MemoryFlowStore`]）；Redis 存储腿与消费循环（消息 → 装载 → 执行）集成
//! 等 worker crate 落地后接线（`docs/specs/rs-worker-design.md` 的
//! flowsource.rs 位），与 registry 同批次。
//!
//! 配置面（键面与 plaita-go `pkg/worker/config.go` 逐字对齐，未配置 =
//! 特性关闭、语义与现状逐字节同）：
//!
//! | env 键 | 缺省 | 说明 |
//! |--------|------|------|
//! | `PLAITA_CONSOLE_FLOW_SOURCE_URL` | 空（关） | console 管理面基址 |
//! | `PLAITA_CONSOLE_FLOW_SOURCE_API_KEY` | 空 | `X-Admin-API-Key` |
//! | `PLAITA_CONSOLE_FLOW_SOURCE_TIMEOUT_SECONDS` | 5 | 单请求帽 |
//! | `PLAITA_CONSOLE_FLOW_SOURCE_NEGATIVE_TTL_SECONDS` | 30 | 确定性 miss 负缓存窗 |
//! | `PLAITA_CONSOLE_FLOW_SOURCE_ATTEMPTS` | 2 | 瞬态重试上限 |
//! | `PLAITA_CONSOLE_FLOW_SOURCE_WRITEBACK` | 关（=1 开） | 回写本地存储（可选自愈；默认关保持 #303「删除后目标侧不复活」语义） |
//! | `PLAITA_DEFAULT_TENANT_ID` | `default` | 空/缺省租户的 `X-Tenant-ID` 拼写 |

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::de::DeserializeOwned;
use serde::Deserialize;

use crate::ir::{engine_flow_payload, flow_definition_hash, FLOW_HASH_ALGO};
use crate::model::Flow;

/// 空 version 哨兵：engine storage 把它记为 versions 集的字面成员；console
/// 侧解析为最新 published 版本（plaita-go flowsource.go `latestVersion` 同款）。
pub const LATEST_VERSION: &str = "latest";

// ---------------------------------------------------------------- 配置面

/// console 读穿透配置。[`FlowSourceConfig::from_env`] 按上表装配并对
/// 数值缺省归一；手工构造（测试/嵌入式宿主）直接给字段值。
#[derive(Debug, Clone)]
pub struct FlowSourceConfig {
    /// console 管理面基址；空 = 特性关闭（现状语义）。
    pub url: String,
    pub api_key: String,
    pub timeout: Duration,
    pub negative_ttl: Duration,
    pub attempts: u32,
    pub write_back: bool,
    pub default_tenant: String,
}

impl Default for FlowSourceConfig {
    fn default() -> Self {
        Self {
            url: String::new(),
            api_key: String::new(),
            timeout: Duration::from_secs(5),
            negative_ttl: Duration::from_secs(30),
            attempts: 2,
            write_back: false,
            default_tenant: "default".to_string(),
        }
    }
}

impl FlowSourceConfig {
    /// 具名 env 装配（键面见模块文档表）。数值键严格正整数：空缺 = 缺省，
    /// 非法 = stderr 告警回缺省（r7/r8 口径：降级必须可观测，绝不静默）。
    pub fn from_env() -> Self {
        let var = |k: &str| std::env::var(k).unwrap_or_default();
        Self {
            url: var("PLAITA_CONSOLE_FLOW_SOURCE_URL"),
            api_key: var("PLAITA_CONSOLE_FLOW_SOURCE_API_KEY"),
            timeout: Duration::from_secs(positive_int_env(
                "PLAITA_CONSOLE_FLOW_SOURCE_TIMEOUT_SECONDS",
                5,
            )),
            negative_ttl: Duration::from_secs(positive_int_env(
                "PLAITA_CONSOLE_FLOW_SOURCE_NEGATIVE_TTL_SECONDS",
                30,
            )),
            attempts: positive_int_env("PLAITA_CONSOLE_FLOW_SOURCE_ATTEMPTS", 2) as u32,
            write_back: var("PLAITA_CONSOLE_FLOW_SOURCE_WRITEBACK") == "1",
            default_tenant: {
                let v = var("PLAITA_DEFAULT_TENANT_ID");
                if v.is_empty() {
                    "default".to_string()
                } else {
                    v
                }
            },
        }
    }
}

/// 严格正整数（r8 `optional_int_env` 口径：`[+-]?\d+` 去空白后须 ≥1）。
fn parse_positive_int(raw: &str) -> Option<u64> {
    raw.trim().parse::<u64>().ok().filter(|v| *v >= 1)
}

fn positive_int_env(name: &str, default: u64) -> u64 {
    let Ok(raw) = std::env::var(name) else {
        return default;
    };
    if raw.trim().is_empty() {
        return default;
    }
    parse_positive_int(&raw).unwrap_or_else(|| {
        eprintln!("[flowsource] {name}={raw:?} is not a positive integer; using default {default}");
        default
    })
}

// ---------------------------------------------------------------- 错误面

/// 装载失败的两态分类（Go `ErrFlowNotFound`/`ErrFlowLoad` 域的 Rust 对应）。
/// 消费循环按 [`FlowSourceError::is_not_found`]（终态化/DLQ）与
/// [`FlowSourceError::is_transient`]（不 ack 等重投）分诊。
#[derive(Debug, Clone)]
pub enum FlowSourceError {
    /// 确定性不存在：本地与 console 都没有（或未发布/定义不可装载）——
    /// 终态，重投不可能成功。
    NotFound(String),
    /// 瞬态：console 不可达/超时/5xx/其余 4xx——绝不伪造成 NotFound
    /// （那会把在途执行终态化），留给载体重投收敛。
    Transient(String),
}

impl FlowSourceError {
    pub fn is_not_found(&self) -> bool {
        matches!(self, FlowSourceError::NotFound(_))
    }

    pub fn is_transient(&self) -> bool {
        matches!(self, FlowSourceError::Transient(_))
    }
}

impl fmt::Display for FlowSourceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FlowSourceError::NotFound(m) => write!(f, "flow not found: {m}"),
            FlowSourceError::Transient(m) => write!(f, "flow load transient: {m}"),
        }
    }
}

impl std::error::Error for FlowSourceError {}

// ---------------------------------------------------------------- 本地腿

/// 本地引擎存储腿（Go 侧 RedisFlowStorage 形状
/// `{ns}:flow:{fid}:{ver}` + `{ns}:flow_versions:{fid}` 的抽象）。worker
/// crate 落地时以 Redis 实现接入；嵌入式宿主用 [`MemoryFlowStore`]。
pub trait LocalFlowStore: Send + Sync {
    /// 定义原文（注入后载荷；缺失 = `None`）。
    fn get_raw(&self, tenant: &str, flow_id: &str, version: &str) -> Option<Vec<u8>>;
    /// 版本注册集（storage 的 flow_versions SET；空 = 无注册）。
    fn list_versions(&self, tenant: &str, flow_id: &str) -> Vec<String>;
    /// 回写（#299 §2 可选自愈）。只读实现返回 `Err`——FlowSource 告警但不
    /// 使本次装载失败（run 已解决，回写失败不得连坐）。
    fn put_raw(&self, tenant: &str, flow_id: &str, version: &str, raw: &[u8])
        -> Result<(), String>;
}

/// 进程内内存实现（嵌入式宿主/测试/影子夹具；console engine_sync 的本地
/// 对应写入面）。
#[derive(Default)]
pub struct MemoryFlowStore {
    entries: Mutex<HashMap<(String, String, String), Vec<u8>>>,
}

impl MemoryFlowStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// 预置一条定义（Go `PresetFlowDefinition` 的本地对应——测试/影子夹具
    /// 用；生产写入方是 console engine_sync / 读穿透回写）。
    pub fn preset(&self, tenant: &str, flow_id: &str, version: &str, raw: &[u8]) {
        self.entries.lock().unwrap().insert(
            (tenant.to_string(), flow_id.to_string(), version.to_string()),
            raw.to_vec(),
        );
    }
}

impl LocalFlowStore for MemoryFlowStore {
    fn get_raw(&self, tenant: &str, flow_id: &str, version: &str) -> Option<Vec<u8>> {
        self.entries
            .lock()
            .unwrap()
            .get(&(tenant.to_string(), flow_id.to_string(), version.to_string()))
            .cloned()
    }

    fn list_versions(&self, tenant: &str, flow_id: &str) -> Vec<String> {
        let entries = self.entries.lock().unwrap();
        let mut versions: Vec<String> = entries
            .keys()
            .filter(|(t, f, _)| t == tenant && f == flow_id)
            .map(|(_, _, v)| v.clone())
            .collect();
        versions.sort();
        versions
    }

    fn put_raw(
        &self,
        tenant: &str,
        flow_id: &str,
        version: &str,
        raw: &[u8],
    ) -> Result<(), String> {
        self.entries.lock().unwrap().insert(
            (tenant.to_string(), flow_id.to_string(), version.to_string()),
            raw.to_vec(),
        );
        Ok(())
    }
}

// ---------------------------------------------------------------- console 臂

/// console 客户端的两态错误（Go `errConsoleNotFound`/`errConsoleUnavailable`
/// 对应）：NotFound = 确定性 miss（负缓存）；Unavailable = 瞬态（重试 +
/// 上抛，绝不进负缓存——定义可能下一刻就经 outbox 到达）。
#[derive(Debug)]
enum ConsoleError {
    NotFound(String),
    Unavailable(String),
}

/// 单次响应读帽：flow 定义大而有界（服务端 golden 夹具 ~100KB 量级），
/// 64MB 远超任何真实定义，挡住失控 console OOM 装载方（Go
/// `consoleMaxBodyBytes` 同值）。
const CONSOLE_MAX_BODY_BYTES: usize = 64 << 20;

/// 读穿透尝试间的固定小停顿：真正的背压是载体重投（瞬态上抛后等重投），
/// 这里只趟过连接抖动（Go `consoleRetryBackoff` 同值）。
const CONSOLE_RETRY_BACKOFF: Duration = Duration::from_millis(250);

/// console 管理面客户端（Go `consoleFlowSource` 对应）。
#[derive(Debug, Clone)]
#[cfg_attr(not(feature = "http"), allow(dead_code))]
struct ConsoleFlowSource {
    base_url: String,
    api_key: String,
    timeout: Duration,
    attempts: u32,
}

impl ConsoleFlowSource {
    fn new(cfg: &FlowSourceConfig) -> Self {
        Self {
            base_url: cfg.url.trim_end_matches('/').to_string(),
            api_key: cfg.api_key.clone(),
            timeout: cfg.timeout,
            attempts: cfg.attempts,
        }
    }

    /// 带瞬态重试的 GET+解码：404/非瞬态立即返回；Unavailable 退避重试至
    /// attempts 耗尽（Go `get` 同构）。
    #[cfg(feature = "http")]
    fn get_json<T: DeserializeOwned>(&self, tenant: &str, path: &str) -> Result<T, ConsoleError> {
        let mut last: Option<ConsoleError> = None;
        for attempt in 0..self.attempts.max(1) {
            if attempt > 0 {
                std::thread::sleep(CONSOLE_RETRY_BACKOFF);
            }
            match self.get_once_json::<T>(tenant, path) {
                Ok(v) => return Ok(v),
                Err(e) if matches!(e, ConsoleError::Unavailable(_)) => last = Some(e),
                Err(e) => return Err(e),
            }
        }
        Err(last.unwrap_or_else(|| {
            ConsoleError::Unavailable("console: no attempts configured".to_string())
        }))
    }

    /// 无 `http` 特性的构建：特征臂不存在。配置面在
    /// [`FlowSource::new`] 已 fail-closed 拒绝（配置了 URL 却无 http 支持 =
    /// 构造错误），此臂仅作为类型完备性兜底。
    #[cfg(not(feature = "http"))]
    fn get_json<T: DeserializeOwned>(&self, _tenant: &str, _path: &str) -> Result<T, ConsoleError> {
        Err(ConsoleError::Unavailable(
            "console flow source: built without the `http` feature".to_string(),
        ))
    }

    #[cfg(feature = "http")]
    fn get_once_json<T: DeserializeOwned>(
        &self,
        tenant: &str,
        path: &str,
    ) -> Result<T, ConsoleError> {
        use std::io::Read;

        let url = format!("{}{}", self.base_url, path);
        let mut req = ureq::request("GET", &url).timeout(self.timeout);
        req = req.set("Accept", "application/json");
        if !self.api_key.is_empty() {
            req = req.set("X-Admin-API-Key", &self.api_key);
        }
        if !tenant.is_empty() {
            req = req.set("X-Tenant-ID", tenant);
        }
        let resp = match req.call() {
            Ok(resp) => resp,
            Err(ureq::Error::Status(404, _)) => {
                return Err(ConsoleError::NotFound("console: not found".to_string()))
            }
            // 其余 4xx/5xx：console 侧异常，瞬态（Go：HTTP %d → Unavailable）
            Err(ureq::Error::Status(code, _)) => {
                return Err(ConsoleError::Unavailable(format!("console: HTTP {code}")))
            }
            Err(e) => return Err(ConsoleError::Unavailable(format!("console: {e}"))),
        };
        let mut body = Vec::new();
        resp.into_reader()
            .take(CONSOLE_MAX_BODY_BYTES as u64 + 1)
            .read_to_end(&mut body)
            .map_err(|e| ConsoleError::Unavailable(format!("console: read response: {e}")))?;
        if body.len() > CONSOLE_MAX_BODY_BYTES {
            return Err(ConsoleError::Unavailable(format!(
                "console: response body exceeds {CONSOLE_MAX_BODY_BYTES} bytes"
            )));
        }
        serde_json::from_slice(&body)
            .map_err(|e| ConsoleError::Unavailable(format!("console: decode response: {e}")))
    }

    /// GET /api/flows/{fid}/versions/{ver} → VersionView。
    fn version_detail(
        &self,
        tenant: &str,
        flow_id: &str,
        version: &str,
    ) -> Result<ConsoleVersion, ConsoleError> {
        let path = format!(
            "/api/flows/{}/versions/{}",
            path_escape(flow_id),
            path_escape(version)
        );
        self.get_json(tenant, &path)
    }

    /// latest 哨兵在 console 侧的解析规则：「最新 published 版本」——按
    /// published_at（缺则 created_at）取新，version 串决胜（Go
    /// `latestPublished` 同款）。
    fn latest_published(&self, tenant: &str, flow_id: &str) -> Result<String, ConsoleError> {
        #[derive(Deserialize, Default)]
        #[serde(default)]
        struct FlowDetail {
            versions: Vec<VersionSummary>,
        }
        #[derive(Deserialize, Default)]
        #[serde(default)]
        struct VersionSummary {
            version: String,
            status: String,
            published_at: String,
            created_at: String,
        }
        let path = format!("/api/flows/{}", path_escape(flow_id));
        let detail: FlowDetail = self.get_json(tenant, &path)?;
        let mut best: Option<(String, String)> = None;
        for v in detail.versions {
            if v.status != "published" {
                continue;
            }
            let mut key = v.published_at;
            if key.is_empty() {
                key = v.created_at;
            }
            key.push('\u{0}');
            key.push_str(&v.version);
            if best.as_ref().map_or(true, |(_, bk)| key > *bk) {
                best = Some((v.version, key));
            }
        }
        best.map(|(v, _)| v)
            .ok_or_else(|| ConsoleError::NotFound("console: no published version".to_string()))
    }
}

/// VersionView 的消费切片（console `flow_versions.definition` = 定义存储
/// 原文，未注入 flow_id/version——注入在引擎载荷按 outbox 口径做）。
#[derive(Deserialize, Default)]
#[serde(default)]
struct ConsoleVersion {
    #[allow(dead_code)]
    flow_id: String,
    version: String,
    status: String,
    definition: String,
}

/// Go `url.PathEscape` 的保守对应：RFC-8259 unreserved 之外的字节全部
/// 百分号编码（服务端解码等价，绝不让 `/` 改变路径形状）。
fn path_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

// ---------------------------------------------------------------- FlowSource

/// 已解析的 flow：注入后载荷 + 指纹 + 已装载引擎流程（Go `FlowDefinition`
/// 对应）。
#[derive(Debug)]
pub struct FlowDefinition {
    pub flow_id: String,
    pub version: String,
    /// 引擎存储形状的定义原文（canonical 注入后载荷；指纹计算域）。
    pub raw: Vec<u8>,
    pub hash: String,
    pub hash_algo: &'static str,
    pub flow: Flow,
}

/// 定义装载入口（Go `FlowSource` 的引擎侧对应）：本地腿命中即返回（零
/// console 调用）；miss 且配置了 [`FlowSourceConfig::url`] 时回源 console
/// （#299 §2），未配置 = 特性关闭、miss 即确定性 NotFound（现状语义）。
pub struct FlowSource {
    store: Arc<dyn LocalFlowStore>,
    console: Option<ConsoleFlowSource>,
    cfg: FlowSourceConfig,
    /// 确定性 miss 的负缓存（Go `negative` 对应）：只由 NotFound 路径写入，
    /// 瞬态绝不写入。
    negative: Mutex<HashMap<String, Instant>>,
}

impl FlowSource {
    /// 未配置 url（特性关闭）与配置了 url 两种形态都从这里出；配置了
    /// url 但构建无 `http` 特性 = 具名构造错误（fail-closed，不留一个
    /// 静默失效的回源臂）。
    pub fn new(store: Arc<dyn LocalFlowStore>, cfg: FlowSourceConfig) -> Result<Self, String> {
        #[cfg(not(feature = "http"))]
        if !cfg.url.is_empty() {
            return Err(
                "console flow source: PLAITA_CONSOLE_FLOW_SOURCE_URL is configured but this \
                 build has no `http` feature (rebuild with default features); refusing a \
                 silently inert read-through arm"
                    .to_string(),
            );
        }
        let console = if cfg.url.is_empty() {
            None
        } else {
            Some(ConsoleFlowSource::new(&cfg))
        };
        Ok(Self {
            store,
            console,
            cfg,
            negative: Mutex::new(HashMap::new()),
        })
    }

    /// env 装配（生产入口；缺省归一见 [`FlowSourceConfig::from_env`]）。
    pub fn from_env(store: Arc<dyn LocalFlowStore>) -> Result<Self, String> {
        Self::new(store, FlowSourceConfig::from_env())
    }

    /// 解析 flow_id(+version?) 到已装载流程。本地与 console 都不存在（或
    /// 未配置回源臂）= 确性 [`FlowSourceError::NotFound`]；console 不可达 /
    /// 装载 IO = [`FlowSourceError::Transient`]。
    pub fn get_flow(
        &self,
        tenant: &str,
        flow_id: &str,
        version: &str,
    ) -> Result<FlowDefinition, FlowSourceError> {
        let cache_key = format!("{tenant}:{flow_id}:{version}");
        let resolved = self.resolve_version(tenant, flow_id, version);
        if let Some(raw) = self.store.get_raw(tenant, flow_id, &resolved) {
            return self.load_local(flow_id, &resolved, raw);
        }
        self.pull_through(tenant, flow_id, &resolved, &cache_key)
    }

    /// 空 version 规则（storage/redis.py:764-782 口径）：prefer 字面
    /// `latest` 成员，否则数值最大版本；注册集为空 → `latest` 哨兵交由
    /// console 侧解析。
    fn resolve_version(&self, tenant: &str, flow_id: &str, version: &str) -> String {
        if !version.is_empty() {
            return version.to_string();
        }
        let versions = self.store.list_versions(tenant, flow_id);
        if versions.is_empty() || versions.iter().any(|v| v == LATEST_VERSION) {
            return LATEST_VERSION.to_string();
        }
        let mut versions = versions;
        versions.sort_by(|a, b| numeric_version_cmp(a, b));
        versions
            .last()
            .cloned()
            .unwrap_or_else(|| LATEST_VERSION.to_string())
    }

    /// 本地直读路径（Go Redis 命中分支同构）：指纹先算（瞬态域），装载拒
    /// （ir 拒载清单，含 hash 门）= 确定性 not-loadable。
    fn load_local(
        &self,
        flow_id: &str,
        version: &str,
        raw: Vec<u8>,
    ) -> Result<FlowDefinition, FlowSourceError> {
        let hash = flow_definition_hash(&raw).map_err(|e| {
            FlowSourceError::Transient(format!("flow {flow_id}:{version} fingerprint: {e}"))
        })?;
        let flow = crate::ir::load(&raw).map_err(|e| {
            FlowSourceError::NotFound(format!("flow {flow_id}:{version} not loadable: {e}"))
        })?;
        Ok(FlowDefinition {
            flow_id: flow_id.to_string(),
            version: version.to_string(),
            raw,
            hash,
            hash_algo: FLOW_HASH_ALGO,
            flow,
        })
    }

    /// #299 §2 读穿透：本地键缺失，回源 console 管理面拉已发布定义。未
    /// 配置 = 精确的现状 miss 行为（确定性 NotFound，零 HTTP 面）。
    fn pull_through(
        &self,
        tenant: &str,
        flow_id: &str,
        resolved: &str,
        cache_key: &str,
    ) -> Result<FlowDefinition, FlowSourceError> {
        let Some(console) = &self.console else {
            return Err(FlowSourceError::NotFound(format!(
                "flow {flow_id} version {resolved}"
            )));
        };
        if self.negative_get(cache_key) {
            return Err(FlowSourceError::NotFound(format!(
                "flow {flow_id} version {resolved} (console lookup negative-cached)"
            )));
        }

        let tenant_header = self.console_tenant(tenant);
        let mut concrete = resolved.to_string();
        if concrete == LATEST_VERSION {
            concrete = console
                .latest_published(&tenant_header, flow_id)
                .map_err(|e| self.pull_through_err(cache_key, flow_id, resolved, e))?;
        }

        let detail = console
            .version_detail(&tenant_header, flow_id, &concrete)
            .map_err(|e| self.pull_through_err(cache_key, flow_id, &concrete, e))?;
        if detail.status != "published" {
            self.negative_put(cache_key);
            return Err(FlowSourceError::NotFound(format!(
                "flow {flow_id} version {concrete} not published on console (status {:?})",
                detail.status
            )));
        }

        // 引擎载荷（outbox 的 engine_payload 口径）：注入 flow_id/version
        // 后的 canonical 字节——其 sha256 即 #60 指纹；喂 ir::load 时 r11
        // 内嵌指纹门在同路径天然执法。
        let raw = engine_flow_payload(flow_id, &concrete, detail.definition.as_bytes())
            .map_err(|e| {
                self.negative_put(cache_key);
                // 确定性（#304 口径）：published-but-unparsable 的定义重投也
                // 不会成功——终态化而非烧投递预算。
                FlowSourceError::NotFound(format!(
                    "console flow {flow_id}:{concrete} definition unparsable: {e}"
                ))
            })?
            .into_bytes();
        let hash = flow_definition_hash(&raw).map_err(|e| {
            self.negative_put(cache_key);
            FlowSourceError::NotFound(format!(
                "console flow {flow_id}:{concrete} fingerprint: {e}"
            ))
        })?;
        let flow = crate::ir::load(&raw).map_err(|e| {
            self.negative_put(cache_key);
            FlowSourceError::NotFound(format!("flow {flow_id}:{concrete} not loadable: {e}"))
        })?;

        if self.cfg.write_back {
            // 可选自愈（#299 §2）：把拉到的定义落回本地存储，同存储的兄弟
            // 装载不再 miss。尽力而为——本次装载已解决，回写失败只告警。
            // 默认关：显式 opt-in 使读穿透对 #303「删除后目标侧不复活」
            // 的清除语义保持只读。
            if let Err(e) = self.store.put_raw(tenant, flow_id, &concrete, &raw) {
                eprintln!(
                    "[flowsource] console flow source: write-back failed for \
                     {flow_id}:{concrete}: {e}"
                );
            }
        }

        Ok(FlowDefinition {
            flow_id: flow_id.to_string(),
            version: concrete,
            raw,
            hash,
            hash_algo: FLOW_HASH_ALGO,
            flow,
        })
    }

    /// console 客户端错误 → 装载错误分类：not-found → 确定性 NotFound
    /// （+负缓存）；瞬态 → Transient **无负缓存**（定义可能下一刻就经
    /// outbox 到达——本特性为之而存在的投递窗口）。
    fn pull_through_err(
        &self,
        cache_key: &str,
        flow_id: &str,
        version: &str,
        err: ConsoleError,
    ) -> FlowSourceError {
        match err {
            ConsoleError::NotFound(why) => {
                self.negative_put(cache_key);
                FlowSourceError::NotFound(format!(
                    "flow {flow_id} version {version} (console: {why})"
                ))
            }
            ConsoleError::Unavailable(why) => {
                FlowSourceError::Transient(format!("console flow source: {why}"))
            }
        }
    }

    /// 任务租户 → `X-Tenant-ID` 头值：空/缺省租户按具名缺省拼写（console
    /// 的租户头必须指向真实租户行，镜像 cfg.Namespace 的缺省路由）。
    fn console_tenant(&self, tenant: &str) -> String {
        if tenant.is_empty() || tenant == self.cfg.default_tenant {
            self.cfg.default_tenant.clone()
        } else {
            tenant.to_string()
        }
    }

    fn negative_get(&self, key: &str) -> bool {
        let mut negative = self.negative.lock().unwrap();
        match negative.get(key) {
            Some(ts) if ts.elapsed() <= self.cfg.negative_ttl => true,
            Some(_) => {
                negative.remove(key);
                false
            }
            None => false,
        }
    }

    fn negative_put(&self, key: &str) {
        self.negative
            .lock()
            .unwrap()
            .insert(key.to_string(), Instant::now());
    }
}

/// storage/redis.py 数值版本序：去 `v` 前缀（有则去无则留）按数值比，
/// 任一侧解析失败退字典序（Go `numericVersionLess` 同口径）。
fn numeric_version_cmp(a: &str, b: &str) -> std::cmp::Ordering {
    let pa = a.strip_prefix('v').unwrap_or(a).parse::<f64>().ok();
    let pb = b.strip_prefix('v').unwrap_or(b).parse::<f64>().ok();
    match (pa, pb) {
        (Some(x), Some(y)) => x.partial_cmp(&y).unwrap_or(std::cmp::Ordering::Equal),
        _ => a.cmp(b),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_positive_int_is_strict() {
        assert_eq!(parse_positive_int("5"), Some(5));
        assert_eq!(parse_positive_int(" 30 "), Some(30));
        assert_eq!(parse_positive_int("0"), None);
        assert_eq!(parse_positive_int("-5"), None);
        assert_eq!(parse_positive_int("1.5"), None);
        assert_eq!(parse_positive_int("1e3"), None);
        assert_eq!(parse_positive_int(""), None);
        assert_eq!(parse_positive_int("abc"), None);
    }

    #[test]
    fn path_escape_keeps_path_shape() {
        assert_eq!(path_escape("hello_world"), "hello_world");
        assert_eq!(path_escape("3"), "3");
        assert_eq!(path_escape("v1.2"), "v1.2");
        assert_eq!(
            path_escape("a/b"),
            "a%2Fb",
            "slash must not change path shape"
        );
        assert_eq!(path_escape("a b"), "a%20b");
        assert_eq!(path_escape("中"), "%E4%B8%AD");
    }

    #[test]
    fn numeric_version_prefers_numeric_order() {
        let mut v = vec!["10", "9", "v2", "latest-x"];
        v.sort_by(|a, b| numeric_version_cmp(a, b));
        // v2=2.0 数值最小居首；9<10 数值序（非字典序）；latest-x 解析不了
        // 退字典序垫底。
        assert_eq!(v, vec!["v2", "9", "10", "latest-x"]);
    }

    #[test]
    fn unconfigured_default_has_empty_url() {
        let cfg = FlowSourceConfig::default();
        assert!(cfg.url.is_empty(), "feature must default to off");
        assert!(!cfg.write_back, "write-back must default to off");
        assert_eq!(cfg.timeout, Duration::from_secs(5));
        assert_eq!(cfg.negative_ttl, Duration::from_secs(30));
        assert_eq!(cfg.attempts, 2);
        assert_eq!(cfg.default_tenant, "default");
    }

    #[test]
    fn error_taxonomy_predicates() {
        let nf = FlowSourceError::NotFound("x".into());
        let tr = FlowSourceError::Transient("y".into());
        assert!(nf.is_not_found() && !nf.is_transient());
        assert!(tr.is_transient() && !tr.is_not_found());
    }
}
