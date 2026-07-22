use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::time::Duration;
use zydecodb_engine::engine::EngineConfig;
use zydecodb_engine::tenant_fair::FairConfig;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: SocketAddr,
    /// Optional Unix-domain-socket path to listen on in addition to TCP. Useful
    /// in multi-tenant hosts to carry local control-plane traffic without a TCP
    /// port per instance. The socket file's permissions are the trust boundary
    /// (TLS is TCP-only); API-key auth still applies.
    #[serde(default)]
    pub listen_unix: Option<PathBuf>,
    #[serde(default = "default_data_dir")]
    pub data_dir: PathBuf,
    #[serde(default = "default_wal_dir")]
    pub wal_dir: PathBuf,
    #[serde(default = "default_block_cache_mb")]
    pub block_cache_mb: usize,
    #[serde(default = "default_max_open_readers")]
    pub max_open_readers: usize,
    #[serde(default = "default_poll_compaction_ms")]
    pub poll_compaction_ms: u64,
    /// Durability model for acknowledged writes. `sync` (default) acks only
    /// after the write is fsynced; `periodic` acks after the buffered append
    /// and fsyncs every `fsync_interval_ms`.
    #[serde(default)]
    pub durability: DurabilityMode,
    /// Fsync cadence for `durability = "periodic"` (ignored for `sync`).
    #[serde(default = "default_fsync_interval_ms")]
    pub fsync_interval_ms: u64,
    #[serde(default)]
    pub shipping: ShippingConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub replica: ReplicaConfig,
    #[serde(default)]
    pub security: SecurityConfig,
    #[serde(default)]
    pub tls: TlsConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
    /// δ-fair multi-tenant isolation (pods). Off by default — enable when
    /// hosting multiple tenants on one process. See `docs/SECURITY.md`.
    #[serde(default)]
    pub fair: FairTomlConfig,
}

/// TOML surface for [`FairConfig`]. Durations are milliseconds.
#[derive(Debug, Clone, Deserialize)]
pub struct FairTomlConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "default_fair_delta_steady_ms")]
    pub delta_steady_ms: u64,
    #[serde(default = "default_fair_delta_buffer_ms")]
    pub delta_buffer_ms: u64,
    #[serde(default = "default_fair_delta_cache_ms")]
    pub delta_cache_ms: u64,
    #[serde(default = "default_fair_ramp_up_k")]
    pub ramp_up_k: u32,
    #[serde(default = "default_fair_tenant_count")]
    pub tenant_count: u32,
    /// 0 = derive from memtable flush threshold at engine open.
    #[serde(default)]
    pub memtable_total_mb: u64,
    #[serde(default)]
    pub fork_b_l0_domains: bool,
    #[serde(default = "default_fair_fork_b_l0_files")]
    pub fork_b_l0_file_threshold: u64,
    /// Optional override for L0 write-stall file count (engine).
    #[serde(default)]
    pub l0_write_stall_threshold: Option<usize>,
}

impl Default for FairTomlConfig {
    fn default() -> Self {
        FairTomlConfig {
            enabled: false,
            delta_steady_ms: default_fair_delta_steady_ms(),
            delta_buffer_ms: default_fair_delta_buffer_ms(),
            delta_cache_ms: default_fair_delta_cache_ms(),
            ramp_up_k: default_fair_ramp_up_k(),
            tenant_count: default_fair_tenant_count(),
            memtable_total_mb: 0,
            fork_b_l0_domains: false,
            fork_b_l0_file_threshold: default_fair_fork_b_l0_files(),
            l0_write_stall_threshold: None,
        }
    }
}

impl FairTomlConfig {
    pub fn to_fair_config(
        &self,
        block_cache_bytes: usize,
        memtable_flush_threshold: usize,
    ) -> FairConfig {
        let mut fair = FairConfig::default();
        fair.enabled = self.enabled;
        fair.delta_steady = Duration::from_millis(self.delta_steady_ms);
        fair.delta_buffer = Duration::from_millis(self.delta_buffer_ms);
        fair.delta_cache = Duration::from_millis(self.delta_cache_ms);
        fair.ramp_up_k = self.ramp_up_k.max(1);
        fair.tenant_count = self.tenant_count.max(1);
        fair.cache_total_bytes = block_cache_bytes as u64;
        fair.memtable_total_bytes = if self.memtable_total_mb > 0 {
            self.memtable_total_mb.saturating_mul(1024 * 1024)
        } else {
            memtable_flush_threshold as u64
        };
        fair.fork_b_l0_domains = self.fork_b_l0_domains;
        fair.fork_b_l0_file_threshold = self.fork_b_l0_file_threshold.max(1);
        fair
    }
}

fn default_fair_delta_steady_ms() -> u64 {
    50
}
fn default_fair_delta_buffer_ms() -> u64 {
    350
}
fn default_fair_delta_cache_ms() -> u64 {
    250
}
fn default_fair_ramp_up_k() -> u32 {
    6
}
fn default_fair_tenant_count() -> u32 {
    8
}
fn default_fair_fork_b_l0_files() -> u64 {
    8
}

/// Per-process runtime tuning. The `low_footprint` profile shrinks resource
/// budgets for high-density deployments that run many small instances on one
/// box (process-per-tenant or small pods).
#[derive(Debug, Clone, Deserialize, Default)]
pub struct RuntimeConfig {
    #[serde(default)]
    pub profile: RuntimeProfile,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RuntimeProfile {
    /// Standard defaults (256 MB cache, 128 readers, 50 ms compaction poll).
    #[default]
    Standard,
    /// Lower per-process footprint: smaller block cache, fewer open readers, and
    /// a slower idle compaction cadence. Trades single-instance throughput for
    /// density across many instances.
    LowFootprint,
}

/// Read-replica mode. When `from` is set the server runs read-only, ingesting
/// sha256-verified WAL segments shipped by a primary (the directory a sidecar
/// delivers `shipped.log` + segments into) and replaying them to stay caught up.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaConfig {
    /// Directory containing the primary's shipped `shipped.log` + WAL segments.
    pub from: Option<PathBuf>,
    /// How often to poll `from` for newly shipped segments.
    #[serde(default = "default_replica_poll_ms")]
    pub poll_ms: u64,
    /// File holding the shared HMAC secret that authenticates each shipped
    /// manifest entry (must match the primary's `[shipping] hmac_key_file`).
    /// Required whenever `from` is set.
    #[serde(default)]
    pub hmac_key_file: Option<PathBuf>,
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        ReplicaConfig {
            from: None,
            poll_ms: default_replica_poll_ms(),
            hmac_key_file: None,
        }
    }
}

/// WAL shipping: each sealed WAL segment is copied/hardlinked into `ship_dir`
/// for an off-box sidecar (disaster recovery, read replicas). Disabled unless
/// `ship_dir` is set.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct ShippingConfig {
    pub ship_dir: Option<PathBuf>,
    /// `"hardlink"` (default, same filesystem) or `"copy"`.
    #[serde(default)]
    pub mode: String,
    /// How often (ms) the primary refreshes the shipped-stream heartbeat so a
    /// replica can detect a dead (vs merely idle) primary. `0` disables it.
    /// Defaults to 1000ms when a config file omits it.
    #[serde(default = "default_heartbeat_ms")]
    pub heartbeat_ms: u64,
    /// File holding the shared HMAC secret. Each `shipped.log` entry carries an
    /// HMAC-SHA256 over the entry so a writable ship directory cannot forge
    /// segments plus matching manifest lines. Required when `ship_dir` is set.
    /// Generate with e.g.: `head -c 32 /dev/urandom > ship.hmac && chmod 600 ship.hmac`
    #[serde(default)]
    pub hmac_key_file: Option<PathBuf>,
}

/// Load a shipping/replica HMAC key file: raw bytes, must be non-empty.
pub fn load_hmac_key(path: &PathBuf) -> Result<Vec<u8>, String> {
    let bytes =
        std::fs::read(path).map_err(|e| format!("hmac_key_file {}: {}", path.display(), e))?;
    if bytes.iter().all(|b| b.is_ascii_whitespace()) {
        return Err(format!("hmac_key_file {} is empty", path.display()));
    }
    Ok(bytes)
}

/// Operational HTTP endpoint (Prometheus `/metrics`, `/healthz`, `/readyz`).
/// Disabled unless `listen` is set; bind to a loopback address in production
/// and scrape it from a local agent. A non-loopback bind is refused unless
/// `allow_remote = true`, and remote binds require a bearer `token`.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricsConfig {
    pub listen: Option<SocketAddr>,
    /// Emit per-tenant request counters (labeled by tenant/command/status).
    /// Opt-in: label cardinality grows with the number of tenants, so leave it
    /// off for deployments with very many tenants per process.
    #[serde(default)]
    pub per_tenant: bool,
    /// Allow binding the metrics endpoint to a non-loopback address. Off by
    /// default; when enabled, a non-empty `token` is required.
    #[serde(default)]
    pub allow_remote: bool,
    /// Bearer token required on `/metrics` when set (`Authorization: Bearer
    /// <token>`). `/healthz` and `/readyz` stay open for probes.
    #[serde(default)]
    pub token: Option<String>,
}

/// TOML durability selector. Maps to [`crate::commit::DurabilityMode`] via
/// [`Config::commit_durability`] (periodic uses `fsync_interval_ms`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum DurabilityMode {
    /// Acknowledge a write only after it is fsynced. Safe against power loss.
    #[default]
    Sync,
    /// Acknowledge after the buffered append; fsync on a fixed interval.
    Periodic,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "lowercase")]
#[derive(Default)]
pub enum RequireAuth {
    #[default]
    Auto,
    True,
    False,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SecurityConfig {
    #[serde(default)]
    pub require_auth: RequireAuth,
    #[serde(default = "default_keys_file")]
    pub keys_file: PathBuf,
    #[serde(default = "default_true")]
    pub allow_unauthenticated_ping: bool,
    #[serde(default = "default_true")]
    pub legacy_single_tenant: bool,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default = "default_rate_limit_rps")]
    pub rate_limit_rps: u32,
    #[serde(default = "default_auth_burst_limit")]
    pub auth_burst_limit: u32,
    /// Close a connection after this many seconds with no requests. Lets pooled
    /// clients hold warm connections (combine with periodic `Ping` keepalives);
    /// 0 disables the idle cap entirely.
    #[serde(default = "default_idle_timeout_secs")]
    pub idle_timeout_secs: u64,
    /// Max documents one query may buffer for an in-memory sort or a filtered
    /// multi-write candidate set. Beyond this the request is rejected so a
    /// single authenticated client cannot exhaust server memory.
    #[serde(default = "default_max_sort_buffer")]
    pub max_sort_buffer: usize,
    #[serde(default)]
    pub audit: AuditConfig,
    #[serde(default)]
    pub quotas: QuotasConfig,
}

impl Default for SecurityConfig {
    fn default() -> Self {
        SecurityConfig {
            require_auth: RequireAuth::Auto,
            keys_file: default_keys_file(),
            allow_unauthenticated_ping: true,
            legacy_single_tenant: true,
            max_connections: default_max_connections(),
            rate_limit_rps: default_rate_limit_rps(),
            auth_burst_limit: default_auth_burst_limit(),
            idle_timeout_secs: default_idle_timeout_secs(),
            max_sort_buffer: default_max_sort_buffer(),
            audit: AuditConfig::default(),
            quotas: QuotasConfig::default(),
        }
    }
}

fn default_max_sort_buffer() -> usize {
    10_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct AuditConfig {
    #[serde(default = "default_true")]
    pub enabled: bool,
    #[serde(default)]
    pub log_client_key: bool,
}

impl Default for AuditConfig {
    fn default() -> Self {
        AuditConfig {
            enabled: true,
            log_client_key: false,
        }
    }
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct QuotasConfig {
    #[serde(default)]
    pub max_bytes_per_tenant: u64,
}

#[derive(Debug, Clone, Deserialize, Default)]
pub struct TlsConfig {
    #[serde(default)]
    pub enabled: bool,
    pub cert: Option<PathBuf>,
    pub key: Option<PathBuf>,
}

fn default_listen() -> SocketAddr {
    "127.0.0.1:9470".parse().expect("listen")
}

fn default_data_dir() -> PathBuf {
    PathBuf::from("/var/lib/zydecodb/data")
}

fn default_wal_dir() -> PathBuf {
    PathBuf::from("/var/lib/zydecodb/wal")
}

fn default_block_cache_mb() -> usize {
    256
}

fn default_max_open_readers() -> usize {
    128
}

fn default_poll_compaction_ms() -> u64 {
    50
}

fn default_fsync_interval_ms() -> u64 {
    100
}

fn default_keys_file() -> PathBuf {
    PathBuf::from("/etc/zydecodb/keys.toml")
}

fn default_true() -> bool {
    true
}

fn default_max_connections() -> usize {
    256
}

fn default_rate_limit_rps() -> u32 {
    1000
}

fn default_auth_burst_limit() -> u32 {
    10
}

fn default_idle_timeout_secs() -> u64 {
    300
}

fn default_replica_poll_ms() -> u64 {
    1000
}

fn default_heartbeat_ms() -> u64 {
    1000
}

impl Config {
    pub fn from_file(path: &std::path::Path) -> Result<Self, Box<dyn std::error::Error>> {
        let text = std::fs::read_to_string(path)?;
        let mut cfg: Config = toml::from_str(&text)?;
        cfg.apply_runtime_profile();
        Ok(cfg)
    }

    /// Zero-config local defaults so `zydecodb serve` works with no config file
    /// and no root: loopback listen (`127.0.0.1:9470`), state under
    /// `~/.zydecodb/` (`data/`, `wal/`, `keys.toml`), and every other knob at
    /// its standard default. Auth stays on `auto`, which resolves to
    /// unauthenticated on a loopback bind.
    pub fn local_default() -> Result<Self, Box<dyn std::error::Error>> {
        let home = std::env::var_os("HOME")
            .filter(|h| !h.is_empty())
            .ok_or("cannot resolve local defaults: HOME is not set (pass --config <file>)")?;
        Ok(Self::local_default_with_home(std::path::Path::new(&home)))
    }

    /// [`Config::local_default`] with an explicit home directory (testable
    /// without mutating process-global env).
    pub fn local_default_with_home(home: &std::path::Path) -> Self {
        // Empty TOML yields the same serde defaults a config file would.
        let mut cfg: Config = toml::from_str("").expect("empty config deserializes to defaults");
        let base = home.join(".zydecodb");
        cfg.data_dir = base.join("data");
        cfg.wal_dir = base.join("wal");
        cfg.security.keys_file = base.join("keys.toml");
        cfg
    }

    /// Apply the low-footprint profile by shrinking per-process budgets — but only
    /// for knobs still at their standard default, so explicit config overrides
    /// always win.
    fn apply_runtime_profile(&mut self) {
        if self.runtime.profile != RuntimeProfile::LowFootprint {
            return;
        }
        if self.block_cache_mb == default_block_cache_mb() {
            self.block_cache_mb = 32;
        }
        if self.max_open_readers == default_max_open_readers() {
            self.max_open_readers = 32;
        }
        if self.poll_compaction_ms == default_poll_compaction_ms() {
            self.poll_compaction_ms = 1000;
        }
    }

    pub fn effective_require_auth(&self) -> bool {
        match self.security.require_auth {
            RequireAuth::True => true,
            RequireAuth::False => false,
            RequireAuth::Auto => !self.listen.ip().is_loopback(),
        }
    }

    /// Build the engine open config (serve + offline admin share this path).
    pub fn to_engine_config(&self) -> EngineConfig {
        let block_cache_bytes = self.block_cache_mb.saturating_mul(1024 * 1024);
        let memtable_flush_threshold = zydecodb_engine::keys::MEMTABLE_FLUSH_THRESHOLD;
        EngineConfig {
            data_dir: self.data_dir.clone(),
            wal_dir: self.wal_dir.clone(),
            block_cache_bytes,
            max_open_readers: self.max_open_readers,
            fair: self
                .fair
                .to_fair_config(block_cache_bytes, memtable_flush_threshold),
            l0_write_stall_threshold: self.fair.l0_write_stall_threshold,
            ..Default::default()
        }
    }

    /// Runtime commit-coordinator mode (interval comes from `fsync_interval_ms`).
    pub fn commit_durability(&self) -> crate::commit::DurabilityMode {
        match self.durability {
            DurabilityMode::Sync => crate::commit::DurabilityMode::Sync,
            DurabilityMode::Periodic => crate::commit::DurabilityMode::Periodic {
                interval: Duration::from_millis(self.fsync_interval_ms.max(1)),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fair_toml_deserializes_and_maps_to_engine() {
        let toml = r#"
listen = "127.0.0.1:9470"
data_dir = "/tmp/d"
wal_dir = "/tmp/w"
block_cache_mb = 64
[fair]
enabled = true
tenant_count = 4
delta_steady_ms = 50
delta_buffer_ms = 350
memtable_total_mb = 32
fork_b_l0_domains = false
l0_write_stall_threshold = 8
"#;
        let cfg: Config = toml::from_str(toml).unwrap();
        assert!(cfg.fair.enabled);
        assert_eq!(cfg.fair.tenant_count, 4);
        let eng = cfg.to_engine_config();
        assert!(eng.fair.enabled);
        assert_eq!(eng.fair.tenant_count, 4);
        assert_eq!(eng.fair.memtable_total_bytes, 32 * 1024 * 1024);
        assert_eq!(eng.l0_write_stall_threshold, Some(8));
    }
}

pub fn is_loopback(addr: &SocketAddr) -> bool {
    matches!(addr.ip(), IpAddr::V4(v4) if v4.is_loopback())
        || matches!(addr.ip(), IpAddr::V6(v6) if v6.is_loopback())
}
