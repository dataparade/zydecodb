use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;

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
}

impl Default for ReplicaConfig {
    fn default() -> Self {
        ReplicaConfig {
            from: None,
            poll_ms: default_replica_poll_ms(),
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
}

/// Operational HTTP endpoint (Prometheus `/metrics`, `/healthz`, `/readyz`).
/// Disabled unless `listen` is set; bind to a loopback address in production
/// and scrape it from a local agent.
#[derive(Debug, Clone, Deserialize, Default)]
pub struct MetricsConfig {
    pub listen: Option<SocketAddr>,
    /// Emit per-tenant request counters (labeled by tenant/command/status).
    /// Opt-in: label cardinality grows with the number of tenants, so leave it
    /// off for deployments with very many tenants per process.
    #[serde(default)]
    pub per_tenant: bool,
}

/// Durability model selector (mirrors [`crate::commit::DurabilityMode`]).
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
            audit: AuditConfig::default(),
            quotas: QuotasConfig::default(),
        }
    }
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
}

pub fn is_loopback(addr: &SocketAddr) -> bool {
    matches!(addr.ip(), IpAddr::V4(v4) if v4.is_loopback())
        || matches!(addr.ip(), IpAddr::V6(v6) if v6.is_loopback())
}
