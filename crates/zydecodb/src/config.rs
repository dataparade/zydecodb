use serde::Deserialize;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
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
    /// hosting multiple tenants on one process. See `docs/GUIDE.md#security`.
    #[serde(default)]
    pub fair: FairTomlConfig,
    /// Bounded aggregation resource limits (`[aggregation]`).
    #[serde(default)]
    pub aggregation: AggregationConfig,
    /// Change-stream retention and subscription caps (`[change_streams]`).
    /// Disabled unless `enabled = true`.
    #[serde(default)]
    pub change_streams: ChangeStreamsConfig,
}

/// Change-stream retention and subscription caps.
#[derive(Debug, Clone, Deserialize)]
pub struct ChangeStreamsConfig {
    #[serde(default)]
    pub enabled: bool,
    /// Directory for retained WAL segment archives. Default: `<data_dir>/change_log`.
    #[serde(default)]
    pub archive_dir: Option<PathBuf>,
    #[serde(default = "default_cs_retention_secs")]
    pub retention_secs: u64,
    #[serde(default = "default_cs_retention_bytes")]
    pub retention_bytes: u64,
    #[serde(default = "default_cs_heartbeat_ms")]
    pub heartbeat_ms: u64,
    #[serde(default = "default_cs_write_timeout_ms")]
    pub write_timeout_ms: u64,
    #[serde(default = "default_cs_max_subscriptions")]
    pub max_subscriptions: usize,
    #[serde(default = "default_cs_max_subscriptions_per_tenant")]
    pub max_subscriptions_per_tenant: usize,
}

impl Default for ChangeStreamsConfig {
    fn default() -> Self {
        ChangeStreamsConfig {
            enabled: false,
            archive_dir: None,
            retention_secs: default_cs_retention_secs(),
            retention_bytes: default_cs_retention_bytes(),
            heartbeat_ms: default_cs_heartbeat_ms(),
            write_timeout_ms: default_cs_write_timeout_ms(),
            max_subscriptions: default_cs_max_subscriptions(),
            max_subscriptions_per_tenant: default_cs_max_subscriptions_per_tenant(),
        }
    }
}

fn default_cs_retention_secs() -> u64 {
    3600
}
fn default_cs_retention_bytes() -> u64 {
    1024 * 1024 * 1024
}
fn default_cs_heartbeat_ms() -> u64 {
    15_000
}
fn default_cs_write_timeout_ms() -> u64 {
    5_000
}
fn default_cs_max_subscriptions() -> usize {
    128
}
fn default_cs_max_subscriptions_per_tenant() -> usize {
    8
}

/// Per-request aggregation budgets. Defaults match
/// [`zydecodb_document::aggregation::AggregationLimits`].
#[derive(Debug, Clone, Deserialize)]
pub struct AggregationConfig {
    #[serde(default = "default_agg_max_scan_docs")]
    pub max_scan_docs: usize,
    #[serde(default = "default_agg_max_groups")]
    pub max_groups: usize,
    #[serde(default = "default_agg_max_memory_bytes")]
    pub max_memory_bytes: usize,
    #[serde(default = "default_agg_max_result_bytes")]
    pub max_result_bytes: usize,
}

impl Default for AggregationConfig {
    fn default() -> Self {
        AggregationConfig {
            max_scan_docs: default_agg_max_scan_docs(),
            max_groups: default_agg_max_groups(),
            max_memory_bytes: default_agg_max_memory_bytes(),
            max_result_bytes: default_agg_max_result_bytes(),
        }
    }
}

fn default_agg_max_scan_docs() -> usize {
    100_000
}
fn default_agg_max_groups() -> usize {
    10_000
}
fn default_agg_max_memory_bytes() -> usize {
    16 * 1024 * 1024
}
fn default_agg_max_result_bytes() -> usize {
    4 * 1024 * 1024
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
    // HMAC with a short key degrades to guessable segment tags: anyone who can
    // write to the ship dir can forge a shipped.log entry a replica trusts.
    if bytes.len() < 32 {
        return Err(format!(
            "hmac_key_file {} holds {} bytes; HMAC keys must be at least 32 bytes \
             (e.g. `head -c 32 /dev/urandom > {}`)",
            path.display(),
            bytes.len(),
            path.display()
        ));
    }
    Ok(bytes)
}

/// Refuse secret files that are group/world-accessible on Unix. Missing files
/// are skipped (other startup paths already handle absence).
fn refuse_insecure_secret_file(path: &Path, label: &str) -> Result<(), String> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let meta = match std::fs::metadata(path) {
            Ok(m) => m,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => {
                return Err(format!("cannot stat {label} {}: {e}", path.display()));
            }
        };
        let mode = meta.permissions().mode();
        if mode & 0o077 != 0 {
            return Err(format!(
                "refusing to start: {label} {} is group/world-accessible \
                 (mode {:04o}); run `chmod 600 {}` so only the owner can read it",
                path.display(),
                mode & 0o777,
                path.display(),
            ));
        }
    }
    #[cfg(not(unix))]
    {
        let _ = (path, label);
    }
    Ok(())
}

/// True when `a` and `b` refer to the same directory (canonicalize when both
/// exist; otherwise compare cleaned path components).
fn paths_equal(a: &Path, b: &Path) -> bool {
    if let (Ok(ca), Ok(cb)) = (std::fs::canonicalize(a), std::fs::canonicalize(b)) {
        return ca == cb;
    }
    let a: PathBuf = a.components().collect();
    let b: PathBuf = b.components().collect();
    a == b
}

fn refuse_path_overlap(
    left_label: &str,
    left: &Path,
    right_label: &str,
    right: Option<&Path>,
) -> Result<(), String> {
    let Some(right) = right else {
        return Ok(());
    };
    if paths_equal(left, right) {
        return Err(format!(
            "refusing to start: {left_label} ({}) must not equal {right_label} ({}) — \
             give the engine its own data/wal directories, separate from the ship/replica \
             staging path",
            left.display(),
            right.display(),
        ));
    }
    Ok(())
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

    /// Production startup guards for `zydecodb serve`. Call before bind.
    ///
    /// Refuses world-readable secret files (Unix), overlapping data/ship/replica
    /// directories, and shipping/replica without an HMAC key. Warns when auth is
    /// required but `max_bytes_per_tenant` is unlimited.
    pub fn validate_serve_startup(&self) -> Result<(), String> {
        refuse_insecure_secret_file(&self.security.keys_file, "security.keys_file")?;
        if let Some(path) = self.shipping.hmac_key_file.as_ref() {
            refuse_insecure_secret_file(path, "shipping.hmac_key_file")?;
        }
        if let Some(path) = self.replica.hmac_key_file.as_ref() {
            refuse_insecure_secret_file(path, "replica.hmac_key_file")?;
        }

        if self.shipping.ship_dir.is_some() && self.shipping.hmac_key_file.is_none() {
            return Err(
                "shipping.ship_dir is set but shipping.hmac_key_file is missing — \
                 shipped manifests must be HMAC-authenticated. Generate a key with \
                 `head -c 32 /dev/urandom > /etc/zydecodb/ship.hmac && chmod 600 \
                 /etc/zydecodb/ship.hmac`, set hmac_key_file to that path, and share \
                 the same file with every replica"
                    .into(),
            );
        }
        if self.replica.from.is_some() && self.replica.hmac_key_file.is_none() {
            return Err(
                "replica.from is set but replica.hmac_key_file is missing — the shipped \
                 stream must be HMAC-authenticated. Set hmac_key_file to the same path \
                 used by the primary's [shipping].hmac_key_file (chmod 600)"
                    .into(),
            );
        }

        refuse_path_overlap(
            "data_dir",
            &self.data_dir,
            "shipping.ship_dir",
            self.shipping.ship_dir.as_deref(),
        )?;
        refuse_path_overlap(
            "wal_dir",
            &self.wal_dir,
            "shipping.ship_dir",
            self.shipping.ship_dir.as_deref(),
        )?;
        refuse_path_overlap(
            "data_dir",
            &self.data_dir,
            "replica.from",
            self.replica.from.as_deref(),
        )?;
        refuse_path_overlap(
            "wal_dir",
            &self.wal_dir,
            "replica.from",
            self.replica.from.as_deref(),
        )?;

        // An explicit `require_auth = false` on a routable bind is full public
        // read/write access. Warn loudly (but do not refuse — some operators
        // legitimately run behind a private network boundary).
        if matches!(self.security.require_auth, RequireAuth::False)
            && !self.listen.ip().is_loopback()
        {
            tracing::warn!(
                listen = %self.listen,
                "security.require_auth = false is set explicitly and the listen address is \
                 not loopback: every reachable client has FULL unauthenticated read/write \
                 access. This is only acceptable on a strictly private network — prefer \
                 require_auth = true (or auto) for any routable bind"
            );
        }

        if self.effective_require_auth() && self.security.quotas.max_bytes_per_tenant == 0 {
            tracing::warn!(
                "authentication is required but security.quotas.max_bytes_per_tenant = 0 \
                 (unlimited). Set a non-zero per-tenant byte quota for production \
                 (e.g. 1073741824 for 1 GiB) so one tenant cannot fill the disk"
            );
        }

        Ok(())
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
    use std::fs;
    use std::sync::Mutex;

    /// tracing::warn in validate_serve_startup is process-global; serialize tests
    /// that exercise it so they do not interleave.
    static VALIDATE_LOCK: Mutex<()> = Mutex::new(());

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

    #[test]
    fn validate_refuses_ship_dir_without_hmac() {
        let _g = VALIDATE_LOCK.lock().unwrap();
        let mut cfg = Config::local_default_with_home(Path::new("/tmp/zydeco-validate-home"));
        cfg.shipping.ship_dir = Some(PathBuf::from("/var/lib/zydecodb/ship"));
        cfg.shipping.hmac_key_file = None;
        let err = cfg.validate_serve_startup().unwrap_err();
        assert!(err.contains("hmac_key_file"), "{err}");
        assert!(err.contains("chmod 600"), "{err}");
    }

    #[test]
    fn load_hmac_key_rejects_short_keys() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("ship.hmac");
        fs::write(&path, b"short-key").unwrap();
        let err = load_hmac_key(&path).unwrap_err();
        assert!(err.contains("at least 32 bytes"), "{err}");

        fs::write(&path, [b'k'; 32]).unwrap();
        assert_eq!(load_hmac_key(&path).unwrap().len(), 32);
    }

    #[test]
    fn validate_refuses_data_dir_equal_ship_dir() {
        let _g = VALIDATE_LOCK.lock().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let shared = dir.path().join("shared");
        fs::create_dir_all(&shared).unwrap();
        let mut cfg = Config::local_default_with_home(dir.path());
        cfg.data_dir = shared.clone();
        cfg.wal_dir = dir.path().join("wal");
        fs::create_dir_all(&cfg.wal_dir).unwrap();
        cfg.shipping.ship_dir = Some(shared);
        cfg.shipping.hmac_key_file = Some(dir.path().join("ship.hmac"));
        fs::write(cfg.shipping.hmac_key_file.as_ref().unwrap(), b"k").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(
                cfg.shipping.hmac_key_file.as_ref().unwrap(),
                fs::Permissions::from_mode(0o600),
            )
            .unwrap();
        }
        let err = cfg.validate_serve_startup().unwrap_err();
        assert!(err.contains("must not equal"), "{err}");
        assert!(err.contains("shipping.ship_dir"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn validate_refuses_world_readable_keys_file() {
        let _g = VALIDATE_LOCK.lock().unwrap();
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::local_default_with_home(dir.path());
        cfg.security.keys_file = dir.path().join("keys.toml");
        fs::write(&cfg.security.keys_file, b"").unwrap();
        fs::set_permissions(&cfg.security.keys_file, fs::Permissions::from_mode(0o644)).unwrap();
        let err = cfg.validate_serve_startup().unwrap_err();
        assert!(err.contains("group/world-accessible"), "{err}");
        assert!(err.contains("chmod 600"), "{err}");
    }

    #[cfg(unix)]
    #[test]
    fn validate_accepts_owner_only_secret_files() {
        let _g = VALIDATE_LOCK.lock().unwrap();
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let mut cfg = Config::local_default_with_home(dir.path());
        cfg.data_dir = dir.path().join("data");
        cfg.wal_dir = dir.path().join("wal");
        fs::create_dir_all(&cfg.data_dir).unwrap();
        fs::create_dir_all(&cfg.wal_dir).unwrap();
        cfg.security.keys_file = dir.path().join("keys.toml");
        fs::write(&cfg.security.keys_file, b"").unwrap();
        fs::set_permissions(&cfg.security.keys_file, fs::Permissions::from_mode(0o600)).unwrap();
        cfg.validate_serve_startup().expect("owner-only keys ok");
    }
}

pub fn is_loopback(addr: &SocketAddr) -> bool {
    matches!(addr.ip(), IpAddr::V4(v4) if v4.is_loopback())
        || matches!(addr.ip(), IpAddr::V6(v6) if v6.is_loopback())
}
