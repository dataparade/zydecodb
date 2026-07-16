use super::keys::KeyStore;
use super::limits::TenantLimits;
use super::ratelimit::AuthBurstLimiter;
use crate::config::Config;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Clone)]
pub struct SecurityRuntime {
    pub require_auth: bool,
    pub allow_unauthenticated_ping: bool,
    pub legacy_single_tenant: bool,
    pub keys: Arc<arc_swap::ArcSwap<KeyStore>>,
    pub audit: crate::config::AuditConfig,
    pub rate_limit_rps: u32,
    pub max_connections: usize,
    /// Close a connection idle for longer than this. `None` disables the cap.
    pub idle_timeout: Option<Duration>,
    /// Read-replica mode: reject all write/DDL commands with `Forbidden`.
    pub read_only: bool,
    pub auth_burst: Arc<AuthBurstLimiter>,
    /// Per-tenant byte caps + rate ceilings, shared with the byte-cap write
    /// policy and reloadable on SIGHUP.
    pub tenant_limits: Arc<TenantLimits>,
    active_connections: Arc<AtomicUsize>,
}

impl SecurityRuntime {
    pub fn from_config(config: &Config) -> Result<Self, super::keys::KeyError> {
        let keys = Arc::new(arc_swap::ArcSwap::from_pointee(KeyStore::load(&config.security.keys_file)?));
        let tenant_limits = Arc::new(TenantLimits::from_records(keys.load().tenant_records()));
        Ok(SecurityRuntime {
            require_auth: config.effective_require_auth(),
            allow_unauthenticated_ping: config.security.allow_unauthenticated_ping,
            legacy_single_tenant: config.security.legacy_single_tenant,
            keys,
            audit: config.security.audit.clone(),
            rate_limit_rps: config.security.rate_limit_rps,
            max_connections: config.security.max_connections,
            idle_timeout: idle_timeout_from_secs(config.security.idle_timeout_secs),
            read_only: config.replica.from.is_some(),
            auth_burst: Arc::new(AuthBurstLimiter::new(config.security.auth_burst_limit)),
            tenant_limits,
            active_connections: Arc::new(AtomicUsize::new(0)),
        })
    }

    pub fn try_acquire_connection(&self) -> bool {
        let current = self.active_connections.load(Ordering::SeqCst);
        if current >= self.max_connections {
            return false;
        }
        self.active_connections.fetch_add(1, Ordering::SeqCst);
        true
    }

    pub fn release_connection(&self) {
        self.active_connections.fetch_sub(1, Ordering::SeqCst);
    }
}

/// Default runtime for tests: no auth required.
impl Default for SecurityRuntime {
    fn default() -> Self {
        SecurityRuntime {
            require_auth: false,
            allow_unauthenticated_ping: true,
            legacy_single_tenant: true,
            keys: Arc::new(arc_swap::ArcSwap::from_pointee(KeyStore::load(std::path::Path::new("/nonexistent")).unwrap())),
            audit: crate::config::AuditConfig::default(),
            rate_limit_rps: 1000,
            max_connections: 256,
            idle_timeout: Some(Duration::from_secs(300)),
            read_only: false,
            auth_burst: Arc::new(AuthBurstLimiter::new(10)),
            tenant_limits: Arc::new(TenantLimits::default()),
            active_connections: Arc::new(AtomicUsize::new(0)),
        }
    }
}

fn idle_timeout_from_secs(secs: u64) -> Option<Duration> {
    if secs == 0 {
        None
    } else {
        Some(Duration::from_secs(secs))
    }
}
