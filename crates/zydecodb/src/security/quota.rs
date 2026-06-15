use super::limits::TenantLimits;
use std::sync::Arc;
use zydecodb_engine::engine::Engine;
use zydecodb_engine::errors::{EngineError, EngineResult};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::policy::WritePolicy;

/// Per-tenant byte quota enforced via the engine WritePolicy hook.
///
/// The effective cap for a tenant is its per-tenant `max_bytes` (from the keys
/// file, via [`TenantLimits`]) when set, otherwise the global `default_max_bytes`
/// (`0` = unlimited). Per-tenant limits reload on SIGHUP without a restart.
pub struct TenantQuotaPolicy {
    default_max_bytes: u64,
    limits: Arc<TenantLimits>,
    usage: Arc<std::sync::Mutex<std::collections::HashMap<[u8; 16], u64>>>,
}

impl TenantQuotaPolicy {
    pub fn new(default_max_bytes: u64, limits: Arc<TenantLimits>) -> Self {
        TenantQuotaPolicy {
            default_max_bytes,
            limits,
            usage: Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
        }
    }

    /// Effective cap for a tenant: per-tenant override, else the global default.
    /// `0` means unlimited.
    fn cap_for(&self, tenant: &[u8; 16]) -> u64 {
        self.limits
            .max_bytes(tenant)
            .unwrap_or(self.default_max_bytes)
    }

    fn tenant_from_key(key: &[u8]) -> Option<[u8; 16]> {
        if key.first() != Some(&KS_USER) || key.len() < 1 + 16 {
            return None;
        }
        let mut tenant = [0u8; 16];
        tenant.copy_from_slice(&key[1..17]);
        Some(tenant)
    }
}

impl WritePolicy for TenantQuotaPolicy {
    fn pre_write(
        &self,
        _engine: &mut Engine,
        key: &[u8],
        value_len: usize,
        existing_value_len: Option<usize>,
        is_delete: bool,
    ) -> EngineResult<()> {
        if is_delete {
            return Ok(());
        }
        let Some(tenant) = Self::tenant_from_key(key) else {
            return Ok(());
        };
        let cap = self.cap_for(&tenant);
        if cap == 0 {
            return Ok(()); // unlimited for this tenant
        }
        let usage = self.usage.lock().unwrap();
        let current = usage.get(&tenant).copied().unwrap_or(0);
        let freed = existing_value_len.unwrap_or(0) as u64;
        let new_total = current.saturating_sub(freed) + value_len as u64;
        if new_total > cap {
            return Err(EngineError::PolicyRejected(format!(
                "tenant quota exceeded ({new_total} > {cap})"
            )));
        }
        Ok(())
    }

    fn post_write(
        &self,
        _engine: &mut Engine,
        key: &[u8],
        value_len: usize,
        existing_value_len: Option<usize>,
        is_delete: bool,
    ) {
        let Some(tenant) = Self::tenant_from_key(key) else {
            return;
        };
        let mut usage = self.usage.lock().unwrap();
        let current = usage.get(&tenant).copied().unwrap_or(0);
        if is_delete {
            let freed = existing_value_len.unwrap_or(0) as u64;
            let new_total = current.saturating_sub(freed);
            if new_total == 0 {
                usage.remove(&tenant);
            } else {
                usage.insert(tenant, new_total);
            }
        } else {
            let freed = existing_value_len.unwrap_or(0) as u64;
            let new_total = current.saturating_sub(freed) + value_len as u64;
            usage.insert(tenant, new_total);
        }
    }
}
