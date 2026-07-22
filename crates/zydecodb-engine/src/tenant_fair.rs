//! δ-fair multi-tenant isolation (Phase 4–5).
//!
//! Quotas/RPS remain the outer admission layer ([`crate::policy::WritePolicy`]).
//! This module implements FairDB-style accounting inside the engine:
//! - memtable reserved + global pools (ρ_buffer)
//! - cache floor protection (ρ_cache; enforced in [`crate::block_cache`])
//! - per-tenant stall / L0 byte-token attribution
//! - optional Fork B escalation flag for per-tenant L0 domains
//!
//! **No WAL capacity reservation** — published FairDB/MS negative result; fairness
//! is enforced at memtable admission, flush attribution, and stall domains.
//!
//! Product claims and enablement: `docs/SECURITY.md` (multi-tenant sharing model).

use crate::errors::{EngineError, EngineResult};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

/// 16-byte tenant id extracted from a storage key (`KS_USER | tenant | …`).
pub type TenantId = [u8; 16];

/// Extract tenant id from a user storage key. Returns `None` for legacy
/// single-tenant keys (no 16-byte tenant segment) or non-user keys.
pub fn tenant_from_user_key(key: &[u8]) -> Option<TenantId> {
    use crate::keys::KS_USER;
    if key.first().copied() != Some(KS_USER) || key.len() < 1 + 16 {
        return None;
    }
    let mut t = [0u8; 16];
    t.copy_from_slice(&key[1..17]);
    Some(t)
}

/// Aggregate per-tenant byte counts from a flush/compaction key batch.
pub fn attribute_tenant_bytes<'a, I>(keys: I) -> HashMap<TenantId, u64>
where
    I: IntoIterator<Item = &'a [u8]>,
{
    let mut out = HashMap::new();
    for key in keys {
        if let Some(t) = tenant_from_user_key(key) {
            // Charge one unit per key if value length unknown; callers pass sizes.
            *out.entry(t).or_default() += 1;
        }
    }
    out
}

/// FairDB write-buffer reservation: minimize ρ s.t. residual reclaimable in δ
/// under k concurrent ramp-ups at flush bandwidth R with granularity B.
///
/// Approximates: ρ = f − min(f, ⌊R·δ / k⌋) with B-aligned residual.
pub fn compute_rho_buffer(
    fair_share: u64,
    flush_granularity_b: u64,
    flush_bandwidth_r: u64,
    delta: Duration,
    k: u32,
) -> u64 {
    if fair_share == 0 {
        return 0;
    }
    if k == 0 || flush_bandwidth_r == 0 {
        return fair_share;
    }
    let reclaimable = ((flush_bandwidth_r as f64) * delta.as_secs_f64() / f64::from(k)) as u64;
    let b = flush_granularity_b.max(1);
    let reclaimable = (reclaimable / b) * b;
    fair_share.saturating_sub(reclaimable.min(fair_share))
}

/// FairDB cache floor: ρ_cache ≥ f − δ·(R_read / (k·AMP)).
pub fn compute_rho_cache(
    fair_share: u64,
    read_bandwidth_r: u64,
    read_amp: u64,
    delta: Duration,
    k: u32,
) -> u64 {
    if fair_share == 0 {
        return 0;
    }
    if k == 0 || read_amp == 0 || read_bandwidth_r == 0 {
        return fair_share;
    }
    let reclaimable =
        ((read_bandwidth_r as f64) * delta.as_secs_f64() / (f64::from(k) * read_amp as f64)) as u64;
    fair_share.saturating_sub(reclaimable.min(fair_share))
}

/// Tunables for δ-fair isolation.
#[derive(Debug, Clone)]
pub struct FairConfig {
    pub enabled: bool,
    /// Steady-state victim p99 delta target (product ship: 50 ms).
    pub delta_steady: Duration,
    /// Paper-like buffer δ used to size ρ_buffer (start 200–350 ms).
    pub delta_buffer: Duration,
    /// Paper-like cache δ used to size ρ_cache.
    pub delta_cache: Duration,
    /// Concurrent ramp-up parameter *k* from FairDB (~8–10% of tenants).
    pub ramp_up_k: u32,
    /// Assumed tenant count for equal-share *f* (pods set from catalog size).
    pub tenant_count: u32,
    /// Total memtable / write-buffer budget managed by the pools.
    pub memtable_total_bytes: u64,
    /// Flush bandwidth estimate (bytes/sec) for ρ_buffer.
    pub flush_bandwidth_bytes_per_sec: u64,
    /// Flush granularity B (bytes).
    pub flush_granularity_bytes: u64,
    /// Total block-cache budget for floor sizing.
    pub cache_total_bytes: u64,
    /// Read bandwidth estimate for ρ_cache.
    pub read_bandwidth_bytes_per_sec: u64,
    /// Read amplification for ρ_cache.
    pub read_amp: u64,
    /// L0 byte-token budget per tenant before stall attribution.
    pub l0_token_budget: i64,
    /// When true, enable Fork B per-tenant L0 stall domains (see docs).
    pub fork_b_l0_domains: bool,
    /// Per-tenant L0 file contribution that triggers Fork B stall.
    pub fork_b_l0_file_threshold: u64,
}

impl Default for FairConfig {
    fn default() -> Self {
        FairConfig {
            enabled: false,
            delta_steady: Duration::from_millis(50),
            delta_buffer: Duration::from_millis(350),
            delta_cache: Duration::from_millis(250),
            ramp_up_k: 6,
            tenant_count: 8,
            memtable_total_bytes: 64 * 1024 * 1024,
            flush_bandwidth_bytes_per_sec: 64 * 1024 * 1024,
            flush_granularity_bytes: 4 * 1024 * 1024,
            cache_total_bytes: 256 * 1024 * 1024,
            read_bandwidth_bytes_per_sec: 256 * 1024 * 1024,
            read_amp: 10,
            l0_token_budget: 32 * 1024 * 1024,
            fork_b_l0_domains: false,
            fork_b_l0_file_threshold: 8,
        }
    }
}

impl FairConfig {
    pub fn fair_share_memtable(&self) -> u64 {
        let n = self.tenant_count.max(1) as u64;
        self.memtable_total_bytes / n
    }

    pub fn fair_share_cache(&self) -> u64 {
        let n = self.tenant_count.max(1) as u64;
        self.cache_total_bytes / n
    }

    pub fn rho_buffer(&self) -> u64 {
        compute_rho_buffer(
            self.fair_share_memtable(),
            self.flush_granularity_bytes,
            self.flush_bandwidth_bytes_per_sec,
            self.delta_buffer,
            self.ramp_up_k,
        )
    }

    pub fn rho_cache(&self) -> u64 {
        compute_rho_cache(
            self.fair_share_cache(),
            self.read_bandwidth_bytes_per_sec,
            self.read_amp,
            self.delta_cache,
            self.ramp_up_k,
        )
    }

    /// Equal-share floor used by the block cache when ρ formulas yield 0.
    pub fn cache_floor_bytes(&self) -> u64 {
        let rho = self.rho_cache();
        if rho > 0 {
            rho
        } else {
            self.fair_share_cache() / 4
        }
    }

    /// Reserved-pool credit per tenant. When the FairDB ρ formula yields 0
    /// (reclaimable within δ ≥ fair share — common on small pods buffers with
    /// optimistic flush bandwidth), keep a floor of `f/4` so below-fair tenants
    /// are not pure global-pool competitors. Mirrors [`Self::cache_floor_bytes`].
    pub fn memtable_reserve_bytes(&self) -> u64 {
        let rho = self.rho_buffer();
        if rho > 0 {
            rho
        } else {
            self.fair_share_memtable() / 4
        }
    }
}

#[derive(Debug, Default, Clone)]
struct TenantUsage {
    cache_bytes: u64,
    memtable_bytes: u64,
    stall_count: u64,
    l0_token_debt: i64,
    /// L0 SSTables attributed to this tenant (Fork B / stall domain).
    l0_files: u64,
    /// Bytes flushed to L0 attributed to this tenant.
    l0_bytes: u64,
    /// Reserved-pool credits remaining for this tenant.
    reserved_remaining: u64,
}

#[derive(Debug)]
struct Pools {
    global_remaining: u64,
    /// Sum of per-tenant ρ held aside from the global elastic pool.
    #[allow(dead_code)]
    reserved_pool_total: u64,
}

/// Shared fair-share accounting. Cheap to clone via `Arc`.
#[derive(Debug)]
pub struct FairShareState {
    inner: Mutex<Inner>,
}

#[derive(Debug)]
struct Inner {
    cfg: FairConfig,
    tenants: HashMap<TenantId, TenantUsage>,
    pools: Pools,
}

impl Default for FairShareState {
    fn default() -> Self {
        Self::new(FairConfig::default())
    }
}

impl FairShareState {
    pub fn new(cfg: FairConfig) -> Self {
        let rho = cfg.memtable_reserve_bytes();
        let n = cfg.tenant_count.max(1) as u64;
        let reserved_pool_total = rho.saturating_mul(n).min(cfg.memtable_total_bytes);
        let global_remaining = cfg.memtable_total_bytes.saturating_sub(reserved_pool_total);
        FairShareState {
            inner: Mutex::new(Inner {
                cfg,
                tenants: HashMap::new(),
                pools: Pools {
                    global_remaining,
                    reserved_pool_total,
                },
            }),
        }
    }

    pub fn config(&self) -> FairConfig {
        self.inner.lock().unwrap().cfg.clone()
    }

    pub fn set_config(&self, cfg: FairConfig) {
        let mut g = self.inner.lock().unwrap();
        let rho = cfg.memtable_reserve_bytes();
        let n = cfg.tenant_count.max(1) as u64;
        let reserved_pool_total = rho.saturating_mul(n).min(cfg.memtable_total_bytes);
        let global_remaining = cfg.memtable_total_bytes.saturating_sub(reserved_pool_total);
        // Reset reserved credits for known tenants to new ρ.
        for u in g.tenants.values_mut() {
            u.reserved_remaining = rho;
        }
        g.pools = Pools {
            global_remaining,
            reserved_pool_total,
        };
        g.cfg = cfg;
    }

    fn ensure_tenant(inner: &mut Inner, tenant: TenantId) -> &mut TenantUsage {
        let rho = inner.cfg.memtable_reserve_bytes();
        inner.tenants.entry(tenant).or_insert_with(|| TenantUsage {
            reserved_remaining: rho,
            ..TenantUsage::default()
        })
    }

    /// FairDB write-buffer admit: below fair share draws reserved then global;
    /// above fair share draws global only. Rejects with `EngineBusy` when pools
    /// cannot cover `bytes` (no WAL reservation path).
    ///
    /// Tenants already over fair share or over L0 token budget are additionally
    /// rejected when the global pool is below one fair-share residual — pacing
    /// them before they deepen flush/L0 pressure (well-behaved tenants still
    /// draw reserved credits).
    pub fn admit_memtable(&self, tenant: TenantId, bytes: u64) -> EngineResult<()> {
        let mut g = self.inner.lock().unwrap();
        if !g.cfg.enabled {
            return Ok(());
        }
        if bytes == 0 {
            return Ok(());
        }
        let fair_share = g.cfg.fair_share_memtable();
        let token_budget = g.cfg.l0_token_budget;
        let usage = g
            .tenants
            .get(&tenant)
            .map(|u| u.memtable_bytes)
            .unwrap_or(0);
        let token_debt = g.tenants.get(&tenant).map(|u| u.l0_token_debt).unwrap_or(0);
        let below_fair = usage < fair_share;
        let over_tokens = token_debt > token_budget;
        let over_share = usage >= fair_share;

        let need = bytes;
        let (from_reserved, from_global) = if below_fair {
            let u = Self::ensure_tenant(&mut g, tenant);
            let from_reserved = need.min(u.reserved_remaining);
            (from_reserved, need - from_reserved)
        } else {
            (0, need)
        };

        if from_global > g.pools.global_remaining {
            return Err(EngineError::EngineBusy(format!(
                "fair memtable: tenant over global pool (need {from_global}, have {})",
                g.pools.global_remaining
            )));
        }

        // Pace noisy tenants: when over share or token budget, require ample
        // global headroom (at least half a fair share) before elastic admit.
        if (over_share || over_tokens) && from_global > 0 {
            let headroom_need = (fair_share / 2).max(from_global);
            if g.pools.global_remaining < headroom_need {
                return Err(EngineError::EngineBusy(format!(
                    "fair memtable: pacing over-share tenant (global {}, need headroom {headroom_need})",
                    g.pools.global_remaining
                )));
            }
        }

        {
            let u = Self::ensure_tenant(&mut g, tenant);
            u.reserved_remaining = u.reserved_remaining.saturating_sub(from_reserved);
            u.memtable_bytes = u.memtable_bytes.saturating_add(bytes);
        }
        g.pools.global_remaining = g.pools.global_remaining.saturating_sub(from_global);
        Ok(())
    }

    /// Release memtable bytes after flush (refill reserved pool first).
    pub fn release_memtable(&self, tenant: TenantId, bytes: u64) {
        let mut g = self.inner.lock().unwrap();
        if !g.cfg.enabled || bytes == 0 {
            return;
        }
        let rho = g.cfg.memtable_reserve_bytes();
        let u = Self::ensure_tenant(&mut g, tenant);
        let was = u.memtable_bytes;
        u.memtable_bytes = u.memtable_bytes.saturating_sub(bytes);
        let freed = was - u.memtable_bytes;

        // Refill reserved up to ρ, remainder to global.
        let room = rho.saturating_sub(u.reserved_remaining);
        let to_reserved = freed.min(room);
        let to_global = freed - to_reserved;
        u.reserved_remaining = u.reserved_remaining.saturating_add(to_reserved);
        g.pools.global_remaining = g.pools.global_remaining.saturating_add(to_global);
    }

    /// Compatibility shim used by older call sites.
    pub fn adjust_memtable(&self, tenant: TenantId, delta: i64) {
        if delta >= 0 {
            let _ = self.admit_memtable(tenant, delta as u64);
        } else {
            self.release_memtable(tenant, (-delta) as u64);
        }
    }

    pub fn record_cache_delta(&self, tenant: TenantId, delta: i64) {
        let mut g = self.inner.lock().unwrap();
        if !g.cfg.enabled {
            return;
        }
        let u = Self::ensure_tenant(&mut g, tenant);
        if delta >= 0 {
            u.cache_bytes = u.cache_bytes.saturating_add(delta as u64);
        } else {
            u.cache_bytes = u.cache_bytes.saturating_sub((-delta) as u64);
        }
    }

    pub fn cache_bytes(&self, tenant: TenantId) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .tenants
            .get(&tenant)
            .map(|u| u.cache_bytes)
            .unwrap_or(0)
    }

    pub fn memtable_bytes(&self, tenant: TenantId) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .tenants
            .get(&tenant)
            .map(|u| u.memtable_bytes)
            .unwrap_or(0)
    }

    /// Usage ratio U_i/f_i for flush scheduling (lower = further behind / prefer).
    pub fn usage_ratio(&self, tenant: TenantId) -> f64 {
        let g = self.inner.lock().unwrap();
        let f = g.cfg.fair_share_memtable().max(1) as f64;
        let u = g
            .tenants
            .get(&tenant)
            .map(|t| t.memtable_bytes)
            .unwrap_or(0) as f64;
        u / f
    }

    /// Dominant tenant among attributed flush bytes (for flush fairness hint).
    pub fn pick_flush_priority_tenant(
        &self,
        attribution: &HashMap<TenantId, u64>,
    ) -> Option<TenantId> {
        if attribution.is_empty() {
            return None;
        }
        // Prefer flushing the tenant with highest U_i/f_i (most over share).
        attribution.keys().copied().max_by(|a, b| {
            self.usage_ratio(*a)
                .partial_cmp(&self.usage_ratio(*b))
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    }

    pub fn note_stall(&self, tenant: TenantId) {
        let mut g = self.inner.lock().unwrap();
        Self::ensure_tenant(&mut g, tenant).stall_count += 1;
    }

    pub fn stall_count(&self, tenant: TenantId) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .tenants
            .get(&tenant)
            .map(|u| u.stall_count)
            .unwrap_or(0)
    }

    pub fn charge_l0_tokens(&self, tenant: TenantId, bytes: i64) {
        let mut g = self.inner.lock().unwrap();
        Self::ensure_tenant(&mut g, tenant).l0_token_debt += bytes;
    }

    pub fn credit_l0_tokens(&self, tenant: TenantId, bytes: i64) {
        let mut g = self.inner.lock().unwrap();
        let u = Self::ensure_tenant(&mut g, tenant);
        u.l0_token_debt -= bytes;
        if u.l0_token_debt < 0 {
            u.l0_token_debt = 0;
        }
    }

    pub fn note_l0_add(&self, tenant: TenantId, files: u64, bytes: u64) {
        let mut g = self.inner.lock().unwrap();
        let u = Self::ensure_tenant(&mut g, tenant);
        u.l0_files = u.l0_files.saturating_add(files);
        u.l0_bytes = u.l0_bytes.saturating_add(bytes);
        u.l0_token_debt += bytes as i64;
    }

    pub fn note_l0_remove(&self, tenant: TenantId, files: u64, bytes: u64) {
        let mut g = self.inner.lock().unwrap();
        let u = Self::ensure_tenant(&mut g, tenant);
        u.l0_files = u.l0_files.saturating_sub(files);
        u.l0_bytes = u.l0_bytes.saturating_sub(bytes);
        u.l0_token_debt = (u.l0_token_debt - bytes as i64).max(0);
    }

    pub fn l0_files(&self, tenant: TenantId) -> u64 {
        self.inner
            .lock()
            .unwrap()
            .tenants
            .get(&tenant)
            .map(|u| u.l0_files)
            .unwrap_or(0)
    }

    /// Whether this tenant should absorb a stall (over fair share, token debt,
    /// or Fork B L0 domain pressure). Compare to fair share — not ρ — so a
    /// well-behaved tenant using its reserved floor is never the stall sink.
    pub fn should_attribute_stall(&self, tenant: TenantId) -> bool {
        let g = self.inner.lock().unwrap();
        if !g.cfg.enabled {
            return false;
        }
        let Some(u) = g.tenants.get(&tenant) else {
            return false;
        };
        let over_mem = u.memtable_bytes > g.cfg.fair_share_memtable();
        let over_tokens = u.l0_token_debt > g.cfg.l0_token_budget;
        let fork_b = g.cfg.fork_b_l0_domains && u.l0_files >= g.cfg.fork_b_l0_file_threshold;
        over_mem || over_tokens || fork_b
    }

    /// Whether a cache eviction victim owned by `tenant` is protected by ρ_cache.
    pub fn cache_floor_protects(&self, tenant: TenantId) -> bool {
        let g = self.inner.lock().unwrap();
        if !g.cfg.enabled {
            return false;
        }
        let floor = g.cfg.cache_floor_bytes();
        if floor == 0 {
            return false;
        }
        let used = g.tenants.get(&tenant).map(|u| u.cache_bytes).unwrap_or(0);
        used <= floor
    }

    pub fn global_pool_remaining(&self) -> u64 {
        self.inner.lock().unwrap().pools.global_remaining
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tid(b: u8) -> TenantId {
        [b; 16]
    }

    #[test]
    fn rho_buffer_shrinks_with_larger_delta() {
        let f = 128 * 1024 * 1024u64;
        let r = 64 * 1024 * 1024u64;
        let b = 4 * 1024 * 1024u64;
        let rho_tight = compute_rho_buffer(f, b, r, Duration::from_millis(200), 2);
        let rho_loose = compute_rho_buffer(f, b, r, Duration::from_millis(500), 2);
        assert!(rho_tight >= rho_loose);
        assert!(rho_tight <= f);
    }

    #[test]
    fn admit_rejects_when_global_exhausted() {
        let mut cfg = FairConfig::default();
        cfg.enabled = true;
        cfg.tenant_count = 2;
        cfg.memtable_total_bytes = 1_000_000;
        cfg.delta_buffer = Duration::from_millis(0); // full ρ = fair share
        cfg.flush_bandwidth_bytes_per_sec = 1; // reclaimable ~0
        let fair = FairShareState::new(cfg);
        // Drain global via tenant A above fair share.
        let f = fair.config().fair_share_memtable();
        fair.admit_memtable(tid(1), f).unwrap();
        // Exhaust global with more from A.
        let global = fair.global_pool_remaining();
        if global > 0 {
            fair.admit_memtable(tid(1), global).unwrap();
        }
        let err = fair.admit_memtable(tid(1), 1).unwrap_err();
        assert!(matches!(err, EngineError::EngineBusy(_)));
    }

    #[test]
    fn release_refills_reserved_then_global() {
        let mut cfg = FairConfig::default();
        cfg.enabled = true;
        cfg.tenant_count = 2;
        cfg.memtable_total_bytes = 8_000_000;
        let fair = FairShareState::new(cfg);
        let rho = fair.config().memtable_reserve_bytes();
        fair.admit_memtable(tid(1), rho).unwrap();
        fair.release_memtable(tid(1), rho);
        assert_eq!(fair.memtable_bytes(tid(1)), 0);
    }

    /// Pods-sized buffer + optimistic flush BW ⇒ ρ_formula=0; floor must still
    /// isolate a below-fair victim after noisy drains the global pool.
    #[test]
    fn memtable_reserve_floor_protects_victim_when_rho_formula_is_zero() {
        let mut cfg = FairConfig::default();
        cfg.enabled = true;
        cfg.tenant_count = 2;
        cfg.memtable_total_bytes = 8 * 1024 * 1024;
        cfg.delta_buffer = Duration::from_millis(350);
        cfg.ramp_up_k = 2;
        cfg.flush_bandwidth_bytes_per_sec = 64 * 1024 * 1024;
        cfg.flush_granularity_bytes = 4 * 1024 * 1024;
        assert_eq!(
            compute_rho_buffer(
                cfg.fair_share_memtable(),
                cfg.flush_granularity_bytes,
                cfg.flush_bandwidth_bytes_per_sec,
                cfg.delta_buffer,
                cfg.ramp_up_k,
            ),
            0,
            "precondition: formula ρ is 0 for this pods profile"
        );
        let floor = cfg.memtable_reserve_bytes();
        assert!(floor > 0, "floor must be non-zero");

        let fair = FairShareState::new(cfg);
        let noisy = tid(2);
        let victim = tid(1);
        // Noisy consumes its fair share + entire global pool.
        let mut admitted = 0u64;
        loop {
            match fair.admit_memtable(noisy, 64 * 1024) {
                Ok(()) => admitted += 64 * 1024,
                Err(_) => break,
            }
            if admitted > 16 * 1024 * 1024 {
                panic!("noisy should have hit a pool limit");
            }
        }
        assert!(admitted > 0);
        // Over-share pacing may leave up to ~f/2 global headroom; noisy is still stuck.
        assert!(
            fair.global_pool_remaining() < fair.config().fair_share_memtable(),
            "noisy should have drained most of the elastic pool"
        );
        // Victim still below fair share → reserved floor admits.
        fair.admit_memtable(victim, 4 * 1024).unwrap();
        // Further noisy demand stays rejected.
        assert!(fair.admit_memtable(noisy, 4 * 1024).is_err());
    }

    #[test]
    fn cache_floor_protects_under_floor() {
        let mut cfg = FairConfig::default();
        cfg.enabled = true;
        let fair = FairShareState::new(cfg);
        assert!(fair.cache_floor_protects(tid(1)));
        let floor = fair.config().cache_floor_bytes();
        fair.record_cache_delta(tid(1), (floor + 1) as i64);
        assert!(!fair.cache_floor_protects(tid(1)));
    }
}
