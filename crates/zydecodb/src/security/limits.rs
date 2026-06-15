//! Per-tenant resource limits: configured byte caps and request-rate ceilings,
//! shared between the rate-limit check in the request path and the engine
//! write-policy byte-cap check. Backed by the `[[tenant]]` tables in the keys
//! file and reloadable in place (SIGHUP) so changes apply without a restart.

use super::keys::{parse_tenant_hex, TenantRecord};
use std::collections::HashMap;
use std::sync::{Mutex, RwLock};
use std::time::Instant;

/// One tenant's configured limits. `None` means "no per-tenant override".
#[derive(Debug, Clone, Copy, Default)]
pub struct TenantLimit {
    pub max_bytes: Option<u64>,
    pub rate_rps: Option<u32>,
}

/// Token-bucket state for one tenant's request-rate limit.
#[derive(Debug)]
struct Bucket {
    tokens: f64,
    last_refill: Instant,
}

/// Shared, reloadable per-tenant limits. Cheap to clone the `Arc` wrapping it.
#[derive(Debug, Default)]
pub struct TenantLimits {
    /// Configured caps, keyed by 16-byte tenant id. Reloaded wholesale on SIGHUP.
    limits: RwLock<HashMap<[u8; 16], TenantLimit>>,
    /// Live token buckets for rate limiting, kept separate from config so a
    /// reload does not reset a tenant's in-flight bucket.
    buckets: Mutex<HashMap<[u8; 16], Bucket>>,
}

impl TenantLimits {
    /// Build from keys-file `[[tenant]]` records, skipping any with an invalid
    /// tenant hex (they are reported at config-edit time, not here).
    pub fn from_records(records: &[TenantRecord]) -> Self {
        let limits = Self::parse_records(records);
        TenantLimits {
            limits: RwLock::new(limits),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Replace the configured limits in place (SIGHUP reload). Live rate buckets
    /// are retained; only the configured ceilings change.
    pub fn reload(&self, records: &[TenantRecord]) {
        let parsed = Self::parse_records(records);
        *self.limits.write().unwrap() = parsed;
    }

    fn parse_records(records: &[TenantRecord]) -> HashMap<[u8; 16], TenantLimit> {
        let mut map = HashMap::new();
        for r in records {
            if let Ok(tenant) = parse_tenant_hex(&r.tenant) {
                map.insert(
                    tenant,
                    TenantLimit {
                        max_bytes: r.max_bytes,
                        rate_rps: r.rate_rps,
                    },
                );
            }
        }
        map
    }

    /// Whether any tenant has a configured byte cap (used to decide whether to
    /// install the byte-cap write policy at all).
    pub fn any_byte_cap(&self) -> bool {
        self.limits
            .read()
            .unwrap()
            .values()
            .any(|l| l.max_bytes.is_some())
    }

    /// The configured byte cap for a tenant, if any.
    pub fn max_bytes(&self, tenant: &[u8; 16]) -> Option<u64> {
        self.limits
            .read()
            .unwrap()
            .get(tenant)
            .and_then(|l| l.max_bytes)
    }

    /// Consume one token from a tenant's rate bucket. Returns `true` (allowed)
    /// when the tenant has no configured rate limit. Mirrors the per-connection
    /// [`super::ratelimit::RateLimiter`] token-bucket math, but shared across all
    /// of a tenant's connections.
    pub fn allow(&self, tenant: &[u8; 16]) -> bool {
        let rps = match self
            .limits
            .read()
            .unwrap()
            .get(tenant)
            .and_then(|l| l.rate_rps)
        {
            Some(r) if r > 0 => r,
            _ => return true,
        };
        let mut buckets = self.buckets.lock().unwrap();
        let now = Instant::now();
        let bucket = buckets.entry(*tenant).or_insert_with(|| Bucket {
            tokens: rps as f64,
            last_refill: now,
        });
        let elapsed = now.duration_since(bucket.last_refill).as_secs_f64();
        bucket.last_refill = now;
        bucket.tokens = (bucket.tokens + elapsed * rps as f64).min(rps as f64);
        if bucket.tokens >= 1.0 {
            bucket.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::security::keys::TenantRecord;

    fn tenant(n: u128) -> [u8; 16] {
        n.to_be_bytes()
    }

    #[test]
    fn byte_cap_and_rate_are_per_tenant() {
        let recs = vec![TenantRecord {
            tenant: format!("{:032x}", 0x0au128),
            max_bytes: Some(100),
            rate_rps: Some(2),
        }];
        let limits = TenantLimits::from_records(&recs);
        let t = tenant(0x0a);

        assert!(limits.any_byte_cap());
        assert_eq!(limits.max_bytes(&t), Some(100));

        // 2 rps: the first two requests pass, the third (same instant) is denied.
        assert!(limits.allow(&t));
        assert!(limits.allow(&t));
        assert!(!limits.allow(&t));

        // A tenant with no configured limits is unconstrained.
        let other = tenant(0x0b);
        assert_eq!(limits.max_bytes(&other), None);
        assert!(limits.allow(&other));
    }

    #[test]
    fn reload_replaces_configured_limits() {
        let limits = TenantLimits::default();
        let t = tenant(1);
        assert_eq!(limits.max_bytes(&t), None);

        limits.reload(&[TenantRecord {
            tenant: format!("{:032x}", 1u128),
            max_bytes: Some(50),
            rate_rps: None,
        }]);
        assert_eq!(limits.max_bytes(&t), Some(50));
    }
}
