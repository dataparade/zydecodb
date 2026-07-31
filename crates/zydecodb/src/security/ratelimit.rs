use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Token bucket for per-connection request rate limiting.
#[derive(Debug)]
pub struct RateLimiter {
    rps: u32,
    tokens: f64,
    last_refill: Instant,
}

impl RateLimiter {
    pub fn new(rps: u32) -> Self {
        RateLimiter {
            rps: rps.max(1),
            tokens: rps as f64,
            last_refill: Instant::now(),
        }
    }

    pub fn allow(&mut self) -> bool {
        let now = Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.last_refill = now;
        self.tokens = (self.tokens + elapsed * self.rps as f64).min(self.rps as f64);
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Tracks failed SessionInit attempts per source IP.
#[derive(Debug, Default)]
pub struct AuthBurstLimiter {
    limit: u32,
    window: Duration,
    failures: Mutex<HashMap<IpAddr, Vec<Instant>>>,
}

/// Hard bound on tracked source IPs. Without it, a distributed credential
/// flood grows the map one entry per spoofed IP until the process OOMs.
const MAX_TRACKED_IPS: usize = 10_000;

impl AuthBurstLimiter {
    pub fn new(limit: u32) -> Self {
        AuthBurstLimiter {
            limit: limit.max(1),
            window: Duration::from_secs(60),
            failures: Mutex::new(HashMap::new()),
        }
    }

    /// Oldest Instant still inside the sliding window. `None` when the host
    /// has been up for less than the window (checked_sub underflow): every
    /// recorded failure is necessarily in-window, and a naive subtraction
    /// would panic.
    fn window_cutoff(&self) -> Option<Instant> {
        Instant::now().checked_sub(self.window)
    }

    pub fn record_failure(&self, ip: IpAddr) {
        let mut map = self.failures.lock().unwrap();
        let now = Instant::now();
        let cutoff = self.window_cutoff();
        if let Some(entry) = map.get_mut(&ip) {
            if let Some(c) = cutoff {
                entry.retain(|t| *t > c);
            }
            entry.push(now);
            return;
        }
        // New IP. Sweep expired entries before growing; if the map is still
        // full, skip tracking this IP (fail-open under a distributed flood —
        // memory safety outranks per-IP precision, and the per-connection
        // and session caps still bound the damage).
        if map.len() >= MAX_TRACKED_IPS {
            map.retain(|_, ts| {
                if let Some(c) = cutoff {
                    ts.retain(|t| *t > c);
                    !ts.is_empty()
                } else {
                    true
                }
            });
            if map.len() >= MAX_TRACKED_IPS {
                return;
            }
        }
        map.insert(ip, vec![now]);
    }

    pub fn is_blocked(&self, ip: IpAddr) -> bool {
        let map = self.failures.lock().unwrap();
        let Some(entry) = map.get(&ip) else {
            return false;
        };
        match self.window_cutoff() {
            Some(cutoff) => entry.iter().filter(|t| **t > cutoff).count() >= self.limit as usize,
            None => entry.len() >= self.limit as usize,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;

    #[test]
    fn records_and_blocks_without_panicking() {
        let limiter = AuthBurstLimiter::new(3);
        let ip = IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1));
        assert!(!limiter.is_blocked(ip));
        limiter.record_failure(ip);
        limiter.record_failure(ip);
        assert!(!limiter.is_blocked(ip));
        limiter.record_failure(ip);
        assert!(limiter.is_blocked(ip));
    }

    #[test]
    fn tracked_ips_are_bounded() {
        let limiter = AuthBurstLimiter::new(5);
        // A flood of distinct IPs (plus the ones already recorded by the
        // neighbouring test in the same process is not shared — each limiter
        // has its own map).
        for i in 0..(MAX_TRACKED_IPS as u32 + 500) {
            limiter.record_failure(IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + i)));
        }
        let map = limiter.failures.lock().unwrap();
        assert!(
            map.len() <= MAX_TRACKED_IPS,
            "map must stay bounded under a distributed flood: {}",
            map.len()
        );
    }

    #[test]
    fn expired_entries_are_pruned_from_the_map() {
        let limiter = AuthBurstLimiter::new(5);
        // Backdate a failure beyond the window by pushing an Instant directly.
        // (On a host booted < 2 min ago there is no "expired" Instant to
        // construct; the sweep cannot be exercised there.)
        let Some(stale_instant) = Instant::now().checked_sub(Duration::from_secs(120)) else {
            return;
        };
        let stale = IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1));
        {
            let mut map = limiter.failures.lock().unwrap();
            map.insert(stale, vec![stale_instant]);
        }
        // Fill the map to force the sweep path.
        for i in 0..MAX_TRACKED_IPS as u32 {
            limiter.record_failure(IpAddr::V4(Ipv4Addr::from(0x0a00_0000 + i)));
        }
        {
            let map = limiter.failures.lock().unwrap();
            assert!(
                !map.contains_key(&stale),
                "expired entries must be swept, not held forever"
            );
        }
        assert!(!limiter.is_blocked(stale));
    }
}
