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

impl AuthBurstLimiter {
    pub fn new(limit: u32) -> Self {
        AuthBurstLimiter {
            limit: limit.max(1),
            window: Duration::from_secs(60),
            failures: Mutex::new(HashMap::new()),
        }
    }

    pub fn record_failure(&self, ip: IpAddr) {
        let mut map = self.failures.lock().unwrap();
        let entry = map.entry(ip).or_default();
        let cutoff = Instant::now() - self.window;
        entry.retain(|t| *t > cutoff);
        entry.push(Instant::now());
    }

    pub fn is_blocked(&self, ip: IpAddr) -> bool {
        let map = self.failures.lock().unwrap();
        let Some(entry) = map.get(&ip) else {
            return false;
        };
        let cutoff = Instant::now() - self.window;
        entry.iter().filter(|t| **t > cutoff).count() >= self.limit as usize
    }
}
