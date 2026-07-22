//! Shared helpers for engine soak binaries.

#![allow(dead_code)]

/// Seeded LCG (same constants as determinism tests).
pub struct Lcg(pub u64);

impl Lcg {
    pub fn new(seed: u64) -> Self {
        Lcg(seed.max(1))
    }
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.0
    }
    pub fn range_u32(&mut self, n: u32) -> u32 {
        (self.next_u64() >> 32) as u32 % n.max(1)
    }
    pub fn range_usize(&mut self, lo: usize, hi_inclusive: usize) -> usize {
        if hi_inclusive <= lo {
            return lo;
        }
        let span = (hi_inclusive - lo + 1) as u32;
        lo + self.range_u32(span) as usize
    }
}

/// Percentile of microsecond samples. `p` in \[0.0, 1.0\].
pub fn percentile_us(samples: &mut [u64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_unstable();
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[idx.min(samples.len() - 1)] as f64
}

/// Percentile where `p` is 0–100 (engine-soak LatencyWindow style).
pub fn percentile_us_pct(samples: &[u64], p: f64) -> u64 {
    if samples.is_empty() {
        return 0;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_unstable();
    let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}
