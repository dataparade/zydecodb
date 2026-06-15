//! Bloom filter for SSTable membership tests. Built at flush time (Week 4 / Step 4.3).
//! Parameters: m = 10n bits, k = 7 hash functions, using xxh3 with two seeds
//! combined via double hashing.

use xxhash_rust::xxh3::xxh3_64_with_seed;

const NUM_HASHES: u32 = 7;
const BITS_PER_KEY: usize = 10;

#[derive(Debug, Clone)]
pub struct BloomFilter {
    bits: Vec<u8>,
    num_bits: usize,
}

impl BloomFilter {
    /// Build a bloom filter sized for `n` expected keys.
    pub fn build(keys: &[Vec<u8>]) -> BloomFilter {
        let n = keys.len().max(1);
        let num_bits = (n * BITS_PER_KEY).max(64);
        let num_bytes = num_bits.div_ceil(8);
        let mut bf = BloomFilter {
            bits: vec![0u8; num_bytes],
            num_bits,
        };
        for k in keys {
            bf.add(k);
        }
        bf
    }

    fn add(&mut self, key: &[u8]) {
        let (h1, h2) = Self::hashes(key);
        for i in 0..NUM_HASHES {
            let bit = Self::nth_bit(h1, h2, i, self.num_bits);
            self.bits[bit / 8] |= 1 << (bit % 8);
        }
    }

    /// Returns true if the key *might* be present, false if *definitely absent*.
    pub fn maybe_contains(&self, key: &[u8]) -> bool {
        if self.num_bits == 0 {
            return true;
        }
        let (h1, h2) = Self::hashes(key);
        for i in 0..NUM_HASHES {
            let bit = Self::nth_bit(h1, h2, i, self.num_bits);
            if self.bits[bit / 8] & (1 << (bit % 8)) == 0 {
                return false;
            }
        }
        true
    }

    fn hashes(key: &[u8]) -> (u64, u64) {
        (
            xxh3_64_with_seed(key, 0),
            xxh3_64_with_seed(key, 0x9E3779B97F4A7C15),
        )
    }

    fn nth_bit(h1: u64, h2: u64, i: u32, num_bits: usize) -> usize {
        let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
        (combined % num_bits as u64) as usize
    }

    /// Serialize: [4 bytes num_bits BE][bit bytes].
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(4 + self.bits.len());
        buf.extend_from_slice(&(self.num_bits as u32).to_be_bytes());
        buf.extend_from_slice(&self.bits);
        buf
    }

    pub fn decode(buf: &[u8]) -> Option<BloomFilter> {
        if buf.len() < 4 {
            return None;
        }
        let num_bits = u32::from_be_bytes(buf[0..4].try_into().ok()?) as usize;
        let bits = buf[4..].to_vec();
        if bits.len() < num_bits.div_ceil(8) {
            return None;
        }
        Some(BloomFilter { bits, num_bits })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives() {
        let keys: Vec<Vec<u8>> = (0..1000u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let bf = BloomFilter::build(&keys);
        for k in &keys {
            assert!(
                bf.maybe_contains(k),
                "must never report a present key absent"
            );
        }
    }

    #[test]
    fn definite_absence_is_common() {
        let keys: Vec<Vec<u8>> = (0..1000u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let bf = BloomFilter::build(&keys);
        let mut absent_detected = 0;
        for i in 1000..2000u32 {
            if !bf.maybe_contains(&i.to_be_bytes()) {
                absent_detected += 1;
            }
        }
        // With ~1% FP rate, the vast majority of absent keys should be detected.
        assert!(absent_detected > 900, "got {}", absent_detected);
    }

    #[test]
    fn encode_decode_round_trips() {
        let keys: Vec<Vec<u8>> = (0..100u32).map(|i| i.to_be_bytes().to_vec()).collect();
        let bf = BloomFilter::build(&keys);
        let bytes = bf.encode();
        let back = BloomFilter::decode(&bytes).unwrap();
        for k in &keys {
            assert!(back.maybe_contains(k));
        }
    }
}
