//! Bounded shared block cache for SSTable data blocks.
//!
//! Before this cache existed, every [`crate::sstable::SstableReader`] held its
//! entire file content as a `Vec<u8>` in RAM — so RSS grew linearly with the
//! number of live SSTables (the soak harness measured ~70 MB per SSTable).
//! With the cache, readers fetch data blocks on demand. Index and bloom
//! metadata are pinned on each open reader (see [`crate::reader_cache`]),
//! not charged here. Hot data blocks stay cached across readers; cold
//! blocks are evicted under memory pressure.
//!
//! ## Design choices
//!
//! - **Key** is `(sstable_id, block_offset)`. Block offsets are stable within
//!   a file (the SSTable layout is immutable once written), so cached blocks
//!   remain valid until the SSTable file itself is unlinked.
//! - **Value** is `Arc<Vec<u8>>`. Readers get a cheap clone of the Arc and
//!   can hand the slice into a decoder without copying.
//! - **Eviction** is approximate LRU via a monotonically-increasing access
//!   counter. On insert-when-full we scan the map to find the lowest counter
//!   and drop it. This is O(N) per eviction, but for the default capacity
//!   (256 MB / 16 KB block = ~16k entries) it costs tens of microseconds —
//!   negligible compared to the disk read it just absorbed.
//! - **Locking** is `std::sync::Mutex` — independent of the engine write mutex
//!   (Phase 4). Phase 5a δ-fair adds per-tenant floors: eviction *skips*
//!   victims whose owner is at/below ρ_cache.
//!
//! ## Not in scope for v1
//!
//! - Eviction priority hints from scan-vs-point reads (scans should not
//!   thrash the cache; defer to a measurement-driven tuning pass).
//! - Per-shard locking (no contention to relieve).
//! - Compression (cache stores blocks as they live on disk; if/when block
//!   compression lands, the cache will store decompressed bytes — but the
//!   key/value shapes do not change).

use crate::tenant_fair::{FairShareState, TenantId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering as AOrd};
use std::sync::{Arc, Mutex};

/// A cached block, keyed by `(sstable_id, block_offset)` and reference-counted
/// so readers can hold an immutable view without copying.
pub type CachedBlock = Arc<Vec<u8>>;

/// Identity of a cached SSTable data block.
#[derive(Debug, Clone, Copy, Hash, PartialEq, Eq)]
pub struct BlockKey {
    pub sstable_id: u64,
    pub block_offset: u64,
}

impl BlockKey {
    pub fn data(sstable_id: u64, block_offset: u64) -> Self {
        BlockKey {
            sstable_id,
            block_offset,
        }
    }
}

struct Slot {
    bytes: CachedBlock,
    /// Monotonic access counter; higher = more recently used.
    last_access: u64,
    /// Tenant charged for this block (point-read insert path). `None` for
    /// unattributed inserts (legacy / compaction bypass).
    owner: Option<TenantId>,
}

struct Inner {
    map: HashMap<BlockKey, Slot>,
    bytes_used: usize,
    /// Soft cap. Insertions evict until `bytes_used <= capacity` (or the
    /// cache is empty). A single oversized block is accepted as-is rather
    /// than refused — it remains the only resident block.
    capacity: usize,
}

/// A bounded LRU-ish cache for SSTable data blocks.
///
/// Construct with [`BlockCache::new`]; share across the engine via `Arc`.
/// Capacity is in bytes; the cache evicts by approximate-LRU to stay under
/// the cap. All operations are O(1) amortized except eviction, which is
/// O(N) over the resident entry count and only runs when the cache is full.
pub struct BlockCache {
    inner: Mutex<Inner>,
    access_counter: AtomicU64,
    /// Cumulative hits/misses/evictions/bytes_read since construction.
    /// Exposed as gauges through [`BlockCache::stats`] for the metrics layer
    /// to ingest without taking the cache lock on every scrape.
    hits: AtomicU64,
    misses: AtomicU64,
    compaction_reads: AtomicU64,
    inserts: AtomicU64,
    evictions: AtomicU64,
    floor_skips: AtomicU64,
    /// Optional δ-fair accounting (Phase 5a). Set via [`BlockCache::with_fair`].
    fair: Mutex<Option<Arc<FairShareState>>>,
}

/// Point-in-time snapshot of the cache's counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub compaction_reads: u64,
    pub inserts: u64,
    pub evictions: u64,
    pub floor_skips: u64,
    pub resident_bytes: u64,
    pub resident_entries: u64,
}

impl BlockCache {
    /// Construct a cache with the given soft byte capacity.
    pub fn new(capacity_bytes: usize) -> Arc<BlockCache> {
        Arc::new(BlockCache {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                bytes_used: 0,
                capacity: capacity_bytes,
            }),
            access_counter: AtomicU64::new(0),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            compaction_reads: AtomicU64::new(0),
            inserts: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            floor_skips: AtomicU64::new(0),
            fair: Mutex::new(None),
        })
    }

    /// Attach δ-fair state for per-tenant cache floors (skip-on-evict).
    pub fn with_fair(self: &Arc<Self>, fair: Arc<FairShareState>) -> Arc<Self> {
        *self.fair.lock().expect("BlockCache fair lock") = Some(fair);
        Arc::clone(self)
    }

    /// Compaction block read that bypasses the user cache (fill_cache=false).
    pub fn record_compaction_read(&self) {
        self.compaction_reads.fetch_add(1, AOrd::Relaxed);
    }

    /// Look up a block. On hit, refreshes its LRU position. On miss, returns
    /// `None` — the caller is expected to fetch from disk and call [`insert`].
    pub fn get(&self, key: BlockKey) -> Option<CachedBlock> {
        let access = self
            .access_counter
            .fetch_add(1, AOrd::Relaxed)
            .wrapping_add(1);
        let mut g = self.inner.lock().expect("BlockCache mutex poisoned");
        match g.map.get_mut(&key) {
            Some(slot) => {
                slot.last_access = access;
                self.hits.fetch_add(1, AOrd::Relaxed);
                Some(slot.bytes.clone())
            }
            None => {
                self.misses.fetch_add(1, AOrd::Relaxed);
                None
            }
        }
    }

    /// Insert a block without tenant attribution (legacy).
    pub fn insert(&self, key: BlockKey, bytes: CachedBlock) {
        self.insert_for_tenant(key, bytes, None);
    }

    /// Insert a block charged to `owner`. Eviction skips victims protected by
    /// the FairDB cache floor (ρ_cache) when fair mode is enabled.
    pub fn insert_for_tenant(&self, key: BlockKey, bytes: CachedBlock, owner: Option<TenantId>) {
        let access = self
            .access_counter
            .fetch_add(1, AOrd::Relaxed)
            .wrapping_add(1);
        let size = bytes.len();
        let fair = self.fair.lock().expect("BlockCache fair lock").clone();

        let mut g = self.inner.lock().expect("BlockCache mutex poisoned");

        if let Some(prev) = g.map.insert(
            key,
            Slot {
                bytes,
                last_access: access,
                owner,
            },
        ) {
            g.bytes_used = g.bytes_used.saturating_sub(prev.bytes.len());
            if let (Some(fair), Some(t)) = (fair.as_ref(), prev.owner) {
                fair.record_cache_delta(t, -(prev.bytes.len() as i64));
            }
        }
        g.bytes_used = g.bytes_used.saturating_add(size);
        if let (Some(fair), Some(t)) = (fair.as_ref(), owner) {
            fair.record_cache_delta(t, size as i64);
        }
        self.inserts.fetch_add(1, AOrd::Relaxed);

        while g.bytes_used > g.capacity && g.map.len() > 1 {
            let (victim, skipped) = Self::pick_eviction_victim(&g, fair.as_deref());
            if skipped > 0 {
                self.floor_skips.fetch_add(skipped, AOrd::Relaxed);
            }
            let Some(victim) = victim else {
                // Every resident entry is floor-protected; stop rather than
                // thrash protected tenants (may briefly exceed capacity).
                break;
            };
            if let Some(removed) = g.map.remove(&victim) {
                g.bytes_used = g.bytes_used.saturating_sub(removed.bytes.len());
                if let (Some(fair), Some(t)) = (fair.as_ref(), removed.owner) {
                    fair.record_cache_delta(t, -(removed.bytes.len() as i64));
                }
                self.evictions.fetch_add(1, AOrd::Relaxed);
            }
        }
    }

    /// Approximate-LRU victim among entries not protected by the cache floor.
    /// Returns `(victim, floor_skips_examined)`.
    fn pick_eviction_victim(g: &Inner, fair: Option<&FairShareState>) -> (Option<BlockKey>, u64) {
        let mut best: Option<(BlockKey, u64)> = None;
        let mut skipped = 0u64;
        for (k, slot) in &g.map {
            if let (Some(fair), Some(t)) = (fair, slot.owner) {
                if fair.cache_floor_protects(t) {
                    skipped += 1;
                    continue;
                }
            }
            match best {
                None => best = Some((*k, slot.last_access)),
                Some((_, best_access)) if slot.last_access < best_access => {
                    best = Some((*k, slot.last_access));
                }
                _ => {}
            }
        }
        (best.map(|(k, _)| k), skipped)
    }

    /// Evict every block belonging to a given SSTable id. Called when an
    /// SSTable file is being unlinked, so stale cache entries don't shadow
    /// the now-gone file.
    pub fn invalidate_sstable(&self, sstable_id: u64) {
        let fair = self.fair.lock().expect("BlockCache fair lock").clone();
        let mut g = self.inner.lock().expect("BlockCache mutex poisoned");
        let to_remove: Vec<BlockKey> = g
            .map
            .keys()
            .filter(|k| k.sstable_id == sstable_id)
            .copied()
            .collect();
        for k in to_remove {
            if let Some(slot) = g.map.remove(&k) {
                g.bytes_used = g.bytes_used.saturating_sub(slot.bytes.len());
                if let (Some(fair), Some(t)) = (fair.as_ref(), slot.owner) {
                    fair.record_cache_delta(t, -(slot.bytes.len() as i64));
                }
            }
        }
    }

    /// Snapshot the counters for metrics ingestion.
    pub fn stats(&self) -> CacheStats {
        let g = self.inner.lock().expect("BlockCache mutex poisoned");
        CacheStats {
            hits: self.hits.load(AOrd::Relaxed),
            misses: self.misses.load(AOrd::Relaxed),
            compaction_reads: self.compaction_reads.load(AOrd::Relaxed),
            inserts: self.inserts.load(AOrd::Relaxed),
            evictions: self.evictions.load(AOrd::Relaxed),
            floor_skips: self.floor_skips.load(AOrd::Relaxed),
            resident_bytes: g.bytes_used as u64,
            resident_entries: g.map.len() as u64,
        }
    }

    /// Soft byte capacity. Useful for tests and for the metrics layer.
    pub fn capacity(&self) -> usize {
        let g = self.inner.lock().expect("BlockCache mutex poisoned");
        g.capacity
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tenant_fair::FairConfig;
    use std::time::Duration;

    fn k(id: u64, off: u64) -> BlockKey {
        BlockKey::data(id, off)
    }

    #[test]
    fn miss_then_hit() {
        let c = BlockCache::new(1024);
        assert!(c.get(k(1, 0)).is_none());
        c.insert(k(1, 0), Arc::new(vec![1, 2, 3]));
        assert_eq!(c.get(k(1, 0)).unwrap().as_slice(), &[1, 2, 3]);
    }

    #[test]
    fn evicts_lru_when_full() {
        let c = BlockCache::new(50);
        c.insert(k(1, 0), Arc::new(vec![0u8; 40]));
        c.insert(k(2, 0), Arc::new(vec![0u8; 40]));
        // First entry should be gone.
        assert!(c.get(k(1, 0)).is_none());
        assert!(c.get(k(2, 0)).is_some());
    }

    #[test]
    fn floor_skips_protected_tenant() {
        let mut cfg = FairConfig::default();
        cfg.enabled = true;
        cfg.tenant_count = 2;
        cfg.cache_total_bytes = 200;
        // Force a high floor so tenant 1 stays protected with a small insert.
        cfg.delta_cache = Duration::from_millis(0);
        cfg.read_bandwidth_bytes_per_sec = 1;
        let fair = Arc::new(crate::tenant_fair::FairShareState::new(cfg));
        let c = BlockCache::new(60);
        c.with_fair(Arc::clone(&fair));

        let t1 = [1u8; 16];
        let t2 = [2u8; 16];
        // Tenant 1 under floor.
        c.insert_for_tenant(k(1, 0), Arc::new(vec![0u8; 40]), Some(t1));
        // Tenant 2 fills over capacity — should evict t2's own older blocks,
        // not t1 while t1 is floor-protected.
        c.insert_for_tenant(k(2, 0), Arc::new(vec![0u8; 40]), Some(t2));
        c.insert_for_tenant(k(2, 1), Arc::new(vec![0u8; 40]), Some(t2));
        assert!(
            c.get(k(1, 0)).is_some(),
            "floor-protected tenant block must survive eviction"
        );
    }

    #[test]
    fn invalidate_clears() {
        let c = BlockCache::new(1024);
        c.insert(k(1, 0), Arc::new(vec![1]));
        c.invalidate_sstable(1);
        assert!(c.get(k(1, 0)).is_none());
    }

    #[test]
    fn oversized_single_block_accepted() {
        let c = BlockCache::new(10);
        c.insert(k(1, 0), Arc::new(vec![0u8; 100]));
        assert!(c.get(k(1, 0)).is_some());
    }

    #[test]
    fn stats_track_hits() {
        let c = BlockCache::new(1024);
        c.insert(k(1, 0), Arc::new(vec![1]));
        let _ = c.get(k(1, 0));
        let _ = c.get(k(9, 0));
        let s = c.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
    }
}
