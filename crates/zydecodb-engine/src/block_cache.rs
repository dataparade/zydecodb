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
//! - **Locking** is `std::sync::Mutex`. The v1 engine is single-threaded so
//!   contention is zero. The mutex exists so that a future background
//!   compaction worker can read from this cache without an API break.
//!
//! ## Not in scope for v1
//!
//! - Eviction priority hints from scan-vs-point reads (scans should not
//!   thrash the cache; defer to a measurement-driven tuning pass).
//! - Per-shard locking (no contention to relieve).
//! - Compression (cache stores blocks as they live on disk; if/when block
//!   compression lands, the cache will store decompressed bytes — but the
//!   key/value shapes do not change).

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
}

/// Point-in-time snapshot of the cache's counters.
#[derive(Debug, Clone, Copy, Default)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub compaction_reads: u64,
    pub inserts: u64,
    pub evictions: u64,
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
        })
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

    /// Insert a block. Evicts approximate-LRU entries until the cache is
    /// under capacity (or the cache contains only this entry, even if it
    /// exceeds capacity).
    pub fn insert(&self, key: BlockKey, bytes: CachedBlock) {
        let access = self
            .access_counter
            .fetch_add(1, AOrd::Relaxed)
            .wrapping_add(1);
        let size = bytes.len();
        let mut g = self.inner.lock().expect("BlockCache mutex poisoned");

        if let Some(prev) = g.map.insert(
            key,
            Slot {
                bytes,
                last_access: access,
            },
        ) {
            g.bytes_used = g.bytes_used.saturating_sub(prev.bytes.len());
        }
        g.bytes_used = g.bytes_used.saturating_add(size);
        self.inserts.fetch_add(1, AOrd::Relaxed);

        while g.bytes_used > g.capacity && g.map.len() > 1 {
            let Some(victim) = g
                .map
                .iter()
                .min_by_key(|(_, slot)| slot.last_access)
                .map(|(k, _)| *k)
            else {
                break;
            };
            if let Some(removed) = g.map.remove(&victim) {
                g.bytes_used = g.bytes_used.saturating_sub(removed.bytes.len());
                self.evictions.fetch_add(1, AOrd::Relaxed);
            }
        }
    }

    /// Evict every block belonging to a given SSTable id. Called when an
    /// SSTable file is being unlinked, so stale cache entries don't shadow
    /// the now-gone file.
    pub fn invalidate_sstable(&self, sstable_id: u64) {
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

    fn k(id: u64, off: u64) -> BlockKey {
        BlockKey::data(id, off)
    }

    #[test]
    fn miss_then_hit() {
        let c = BlockCache::new(1024);
        assert!(c.get(k(1, 0)).is_none());
        c.insert(k(1, 0), Arc::new(vec![1, 2, 3]));
        let got = c.get(k(1, 0)).unwrap();
        assert_eq!(*got, vec![1, 2, 3]);
        let s = c.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 1);
        assert_eq!(s.resident_entries, 1);
        assert_eq!(s.resident_bytes, 3);
    }

    #[test]
    fn eviction_under_capacity_pressure() {
        let c = BlockCache::new(50);
        c.insert(k(1, 0), Arc::new(vec![0u8; 30]));
        c.insert(k(1, 1), Arc::new(vec![0u8; 30])); // pushes total to 60 > 50 => evicts oldest
        let s = c.stats();
        assert_eq!(s.resident_entries, 1, "one entry must have been evicted");
        assert!(s.resident_bytes <= 50);
        assert_eq!(s.evictions, 1);
    }

    #[test]
    fn lru_order_evicts_least_recently_used() {
        let c = BlockCache::new(60);
        c.insert(k(1, 0), Arc::new(vec![0u8; 20]));
        c.insert(k(1, 1), Arc::new(vec![0u8; 20]));
        c.insert(k(1, 2), Arc::new(vec![0u8; 20])); // full, no evict yet
                                                    // touch (1,0) so it becomes most-recently-used
        let _ = c.get(k(1, 0));
        c.insert(k(1, 3), Arc::new(vec![0u8; 20])); // forces eviction; victim should be (1,1)
        assert!(c.get(k(1, 0)).is_some(), "MRU survived");
        assert!(c.get(k(1, 1)).is_none(), "LRU (1,1) was evicted");
        assert!(c.get(k(1, 2)).is_some());
        assert!(c.get(k(1, 3)).is_some());
    }

    #[test]
    fn invalidate_drops_all_blocks_for_sstable() {
        let c = BlockCache::new(1024);
        c.insert(k(1, 0), Arc::new(vec![0u8; 10]));
        c.insert(k(1, 100), Arc::new(vec![0u8; 10]));
        c.insert(k(2, 0), Arc::new(vec![0u8; 10]));
        c.invalidate_sstable(1);
        let s = c.stats();
        assert_eq!(s.resident_entries, 1, "only sstable 2's block remains");
        assert!(c.get(k(1, 0)).is_none());
        assert!(c.get(k(2, 0)).is_some());
    }

    #[test]
    fn reinserting_same_key_replaces_value_and_keeps_one_entry() {
        let c = BlockCache::new(1024);
        c.insert(k(1, 0), Arc::new(vec![1u8; 10]));
        c.insert(k(1, 0), Arc::new(vec![2u8; 20]));
        let s = c.stats();
        assert_eq!(s.resident_entries, 1);
        assert_eq!(s.resident_bytes, 20);
        assert_eq!(*c.get(k(1, 0)).unwrap(), vec![2u8; 20]);
    }

    #[test]
    fn oversized_single_block_is_kept() {
        let c = BlockCache::new(10);
        c.insert(k(1, 0), Arc::new(vec![0u8; 100]));
        let s = c.stats();
        assert_eq!(s.resident_entries, 1, "oversized single block must remain");
        assert!(c.get(k(1, 0)).is_some());
    }
}
