//! LRU result cache for point lookups keyed by user key.
//!
//! Unlike the SSTable block cache, entries survive compaction because they are
//! not addressed by `(sstable_id, block_offset)`.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

struct Slot {
    value: Vec<u8>,
    last_access: u64,
}

struct Inner {
    map: HashMap<Vec<u8>, Slot>,
    bytes_used: usize,
    capacity: usize,
}

/// Point-lookup cache keyed by user key.
pub struct ResultCache {
    inner: Mutex<Inner>,
    access_counter: AtomicU64,
    evictions: AtomicU64,
}

impl ResultCache {
    pub fn new(capacity_bytes: usize) -> Arc<ResultCache> {
        Arc::new(ResultCache {
            inner: Mutex::new(Inner {
                map: HashMap::new(),
                bytes_used: 0,
                capacity: capacity_bytes,
            }),
            access_counter: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        let access = self
            .access_counter
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let mut g = self.inner.lock().expect("ResultCache mutex poisoned");
        match g.map.get_mut(key) {
            Some(slot) => {
                slot.last_access = access;
                Some(slot.value.clone())
            }
            None => None,
        }
    }

    pub fn insert(&self, key: Vec<u8>, value: Vec<u8>) {
        let access = self
            .access_counter
            .fetch_add(1, Ordering::Relaxed)
            .wrapping_add(1);
        let key_len = key.len();
        let entry_bytes = key_len + value.len();
        let mut g = self.inner.lock().expect("ResultCache mutex poisoned");
        if let Some(prev) = g.map.insert(
            key,
            Slot {
                value,
                last_access: access,
            },
        ) {
            g.bytes_used = g.bytes_used.saturating_sub(key_len + prev.value.len());
        }
        g.bytes_used = g.bytes_used.saturating_add(entry_bytes);

        while g.bytes_used > g.capacity && g.map.len() > 1 {
            let Some(victim_key) = g
                .map
                .iter()
                .min_by_key(|(_, slot)| slot.last_access)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            if let Some(slot) = g.map.remove(&victim_key) {
                g.bytes_used = g
                    .bytes_used
                    .saturating_sub(victim_key.len() + slot.value.len());
                self.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }
    }

    pub fn invalidate(&self, key: &[u8]) {
        let mut g = self.inner.lock().expect("ResultCache mutex poisoned");
        if let Some(slot) = g.map.remove(key) {
            g.bytes_used = g.bytes_used.saturating_sub(key.len() + slot.value.len());
        }
    }

    pub fn resident_bytes(&self) -> u64 {
        let g = self.inner.lock().expect("ResultCache mutex poisoned");
        g.bytes_used as u64
    }

    pub fn evictions(&self) -> u64 {
        self.evictions.load(Ordering::Relaxed)
    }
}
