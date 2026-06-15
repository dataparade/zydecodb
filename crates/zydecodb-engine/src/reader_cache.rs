//! LRU cache of open [`crate::sstable::SstableReader`] handles.
//!
//! Index and bloom metadata are pinned on each reader (not in the data block
//! cache). Total metadata memory is bounded by capping how many readers stay
//! open at once — the RocksDB `max_open_files` / table-cache model.

use crate::block_cache::BlockCache;
use crate::errors::EngineResult;
use crate::sstable::SstableReader;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};

struct Slot {
    reader: Arc<SstableReader>,
    last_access: u64,
}

struct Inner {
    entries: HashMap<u64, Slot>,
    pins: HashMap<u64, u32>,
    access_counter: u64,
}

/// Shared table cache: open readers keyed by SSTable id.
pub struct ReaderCache {
    max_readers: usize,
    inner: Mutex<Inner>,
}

impl ReaderCache {
    /// `max_open_readers == 0` means unlimited (no eviction).
    pub fn new(max_open_readers: usize) -> Arc<ReaderCache> {
        Arc::new(ReaderCache {
            max_readers: max_open_readers,
            inner: Mutex::new(Inner {
                entries: HashMap::new(),
                pins: HashMap::new(),
                access_counter: 0,
            }),
        })
    }

    pub fn get_or_open(
        &self,
        path: &Path,
        id: u64,
        block_cache: Arc<BlockCache>,
    ) -> EngineResult<Arc<SstableReader>> {
        {
            let mut g = self.inner.lock().expect("ReaderCache mutex poisoned");
            if let Some(reader) = g.entries.get(&id).map(|s| Arc::clone(&s.reader)) {
                let access = g.access_counter.wrapping_add(1);
                g.access_counter = access;
                g.entries.get_mut(&id).expect("entry").last_access = access;
                return Ok(reader);
            }
        }

        let reader = Arc::new(SstableReader::open_from_path(path, id, block_cache)?);

        let mut g = self.inner.lock().expect("ReaderCache mutex poisoned");
        if let Some(existing) = g.entries.get(&id).map(|s| Arc::clone(&s.reader)) {
            let access = g.access_counter.wrapping_add(1);
            g.access_counter = access;
            g.entries.get_mut(&id).expect("entry").last_access = access;
            return Ok(existing);
        }
        let access = g.access_counter.wrapping_add(1);
        g.access_counter = access;
        g.entries.insert(
            id,
            Slot {
                reader: Arc::clone(&reader),
                last_access: access,
            },
        );
        self.evict_if_needed(&mut g);
        Ok(reader)
    }

    pub fn remove(&self, id: u64) {
        let mut g = self.inner.lock().expect("ReaderCache mutex poisoned");
        g.entries.remove(&id);
        g.pins.remove(&id);
    }

    pub fn pin(&self, id: u64) {
        let mut g = self.inner.lock().expect("ReaderCache mutex poisoned");
        *g.pins.entry(id).or_insert(0) += 1;
    }

    pub fn unpin(&self, id: u64) {
        let mut g = self.inner.lock().expect("ReaderCache mutex poisoned");
        if let Some(c) = g.pins.get_mut(&id) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                g.pins.remove(&id);
            }
        }
        self.evict_if_needed(&mut g);
    }

    pub fn open_count(&self) -> usize {
        self.inner
            .lock()
            .expect("ReaderCache mutex poisoned")
            .entries
            .len()
    }

    fn evict_if_needed(&self, g: &mut Inner) {
        if self.max_readers == 0 {
            return;
        }
        while g.entries.len() > self.max_readers {
            let victim = g
                .entries
                .iter()
                .filter(|(id, _)| g.pins.get(id).copied().unwrap_or(0) == 0)
                .min_by_key(|(_, slot)| slot.last_access)
                .map(|(id, _)| *id);
            let Some(id) = victim else {
                break;
            };
            g.entries.remove(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::Entry;
    use crate::keys::{EntryKind, InternalKey};
    use crate::sstable;

    fn uk(s: &[u8]) -> Vec<u8> {
        s.to_vec()
    }

    #[test]
    fn evicts_lru_when_over_cap() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache = BlockCache::new(1024 * 1024);
        let rc = ReaderCache::new(2);

        let mut ids = Vec::new();
        for i in 0..3u64 {
            let pairs = vec![(
                InternalKey::new(uk(format!("k{}", i).as_bytes()), 1, EntryKind::Value),
                Entry::value(b"v".to_vec(), None),
            )];
            let sst = sstable::build(&pairs, false);
            let path = dir.path().join(format!("{:08}.sst", i));
            std::fs::write(&path, &sst.bytes).unwrap();
            let _ = rc.get_or_open(&path, i, cache.clone()).unwrap();
            ids.push(i);
        }
        assert_eq!(rc.open_count(), 2);
        // touch id 0 so id 1 is LRU
        let path0 = dir.path().join("00000000.sst");
        let _ = rc.get_or_open(&path0, 0, cache.clone()).unwrap();
        let path2 = dir.path().join("00000002.sst");
        let _ = rc.get_or_open(&path2, 2, cache.clone()).unwrap();
        assert_eq!(rc.open_count(), 2);
        // id 1 should have been evicted; reopen works
        let path1 = dir.path().join("00000001.sst");
        let r = rc.get_or_open(&path1, 1, cache.clone()).unwrap();
        assert!(r.get_latest(&uk(b"k1")).unwrap().is_some());
    }

    #[test]
    fn pinned_entries_are_not_evicted() {
        let dir = tempfile::TempDir::new().unwrap();
        let cache = BlockCache::new(1024 * 1024);
        let rc = ReaderCache::new(1);

        for i in 0..2u64 {
            let pairs = vec![(
                InternalKey::new(uk(format!("k{}", i).as_bytes()), 1, EntryKind::Value),
                Entry::value(b"v".to_vec(), None),
            )];
            let sst = sstable::build(&pairs, false);
            let path = dir.path().join(format!("{:08}.sst", i));
            std::fs::write(&path, &sst.bytes).unwrap();
            rc.pin(i);
            let _ = rc.get_or_open(&path, i, cache.clone()).unwrap();
        }
        assert_eq!(rc.open_count(), 2);
    }
}
