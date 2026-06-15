//! Long-lived owned snapshots with SSTable pinning.

use crate::entry::Entry;
use crate::errors::EngineResult;
use crate::iter::EntryIterator;
use crate::keys::{EntryKind, InternalKey};
use crate::manifest::SstableMeta;
use crate::memtable::Memtable;
use crate::sstable::SstableReader;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::{SystemTime, UNIX_EPOCH};

/// Shared pin + snapshot watermark state between the engine and held snapshots.
pub(crate) struct PinState {
    pub pin_counts: BTreeMap<u64, u32>,
    pub live_snapshot_seqs: BTreeMap<u64, u32>,
    pub deferred_unlinks: Vec<u64>,
}

impl PinState {
    pub fn acquire_pins(&mut self, ids: &[u64], seq_upper: u64) {
        for id in ids {
            *self.pin_counts.entry(*id).or_insert(0) += 1;
        }
        *self.live_snapshot_seqs.entry(seq_upper).or_insert(0) += 1;
    }

    pub fn release_pins(&mut self, ids: &[u64], seq_upper: u64) -> Vec<u64> {
        for id in ids {
            if let Some(c) = self.pin_counts.get_mut(id) {
                *c = c.saturating_sub(1);
                if *c == 0 {
                    self.pin_counts.remove(id);
                }
            }
        }
        if let Some(c) = self.live_snapshot_seqs.get_mut(&seq_upper) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                self.live_snapshot_seqs.remove(&seq_upper);
            }
        }
        let mut ready = Vec::new();
        self.deferred_unlinks.retain(|id| {
            if self.pin_counts.get(id).copied().unwrap_or(0) == 0 {
                ready.push(*id);
                false
            } else {
                true
            }
        });
        ready
    }
}

/// An owned snapshot that survives concurrent engine mutation.
pub struct SnapshotHandle {
    seq_upper: u64,
    active: Memtable,
    immutables: Vec<Memtable>,
    sstables: Vec<Arc<SstableReader>>,
    sstable_metas: Vec<SstableMeta>,
    sstable_ids: Vec<u64>,
    pin_state: Arc<Mutex<PinState>>,
    data_dir: std::path::PathBuf,
    block_cache: Arc<crate::block_cache::BlockCache>,
    reader_cache: Arc<crate::reader_cache::ReaderCache>,
}

impl SnapshotHandle {
    #[allow(clippy::too_many_arguments)] // a snapshot captures the full read-path state in one shot
    pub(crate) fn new(
        seq_upper: u64,
        active: Memtable,
        immutables: Vec<Memtable>,
        sstables: Vec<Arc<SstableReader>>,
        sstable_metas: Vec<SstableMeta>,
        sstable_ids: Vec<u64>,
        pin_state: Arc<Mutex<PinState>>,
        data_dir: std::path::PathBuf,
        block_cache: Arc<crate::block_cache::BlockCache>,
        reader_cache: Arc<crate::reader_cache::ReaderCache>,
    ) -> Self {
        {
            let mut ps = pin_state.lock().expect("pin state lock");
            ps.acquire_pins(&sstable_ids, seq_upper);
        }
        SnapshotHandle {
            seq_upper,
            active,
            immutables,
            sstables,
            sstable_metas,
            sstable_ids,
            pin_state,
            data_dir,
            block_cache,
            reader_cache,
        }
    }

    pub fn seq_upper(&self) -> u64 {
        self.seq_upper
    }

    pub fn get(&self, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        let now = now_ms();
        if let Some(v) = get_from_memtable(&self.active, key, self.seq_upper, now)? {
            return Ok(Some(v));
        }
        for mt in self.immutables.iter().rev() {
            if let Some(v) = get_from_memtable(mt, key, self.seq_upper, now)? {
                return Ok(Some(v));
            }
        }
        for (sst, meta) in self.sstables.iter().zip(self.sstable_metas.iter()) {
            if !key_in_range(key, meta) {
                continue;
            }
            if !sst.might_contain(key) {
                continue;
            }
            if let Some((ik, entry)) = sst.get_latest(key)? {
                if ik.seq <= self.seq_upper {
                    return Ok(resolve(&entry, now));
                }
            }
        }
        Ok(None)
    }

    pub fn scan(&self, lo: Vec<u8>, hi: Vec<u8>) -> EngineResult<OwnedRangeIter<'_>> {
        let now_ms = now_ms();
        let sst_refs: Vec<Arc<SstableReader>> = self
            .sstables
            .iter()
            .zip(self.sstable_metas.iter())
            .filter(|(_, meta)| range_overlaps(meta, &lo, &hi))
            .map(|(r, _)| r.clone())
            .collect();
        let inner = crate::snapshot::build_sources(
            &self.active,
            self.immutables.iter(),
            &sst_refs,
            self.seq_upper,
            lo,
            hi,
        )?;
        Ok(OwnedRangeIter { inner, now_ms })
    }
}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        let ready = {
            let mut ps = self.pin_state.lock().expect("pin state lock");
            ps.release_pins(&self.sstable_ids, self.seq_upper)
        };
        for id in &self.sstable_ids {
            self.reader_cache.unpin(*id);
        }
        for id in ready {
            let path = self.data_dir.join(format!("{:08}.sst", id));
            let _ = std::fs::remove_file(&path);
            self.block_cache.invalidate_sstable(id);
            self.reader_cache.remove(id);
        }
    }
}

pub struct OwnedRangeIter<'a> {
    inner: crate::iter::MergingIterator<'a>,
    now_ms: u64,
}

impl<'a> Iterator for OwnedRangeIter<'a> {
    type Item = EngineResult<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                Err(e) => return Some(Err(e)),
                Ok(None) => return None,
                Ok(Some((k, e))) => {
                    if e.is_tombstone() || e.is_expired(self.now_ms) {
                        continue;
                    }
                    if let Some(v) = e.value {
                        return Some(Ok((k.user_key, v)));
                    }
                }
            }
        }
    }
}

fn get_from_memtable(
    mt: &Memtable,
    user_key: &[u8],
    seq_upper: u64,
    now: u64,
) -> EngineResult<Option<Vec<u8>>> {
    if seq_upper == u64::MAX {
        if let Some((_ik, entry)) = mt.get_latest(user_key) {
            return Ok(resolve(entry, now));
        }
        return Ok(None);
    }
    use std::ops::Bound;
    let lower = InternalKey::new(user_key.to_vec(), u64::MAX, EntryKind::Value);
    for (k, e) in mt
        .iter_internal()
        .range::<InternalKey, _>((Bound::Included(lower), Bound::Unbounded))
    {
        if k.user_key.as_slice() != user_key {
            return Ok(None);
        }
        if k.seq <= seq_upper {
            return Ok(resolve(e, now));
        }
    }
    Ok(None)
}

fn resolve(entry: &Entry, now: u64) -> Option<Vec<u8>> {
    if entry.is_tombstone() || entry.is_expired(now) {
        return None;
    }
    entry.value.clone()
}

fn key_in_range(key: &[u8], meta: &SstableMeta) -> bool {
    key >= meta.min_key.as_slice() && key <= meta.max_key.as_slice()
}

fn range_overlaps(meta: &SstableMeta, lo: &[u8], hi: &[u8]) -> bool {
    if !hi.is_empty() && meta.min_key.as_slice() >= hi {
        return false;
    }
    if meta.max_key.as_slice() < lo {
        return false;
    }
    true
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
