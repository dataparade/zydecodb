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
///
/// Memtables are `Arc`-shared with the engine (cheap pin); writers use
/// `Arc::make_mut` so a live snapshot is not mutated in place.
pub struct SnapshotHandle {
    seq_upper: u64,
    active: Arc<Memtable>,
    immutables: Vec<Arc<Memtable>>,
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
        active: Arc<Memtable>,
        immutables: Vec<Arc<Memtable>>,
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
        Ok(self.get_with_seq(key)?.map(|(v, _)| v))
    }

    /// Point lookup returning `(value, seq)` of the visible entry. The seq is
    /// the opaque document revision for optimistic concurrency.
    pub fn get_with_seq(&self, key: &[u8]) -> EngineResult<Option<(Vec<u8>, u64)>> {
        let now = now_ms();
        // A hit in any source shadows everything older, even when it resolves
        // to None (tombstone or expired): the delete/expiry is newer than
        // anything below, so falling through would resurrect a stale value.
        if let Some((ik, entry)) = first_visible_in_memtable(&self.active, key, self.seq_upper) {
            return Ok(resolve_with_seq(&ik, &entry, now));
        }
        for mt in self.immutables.iter().rev() {
            if let Some((ik, entry)) = first_visible_in_memtable(mt, key, self.seq_upper) {
                return Ok(resolve_with_seq(&ik, &entry, now));
            }
        }
        // Newest SSTable first — catalog is newest-last (matches Engine::snapshot_get).
        for (sst, meta) in self.sstables.iter().zip(self.sstable_metas.iter()).rev() {
            if !key_in_range(key, meta) {
                continue;
            }
            if !sst.might_contain(key) {
                continue;
            }
            let found = if self.seq_upper == u64::MAX {
                sst.get_latest(key)?
            } else {
                // Bounded ceiling: this table's newest entry for the key may
                // be newer than the ceiling. Walk the key's block range for
                // the newest visible version — skipping the table outright
                // would surface a too-old version from an older table.
                newest_visible_in_sstable(sst, key, self.seq_upper)?
            };
            if let Some((ik, entry)) = found {
                return Ok(resolve_with_seq(&ik, &entry, now));
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
            self.active.as_ref(),
            self.immutables.iter().map(|m| m.as_ref()),
            &sst_refs,
            self.seq_upper,
            lo,
            hi,
        )?;
        Ok(OwnedRangeIter { inner, now_ms })
    }

    /// Range scan over user keys `[lo, hi)` yielding pairs in user-key DESC order.
    pub fn scan_rev(&self, lo: Vec<u8>, hi: Vec<u8>) -> EngineResult<OwnedRangeIter<'_>> {
        let now_ms = now_ms();
        let sst_refs: Vec<Arc<SstableReader>> = self
            .sstables
            .iter()
            .zip(self.sstable_metas.iter())
            .filter(|(_, meta)| range_overlaps(meta, &lo, &hi))
            .map(|(r, _)| r.clone())
            .collect();
        let inner = crate::snapshot::build_sources_rev(
            self.active.as_ref(),
            self.immutables.iter().map(|m| m.as_ref()),
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

/// First entry for `user_key` visible at `seq_upper`, raw (tombstones
/// included) so the caller can distinguish "not here" from "deleted here" —
/// resolving inside the helper would conflate the two and let deleted keys
/// fall through to older sources.
fn first_visible_in_memtable(
    mt: &Memtable,
    user_key: &[u8],
    seq_upper: u64,
) -> Option<(InternalKey, Entry)> {
    if seq_upper == u64::MAX {
        return mt.get_latest(user_key).map(|(k, e)| (k.clone(), e.clone()));
    }
    use std::ops::Bound;
    let lower = InternalKey::new(user_key.to_vec(), u64::MAX, EntryKind::Value);
    for (k, e) in mt
        .iter_internal()
        .range::<InternalKey, _>((Bound::Included(lower), Bound::Unbounded))
    {
        if k.user_key.as_slice() != user_key {
            return None;
        }
        if k.seq <= seq_upper {
            return Some((k.clone(), e.clone()));
        }
    }
    None
}

/// Newest entry for `user_key` in this table with seq ≤ `seq_upper`, raw.
/// Mirrors `Engine::newest_visible_in_sstable` for the owned-snapshot path.
fn newest_visible_in_sstable(
    reader: &Arc<SstableReader>,
    user_key: &[u8],
    seq_upper: u64,
) -> EngineResult<Option<(InternalKey, Entry)>> {
    let hi = crate::engine::Engine::next_user_key(user_key);
    let mut it = reader.clone().range_iter(user_key.to_vec(), hi)?;
    while let Some((ik, entry)) = it.next()? {
        if ik.user_key.as_slice() != user_key {
            continue;
        }
        if ik.seq <= seq_upper {
            return Ok(Some((ik, entry)));
        }
    }
    Ok(None)
}

fn resolve_with_seq(ik: &InternalKey, entry: &Entry, now: u64) -> Option<(Vec<u8>, u64)> {
    if entry.is_tombstone() || entry.is_expired(now) {
        return None;
    }
    entry.value.clone().map(|v| (v, ik.seq))
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
