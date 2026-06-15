//! Read-only snapshots and range iteration over the LSM.
//!
//! v1 ships **scoped snapshots only**. A snapshot is created from an
//! `&Engine` borrow, captures the engine's current sequence-number ceiling,
//! and is used for the duration of that borrow. Because the borrow is
//! shared, no mutations (PUT, DEL, flush, compaction) can run while a
//! snapshot is alive — so the snapshot does NOT need to pin SSTable files
//! or clone memtables. Once the borrow drops, the engine is free to mutate;
//! a fresh `snapshot()` call gives a new view.
//!
//! Long-lived snapshots that survive across engine ops (needed for
//! always-consistent online backups) are v2. The shape on disk is identical
//! either way — the only new state v2 needs is per-snapshot SSTable pinning
//! and a memtable-Arc convention. v1's API choice here does not lock us
//! out of that.
//!
//! ## Semantics
//!
//! A `SnapshotView` carries a `seq_upper`. Reads return the newest entry
//! for each user key whose `seq <= seq_upper`. Tombstones at or below
//! `seq_upper` shadow older values. Writes that happen *after* the snapshot
//! was taken cannot affect a snapshot's reads because their seqs are above
//! `seq_upper` — but this case never arises in v1 (scoped), because the
//! shared borrow blocks writes anyway.
//!
//! ## Why this exists when reads also work directly off the engine
//!
//! - Range scans need to walk multiple sources (memtable + immutables +
//!   SSTables) merged in InternalKey order. That logic belongs in one
//!   place, not duplicated between `get` and `scan`.
//! - When MVCC / long-lived snapshots arrive, the `get` and `scan` paths
//!   already route through `SnapshotView`, so the upgrade is to give the
//!   snapshot a real owned state instead of a borrowed-engine view.

use crate::entry::Entry;
use crate::errors::EngineResult;
use crate::iter::{EntryIterator, MemtableIter, MergeMode, MergingIterator};
use crate::keys::{EntryKind, InternalKey};
use std::sync::Arc;

/// A scoped, read-only view over the engine's state at a single point in
/// sequence-number history. Created via [`crate::engine::Engine::snapshot`].
///
/// `SnapshotView` borrows the engine. Holding one prevents any concurrent
/// engine mutation (the borrow is shared, so `&mut self` is unavailable to
/// callers). Drop the view to release the borrow.
pub struct SnapshotView<'a> {
    pub(crate) engine: &'a crate::engine::Engine,
    pub(crate) seq_upper: u64,
}

impl<'a> SnapshotView<'a> {
    /// Point lookup: returns the newest value for `key` at this snapshot,
    /// or None if the key is missing, tombstoned, or expired.
    pub fn get(&self, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        self.engine.snapshot_get(self.seq_upper, key)
    }

    /// Range scan over user keys `[lo, hi)`. Returns a streaming iterator
    /// that yields `(user_key, value)` pairs in user-key ASC order.
    /// Tombstones are suppressed; expired entries are suppressed.
    pub fn scan(&self, lo: Vec<u8>, hi: Vec<u8>) -> EngineResult<RangeIter<'a>> {
        let now_ms = current_time_ms();
        let inner = self.engine.build_merging_iterator(self.seq_upper, lo, hi)?;
        Ok(RangeIter { inner, now_ms })
    }

    /// Prefix scan. Sugar over `scan` with `hi` = `prefix` followed by
    /// the smallest byte string lexicographically greater than `prefix`.
    pub fn prefix_scan(&self, prefix: Vec<u8>) -> EngineResult<RangeIter<'a>> {
        let hi = next_after_prefix(&prefix);
        self.scan(prefix, hi)
    }

    /// Sequence-number ceiling for this snapshot. Reads see entries with
    /// `seq <= seq_upper`.
    pub fn seq_upper(&self) -> u64 {
        self.seq_upper
    }
}

/// Streaming iterator over a range scan. Yields `(user_key, value)` pairs
/// in user-key ASC order, with tombstones and expired entries suppressed.
///
/// The iterator borrows from the snapshot's engine; it must not outlive
/// the snapshot.
pub struct RangeIter<'a> {
    inner: MergingIterator<'a>,
    now_ms: u64,
}

impl<'a> Iterator for RangeIter<'a> {
    type Item = EngineResult<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.inner.next() {
                Err(e) => return Some(Err(e)),
                Ok(None) => return None,
                Ok(Some((k, e))) => {
                    if e.is_tombstone() {
                        // Dedup mode already suppresses tombstones, but in
                        // case mode changes upstream we belt-and-suspenders
                        // it here.
                        continue;
                    }
                    if e.is_expired(self.now_ms) {
                        continue;
                    }
                    match e.value {
                        Some(v) => return Some(Ok((k.user_key, v))),
                        None => continue,
                    }
                }
            }
        }
    }
}

/// Build a `MergingIterator` over the engine's read sources, filtered to
/// `seq <= seq_upper`, ranged on user keys `[lo, hi)`.
///
/// Called by [`SnapshotView::scan`]; lives here (not in engine.rs) to keep
/// the multi-source merging logic in one module.
pub(crate) fn build_sources<'a>(
    active: &'a crate::memtable::Memtable,
    immutable: impl Iterator<Item = &'a crate::memtable::Memtable>,
    sstables: &[Arc<crate::sstable::SstableReader>],
    seq_upper: u64,
    lo: Vec<u8>,
    hi: Vec<u8>,
) -> EngineResult<MergingIterator<'a>> {
    let mut sources: Vec<Box<dyn EntryIterator + 'a>> = Vec::new();

    sources.push(Box::new(SeqFilter::new(
        if hi.is_empty() {
            MemtableIter::full(active)
        } else {
            MemtableIter::range(active, &lo, &hi)
        },
        seq_upper,
    )));
    for mt in immutable {
        sources.push(Box::new(SeqFilter::new(
            if hi.is_empty() {
                MemtableIter::full(mt)
            } else {
                MemtableIter::range(mt, &lo, &hi)
            },
            seq_upper,
        )));
    }
    for sst in sstables {
        let it = if hi.is_empty() {
            sst.clone().full_iter()?
        } else {
            sst.clone().range_iter(lo.clone(), hi.clone())?
        };
        sources.push(Box::new(SeqFilter::new(it, seq_upper)));
    }

    MergingIterator::new(sources, MergeMode::Dedup)
}

/// Wraps a source iterator and drops any entry whose `seq > seq_upper`.
/// This is what makes a snapshot ignore writes newer than its capture point.
struct SeqFilter<I: EntryIterator> {
    inner: I,
    seq_upper: u64,
}
impl<I: EntryIterator> SeqFilter<I> {
    fn new(inner: I, seq_upper: u64) -> Self {
        SeqFilter { inner, seq_upper }
    }
}
impl<I: EntryIterator> EntryIterator for SeqFilter<I> {
    fn next(&mut self) -> EngineResult<Option<(InternalKey, Entry)>> {
        loop {
            match self.inner.next()? {
                None => return Ok(None),
                Some((k, _)) if k.seq > self.seq_upper => continue,
                Some(pair) => return Ok(Some(pair)),
            }
        }
    }
}

/// Build the user key just past `prefix` for a `[prefix, next)` half-open
/// range. Returns an empty Vec to mean "unbounded above" when `prefix` is
/// all-0xFF (the half-open trick can't represent that).
fn next_after_prefix(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    // Find rightmost byte that isn't 0xFF and bump it; truncate the rest.
    for i in (0..out.len()).rev() {
        if out[i] != 0xFF {
            out[i] += 1;
            out.truncate(i + 1);
            return out;
        }
    }
    // All bytes were 0xFF — no representable upper bound; return empty
    // (the iterator treats empty `hi` as unbounded).
    Vec::new()
}

fn current_time_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// Type alias used by the entry kind check in RangeIter without an import dance.
#[allow(dead_code)]
const _: EntryKind = EntryKind::Value;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn next_after_prefix_bumps_last_byte() {
        assert_eq!(next_after_prefix(b"abc"), b"abd".to_vec());
        assert_eq!(next_after_prefix(b"a\xFF"), b"b".to_vec());
        assert_eq!(next_after_prefix(b"\xFF\xFF"), Vec::<u8>::new());
    }
}
