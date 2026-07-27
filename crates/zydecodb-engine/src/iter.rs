//! Iteration primitives shared across compaction, range scans, and snapshots.
//!
//! The engine's read paths and compaction all need the same thing: a sorted
//! stream of `(InternalKey, Entry)` pairs drawn from N input sources
//! (memtables and SSTables), merged in InternalKey order, with the option to
//! collapse multiple versions of a key to its newest and to drop tombstones.
//!
//! This module factors that out so the read path and compaction share one
//! correctness story rather than two parallel implementations.
//!
//! Ordering invariant: every input source MUST yield entries in
//! `InternalKey` order (user_key ASC, seq DESC). The merge depends on it.

use crate::entry::Entry;
use crate::errors::EngineResult;
use crate::keys::InternalKey;
use std::cmp::Ordering;
use std::collections::BinaryHeap;

/// An ordered iterator over `(InternalKey, Entry)` pairs.
///
/// `next` returns the next entry in the source's order, or `Ok(None)` at
/// end-of-stream. Errors abort iteration; the caller should treat the
/// iterator as exhausted after an error.
pub trait EntryIterator {
    fn next(&mut self) -> EngineResult<Option<(InternalKey, Entry)>>;
}

/// What a [`MergingIterator`] does with multiple versions of the same user
/// key, and whether it returns tombstones.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MergeMode {
    /// User-facing reads: collapse to the newest version per user key, then
    /// suppress that version if it is a tombstone. Yields at most one entry
    /// per user key.
    Dedup,
    /// Compaction: yield every version of every key so compaction can decide
    /// what to keep (e.g. drop tombstones below the oldest live snapshot).
    Raw,
}

/// Forward = user_key ascending; Reverse = user_key descending. In both cases
/// sources must present the newest visible version of each user key first
/// within that key's group so [`MergeMode::Dedup`] can skip the rest.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScanDirection {
    Forward,
    Reverse,
}

/// K-way merging iterator. Pulls from a fixed set of input iterators via a
/// heap; in [`MergeMode::Dedup`] mode it suppresses shadowed versions and
/// tombstones.
///
/// The lifetime `'a` lets sources borrow from owned engine state (memtables,
/// SSTable readers). Use `'static` when every source is owned (e.g. tests,
/// or when sources hold their own `Arc`s).
pub struct MergingIterator<'a> {
    sources: Vec<Box<dyn EntryIterator + 'a>>,
    heap: BinaryHeap<HeapEntry>,
    mode: MergeMode,
    direction: ScanDirection,
    /// In Dedup mode, the user_key of the most recently yielded entry. Used
    /// to suppress all later versions of the same key (lower seq).
    last_user_key: Option<Vec<u8>>,
}

impl<'a> MergingIterator<'a> {
    pub fn new(sources: Vec<Box<dyn EntryIterator + 'a>>, mode: MergeMode) -> EngineResult<Self> {
        Self::new_directed(sources, mode, ScanDirection::Forward)
    }

    pub fn new_directed(
        mut sources: Vec<Box<dyn EntryIterator + 'a>>,
        mode: MergeMode,
        direction: ScanDirection,
    ) -> EngineResult<Self> {
        let mut heap = BinaryHeap::with_capacity(sources.len());
        for (idx, src) in sources.iter_mut().enumerate() {
            if let Some((k, e)) = src.next()? {
                heap.push(HeapEntry {
                    key: k,
                    entry: e,
                    source: idx,
                    direction,
                });
            }
        }
        Ok(MergingIterator {
            sources,
            heap,
            mode,
            direction,
            last_user_key: None,
        })
    }
}

impl<'a> EntryIterator for MergingIterator<'a> {
    fn next(&mut self) -> EngineResult<Option<(InternalKey, Entry)>> {
        loop {
            let Some(top) = self.heap.pop() else {
                return Ok(None);
            };
            // Refill the source that just produced this entry.
            if let Some((k, e)) = self.sources[top.source].next()? {
                self.heap.push(HeapEntry {
                    key: k,
                    entry: e,
                    source: top.source,
                    direction: self.direction,
                });
            }

            match self.mode {
                MergeMode::Raw => return Ok(Some((top.key, top.entry))),
                MergeMode::Dedup => {
                    if let Some(prev) = &self.last_user_key {
                        if prev.as_slice() == top.key.user_key.as_slice() {
                            // Older version of an already-yielded key; skip.
                            continue;
                        }
                    }
                    self.last_user_key = Some(top.key.user_key.clone());
                    if top.key.kind == crate::keys::EntryKind::Tombstone {
                        // Newest version is a delete; suppress in Dedup mode.
                        continue;
                    }
                    return Ok(Some((top.key, top.entry)));
                }
            }
        }
    }
}

/// Heap node. Ordering depends on [`ScanDirection`]:
/// - Forward: pop smallest `InternalKey` first (user_key ASC, seq DESC).
/// - Reverse: pop highest user_key first, then highest seq (newest first).
struct HeapEntry {
    key: InternalKey,
    entry: Entry,
    /// Source-iterator index into `MergingIterator::sources`. Tiebreaker
    /// when two iterators yield the exact same InternalKey (which they
    /// shouldn't in a well-formed LSM, but we order deterministically
    /// regardless).
    source: usize,
    direction: ScanDirection,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key && self.source == other.source
    }
}
impl Eq for HeapEntry {}

impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; invert so the "next" entry pops first.
        match self.direction {
            ScanDirection::Forward => other
                .key
                .cmp(&self.key)
                .then_with(|| other.source.cmp(&self.source)),
            ScanDirection::Reverse => {
                // Prefer higher user_key, then higher seq.
                let by_key = self.key.user_key.cmp(&other.key.user_key);
                let by_seq = self.key.seq.cmp(&other.key.seq);
                by_key
                    .then(by_seq)
                    .then_with(|| other.source.cmp(&self.source))
            }
        }
    }
}

// --------------------------------------------------------------------------
// Memtable iterator
// --------------------------------------------------------------------------

/// Iterator over a [`crate::memtable::Memtable`] in `InternalKey` order,
/// optionally bounded to a user-key range `[lo, hi)`.
///
/// Holds a reference to the memtable; the borrow must outlive the iterator.
pub struct MemtableIter<'a> {
    inner: std::collections::btree_map::Range<'a, InternalKey, Entry>,
}

impl<'a> MemtableIter<'a> {
    pub fn full(mt: &'a crate::memtable::Memtable) -> MemtableIter<'a> {
        MemtableIter {
            inner: mt.range_internal_unbounded(),
        }
    }

    pub fn range(mt: &'a crate::memtable::Memtable, lo: &[u8], hi: &[u8]) -> MemtableIter<'a> {
        MemtableIter {
            inner: mt.range_internal(lo, hi),
        }
    }
}

impl<'a> EntryIterator for MemtableIter<'a> {
    fn next(&mut self) -> EngineResult<Option<(InternalKey, Entry)>> {
        Ok(self.inner.next().map(|(k, e)| (k.clone(), e.clone())))
    }
}

/// Reverse memtable range iterator. Yields user keys descending; within each
/// user key, emits the highest-seq version first (for Dedup).
pub struct MemtableRevIter<'a> {
    /// Remaining entries from the range, already ordered for reverse yield:
    /// user_key DESC, seq DESC.
    pending: std::vec::IntoIter<(InternalKey, Entry)>,
    _mt: std::marker::PhantomData<&'a crate::memtable::Memtable>,
}

impl<'a> MemtableRevIter<'a> {
    pub fn range(mt: &'a crate::memtable::Memtable, lo: &[u8], hi: &[u8]) -> MemtableRevIter<'a> {
        let mut pairs: Vec<(InternalKey, Entry)> = mt
            .range_internal(lo, hi)
            .map(|(k, e)| (k.clone(), e.clone()))
            .collect();
        // user_key DESC, seq DESC (newest first within a key).
        pairs.sort_by(|a, b| {
            b.0.user_key
                .cmp(&a.0.user_key)
                .then_with(|| b.0.seq.cmp(&a.0.seq))
        });
        MemtableRevIter {
            pending: pairs.into_iter(),
            _mt: std::marker::PhantomData,
        }
    }
}

impl<'a> EntryIterator for MemtableRevIter<'a> {
    fn next(&mut self) -> EngineResult<Option<(InternalKey, Entry)>> {
        Ok(self.pending.next())
    }
}

// --------------------------------------------------------------------------
// Tests
// --------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::entry::Entry;
    use crate::keys::EntryKind;

    fn ik(k: &[u8], seq: u64, kind: EntryKind) -> InternalKey {
        InternalKey::new(k.to_vec(), seq, kind)
    }

    struct VecIter {
        items: std::vec::IntoIter<(InternalKey, Entry)>,
    }
    impl EntryIterator for VecIter {
        fn next(&mut self) -> EngineResult<Option<(InternalKey, Entry)>> {
            Ok(self.items.next())
        }
    }
    fn src(pairs: Vec<(InternalKey, Entry)>) -> Box<dyn EntryIterator + 'static> {
        Box::new(VecIter {
            items: pairs.into_iter(),
        })
    }

    fn collect(mut it: MergingIterator<'static>) -> EngineResult<Vec<(InternalKey, Entry)>> {
        let mut out = Vec::new();
        while let Some(pair) = it.next()? {
            out.push(pair);
        }
        Ok(out)
    }

    #[test]
    fn raw_mode_yields_every_version_in_sorted_order() {
        let a = vec![
            (
                ik(b"a", 5, EntryKind::Value),
                Entry::value(b"new".to_vec(), None),
            ),
            (
                ik(b"a", 1, EntryKind::Value),
                Entry::value(b"old".to_vec(), None),
            ),
        ];
        let b = vec![(
            ik(b"b", 3, EntryKind::Value),
            Entry::value(b"x".to_vec(), None),
        )];
        let it = MergingIterator::new(vec![src(a), src(b)], MergeMode::Raw).unwrap();
        let got = collect(it).unwrap();
        assert_eq!(got.len(), 3);
        assert_eq!(got[0].0.user_key, b"a");
        assert_eq!(got[0].0.seq, 5);
        assert_eq!(got[1].0.user_key, b"a");
        assert_eq!(got[1].0.seq, 1);
        assert_eq!(got[2].0.user_key, b"b");
    }

    #[test]
    fn dedup_keeps_only_newest_per_user_key() {
        let a = vec![(
            ik(b"k", 5, EntryKind::Value),
            Entry::value(b"new".to_vec(), None),
        )];
        let b = vec![(
            ik(b"k", 1, EntryKind::Value),
            Entry::value(b"old".to_vec(), None),
        )];
        let it = MergingIterator::new(vec![src(a), src(b)], MergeMode::Dedup).unwrap();
        let got = collect(it).unwrap();
        assert_eq!(got.len(), 1);
        assert_eq!(got[0].0.seq, 5);
        assert_eq!(got[0].1.value.as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn dedup_suppresses_keys_whose_newest_version_is_a_tombstone() {
        let a = vec![
            (ik(b"k", 9, EntryKind::Tombstone), Entry::tombstone()),
            (
                ik(b"k", 1, EntryKind::Value),
                Entry::value(b"v".to_vec(), None),
            ),
        ];
        let it = MergingIterator::new(vec![src(a)], MergeMode::Dedup).unwrap();
        let got = collect(it).unwrap();
        assert!(got.is_empty(), "tombstoned key must not surface in Dedup");
    }

    #[test]
    fn raw_mode_preserves_tombstones_for_compaction() {
        let a = vec![
            (ik(b"k", 9, EntryKind::Tombstone), Entry::tombstone()),
            (
                ik(b"k", 1, EntryKind::Value),
                Entry::value(b"v".to_vec(), None),
            ),
        ];
        let it = MergingIterator::new(vec![src(a)], MergeMode::Raw).unwrap();
        let got = collect(it).unwrap();
        assert_eq!(got.len(), 2);
        assert_eq!(got[0].0.kind, EntryKind::Tombstone);
    }

    #[test]
    fn interleaved_keys_from_many_sources_emerge_in_global_order() {
        let a = vec![
            (
                ik(b"a", 1, EntryKind::Value),
                Entry::value(b"1".to_vec(), None),
            ),
            (
                ik(b"c", 3, EntryKind::Value),
                Entry::value(b"3".to_vec(), None),
            ),
        ];
        let b = vec![
            (
                ik(b"b", 2, EntryKind::Value),
                Entry::value(b"2".to_vec(), None),
            ),
            (
                ik(b"d", 4, EntryKind::Value),
                Entry::value(b"4".to_vec(), None),
            ),
        ];
        let c = vec![(
            ik(b"e", 5, EntryKind::Value),
            Entry::value(b"5".to_vec(), None),
        )];
        let it = MergingIterator::new(vec![src(a), src(b), src(c)], MergeMode::Dedup).unwrap();
        let got = collect(it).unwrap();
        let keys: Vec<&[u8]> = got.iter().map(|(k, _)| k.user_key.as_slice()).collect();
        assert_eq!(
            keys,
            vec![&b"a"[..], &b"b"[..], &b"c"[..], &b"d"[..], &b"e"[..]]
        );
    }

    #[test]
    fn empty_input_yields_nothing() {
        let it = MergingIterator::new(vec![], MergeMode::Dedup).unwrap();
        let got = collect(it).unwrap();
        assert!(got.is_empty());
    }
}
