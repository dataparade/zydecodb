//! In-memory sorted store. `BTreeMap`, not `HashMap` — ordered iteration is the
//! foundation of every future relational feature.
//!
//! Implemented in Step 1.2.

use crate::entry::Entry;
use crate::keys::{EntryKind, InternalKey};
use std::collections::BTreeMap;
use std::ops::Bound;

#[derive(Debug, Default, Clone)]
pub struct Memtable {
    map: BTreeMap<InternalKey, Entry>,
    size_bytes: usize,
    min_seq: u64,
    max_seq: u64,
}

impl Memtable {
    pub fn new() -> Self {
        Memtable {
            map: BTreeMap::new(),
            size_bytes: 0,
            min_seq: u64::MAX,
            max_seq: 0,
        }
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    pub fn min_seq(&self) -> u64 {
        if self.map.is_empty() {
            0
        } else {
            self.min_seq
        }
    }

    pub fn max_seq(&self) -> u64 {
        self.max_seq
    }

    /// Insert an entry. Overhead accounting is approximate (key + value + fixed
    /// per-entry overhead for the InternalKey + Entry bookkeeping).
    pub fn insert(&mut self, key: InternalKey, entry: Entry) {
        self.min_seq = self.min_seq.min(key.seq);
        self.max_seq = self.max_seq.max(key.seq);
        let added = entry_footprint(&key, &entry);
        if let Some(old) = self.map.insert(key.clone(), entry) {
            // Replacing the exact same InternalKey (same user_key+seq+kind);
            // adjust accounting by the delta.
            let old_fp = entry_footprint(&key, &old);
            self.size_bytes = self.size_bytes + added - old_fp;
        } else {
            self.size_bytes += added;
        }
    }

    /// Point lookup: the newest entry for `user_key` (any kind). Returns the
    /// `(InternalKey, Entry)` so the caller can distinguish Value vs Tombstone.
    pub fn get_latest(&self, user_key: &[u8]) -> Option<(&InternalKey, &Entry)> {
        // Seek to (user_key, u64::MAX). Because seq sorts DESC, the first entry
        // at or after this bound with a matching user_key is the newest.
        let lower = InternalKey::new(user_key.to_vec(), u64::MAX, EntryKind::Value);
        self.map
            .range((Bound::Included(lower), Bound::Unbounded))
            .next()
            .filter(|(k, _)| k.user_key == user_key)
    }

    /// Ordered iteration over all entries. Used by flush and (later) compaction.
    pub fn iter(&self) -> impl Iterator<Item = (&InternalKey, &Entry)> {
        self.map.iter()
    }

    /// Range scan over user keys. Internal API; not surfaced over IPC in v1.
    pub fn range(&self, start: &[u8], end: &[u8]) -> impl Iterator<Item = (&InternalKey, &Entry)> {
        let lo = InternalKey::new(start.to_vec(), u64::MAX, EntryKind::Value);
        let hi = InternalKey::new(end.to_vec(), u64::MAX, EntryKind::Value);
        self.map.range((Bound::Included(lo), Bound::Excluded(hi)))
    }

    /// Concrete-typed full-range iterator. Used by [`crate::iter::MemtableIter`]
    /// when it needs to store the iterator in a struct (`impl Iterator` cannot
    /// name its type for that).
    pub fn range_internal_unbounded(
        &self,
    ) -> std::collections::btree_map::Range<'_, InternalKey, Entry> {
        self.map.range::<InternalKey, _>(..)
    }

    /// Direct access to the backing BTreeMap for callers that need to
    /// build their own range (e.g. snapshot lookups that bound by both
    /// user_key AND seq). Read-only.
    pub fn iter_internal(&self) -> &BTreeMap<InternalKey, Entry> {
        &self.map
    }

    /// Concrete-typed bounded range iterator over user-key range `[lo, hi)`.
    pub fn range_internal(
        &self,
        lo: &[u8],
        hi: &[u8],
    ) -> std::collections::btree_map::Range<'_, InternalKey, Entry> {
        let lo_k = InternalKey::new(lo.to_vec(), u64::MAX, EntryKind::Value);
        let hi_k = InternalKey::new(hi.to_vec(), u64::MAX, EntryKind::Value);
        self.map
            .range((Bound::Included(lo_k), Bound::Excluded(hi_k)))
    }
}

fn entry_footprint(key: &InternalKey, entry: &Entry) -> usize {
    // user_key bytes + value bytes + fixed overhead (seq 8 + kind 1 + expires 8 +
    // map node overhead estimate 48).
    key.user_key.len() + entry.value_len() + 8 + 1 + 8 + 48
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uk(k: &[u8]) -> Vec<u8> {
        let mut v = vec![crate::keys::KS_USER];
        v.extend_from_slice(k);
        v
    }

    #[test]
    fn get_latest_returns_newest_seq() {
        let mut m = Memtable::new();
        m.insert(
            InternalKey::new(uk(b"a"), 1, EntryKind::Value),
            Entry::value(b"old".to_vec(), None),
        );
        m.insert(
            InternalKey::new(uk(b"a"), 2, EntryKind::Value),
            Entry::value(b"new".to_vec(), None),
        );
        let (k, e) = m.get_latest(&uk(b"a")).unwrap();
        assert_eq!(k.seq, 2);
        assert_eq!(e.value.as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn tombstone_shadows_value() {
        let mut m = Memtable::new();
        m.insert(
            InternalKey::new(uk(b"a"), 1, EntryKind::Value),
            Entry::value(b"v".to_vec(), None),
        );
        m.insert(
            InternalKey::new(uk(b"a"), 2, EntryKind::Tombstone),
            Entry::tombstone(),
        );
        let (k, e) = m.get_latest(&uk(b"a")).unwrap();
        assert_eq!(k.seq, 2);
        assert!(e.is_tombstone());
    }

    #[test]
    fn size_accounting_grows_and_tracks() {
        let mut m = Memtable::new();
        assert_eq!(m.size_bytes(), 0);
        m.insert(
            InternalKey::new(uk(b"a"), 1, EntryKind::Value),
            Entry::value(vec![0u8; 100], None),
        );
        let after_one = m.size_bytes();
        assert!(after_one >= 100);
        m.insert(
            InternalKey::new(uk(b"b"), 2, EntryKind::Value),
            Entry::value(vec![0u8; 100], None),
        );
        assert!(m.size_bytes() > after_one);
    }

    #[test]
    fn seq_bounds_tracked() {
        let mut m = Memtable::new();
        m.insert(
            InternalKey::new(uk(b"a"), 5, EntryKind::Value),
            Entry::value(b"x".to_vec(), None),
        );
        m.insert(
            InternalKey::new(uk(b"b"), 9, EntryKind::Value),
            Entry::value(b"y".to_vec(), None),
        );
        m.insert(
            InternalKey::new(uk(b"c"), 3, EntryKind::Value),
            Entry::value(b"z".to_vec(), None),
        );
        assert_eq!(m.min_seq(), 3);
        assert_eq!(m.max_seq(), 9);
    }

    #[test]
    fn missing_key_returns_none() {
        let m = Memtable::new();
        assert!(m.get_latest(&uk(b"nope")).is_none());
    }
}
