use super::*;

impl Engine {
    /// GET the latest value for a key. Returns None for missing or tombstoned keys.
    pub fn get(&self, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        keys::validate_user_key(key)?;
        // Reads route through the snapshot path so there's one merging
        // implementation for both point and range reads. `seq_upper = u64::MAX`
        // means "see everything currently in the engine."
        self.snapshot_get(u64::MAX, key)
    }

    /// Number of live SSTables at each level, for tests and operational
    /// inspection. Returns a `Vec<(level, count)>` in ascending-level order.
    pub fn live_sstable_levels(&self) -> Vec<(u8, usize)> {
        let mut by: std::collections::BTreeMap<u8, usize> = std::collections::BTreeMap::new();
        for s in &self.sstables {
            *by.entry(s.meta.level).or_insert(0) += 1;
        }
        by.into_iter().collect()
    }

    /// Open a read-only, point-in-time view over the engine's current state.
    ///
    /// The returned [`crate::snapshot::SnapshotView`] borrows the engine;
    /// while it lives, no mutations can run (the borrow is shared, so
    /// `&mut self` is unavailable to other callers). Drop the view to
    /// release the borrow.
    ///
    /// v1 ships scoped snapshots only. Long-lived snapshots that survive
    /// across writes, flushes, and compactions are deferred to v2; the
    /// API shape here is forward-compatible (the same `SnapshotView`
    /// type will gain owned variants).
    pub fn snapshot(&self) -> crate::snapshot::SnapshotView<'_> {
        crate::snapshot::SnapshotView {
            engine: self,
            seq_upper: self.seq.peek().saturating_sub(1),
        }
    }

    /// Create a long-lived owned snapshot that pins live SSTables until dropped.
    pub fn snapshot_owned(&self) -> SnapshotHandle {
        self.snapshot_with_ceiling(self.seq.peek().saturating_sub(1))
    }

    /// Owned snapshot pinned at a caller-provided sequence ceiling, for
    /// repeatable-read pagination across stateless requests (the ceiling is
    /// carried in the page cursor). The ceiling is clamped to the engine's
    /// current max, so a cursor can never read writes newer than itself.
    ///
    /// Consistency: this rebuilds a fresh snapshot filtered to `seq_upper`. It
    /// is repeatable as long as the versions at `seq_upper` are still retained
    /// (compaction GC only drops versions at or below the oldest *live*
    /// snapshot). It never exposes writes newer than `seq_upper`, so pagination
    /// stays stable; after a very long gap a concurrently-rewritten row may drop
    /// out of later pages rather than reappear with newer data.
    pub fn snapshot_at(&self, seq_upper: u64) -> SnapshotHandle {
        let ceiling = seq_upper.min(self.seq.peek().saturating_sub(1));
        self.snapshot_with_ceiling(ceiling)
    }

    pub(crate) fn snapshot_with_ceiling(&self, seq_upper: u64) -> SnapshotHandle {
        let sstable_ids: Vec<u64> = self.sstables.iter().map(|s| s.meta.id).collect();
        for id in &sstable_ids {
            self.reader_cache.pin(*id);
        }
        SnapshotHandle::new(
            seq_upper,
            Arc::clone(&self.active),
            self.immutable.iter().cloned().collect(),
            self.sstables.iter().map(|s| s.reader.clone()).collect(),
            self.sstables.iter().map(|s| s.meta.clone()).collect(),
            sstable_ids,
            self.pin_state.clone(),
            self.cfg.data_dir.clone(),
            self.block_cache.clone(),
            self.reader_cache.clone(),
        )
    }

    /// Count of SSTable readers currently resident in the table cache.
    pub fn reader_cache_open_count(&self) -> usize {
        self.reader_cache.open_count()
    }

    /// Streaming range scan over user keys in `[lo, hi)` (half-open).
    ///
    /// Returns a [`crate::snapshot::RangeIter`] that yields
    /// `(user_key, value)` pairs in user-key ascending order. Tombstones
    /// and expired entries are suppressed; multiple versions of the same
    /// key are collapsed to the newest.
    ///
    /// The iterator borrows the engine; while it is alive, no mutations
    /// can run on the engine (Rust's borrow checker enforces this). For
    /// long scans, prefer to drop the iterator promptly or to `.collect()`
    /// up front. Internally this is equivalent to
    /// `self.snapshot().scan(lo, hi)` — a fresh scoped snapshot per call.
    ///
    /// Keys MUST be in the user keyspace (prefix byte `0x01`); the engine
    /// does not validate the bounds here, but a scan whose range falls
    /// outside the user keyspace will simply return no entries.
    pub fn scan(&self, lo: Vec<u8>, hi: Vec<u8>) -> EngineResult<crate::snapshot::RangeIter<'_>> {
        self.snapshot().scan(lo, hi)
    }

    /// Streaming scan over every key whose user key starts with `prefix`.
    ///
    /// Sugar over [`Engine::scan`] with `hi` set to the smallest byte
    /// string lexicographically greater than `prefix` (so a prefix of
    /// `b"user:42:"` yields keys `b"user:42:name"`, `b"user:42:email"`,
    /// etc., but never crosses into `b"user:43:..."`).
    pub fn prefix_scan(&self, prefix: Vec<u8>) -> EngineResult<crate::snapshot::RangeIter<'_>> {
        self.snapshot().prefix_scan(prefix)
    }

    /// Internal point-lookup that respects a sequence-number ceiling.
    /// Used by [`crate::snapshot::SnapshotView::get`] and by [`Engine::get`].
    ///
    /// Walks active memtable -> immutable memtables (newest first) ->
    /// SSTables (newest first), returning the first entry whose seq is at
    /// or below `seq_upper`. Tombstones suppress; expired entries suppress.
    pub fn snapshot_get(&self, seq_upper: u64, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        keys::validate_user_key(key).or_else(|_| keys::validate_system_key(key))?;
        let now = Self::now_ms();

        if let Some((ik, entry)) = self.first_visible_in_memtable(&self.active, key, seq_upper) {
            return Ok(self.resolve(&ik, &entry, now));
        }
        for mt in self.immutable.iter().rev() {
            if let Some((ik, entry)) = self.first_visible_in_memtable(mt, key, seq_upper) {
                return Ok(self.resolve(&ik, &entry, now));
            }
        }
        if seq_upper == u64::MAX {
            if let Some(rc) = &self.result_cache {
                if let Some(hit) = rc.get(key) {
                    if let Some(m) = &self.metrics {
                        m.result_cache_hits_total.inc();
                    }
                    return Ok(Some(hit));
                }
                if let Some(m) = &self.metrics {
                    m.result_cache_misses_total.inc();
                }
            }
        }

        let start = Instant::now();
        for sst in self.sstables.iter().rev() {
            if !Self::sstable_might_hold_key(&sst.meta, key) {
                continue;
            }
            let bloom_maybe = sst.reader.might_contain(key);
            if !bloom_maybe {
                continue;
            }
            let found = if seq_upper == u64::MAX {
                // Fast path: bloom + index block lookup. This is what every
                // normal `get()` hits; the previous `scan_all()` loop here
                // was O(table) per GET and collapsed soak throughput.
                sst.reader.get_latest(key)?
            } else {
                // Bounded snapshot: the newest entry for this key in this
                // SSTable may have seq > seq_upper, so walk only the key's
                // block range (still index-bounded, not a full table scan).
                self.newest_visible_in_sstable(sst.reader.clone(), key, seq_upper)?
            };
            match found {
                Some((ik, entry)) => {
                    if let Some(m) = &self.metrics {
                        m.sstable_get_duration_seconds
                            .observe(start.elapsed().as_secs_f64());
                    }
                    let resolved = self.resolve(&ik, &entry, now);
                    if let (Some(rc), Some(v)) = (&self.result_cache, &resolved) {
                        rc.insert(key.to_vec(), v.clone());
                    }
                    return Ok(resolved);
                }
                None => {
                    if let Some(m) = &self.metrics {
                        m.bloom_false_positives_total.inc();
                    }
                }
            }
        }
        Ok(None)
    }

    /// Newest `(InternalKey, Entry)` for `user_key` in `reader` with
    /// `seq <= seq_upper`, or None. Uses `range_iter` over `[key, hi)` so
    /// only candidate blocks are touched.
    pub(crate) fn newest_visible_in_sstable(
        &self,
        reader: Arc<SstableReader>,
        user_key: &[u8],
        seq_upper: u64,
    ) -> EngineResult<Option<(InternalKey, Entry)>> {
        use crate::iter::EntryIterator;
        let hi = Self::next_user_key(user_key);
        let mut it = reader.range_iter(user_key.to_vec(), hi)?;
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

    /// Build a merging iterator for a snapshot's range scan. Lives here
    /// (not in snapshot.rs) so it can reach engine-private state directly.
    pub(crate) fn build_merging_iterator(
        &self,
        seq_upper: u64,
        lo: Vec<u8>,
        hi: Vec<u8>,
    ) -> EngineResult<crate::iter::MergingIterator<'_>> {
        crate::snapshot::build_sources(
            self.active.as_ref(),
            self.immutable.iter().map(|m| m.as_ref()),
            &self
                .sstables
                .iter()
                .filter(|s| Self::sstable_overlaps_range(&s.meta, &lo, &hi))
                .map(|s| s.reader.clone())
                .collect::<Vec<_>>(),
            seq_upper,
            lo,
            hi,
        )
    }

    pub(crate) fn sstable_might_hold_key(meta: &SstableMeta, key: &[u8]) -> bool {
        key >= meta.min_key.as_slice() && key <= meta.max_key.as_slice()
    }

    pub(crate) fn sstable_overlaps_range(meta: &SstableMeta, lo: &[u8], hi: &[u8]) -> bool {
        if !hi.is_empty() && meta.min_key.as_slice() >= hi {
            return false;
        }
        if meta.max_key.as_slice() < lo {
            return false;
        }
        true
    }

    /// Find the newest entry for `user_key` in `mt` whose seq is at or
    /// below `seq_upper`. Returns owned copies so the caller doesn't need
    /// to hold the memtable borrow.
    pub(crate) fn first_visible_in_memtable(
        &self,
        mt: &Memtable,
        user_key: &[u8],
        seq_upper: u64,
    ) -> Option<(InternalKey, Entry)> {
        // `get_latest` returns the newest seq; if that's still above the
        // ceiling we must walk the rest manually. Common case (no snapshot)
        // is seq_upper == u64::MAX which the fast path handles.
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

    /// Smallest user key strictly greater than `key`, for half-open range
    /// `[key, hi)` in bounded snapshot lookups.
    pub(crate) fn next_user_key(key: &[u8]) -> Vec<u8> {
        let mut out = key.to_vec();
        for i in (0..out.len()).rev() {
            if out[i] != 0xFF {
                out[i] += 1;
                out.truncate(i + 1);
                return out;
            }
        }
        out.push(0x00);
        out
    }

    /// Resolve an entry to a value: None for tombstone or expired.
    /// (Not-found accounting is the caller's concern so it can be labeled
    /// and so internal `sys_*` calls don't inflate user-facing counters.)
    pub(crate) fn resolve(&self, _ik: &InternalKey, entry: &Entry, now: u64) -> Option<Vec<u8>> {
        if entry.is_tombstone() || entry.is_expired(now) {
            return None;
        }
        entry.value.clone()
    }
}
