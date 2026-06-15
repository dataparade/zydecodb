//! Decoupled WAL durability state.
//!
//! The engine appends to the active WAL segment under its own (engine) lock, but
//! the `fsync` that makes those appends durable does NOT need the engine lock --
//! it needs only the active segment's file handle and the buffered/synced
//! sequence watermarks. Hoisting the `fsync` out from under the engine mutex lets
//! memtable inserts and snapshot captures proceed while a (slow) `fsync` is in
//! flight. This is the standard LSM commit-pipeline design (cf. RocksDB group
//! commit, Pebble's sync loop).
//!
//! Invariant: the unsynced suffix `(synced_seq, buffered_seq]` always lives in
//! the segment currently published as `active`. WAL rotation preserves this by
//! syncing the old segment *before* publishing the new one (see
//! `Engine::append_bytes_buffered`), so an `fsync` of the active segment always
//! covers the whole unsynced suffix.

use crate::errors::EngineResult;
use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Shared WAL durability state. Cheap to clone the wrapping `Arc`. The engine
/// owns the write/rotation path; the commit coordinator owns the `fsync` -- both
/// reach durable state through this type without serializing on the engine mutex.
pub struct WalSync {
    /// The active WAL segment, shared with the engine writer. Cloned (an `Arc`
    /// bump, O(1)) to `fsync` without holding this lock across the I/O; replaced
    /// on rotation. Appends and `fsync` on the same fd are safe at the OS level.
    active: Mutex<Option<Arc<File>>>,
    /// Highest seq appended to the active segment's page cache (durable or not).
    buffered_seq: AtomicU64,
    /// Highest seq known `fsync`ed to disk.
    synced_seq: AtomicU64,
    /// Optional metrics sink (set after `Engine::with_metrics`).
    metrics: Mutex<Option<Arc<crate::metrics::Metrics>>>,
}

impl WalSync {
    /// Construct with both watermarks at `initial_seq` (the max seq recovered at
    /// `Engine::open`). No active segment yet; the engine publishes one via
    /// [`set_active`](Self::set_active) when it opens the first segment.
    pub fn new(initial_seq: u64) -> Arc<WalSync> {
        Arc::new(WalSync {
            active: Mutex::new(None),
            buffered_seq: AtomicU64::new(initial_seq),
            synced_seq: AtomicU64::new(initial_seq),
            metrics: Mutex::new(None),
        })
    }

    /// Install (or clear) the metrics sink. Called from `Engine::with_metrics`.
    pub fn set_metrics(&self, metrics: Option<Arc<crate::metrics::Metrics>>) {
        *self.metrics.lock().unwrap() = metrics;
    }

    /// Publish a freshly-opened active segment. Called on open and on rotation,
    /// always with the engine lock held and only after the prior segment has been
    /// synced (preserving the module invariant).
    pub fn set_active(&self, file: Arc<File>) {
        *self.active.lock().unwrap() = Some(file);
    }

    /// Highest seq written to the WAL page cache (durable or not).
    pub fn buffered_seq(&self) -> u64 {
        self.buffered_seq.load(Ordering::Acquire)
    }

    /// Highest seq known `fsync`ed to disk.
    pub fn synced_seq(&self) -> u64 {
        self.synced_seq.load(Ordering::Acquire)
    }

    /// Advance the buffered watermark after an append. Engine lock held; the
    /// `Release` in `fetch_max` publishes the preceding `write_all` (and any
    /// active-segment swap that happened earlier in the same append) to a syncer
    /// that observes the new value via [`buffered_seq`](Self::buffered_seq).
    pub fn advance_buffered(&self, seq: u64) {
        self.buffered_seq.fetch_max(seq, Ordering::AcqRel);
    }

    /// Force both watermarks forward (replica apply / flush bookkeeping). Engine
    /// lock held.
    pub fn set_watermarks(&self, seq: u64) {
        self.buffered_seq.fetch_max(seq, Ordering::AcqRel);
        self.synced_seq.fetch_max(seq, Ordering::AcqRel);
    }

    /// Fsync the active segment, making all buffered appends durable. Returns the
    /// highest seq now guaranteed on disk. Safe to call WITHOUT the engine lock,
    /// so memtable inserts and snapshot captures proceed concurrently. A no-op
    /// when nothing new has been buffered since the last sync.
    pub fn sync(&self) -> EngineResult<u64> {
        // Capture the target BEFORE the active fd (module invariant): observing
        // new-segment data in `buffered_seq` via an Acquire load pairs with the
        // writer's Release, so the matching active swap is already visible.
        let target = self.buffered_seq.load(Ordering::Acquire);
        let synced = self.synced_seq.load(Ordering::Acquire);
        if target <= synced {
            return Ok(synced);
        }
        crate::engine_fail_point!(crate::failpoints::WAL_BEFORE_FSYNC);
        let start = Instant::now();
        fail::fail_point!(crate::failpoints::WAL_LIE_FSYNC, |_| {
            // Simulate a lying fsync: advance the watermark without durability.
            if let Some(m) = self.metrics.lock().unwrap().as_ref() {
                m.wal_fsync_duration_seconds.observe(0.0);
            }
            self.synced_seq.fetch_max(target, Ordering::AcqRel);
            Ok(target)
        });
        // Clone the active fd (O(1)) and release the lock BEFORE the fsync, so the
        // slow I/O never blocks an append's segment swap or another syncer.
        let file = self.active.lock().unwrap().clone();
        if let Some(f) = file {
            f.sync_all()?;
        }
        crate::engine_fail_point!(crate::failpoints::WAL_AFTER_FSYNC);
        if let Some(m) = self.metrics.lock().unwrap().as_ref() {
            m.wal_fsync_duration_seconds
                .observe(start.elapsed().as_secs_f64());
        }
        self.synced_seq.fetch_max(target, Ordering::AcqRel);
        Ok(target)
    }
}
