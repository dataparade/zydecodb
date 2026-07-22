//! The engine orchestrates memtable, WAL, SSTables, and the manifest.
//!
//! The engine uses `&mut self` for writes — it is a single-owner type with no
//! internal locking. Background flush/compaction/apply run on dedicated OS
//! threads and communicate via channels. The server shares the engine across
//! connection threads behind an [`crate::engine_handle::EngineHandle`], which
//! serializes the write domain (`Mutex<Engine>`) while cache / fair / WAL sync
//! use separate locks. Long reads take the write mutex only briefly to build
//! an owned snapshot.

use crate::entry::Entry;
use crate::errors::{EngineError, EngineResult};
use crate::keys::{self, EntryKind, InternalKey};
use crate::manifest::{self, ManifestRecord, SstableMeta};
use crate::memtable::Memtable;
use crate::owned_snapshot::SnapshotHandle;
use crate::policy::{NoopWritePolicy, WritePolicy};
use crate::seq::SeqAllocator;
use crate::sstable::SstableReader;
use crate::stats::{SstableStat, Stats, WalSegmentStat};
use crate::wal::{self, WalRecord};
use std::collections::{BTreeMap, VecDeque};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Per-engine configuration.
///
/// Use [`EngineConfig::default`] and the struct-update pattern to override
/// only what you care about:
///
/// ```no_run
/// use zydecodb_engine::engine::{Engine, EngineConfig};
/// let engine = Engine::open(EngineConfig {
///     data_dir: "/var/lib/zydecodb".into(),
///     wal_dir:  "/var/lib/zydecodb/wal".into(),
///     block_cache_bytes: 512 * 1024 * 1024,
///     ..Default::default()
/// }).unwrap();
/// ```
#[derive(Clone)]
pub struct EngineConfig {
    /// Directory holding SSTables and the manifest. Created if missing.
    pub data_dir: PathBuf,
    /// Directory holding WAL segments. Created if missing. Putting it on a
    /// separate disk from `data_dir` improves write latency under
    /// compaction pressure (the WAL fsync and the compaction writes no
    /// longer contend on the same device queue).
    pub wal_dir: PathBuf,
    /// Soft byte cap on the shared SSTable block cache. Default 256 MB.
    /// Tune up for read-heavy workloads with large hot sets; tune down for
    /// memory-constrained embedders. Capacity is enforced approximately —
    /// a single block larger than the cap is still kept (it just becomes
    /// the only resident block).
    pub block_cache_bytes: usize,
    /// LSM compaction tunables. See [`crate::compaction::CompactionConfig`]
    /// for per-field semantics and defaults (leveled-light, L0→L1→L2, 10x
    /// size ratio per level).
    pub compaction: crate::compaction::CompactionConfig,
    /// Point-lookup result cache (keyed by user key). Survives compaction
    /// invalidation unlike the block cache. 0 = disabled.
    pub result_cache_bytes: usize,
    /// Active memtable size before freeze (default 64 MB).
    pub memtable_flush_threshold: usize,
    /// Immutable memtable queue depth before write stall (default 4).
    pub max_immutable_memtables: usize,
    /// Minimum interval between manifest `fsync`s during background apply.
    /// Writes are buffered; one fsync may cover multiple flush/compaction
    /// catalog updates. Forced on [`Engine::shutdown`] and [`Engine::force_flush`].
    pub manifest_sync_debounce_ms: u64,
    /// Cap on open SSTable readers (table cache). Metadata is pinned per
    /// reader; 0 = unlimited. Default 128 — high enough that soak workloads
    /// never evict; bounds RSS at large scale.
    pub max_open_readers: usize,
    /// Point-in-time restore ceiling: when set, WAL replay during `open` ignores
    /// any record with `seq > N`, so the engine boots at exactly that sequence.
    /// `None` (default) replays the entire WAL. Used by `admin restore`.
    pub wal_replay_max_seq: Option<u64>,
    /// δ-fair multi-tenant isolation (Phase 5). Disabled by default; enable
    /// under pods when per-tenant limits exist. See [`crate::tenant_fair`].
    pub fair: crate::tenant_fair::FairConfig,
    /// Override L0 file count that triggers write stall. `None` uses
    /// `l0_trigger * 5` (floored at 20). Tests and dense pods may lower this.
    pub l0_write_stall_threshold: Option<usize>,
}

impl Default for EngineConfig {
    fn default() -> Self {
        EngineConfig {
            data_dir: PathBuf::from("data"),
            wal_dir: PathBuf::from("data/wal"),
            block_cache_bytes: 256 * 1024 * 1024,
            result_cache_bytes: 0,
            compaction: crate::compaction::CompactionConfig::default(),
            memtable_flush_threshold: keys::MEMTABLE_FLUSH_THRESHOLD,
            max_immutable_memtables: keys::MAX_IMMUTABLE_MEMTABLES,
            manifest_sync_debounce_ms: 50,
            max_open_readers: 128,
            wal_replay_max_seq: None,
            fair: crate::tenant_fair::FairConfig::default(),
            l0_write_stall_threshold: None,
        }
    }
}

/// A live SSTable in the engine catalog.
struct LoadedSstable {
    meta: SstableMeta,
    /// Pinned for the file's lifetime in the catalog (opened via [`ReaderCache`]).
    reader: Arc<SstableReader>,
}

/// One operation in an atomic [`Engine::write_batch`]: a put (with optional
/// `expires_at`, `0` = none) or a delete. Keys are full storage keys in the
/// user keyspace, exactly as for [`Engine::put`] / [`Engine::del`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BatchOp {
    Put {
        key: Vec<u8>,
        value: Vec<u8>,
        expires_at: u64,
    },
    Del {
        key: Vec<u8>,
    },
}

impl BatchOp {
    /// The storage key this op targets.
    pub fn key(&self) -> &[u8] {
        match self {
            BatchOp::Put { key, .. } => key,
            BatchOp::Del { key } => key,
        }
    }

    fn value_len(&self) -> usize {
        match self {
            BatchOp::Put { value, .. } => value.len(),
            BatchOp::Del { .. } => 0,
        }
    }

    fn is_delete(&self) -> bool {
        matches!(self, BatchOp::Del { .. })
    }
}

pub struct Engine {
    cfg: EngineConfig,
    seq: Arc<SeqAllocator>,
    /// Active memtable behind `Arc` so owned snapshots pin without deep-clone.
    /// Writers use [`Arc::make_mut`] (COW when a snapshot still holds the Arc).
    active: Arc<Memtable>,
    immutable: VecDeque<Arc<Memtable>>,
    sstables: Vec<LoadedSstable>, // newest last
    next_sstable_id: u64,
    active_wal_id: u64,
    /// Active WAL segment file. Held as an `Arc` so the same fd is shared with
    /// [`wal_sync`](Self::wal_sync) and can be `fsync`ed off the engine lock.
    active_wal: Option<Arc<File>>,
    active_wal_size: usize,
    in_flight_wal_bytes: usize,
    /// Cached max `seq` of each *sealed* (not active) WAL segment on disk.
    /// Populated when a segment is sealed (the seq is known at seal time —
    /// it's `last_buffered_seq`), and reconstructed on `Engine::open` for
    /// segments left from a prior process. Used by [`wal_segments_covered`]
    /// to decide which segments a flush has made redundant WITHOUT
    /// re-reading and re-decoding every segment file. Previously that scan
    /// cost 400-700ms per flush at scale (see soak harness findings).
    sealed_segment_max_seq: BTreeMap<u64, u64>,
    /// Decoupled WAL durability state (active segment fd + buffered/synced
    /// watermarks). Shared with the commit coordinator so the group-commit
    /// `fsync` runs without holding the engine mutex.
    wal_sync: Arc<crate::wal_sync::WalSync>,
    /// When true, writers buffer their WAL append and rely on a coordinator to
    /// batch the fsync (group commit). When false, every append fsyncs inline.
    group_commit: bool,
    /// Off-box durability: when set, each sealed WAL segment is shipped (hardlink
    /// or copy) into this directory for an operator sidecar to transport. Empty =
    /// disabled.
    ship_dir: Option<PathBuf>,
    /// How sealed segments reach `ship_dir`: hardlink (default, atomic, free) or
    /// copy (required across filesystems).
    ship_mode: crate::shipping::ShipMode,
    /// Optional HMAC key authenticating each `shipped.log` entry so a writable
    /// ship directory cannot forge segments plus matching manifest lines.
    ship_hmac_key: Option<Vec<u8>>,
    manifest_file: Arc<Mutex<File>>,
    metrics: Option<Arc<crate::metrics::Metrics>>,
    /// Shared block cache for all SSTable readers. Constructed at engine
    /// open from `cfg.block_cache_bytes`; lives for the engine's lifetime.
    block_cache: Arc<crate::block_cache::BlockCache>,
    reader_cache: Arc<crate::reader_cache::ReaderCache>,
    result_cache: Option<Arc<crate::result_cache::ResultCache>>,
    started: Instant,
    /// True when this engine instance was opened from a clean-shutdown marker
    /// (the previous process flushed and exited gracefully, so no unflushed WAL
    /// data needed replaying). Observational; surfaced via metrics.
    clean_boot: bool,
    /// Write-time policy hook (see [`WritePolicy`]). Defaults to a no-op so the
    /// engine is unencumbered; an embedder installs a real policy via
    /// [`Engine::with_write_policy`]. Held in an `Arc` so it can be cloned out
    /// before invocation, avoiding a self-borrow.
    policy: Arc<dyn WritePolicy>,
    /// Background compaction scheduler (dedicated OS thread).
    compaction_scheduler: crate::compaction_worker::CompactionScheduler,
    /// Background memtable flush scheduler (dedicated OS thread).
    flush_scheduler: crate::flush_worker::FlushScheduler,
    /// Background manifest fsync for flush/compaction catalog updates.
    apply_scheduler: crate::apply_worker::ApplyScheduler,
    /// Shared with the compaction worker for output SSTable id allocation.
    next_sstable_id_atomic: Arc<AtomicU64>,
    /// Pins for long-lived snapshots and deferred SSTable unlinks.
    pin_state: Arc<std::sync::Mutex<crate::owned_snapshot::PinState>>,
    /// When true, PUT/DEL return EngineBusy (maintenance window).
    freeze_writes: bool,
    /// Data-dir dentry sync deferred from apply; flushed at end of poll/shutdown.
    dir_fsync_pending: bool,
    /// Per-sample-window apply timing for soak harness (count, sum_ns, max_ns).
    apply_window_count: AtomicU64,
    apply_window_sum_ns: AtomicU64,
    apply_window_max_ns: AtomicU64,
    /// Manifest bytes appended since the last `sync_all`.
    manifest_dirty: bool,
    last_manifest_sync: Instant,
    /// Exclusive advisory lock on `data_dir/LOCK`, held for the lifetime of the
    /// engine. Guards against two processes opening the same directory (which
    /// would corrupt the shared manifest/WAL/SSTables). Released automatically
    /// when the file handle drops. Never read after construction; the value
    /// exists solely to keep the lock held.
    #[allow(dead_code)]
    data_dir_lock: File,
    /// Write slowdown requested by the last backpressure check. Callers that
    /// hold the engine mutex must [`Self::take_write_slowdown`] and sleep
    /// *after* releasing the lock — never sleep under the mutex.
    pending_write_slowdown: std::time::Duration,
    /// δ-fair accounting (Phase 4–5). Disabled by default.
    fair: std::sync::Arc<crate::tenant_fair::FairShareState>,
}

/// Name of the clean-shutdown marker written in the data dir by
/// [`Engine::shutdown`] and consumed (then removed) on the next [`Engine::open`].
const CLEAN_SHUTDOWN_MARKER: &str = "CLEAN_SHUTDOWN";

impl Drop for Engine {
    fn drop(&mut self) {
        // Failpoint panic tests may unwind through Drop while the point is still
        // armed; never double-panic during background drain.
        let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _ = self.drain_background_work();
            let _ = self.sync_manifest();
            let _ = self.maybe_sync_data_dir();
        }));
        self.apply_scheduler.shutdown();
    }
}

mod open;
mod write;
mod read;
mod catalog;
mod maintain;
mod stats;

// Allow reading a segment's seek position cleanly in tests/util.
#[allow(dead_code)]
pub(crate) fn file_len(f: &mut File) -> std::io::Result<u64> {
    let pos = f.stream_position()?;
    let end = f.seek(SeekFrom::End(0))?;
    f.seek(SeekFrom::Start(pos))?;
    Ok(end)
}

#[cfg(test)]
#[path = "../engine_tests.rs"]
mod tests;
