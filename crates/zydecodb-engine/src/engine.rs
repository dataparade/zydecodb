//! The engine orchestrates memtable, WAL, SSTables, and the manifest.
//!
//! The engine uses `&mut self` for writes — it is a single-owner type with no
//! internal locking. Background flush/compaction/apply run on dedicated OS
//! threads and communicate via channels. The server shares the engine across
//! connection threads behind an `Arc<Mutex<Engine>>`, which serializes engine
//! operations; long reads take the lock only briefly to build an owned snapshot.

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
    active: Memtable,
    immutable: VecDeque<Memtable>,
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

impl Engine {
    /// Open (or create) an engine rooted at the configured directories, running
    /// full recovery (manifest -> orphan cleanup -> WAL replay).
    pub fn open(cfg: EngineConfig) -> EngineResult<Engine> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        std::fs::create_dir_all(&cfg.wal_dir)?;

        // Take the exclusive data_dir lock before touching any artifact. A second
        // process (or a stray offline CLI command) opening the same directory is
        // rejected here rather than racing on the manifest/WAL/SSTables.
        let data_dir_lock = Self::acquire_data_dir_lock(&cfg.data_dir)?;

        // Detect (and immediately clear) the clean-shutdown marker. We clear it
        // up front so that if THIS process later crashes, the next boot correctly
        // reports an unclean start rather than a stale clean one.
        let marker_path = cfg.data_dir.join(CLEAN_SHUTDOWN_MARKER);
        let clean_boot = marker_path.exists();
        if clean_boot {
            let _ = std::fs::remove_file(&marker_path);
            tracing::info!("clean-shutdown marker found; previous stop was graceful");
        } else {
            tracing::info!("no clean-shutdown marker; recovering via WAL replay");
        }

        // 1. Replay manifest.
        let manifest_path = cfg.data_dir.join("MANIFEST");
        let state = manifest::load(&manifest_path)?;
        tracing::info!(
            live_sstables = state.live_sstables.len(),
            last_durable_seq = state.last_durable_seq,
            "manifest replayed"
        );

        // 2. Delete orphan .sst files (Invariant R2).
        let live_ids: std::collections::HashSet<u64> =
            state.live_sstables.iter().map(|m| m.id).collect();
        Self::delete_orphan_sstables(&cfg.data_dir, &live_ids)?;

        // Construct shared caches before opening any reader.
        let block_cache = crate::block_cache::BlockCache::new(cfg.block_cache_bytes);
        let reader_cache = crate::reader_cache::ReaderCache::new(cfg.max_open_readers);
        let result_cache = if cfg.result_cache_bytes > 0 {
            Some(crate::result_cache::ResultCache::new(
                cfg.result_cache_bytes,
            ))
        } else {
            None
        };

        // Load live SSTables (oldest id first => newest last).
        let mut metas = state.live_sstables.clone();
        metas.sort_by_key(|m| m.id);
        let mut max_sstable_id = 0u64;
        let mut max_seq_seen = state.last_durable_seq;
        let mut sstables = Vec::new();
        let mut legacy_sstables = 0usize;
        for meta in metas {
            max_sstable_id = max_sstable_id.max(meta.id);
            max_seq_seen = max_seq_seen.max(meta.max_seq);
            let path = Self::sstable_path(&cfg.data_dir, meta.id);
            let reader = reader_cache.get_or_open(&path, meta.id, block_cache.clone())?;
            if reader.format_version() < crate::sstable::FORMAT_VERSION {
                legacy_sstables += 1;
            }
            sstables.push(LoadedSstable { meta, reader });
        }
        // Surface the on-disk format mix so operators can see when a `data_dir`
        // still holds pre-upgrade (legacy-format) SSTables. They remain readable;
        // `zydecodb admin upgrade` (or ongoing compaction) rewrites them forward.
        if legacy_sstables > 0 {
            tracing::warn!(
                legacy_sstables,
                total_sstables = sstables.len(),
                current_format = crate::sstable::FORMAT_VERSION,
                "on-disk SSTables include legacy-format files (readable; run `admin upgrade` to rewrite)"
            );
        } else if !sstables.is_empty() {
            tracing::info!(
                total_sstables = sstables.len(),
                format_version = crate::sstable::FORMAT_VERSION,
                "all SSTables at current on-disk format"
            );
        }

        // 3. Replay WAL segments (Invariant R1: skip seq <= max_seq of live SSTables).
        //    As a side effect of the scan, record each segment's max seq into
        //    `sealed_segment_max_seq` so post-recovery flushes can compute WAL
        //    coverage without re-reading any segment files. (All segments found
        //    here become "sealed" after open, because open_new_wal_segment will
        //    allocate a fresh active segment id beyond max_wal_id+1.)
        let mut active = Memtable::new();
        let segments = wal::list_segments(&cfg.wal_dir)?;
        let sstable_max_seq = sstables.iter().map(|s| s.meta.max_seq).max().unwrap_or(0);
        let mut replayed = 0usize;
        let mut max_wal_id = 0u64;
        let mut sealed_segment_max_seq: BTreeMap<u64, u64> = BTreeMap::new();
        // Only the highest-id segment was the active one at crash time, so it is
        // the only segment allowed to end with a torn tail. Any earlier segment
        // was sealed and fsynced complete before the roll, so damage there is
        // bit-rot, not a crash artifact.
        let active_wal_id_at_crash = segments.iter().map(|(id, _)| *id).max();
        for (id, path) in &segments {
            max_wal_id = max_wal_id.max(*id);
            let is_active = Some(*id) == active_wal_id_at_crash;
            let (records, outcome) = Self::read_segment(path)?;
            match outcome {
                wal::ReplayOutcome::Clean => {}
                wal::ReplayOutcome::TornTail if is_active => {
                    tracing::info!(segment = %path.display(), "truncated torn WAL tail on replay");
                    Self::truncate_torn_segment(path)?;
                }
                wal::ReplayOutcome::TornTail => {
                    // A sealed segment must be intact; an incomplete record here
                    // means the file was truncated/damaged after sealing.
                    return Err(EngineError::Io(format!(
                        "WAL: corruption detected in sealed segment {} \
                         (incomplete record in a segment that was sealed and \
                         fsynced; refusing to open to avoid silently dropping \
                         committed data)",
                        path.display()
                    )));
                }
                wal::ReplayOutcome::Corruption => {
                    // A damaged record with intact records after it: truncating
                    // would silently drop committed data, so refuse loudly.
                    return Err(EngineError::Io(format!(
                        "WAL: corruption detected in segment {} \
                         (a damaged record is followed by intact records; \
                         refusing to open to avoid silently dropping committed \
                         data)",
                        path.display()
                    )));
                }
            }
            // Capture this segment's max seq for the cache, regardless of
            // whether its records get replayed into the memtable (a segment
            // covered entirely by an SSTable is still on disk and still needs
            // a max_seq entry until the next flush truncates it).
            let seg_max = records.iter().map(|r| r.seq()).max().unwrap_or(0);
            if seg_max > 0 {
                sealed_segment_max_seq.insert(*id, seg_max);
            }
            for rec in records {
                let rec_seq = rec.seq();
                if rec_seq <= sstable_max_seq {
                    continue; // R1: already durable in an SSTable
                }
                // Point-in-time restore: stop replaying past the requested seq.
                if let Some(ceiling) = cfg.wal_replay_max_seq {
                    if rec_seq > ceiling {
                        continue;
                    }
                }
                max_seq_seen = max_seq_seen.max(rec_seq);
                // A batch expands to N pairs that all share the batch seq; a
                // single record yields one pair.
                for (k, e) in rec.into_memtable_pairs() {
                    active.insert(k, e);
                    replayed += 1;
                }
            }
        }
        tracing::info!(replayed, "WAL replay complete");

        // 4/5. Seed allocator to max(seq)+1.
        let seq = Arc::new(SeqAllocator::new(max_seq_seen + 1));

        // Open manifest for appending (shared with the apply worker).
        let manifest_file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&manifest_path)?,
        ));

        let next_sstable_id = max_sstable_id + 1;
        let active_wal_id = max_wal_id + 1;
        let next_sstable_id_atomic = Arc::new(AtomicU64::new(next_sstable_id));

        let compaction_scheduler = crate::compaction_worker::CompactionScheduler::new(
            cfg.data_dir.clone(),
            cfg.compaction,
            block_cache.clone(),
            next_sstable_id_atomic.clone(),
        );
        let flush_scheduler = crate::flush_worker::FlushScheduler::new();
        let manifest_sync_window = Arc::new(crate::apply_worker::ManifestSyncWindow::default());
        let apply_scheduler =
            crate::apply_worker::ApplyScheduler::new(manifest_file.clone(), manifest_sync_window);

        let mut engine = Engine {
            cfg,
            seq,
            active,
            immutable: VecDeque::new(),
            sstables,
            next_sstable_id,
            active_wal_id,
            active_wal: None,
            active_wal_size: 0,
            in_flight_wal_bytes: 0,
            sealed_segment_max_seq,
            wal_sync: crate::wal_sync::WalSync::new(max_seq_seen),
            group_commit: true,
            ship_dir: None,
            ship_mode: crate::shipping::ShipMode::Hardlink,
            ship_hmac_key: None,
            manifest_file,
            metrics: None,
            block_cache,
            reader_cache,
            result_cache,
            started: Instant::now(),
            clean_boot,
            policy: Arc::new(NoopWritePolicy),
            compaction_scheduler,
            flush_scheduler,
            apply_scheduler,
            next_sstable_id_atomic,
            pin_state: Arc::new(std::sync::Mutex::new(crate::owned_snapshot::PinState {
                pin_counts: BTreeMap::new(),
                live_snapshot_seqs: BTreeMap::new(),
                deferred_unlinks: Vec::new(),
            })),
            freeze_writes: false,
            dir_fsync_pending: false,
            apply_window_count: AtomicU64::new(0),
            apply_window_sum_ns: AtomicU64::new(0),
            apply_window_max_ns: AtomicU64::new(0),
            manifest_dirty: false,
            last_manifest_sync: Instant::now(),
            data_dir_lock,
        };
        engine.open_new_wal_segment()?;
        engine.update_gauges();
        Ok(engine)
    }

    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::Metrics>) -> Self {
        metrics.last_shutdown_clean.set(self.clean_boot as i64);
        self.apply_scheduler.set_metrics(metrics.clone());
        self.wal_sync.set_metrics(Some(metrics.clone()));
        self.metrics = Some(metrics);
        self.update_gauges();
        self
    }

    /// Shared WAL durability handle, so a commit coordinator can `fsync` the WAL
    /// without taking the engine mutex. See [`crate::wal_sync::WalSync`].
    pub fn wal_sync(&self) -> Arc<crate::wal_sync::WalSync> {
        Arc::clone(&self.wal_sync)
    }

    /// Toggle group commit. When false, every WAL append fsyncs inline (the
    /// pre-group-commit behavior). Defaults to true.
    pub fn with_group_commit(mut self, enabled: bool) -> Self {
        self.group_commit = enabled;
        self
    }

    /// Install a write-time policy (see [`crate::policy::WritePolicy`]). The
    /// policy is consulted around every user PUT/DEL. Defaults to a no-op.
    pub fn with_write_policy(mut self, policy: Arc<dyn WritePolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Enable WAL shipping: each sealed segment is hardlinked/copied into
    /// `ship_dir` for an operator sidecar to transport off-box. Empty path
    /// disables it.
    pub fn with_shipping(
        mut self,
        ship_dir: Option<PathBuf>,
        ship_mode: crate::shipping::ShipMode,
    ) -> Self {
        self.ship_dir = ship_dir;
        self.ship_mode = ship_mode;
        self
    }

    /// Set the HMAC key that authenticates each `shipped.log` entry. `None`
    /// keeps the legacy unauthenticated 3-field format (dev only).
    pub fn with_shipping_hmac_key(mut self, key: Option<Vec<u8>>) -> Self {
        self.ship_hmac_key = key;
        self
    }

    /// Whether group commit is enabled (writers buffer; a coordinator fsyncs).
    pub fn group_commit_enabled(&self) -> bool {
        self.group_commit
    }

    /// Whether this instance booted from a clean-shutdown marker.
    pub fn was_clean_shutdown(&self) -> bool {
        self.clean_boot
    }

    /// Drain flush/compaction workers and the apply queue (no new writes).
    pub fn drain_background_work(&mut self) -> EngineResult<()> {
        let _ = self.poll_flush()?;
        let _ = self.poll_compaction()?;
        self.drain_compaction()?;
        self.finish_pending_applies()?;
        Ok(())
    }

    /// Graceful shutdown: flush all in-memory data to durable SSTables (which also
    /// truncates the WAL via the manifest), then write a clean-shutdown marker so
    /// the next boot can skip WAL replay and verify a graceful stop. Idempotent.
    pub fn shutdown(&mut self) -> EngineResult<()> {
        self.force_flush()?;
        // Sync and ship the active segment so the sidecar has the final WAL bytes.
        self.sync_wal()?;
        self.ship_sealed_segment(self.active_wal_id);
        let marker_path = self.cfg.data_dir.join(CLEAN_SHUTDOWN_MARKER);
        let last_seq = self.seq.peek().saturating_sub(1);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&marker_path)?;
        f.write_all(&last_seq.to_be_bytes())?;
        f.sync_all()?;
        Self::fsync_dir(&self.cfg.data_dir)?;
        tracing::info!(last_seq, "clean shutdown complete; marker written");
        Ok(())
    }

    fn sstable_path(data_dir: &Path, id: u64) -> PathBuf {
        data_dir.join(format!("{:08}.sst", id))
    }

    /// Acquire an exclusive advisory lock on `data_dir/LOCK`. Returns the held
    /// file handle (drop releases the lock) or [`EngineError::Locked`] if another
    /// process holds it. Uses the stable `std::fs::File` lock API (Rust 1.89+).
    fn acquire_data_dir_lock(data_dir: &Path) -> EngineResult<File> {
        let lock_path = data_dir.join("LOCK");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;
        match file.try_lock() {
            Ok(()) => Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => Err(EngineError::Locked(format!(
                "{} is already locked by another zydecodb process",
                data_dir.display()
            ))),
            Err(std::fs::TryLockError::Error(e)) => Err(EngineError::Io(e.to_string())),
        }
    }

    fn delete_orphan_sstables(
        data_dir: &Path,
        live_ids: &std::collections::HashSet<u64>,
    ) -> EngineResult<()> {
        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stripped) = name.strip_suffix(".sst") {
                if let Ok(id) = stripped.parse::<u64>() {
                    if !live_ids.contains(&id) {
                        tracing::info!(orphan = %name, "deleting orphan SSTable");
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }

    fn read_segment(path: &Path) -> EngineResult<(Vec<wal::WalEntry>, wal::ReplayOutcome)> {
        let mut f = File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        if buf.len() < wal::SEGMENT_HEADER_LEN {
            return Ok((Vec::new(), wal::ReplayOutcome::Clean));
        }
        // Header: [8] first_seq [1] format version. Reject unknown versions so a
        // v1 segment (with its old, wider record layout) is never misparsed as
        // v2.
        let version = buf[8];
        if version != wal::WAL_FORMAT_VERSION {
            return Err(EngineError::Io(format!(
                "WAL: unsupported segment format version 0x{:02x} (expected 0x{:02x})",
                version,
                wal::WAL_FORMAT_VERSION
            )));
        }
        let body = &buf[wal::SEGMENT_HEADER_LEN..];
        Ok(wal::replay_segment_body(body))
    }

    fn truncate_torn_segment(path: &Path) -> EngineResult<()> {
        // Re-read, find the valid prefix length, truncate the file to it.
        let mut f = File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        if buf.len() < wal::SEGMENT_HEADER_LEN {
            return Ok(());
        }
        let body = &buf[wal::SEGMENT_HEADER_LEN..];
        let mut offset = 0;
        while let Ok(Some((_, consumed))) = WalRecord::decode_one(&body[offset..]) {
            offset += consumed;
        }
        let valid_len = wal::SEGMENT_HEADER_LEN + offset;
        let f = OpenOptions::new().write(true).open(path)?;
        f.set_len(valid_len as u64)?;
        f.sync_all()?;
        Ok(())
    }

    fn open_new_wal_segment(&mut self) -> EngineResult<()> {
        crate::engine_fail_point!(crate::failpoints::WAL_BEFORE_SEGMENT_ROLL);
        let first_seq = self.seq.peek();
        let name = wal::segment_filename(self.active_wal_id);
        let path = self.cfg.wal_dir.join(name);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        f.write_all(&first_seq.to_be_bytes())?;
        f.write_all(&[wal::WAL_FORMAT_VERSION])?;
        f.sync_all()?;
        crate::engine_fail_point!(crate::failpoints::WAL_AFTER_SEGMENT_ROLL);
        // Share the fd with the durability state so the coordinator can fsync it
        // off the engine lock. Publishing here (after the prior segment was synced
        // in the rotation path) maintains the WalSync invariant.
        let f = Arc::new(f);
        self.wal_sync.set_active(Arc::clone(&f));
        self.active_wal = Some(f);
        self.active_wal_size = wal::SEGMENT_HEADER_LEN;
        tracing::info!(segment = %path.display(), first_seq, "opened new WAL segment");
        Ok(())
    }

    /// Append a record to the active WAL segment and fsync immediately. Used on
    /// the per-record durability path (group commit disabled) and for internal
    /// system writes that want their own durability point.
    /// Ship a freshly-sealed WAL segment off-box (hardlink/copy into `ship_dir`)
    /// and record it in `shipped.log`. Best-effort: a shipping failure is logged
    /// but does not fail the write path — the live WAL remains the source of
    /// truth and the sidecar can reconcile from `shipped.log`. No-op when
    /// shipping is disabled.
    fn ship_sealed_segment(&self, segment_id: u64) {
        let Some(ship_dir) = &self.ship_dir else {
            return;
        };
        let name = wal::segment_filename(segment_id);
        let src = self.cfg.wal_dir.join(&name);
        if let Err(e) = crate::shipping::ship_segment(
            &src,
            ship_dir,
            segment_id,
            self.wal_sync.synced_seq(),
            self.ship_mode,
            self.ship_hmac_key.as_deref(),
        ) {
            tracing::error!(error = %e, segment = segment_id, "WAL shipping failed");
        } else {
            tracing::info!(segment = segment_id, "shipped sealed WAL segment");
        }
    }

    fn append_wal(&mut self, rec: &WalRecord) -> EngineResult<()> {
        self.append_wal_buffered(rec)?;
        self.sync_wal()?;
        Ok(())
    }

    /// Append a record's bytes to the active WAL segment WITHOUT fsync. The bytes
    /// reach the OS page cache; durability is established later by [`sync_wal`].
    /// This is the write half of group commit: many records are buffered, then a
    /// single fsync makes the whole batch durable.
    fn append_wal_buffered(&mut self, rec: &WalRecord) -> EngineResult<()> {
        let bytes = rec.encode();
        self.append_bytes_buffered(&bytes, rec.seq)
    }

    /// Append pre-encoded WAL `bytes` carrying sequence `seq` to the active
    /// segment WITHOUT fsync, rolling the segment first if it would overflow.
    /// Shared by single-record appends and by atomic batch records (one
    /// self-framed record, one CRC, one seq).
    fn append_bytes_buffered(&mut self, bytes: &[u8], seq: u64) -> EngineResult<()> {
        if wal::should_roll(self.active_wal_size, bytes.len()) {
            // A new segment starts fully durable (its header is fsynced in
            // open_new_wal_segment), so the prior segment's tail must be synced
            // first to preserve ordering of the durability watermark. After the
            // sync the now-sealed segment is shipped off-box (if enabled).
            self.sync_wal()?;
            let sealed_id = self.active_wal_id;
            // The just-sealed segment's max seq is exactly the buffered watermark:
            // it's the highest seq appended, and the segment is non-empty
            // (should_roll requires current_size > 0). Cache it so subsequent
            // flushes can decide WAL coverage without re-reading the file.
            self.sealed_segment_max_seq
                .insert(sealed_id, self.wal_sync.buffered_seq());
            self.ship_sealed_segment(sealed_id);
            self.active_wal_id += 1;
            self.open_new_wal_segment()?;
        }
        // The engine lock serializes writers, so a shared `&File` is safe here;
        // only the (concurrent) coordinator reads this fd to fsync, which is
        // independent of the append.
        let arc = self
            .active_wal
            .as_ref()
            .ok_or_else(|| EngineError::Io("no active WAL segment".into()))?;
        let mut f: &File = arc;
        crate::engine_fail_point!(crate::failpoints::WAL_BEFORE_APPEND);
        f.write_all(bytes)?;
        crate::engine_fail_point!(crate::failpoints::WAL_AFTER_APPEND);
        if let Some(m) = &self.metrics {
            m.wal_bytes_written_total.inc_by(bytes.len() as u64);
        }
        self.active_wal_size += bytes.len();
        self.in_flight_wal_bytes += bytes.len();
        // Publish the new buffered watermark AFTER the write_all above; the
        // Release in `advance_buffered` makes the bytes visible to a syncer that
        // observes this seq.
        self.wal_sync.advance_buffered(seq);
        Ok(())
    }

    /// Fsync the active WAL segment, making all buffered appends durable. Returns
    /// the highest sequence number now guaranteed on disk. No-op when there is
    /// nothing new to sync. This is the single shared fsync of group commit.
    pub fn sync_wal(&mut self) -> EngineResult<u64> {
        // Delegates to the decoupled durability state. `WalSync::sync` releases
        // its fd lock before the actual `fsync(2)`, so even when the engine lock
        // is held by an internal caller the slow I/O is the same single shared
        // group-commit fsync -- the win is that the *commit coordinator* path no
        // longer needs the engine mutex at all.
        self.wal_sync.sync()
    }

    /// Highest seq buffered into the WAL (durable or not).
    pub fn last_buffered_seq(&self) -> u64 {
        self.wal_sync.buffered_seq()
    }

    fn now_ms() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// PUT a key/value with optional expiry. Validates limits, consults the
    /// write policy, writes WAL, then memtable. Returns the assigned sequence
    /// number so the caller can await its durability via [`sync_wal`] (group
    /// commit). When group commit is disabled the WAL is already fsynced on
    /// return.
    ///
    /// The engine treats the key as opaque bytes. Any caller-side accounting or
    /// gating runs through the installed [`crate::policy::WritePolicy`], which
    /// receives both the bytes and a mutable handle to the engine's
    /// system keyspace.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: u64) -> EngineResult<u64> {
        keys::validate_user_key(&key)?;
        keys::validate_value(&value)?;
        self.check_backpressure()?;

        // Policy gate: reject before any WAL/memtable mutation. The policy is
        // cloned out so it can borrow `self` mutably (read/write the system
        // keyspace) without aliasing the field.
        let existing_value_len = self.get(&key)?.map(|v| v.len());
        let policy = Arc::clone(&self.policy);
        policy.pre_write(self, &key, value.len(), existing_value_len, false)?;

        let seq = self.seq.next();
        let rec = WalRecord::put(seq, expires_at, key.clone(), value.clone());
        if self.group_commit {
            self.append_wal_buffered(&rec)?;
        } else {
            self.append_wal(&rec)?;
        }

        let ik = InternalKey::new(key.clone(), seq, EntryKind::Value);
        let entry = Entry::value(
            value.clone(),
            if expires_at == 0 {
                None
            } else {
                Some(expires_at)
            },
        );
        crate::engine_fail_point!(crate::failpoints::ENGINE_BEFORE_MEMTABLE_INSERT);
        self.active.insert(ik, entry);
        crate::engine_fail_point!(crate::failpoints::ENGINE_AFTER_MEMTABLE_INSERT);
        if let Some(rc) = &self.result_cache {
            rc.invalidate(&key);
        }

        // Post-write bookkeeping (e.g. durable usage counters), on the same
        // commit path so it joins this write's group-commit fsync.
        policy.post_write(self, &key, value.len(), existing_value_len, false);

        if let Some(m) = &self.metrics {
            m.user_bytes_written_total.inc_by(value.len() as u64);
        }

        // Per-caller operation counts (e.g. labeled by routing context) are
        // the caller's responsibility — see crate::metrics docs. The engine
        // deliberately stays out of label cardinality decisions.
        self.maybe_freeze();
        self.try_submit_flush();
        self.maybe_submit_compaction();
        self.update_gauges();
        Ok(seq)
    }

    /// DEL a key (writes a tombstone). Returns whether the key existed and the
    /// assigned sequence number (for group-commit durability waiting).
    pub fn del(&mut self, key: Vec<u8>) -> EngineResult<(bool, u64)> {
        keys::validate_user_key(&key)?;
        self.check_backpressure()?;

        let existing_value_len = self.get(&key)?.map(|v| v.len());
        let existed = existing_value_len.is_some();

        // A delete cannot be rejected by a usage-style policy, but we still call
        // pre_write for symmetry and to let custom policies veto if they choose.
        let policy = Arc::clone(&self.policy);
        policy.pre_write(self, &key, 0, existing_value_len, true)?;

        let seq = self.seq.next();
        let rec = WalRecord::del(seq, key.clone());
        if self.group_commit {
            self.append_wal_buffered(&rec)?;
        } else {
            self.append_wal(&rec)?;
        }

        let ik = InternalKey::new(key.clone(), seq, EntryKind::Tombstone);
        self.active.insert(ik, Entry::tombstone());
        if let Some(rc) = &self.result_cache {
            rc.invalidate(&key);
        }

        // Post-write bookkeeping: a policy may release usage for the freed key.
        policy.post_write(self, &key, 0, existing_value_len, true);

        // See `put`: per-caller operation counts are the caller's concern.
        self.maybe_freeze();
        self.try_submit_flush();
        self.maybe_submit_compaction();
        self.update_gauges();
        Ok((existed, seq))
    }

    /// Atomically apply a batch of puts and deletes. Either every op is
    /// persisted or none is: the whole batch is written as a SINGLE self-framed
    /// WAL record with one CRC, so a torn crash mid-batch replays no ops (the
    /// torn tail is truncated). All ops share one sequence number; duplicate
    /// user keys within a batch are rejected (they would collide at that seq).
    ///
    /// The policy is consulted for every op BEFORE any mutation — any rejection
    /// aborts the entire batch with nothing persisted. Returns the batch's
    /// assigned sequence number (await durability via [`sync_wal`] under group
    /// commit; when group commit is disabled the WAL is fsynced on return).
    pub fn write_batch(&mut self, ops: Vec<BatchOp>) -> EngineResult<u64> {
        if ops.is_empty() {
            return Err(EngineError::InvalidKey("empty batch".into()));
        }
        if ops.len() > keys::MAX_BATCH_KEYS {
            return Err(EngineError::InvalidKey(format!(
                "batch size {} exceeds MAX_BATCH_KEYS {}",
                ops.len(),
                keys::MAX_BATCH_KEYS
            )));
        }

        // Validate every key/value and reject duplicate user keys up front,
        // before any mutation. (Two ops on the same key would collide at the
        // shared batch seq, making newest-wins ambiguous.)
        {
            let mut seen: std::collections::HashSet<&[u8]> =
                std::collections::HashSet::with_capacity(ops.len());
            for op in &ops {
                keys::validate_user_key(op.key())?;
                if let BatchOp::Put { value, .. } = op {
                    keys::validate_value(value)?;
                }
                if !seen.insert(op.key()) {
                    return Err(EngineError::InvalidKey("duplicate key in batch".into()));
                }
            }
        }
        self.check_backpressure()?;

        // Policy gate: consult the policy for every op BEFORE any mutation. Any
        // rejection aborts the whole batch with nothing persisted. Existing
        // value lengths are captured here for the matching post_write calls.
        let policy = Arc::clone(&self.policy);
        let mut existing_lens: Vec<Option<usize>> = Vec::with_capacity(ops.len());
        for op in &ops {
            let existing = self.get(op.key())?.map(|v| v.len());
            existing_lens.push(existing);
            policy.pre_write(self, op.key(), op.value_len(), existing, op.is_delete())?;
        }

        // One seq for the whole batch.
        let seq = self.seq.next();

        // Build one self-framed batch WAL record (one CRC = atomic on a torn
        // crash) and append it on the group-commit path.
        let wal_ops: Vec<wal::WalOp> = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put {
                    key,
                    value,
                    expires_at,
                } => wal::WalOp {
                    command: wal::WAL_PUT,
                    expires_at: *expires_at,
                    key: key.clone(),
                    value: value.clone(),
                },
                BatchOp::Del { key } => wal::WalOp {
                    command: wal::WAL_DEL,
                    expires_at: 0,
                    key: key.clone(),
                    value: Vec::new(),
                },
            })
            .collect();
        let rec_bytes = wal::WalBatch { seq, ops: wal_ops }.encode();
        self.append_bytes_buffered(&rec_bytes, seq)?;
        if !self.group_commit {
            self.sync_wal()?;
        }

        // Insert every op into the memtable under the shared batch seq.
        crate::engine_fail_point!(crate::failpoints::ENGINE_BEFORE_MEMTABLE_INSERT);
        for op in &ops {
            let (ik, entry) = match op {
                BatchOp::Put {
                    key,
                    value,
                    expires_at,
                } => (
                    InternalKey::new(key.clone(), seq, EntryKind::Value),
                    Entry::value(
                        value.clone(),
                        if *expires_at == 0 {
                            None
                        } else {
                            Some(*expires_at)
                        },
                    ),
                ),
                BatchOp::Del { key } => (
                    InternalKey::new(key.clone(), seq, EntryKind::Tombstone),
                    Entry::tombstone(),
                ),
            };
            self.active.insert(ik, entry);
            if let Some(rc) = &self.result_cache {
                rc.invalidate(op.key());
            }
        }
        crate::engine_fail_point!(crate::failpoints::ENGINE_AFTER_MEMTABLE_INSERT);

        // Post-write bookkeeping on the same commit path as the user write.
        for (op, existing) in ops.iter().zip(existing_lens.iter()) {
            policy.post_write(self, op.key(), op.value_len(), *existing, op.is_delete());
        }

        if let Some(m) = &self.metrics {
            let total: usize = ops.iter().map(|op| op.value_len()).sum();
            m.user_bytes_written_total.inc_by(total as u64);
        }

        self.maybe_freeze();
        self.try_submit_flush();
        self.maybe_submit_compaction();
        self.update_gauges();
        Ok(seq)
    }

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

    fn snapshot_with_ceiling(&self, seq_upper: u64) -> SnapshotHandle {
        let sstable_ids: Vec<u64> = self.sstables.iter().map(|s| s.meta.id).collect();
        for id in &sstable_ids {
            self.reader_cache.pin(*id);
        }
        SnapshotHandle::new(
            seq_upper,
            self.active.clone(),
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

    /// Force flush of active + immutable memtables, then drain compaction.
    pub fn flush(&mut self) -> EngineResult<()> {
        self.force_flush()
    }

    /// Refuse new writes until [`Engine::unfreeze_writes`] is called.
    pub fn freeze_writes(&mut self) {
        self.freeze_writes = true;
    }

    pub fn unfreeze_writes(&mut self) {
        self.freeze_writes = false;
    }

    pub fn writes_frozen(&self) -> bool {
        self.freeze_writes
    }

    /// Sum of live SSTable bytes on disk (approximate disk usage for data files).
    pub fn estimate_disk_bytes(&self) -> u64 {
        self.sstables.iter().map(|s| s.meta.size_bytes).sum()
    }

    /// Sum of live user-key bytes (latest non-tombstone, non-expired values).
    pub fn estimate_logical_live_bytes(&self) -> EngineResult<u64> {
        use crate::keys::KS_USER;
        let lo = vec![KS_USER];
        let hi = vec![KS_USER + 1];
        let mut total = 0u64;
        let iter = self.scan(lo, hi)?;
        for row in iter {
            let (k, v) = row?;
            total = total.saturating_add(k.len() as u64 + v.len() as u64);
        }
        Ok(total.max(1))
    }

    /// Physical SSTable bytes divided by logical live bytes.
    pub fn space_amplification(&self) -> EngineResult<f64> {
        let physical = self.estimate_disk_bytes();
        let logical = self.estimate_logical_live_bytes()?;
        Ok(physical as f64 / logical.max(1) as f64)
    }

    /// Refresh disk / space-amplification gauges (full keyspace scan).
    pub fn refresh_space_metrics(&self) {
        if let Some(m) = &self.metrics {
            let disk = self.estimate_disk_bytes();
            m.disk_bytes_total.set(disk as i64);
            if let Ok(logical) = self.estimate_logical_live_bytes() {
                m.logical_live_bytes.set(logical as i64);
                m.space_amplification
                    .set(disk as f64 / logical.max(1) as f64);
            }
            if let Some(rc) = &self.result_cache {
                m.result_cache_resident_bytes
                    .set(rc.resident_bytes() as i64);
                m.result_cache_evictions_total.inc_by(
                    rc.evictions()
                        .saturating_sub(m.result_cache_evictions_total.get()),
                );
            }
        }
    }

    /// Per-level SSTable byte totals.
    pub fn disk_bytes_by_level(&self) -> Vec<(u8, u64)> {
        let mut by: BTreeMap<u8, u64> = BTreeMap::new();
        for s in &self.sstables {
            *by.entry(s.meta.level).or_insert(0) += s.meta.size_bytes;
        }
        by.into_iter().collect()
    }

    /// Run one compaction job synchronously if the planner finds work.
    pub fn compact_range(&mut self) -> EngineResult<bool> {
        self.compact_once()
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

    /// Delete every key whose storage key starts with `prefix`, writing
    /// tombstones in atomic [`Engine::write_batch`] chunks. Returns the number of
    /// keys deleted. Intended for coarse operations such as tenant offboarding.
    ///
    /// Matching keys are collected from a single consistent snapshot first (so
    /// memory scales with the number of live keys under the prefix), then deleted
    /// in `MAX_BATCH_KEYS`-sized batches. Keys written concurrently after the scan
    /// are not included. The tombstoned space is reclaimed lazily by background
    /// compaction, or immediately via [`Engine::compact_all`].
    pub fn delete_prefix(&mut self, prefix: Vec<u8>) -> EngineResult<u64> {
        let keys_to_delete: Vec<Vec<u8>> = {
            let iter = self.prefix_scan(prefix)?;
            let mut out = Vec::new();
            for item in iter {
                let (k, _v) = item?;
                out.push(k);
            }
            out
        };
        let total = keys_to_delete.len() as u64;
        for chunk in keys_to_delete.chunks(keys::MAX_BATCH_KEYS) {
            let ops: Vec<BatchOp> = chunk
                .iter()
                .map(|k| BatchOp::Del { key: k.clone() })
                .collect();
            self.write_batch(ops)?;
        }
        Ok(total)
    }

    /// Flush the active memtable, then run compaction jobs until the planner
    /// finds no more work. Used by offline maintenance (e.g. reclaiming space
    /// after [`Engine::delete_prefix`]); flushing first ensures freshly written
    /// tombstones participate in the compaction that drops them.
    pub fn compact_all(&mut self) -> EngineResult<()> {
        self.force_flush()?;
        while self.compact_once()? {}
        Ok(())
    }

    /// Capture a consistent base snapshot of the current durable state into
    /// `dir`. Flushes the active memtable (so all data lives in SSTables +
    /// manifest), drains background work, then hardlinks the live SSTable set
    /// and copies the `MANIFEST` into `dir`, plus a small `SNAPMETA` recording
    /// the snapshot sequence. Hardlinks are instant and share storage on the
    /// same filesystem (a full copy is used across filesystems); SSTable
    /// immutability makes the result internally consistent. Returns the snapshot
    /// sequence (the max seq durable at capture time).
    ///
    /// Run offline against a stopped node, or against a replica's `data_dir` for
    /// zero primary impact. The snapshot directory is itself a valid `data_dir`
    /// (open it with an empty WAL to read state as of the snapshot); combine it
    /// with shipped WAL for point-in-time restore.
    pub fn snapshot_to(&mut self, dir: &Path) -> EngineResult<u64> {
        self.force_flush()?;
        self.drain_background_work()?;
        self.sync_manifest()?;
        std::fs::create_dir_all(dir)?;

        for s in &self.sstables {
            let src = Self::sstable_path(&self.cfg.data_dir, s.meta.id);
            let dst = Self::sstable_path(dir, s.meta.id);
            Self::link_or_copy(&src, &dst)?;
        }

        let manifest_src = self.cfg.data_dir.join("MANIFEST");
        let manifest_dst = dir.join("MANIFEST");
        std::fs::copy(&manifest_src, &manifest_dst)?;

        let snapshot_seq = self.current_seq();
        let snapmeta = dir.join("SNAPMETA");
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&snapmeta)?;
        f.write_all(format!("{{\"snapshot_seq\":{snapshot_seq}}}\n").as_bytes())?;
        f.sync_all()?;
        Self::fsync_dir(dir)?;
        tracing::info!(snapshot_seq, dir = %dir.display(), "base snapshot written");
        Ok(snapshot_seq)
    }

    /// Hardlink `src` to `dst`, falling back to a full copy when hardlinking is
    /// not possible (e.g. across filesystems).
    fn link_or_copy(src: &Path, dst: &Path) -> EngineResult<()> {
        match std::fs::hard_link(src, dst) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(_) => {
                std::fs::copy(src, dst)?;
                Ok(())
            }
        }
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
    fn newest_visible_in_sstable(
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
            &self.active,
            self.immutable.iter(),
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

    fn sstable_might_hold_key(meta: &SstableMeta, key: &[u8]) -> bool {
        key >= meta.min_key.as_slice() && key <= meta.max_key.as_slice()
    }

    fn sstable_overlaps_range(meta: &SstableMeta, lo: &[u8], hi: &[u8]) -> bool {
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
    fn first_visible_in_memtable(
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
    fn next_user_key(key: &[u8]) -> Vec<u8> {
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

    // ---- System keyspace (opaque caller-defined metadata sidecar) ----
    //
    // These mirror put/get/del but accept only `KS_SYSTEM` keys. They share the
    // same WAL/memtable/SSTable path, so system records are durable and recover
    // on restart for free. User writes can never reach this keyspace (the user
    // path enforces `KS_USER`); only the embedder calls these. The engine
    // treats their contents as opaque bytes.

    /// PUT a system record with its own durability point (inline fsync). `key`
    /// must be in the system keyspace (`0x00`). Used for standalone metadata
    /// writes that are not part of a surrounding user write's commit batch.
    pub fn sys_put(&mut self, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        keys::validate_system_key(&key)?;
        keys::validate_value(&value)?;
        self.check_backpressure()?;

        let seq = self.seq.next();
        let rec = WalRecord::put(seq, 0, key.clone(), value.clone());
        self.append_wal(&rec)?;

        let ik = InternalKey::new(key, seq, EntryKind::Value);
        self.active.insert(ik, Entry::value(value, None));

        self.maybe_freeze();
        self.update_gauges();
        Ok(())
    }

    /// Write a system record on the same buffered WAL path as the surrounding
    /// user write, so it joins the same group-commit fsync. Intended for a
    /// [`crate::policy::WritePolicy`] to persist bookkeeping (e.g. usage
    /// counters) from within `post_write`. Does NOT fsync here — the caller's
    /// commit covers it. When group commit is off it still buffers; the user
    /// path fsyncs inline.
    pub fn sys_put_policy(&mut self, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        keys::validate_system_key(&key)?;
        keys::validate_value(&value)?;
        let seq = self.seq.next();
        let rec = WalRecord::put(seq, 0, key.clone(), value.clone());
        if self.group_commit {
            self.append_wal_buffered(&rec)?;
        } else {
            self.append_wal(&rec)?;
        }
        let ik = InternalKey::new(key, seq, EntryKind::Value);
        self.active.insert(ik, Entry::value(value, None));
        Ok(())
    }

    /// GET a system record. `key` must be in the system keyspace (`0x00`).
    pub fn sys_get(&self, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        keys::validate_system_key(key)?;
        // System keys share the same LSM as user keys; reuse the snapshot
        // path so there's one read implementation.
        self.snapshot_get(u64::MAX, key)
    }

    /// DEL a system record (tombstone). Returns whether it existed.
    pub fn sys_del(&mut self, key: Vec<u8>) -> EngineResult<bool> {
        keys::validate_system_key(&key)?;
        self.check_backpressure()?;

        let existed = self.sys_get(&key)?.is_some();

        let seq = self.seq.next();
        let rec = WalRecord::del(seq, key.clone());
        self.append_wal(&rec)?;

        let ik = InternalKey::new(key, seq, EntryKind::Tombstone);
        self.active.insert(ik, Entry::tombstone());

        self.maybe_freeze();
        self.update_gauges();
        Ok(existed)
    }

    /// Resolve an entry to a value: None for tombstone or expired.
    /// (Not-found accounting is the caller's concern so it can be labeled
    /// and so internal `sys_*` calls don't inflate user-facing counters.)
    fn resolve(&self, _ik: &InternalKey, entry: &Entry, now: u64) -> Option<Vec<u8>> {
        if entry.is_tombstone() || entry.is_expired(now) {
            return None;
        }
        entry.value.clone()
    }

    fn check_backpressure(&self) -> EngineResult<()> {
        if self.freeze_writes {
            return Err(EngineError::EngineBusy("writes frozen".into()));
        }
        if self.in_flight_wal_bytes > keys::MAX_IN_FLIGHT_WAL_BYTES {
            return Err(EngineError::EngineBusy("WAL in-flight limit".into()));
        }
        if self.immutable.len() >= self.cfg.max_immutable_memtables {
            return Err(EngineError::EngineBusy("flush queue full".into()));
        }
        let queue_depth = self.compaction_scheduler.queue_depth()
            + self.flush_scheduler.queue_depth()
            + if self.compaction_scheduler.compaction_needed() {
                1
            } else {
                0
            };
        if queue_depth >= 4 {
            return Err(EngineError::EngineBusy(format!(
                "compaction backlog (queue depth {})",
                queue_depth
            )));
        }
        // L0 stall: compaction can't keep up with flush. Threshold is 5x the
        // compaction trigger — gives compaction headroom to catch up before
        // we refuse writes. Tuned conservatively; the soak will inform it.
        let l0_count = self.sstables.iter().filter(|s| s.meta.level == 0).count();
        let l0_stall_threshold = self.cfg.compaction.l0_trigger.saturating_mul(5).max(20);
        if l0_count >= l0_stall_threshold {
            return Err(EngineError::EngineBusy(format!(
                "L0 compaction backlog ({} files)",
                l0_count
            )));
        }
        let pending = self.estimate_pending_compaction_bytes();
        let hard = self.cfg.compaction.hard_pending_compaction_bytes;
        let soft = self.cfg.compaction.soft_pending_compaction_bytes;
        if hard > 0 && pending >= hard {
            // Slow writes instead of rejecting them — matches RocksDB slowdown
            // semantics and keeps soak/client loops from error-storming.
            let ratio = (pending - hard) as f64 / hard as f64;
            let delay_ms = (1.0 + ratio * 4.0).min(5.0) as u64;
            std::thread::sleep(std::time::Duration::from_millis(delay_ms));
        } else if soft > 0 && pending > soft {
            let ratio = (pending - soft) as f64 / soft as f64;
            let delay_us = (100.0 + ratio * 900.0).min(1000.0) as u64;
            std::thread::sleep(std::time::Duration::from_micros(delay_us));
        }
        Ok(())
    }

    /// Freeze the active memtable if it exceeds the flush threshold, then flush
    /// synchronously in v1 (single executor; a background task is a Sprint 2 nicety).
    fn maybe_freeze(&mut self) {
        if self.active.size_bytes() <= self.cfg.memtable_flush_threshold {
            return;
        }
        let frozen = std::mem::replace(&mut self.active, Memtable::new());
        self.immutable.push_back(frozen);
        self.update_gauges();
        self.try_submit_flush();
    }

    fn try_submit_flush(&mut self) -> bool {
        if self.flush_scheduler.is_worker_busy() {
            return false;
        }
        let Some(mt) = self.immutable.front() else {
            return false;
        };
        if mt.is_empty() {
            self.immutable.pop_front();
            self.update_gauges();
            return self.try_submit_flush();
        }
        let pairs: Vec<(InternalKey, Entry)> =
            mt.iter().map(|(k, e)| (k.clone(), e.clone())).collect();
        let submitted = self.flush_scheduler.try_submit(
            pairs,
            self.cfg.data_dir.clone(),
            self.next_sstable_id_atomic.clone(),
        );
        if submitted {
            self.immutable.pop_front();
            self.update_gauges();
        }
        submitted
    }

    fn submit_flush_apply(
        &mut self,
        result: crate::flush_worker::FlushExecuteResult,
    ) -> EngineResult<()> {
        let crate::flush_worker::FlushExecuteResult { meta, max_seq } = result;
        let mut manifest_records = vec![ManifestRecord::SstableAdd(meta.clone())];
        let covered_up_to = self.wal_segments_covered(max_seq)?;
        if covered_up_to > 0 {
            manifest_records.push(ManifestRecord::WalTruncate {
                up_to_segment_id: covered_up_to,
            });
        }
        self.apply_scheduler
            .submit(crate::apply_worker::ReadyCatalogApply {
                manifest_records,
                add_metas: vec![meta],
                remove_sstable_ids: vec![],
                unlink_wal_up_to: if covered_up_to > 0 {
                    Some(covered_up_to)
                } else {
                    None
                },
                flush_max_seq: Some(max_seq),
                compaction: None,
            })
    }

    /// Force a flush of the active memtable (used by tests and shutdown).
    pub fn force_flush(&mut self) -> EngineResult<()> {
        if self.active.is_empty() && self.immutable.is_empty() {
            return Ok(());
        }
        if !self.active.is_empty() {
            let frozen = std::mem::replace(&mut self.active, Memtable::new());
            self.immutable.push_back(frozen);
        }
        self.drain_flush()?;
        self.drain_compaction()?;
        self.finish_pending_applies()?;
        self.maybe_sync_data_dir()?;
        Ok(())
    }

    fn drain_flush(&mut self) -> EngineResult<()> {
        loop {
            self.poll_flush()?;
            if let Some(err) = self.flush_scheduler.take_worker_failure() {
                if err.contains("injected failpoint") {
                    return Err(EngineError::Io(err));
                }
                tracing::warn!(error = %err, "flush worker error; continuing drain");
            }
            if self.flush_scheduler.is_worker_busy() {
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            if !self.immutable.is_empty() && self.try_submit_flush() {
                continue;
            }
            if self.immutable.is_empty() {
                self.finish_pending_applies()?;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(())
    }

    pub fn poll_flush(&mut self) -> EngineResult<usize> {
        let results = self.flush_scheduler.poll_results();
        let mut applied = 0usize;
        for result in results {
            match result {
                Ok(r) => {
                    self.submit_flush_apply(r)?;
                    applied += 1;
                }
                Err(e) => {
                    tracing::error!(error = %e, "background flush failed");
                    self.flush_scheduler.note_worker_failed(e);
                }
            }
        }
        self.try_submit_flush();
        applied += self.drain_catalog_apply()?;
        Ok(applied)
    }

    /// Signal the compaction worker that work may be available. Non-blocking.
    pub fn request_compaction(&self) {
        self.compaction_scheduler.request_compaction();
    }

    /// Apply completed background compaction jobs. Returns the number of jobs
    /// applied. Call from idle loops, after flush, or in soak harnesses.
    pub fn poll_compaction(&mut self) -> EngineResult<usize> {
        let flushed = self.poll_flush()?;
        let results = self.compaction_scheduler.poll_results();
        let mut applied = 0usize;
        let mut worker_failed = false;
        for result in results {
            match result {
                Ok(r) => {
                    self.compaction_scheduler.clear_staged();
                    self.submit_compaction_apply(r)?;
                    applied += 1;
                }
                Err(e) => {
                    tracing::error!(error = %e, "background compaction failed");
                    self.compaction_scheduler.clear_staged();
                    self.compaction_scheduler.note_worker_failed(e);
                    worker_failed = true;
                }
            }
        }
        applied += self.drain_catalog_apply()?;
        self.finish_pending_applies()?;
        // Don't resubmit a compaction in the same pass that just recorded a
        // worker failure: `submit_work` clears the `worker_failed` flag, which
        // would erase the failure before `drain_compaction` (the only consumer)
        // can observe it. With a deterministic failure that never advances the
        // catalog -- e.g. an injected rename failpoint -- the same job is
        // immediately replannable, so the resubmit-then-clear loop spins
        // forever and `drain_compaction` never returns. Leaving the flag set
        // lets the failure surface; a later poll resumes normal background
        // retry once the flag is consumed or the catalog changes.
        if !worker_failed {
            self.maybe_submit_compaction();
        }
        self.try_submit_flush();
        self.update_compaction_gauges();
        self.maybe_sync_data_dir()?;
        Ok(flushed + applied)
    }

    /// Block until pending compaction work is drained. Used by tests, shutdown,
    /// and `force_flush`.
    pub fn drain_compaction(&mut self) -> EngineResult<()> {
        self.request_compaction();
        loop {
            self.poll_compaction()?;
            if let Some(err) = self.compaction_scheduler.take_worker_failure() {
                if err.contains("injected failpoint") {
                    return Err(EngineError::Io(err));
                }
                // Worker panic (e.g. failpoint "panic" mode) — stop retrying in this
                // drain; the catalog is unchanged and a later drain can replan.
                tracing::warn!(error = %err, "compaction worker error; aborting drain");
                break;
            }
            if !self.compaction_scheduler.is_worker_busy() {
                if self.plan_compaction_job().is_none()
                    && !self.compaction_scheduler.compaction_needed()
                {
                    break;
                }
                if self.maybe_submit_compaction() {
                    continue;
                }
                if self.plan_compaction_job().is_none() {
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        self.poll_compaction()?;
        self.finish_pending_applies()?;
        Ok(())
    }

    fn compaction_gc_watermark(&self) -> u64 {
        let ps = self.pin_state.lock().expect("pin state lock");
        ps.live_snapshot_seqs
            .keys()
            .next()
            .copied()
            .unwrap_or(self.wal_sync.synced_seq())
    }

    fn compaction_allow_tombstone_drop(&self) -> bool {
        let ps = self.pin_state.lock().expect("pin state lock");
        ps.live_snapshot_seqs.is_empty()
    }

    fn compaction_drop_tombstones(&self, job: &crate::compaction::CompactionJob) -> bool {
        job.output_level == self.cfg.compaction.max_level && self.compaction_allow_tombstone_drop()
    }

    fn maybe_submit_compaction(&mut self) -> bool {
        if self.compaction_scheduler.try_submit_staged() {
            return true;
        }
        let Some((job, input_metas)) = self.plan_compaction_job() else {
            if !self.compaction_scheduler.is_worker_busy() {
                self.compaction_scheduler.clear_compaction_needed();
            }
            return false;
        };
        let drop_tombstones = self.compaction_drop_tombstones(&job);
        self.compaction_scheduler.try_submit(
            job,
            input_metas,
            self.cfg.data_dir.clone(),
            self.cfg.compaction,
            self.block_cache.clone(),
            self.reader_cache.clone(),
            self.next_sstable_id_atomic.clone(),
            self.compaction_gc_watermark(),
            drop_tombstones,
        )
    }

    fn plan_compaction_job(&self) -> Option<(crate::compaction::CompactionJob, Vec<SstableMeta>)> {
        let metas: Vec<SstableMeta> = self.sstables.iter().map(|s| s.meta.clone()).collect();
        let planner = crate::compaction::CompactionPlanner::new(&metas, &self.cfg.compaction);
        let job = planner.plan()?;
        let input_id_set: std::collections::HashSet<u64> = job.inputs.iter().copied().collect();
        let input_metas: Vec<SstableMeta> = metas
            .into_iter()
            .filter(|m| input_id_set.contains(&m.id))
            .collect();
        if input_metas.len() != job.inputs.len() {
            return None;
        }
        let is_l2_gc =
            job.input_level == job.output_level && job.input_level == self.cfg.compaction.max_level;
        if job.input_level == job.output_level
            && !is_l2_gc
            && !crate::compaction::same_level_compaction_would_make_progress(
                &input_metas,
                self.cfg.compaction.target_file_bytes,
            )
        {
            tracing::warn!(
                input_level = job.input_level,
                inputs = job.inputs.len(),
                "rejecting same-level compaction that cannot reduce file count"
            );
            if let Some(m) = &self.metrics {
                m.compaction_rejected_no_progress.inc();
            }
            return None;
        }
        Some((job, input_metas))
    }

    fn submit_compaction_apply(
        &mut self,
        result: crate::compaction_worker::CompactionExecuteResult,
    ) -> EngineResult<()> {
        let crate::compaction_worker::CompactionExecuteResult {
            job,
            new_metas,
            input_metas,
            bytes_read,
            bytes_written,
            versions_dropped,
            tombstones_dropped,
            worker_elapsed,
        } = result;

        let mut manifest_records: Vec<ManifestRecord> =
            Vec::with_capacity(new_metas.len() + input_metas.len());
        for m in &new_metas {
            manifest_records.push(ManifestRecord::SstableAdd(m.clone()));
        }
        for m in &input_metas {
            manifest_records.push(ManifestRecord::SstableRemove { id: m.id });
        }
        self.apply_scheduler
            .submit(crate::apply_worker::ReadyCatalogApply {
                manifest_records,
                add_metas: new_metas,
                remove_sstable_ids: input_metas.iter().map(|m| m.id).collect(),
                unlink_wal_up_to: None,
                flush_max_seq: None,
                compaction: Some(crate::apply_worker::CompactionApplyMetrics {
                    job,
                    bytes_read,
                    bytes_written,
                    versions_dropped,
                    tombstones_dropped,
                    worker_elapsed,
                }),
            })
    }

    fn drain_catalog_apply(&mut self) -> EngineResult<usize> {
        let ready = self.apply_scheduler.drain_ready();
        let mut count = 0usize;
        for apply in ready {
            self.publish_catalog_apply(apply)?;
            count += 1;
        }
        Ok(count)
    }

    fn finish_pending_applies(&mut self) -> EngineResult<()> {
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if self.apply_scheduler.take_failed() {
                return Err(EngineError::Io("background apply failed".into()));
            }
            self.drain_catalog_apply()?;
            if self.apply_scheduler.pending_applies() == 0
                && self.apply_scheduler.ready_count() == 0
            {
                if self.apply_scheduler.take_failed() {
                    return Err(EngineError::Io("background apply failed".into()));
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(EngineError::Io("apply worker drain timeout".into()));
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    fn publish_catalog_apply(
        &mut self,
        apply: crate::apply_worker::ReadyCatalogApply,
    ) -> EngineResult<()> {
        let apply_start = Instant::now();

        let remove_set: std::collections::HashSet<u64> =
            apply.remove_sstable_ids.iter().copied().collect();
        self.sstables.retain(|s| !remove_set.contains(&s.meta.id));
        for m in &apply.add_metas {
            let path = Self::sstable_path(&self.cfg.data_dir, m.id);
            let reader = self
                .reader_cache
                .get_or_open(&path, m.id, self.block_cache.clone())?;
            self.sstables.push(LoadedSstable {
                meta: m.clone(),
                reader,
            });
        }
        self.sstables.sort_by_key(|s| s.meta.id);

        self.next_sstable_id = self
            .next_sstable_id
            .max(self.next_sstable_id_atomic.load(Ordering::Acquire));
        for m in &apply.add_metas {
            self.next_sstable_id = self.next_sstable_id.max(m.id + 1);
        }
        self.next_sstable_id_atomic
            .store(self.next_sstable_id, Ordering::Release);

        crate::failpoints::failpoint_result(crate::failpoints::APPLY_AFTER_PUBLISH_BEFORE_UNLINK)?;
        if apply.compaction.is_some() {
            crate::failpoints::failpoint_result(
                crate::failpoints::COMPACTION_AFTER_MANIFEST_BEFORE_UNLINK,
            )?;
        }

        for id in &apply.remove_sstable_ids {
            self.try_unlink_sstable(*id);
        }
        if let Some(up_to) = apply.unlink_wal_up_to {
            self.unlink_wal_segments_up_to(up_to)?;
        }
        self.dir_fsync_pending = true;

        if let Some(max_seq) = apply.flush_max_seq {
            if let Some(m) = &self.metrics {
                m.sstable_flushes_total.inc();
                m.last_durable_seq.set(max_seq as i64);
            }
            self.in_flight_wal_bytes = 0;
            self.wal_sync.set_watermarks(max_seq);
            tracing::info!(
                sstable_id = apply.add_metas.first().map(|m| m.id),
                max_seq,
                "flush applied (background)"
            );
        }

        if let Some(c) = apply.compaction {
            if let Some(metrics) = &self.metrics {
                metrics.compaction_jobs_total.inc();
                metrics
                    .compaction_jobs_by_input_level
                    .with_label_values(&[&c.job.input_level.to_string()])
                    .inc();
                metrics.compaction_bytes_read_total.inc_by(c.bytes_read);
                metrics
                    .compaction_bytes_written_total
                    .inc_by(c.bytes_written);
                metrics
                    .compaction_versions_dropped_total
                    .inc_by(c.versions_dropped);
                metrics
                    .compaction_tombstones_dropped_total
                    .inc_by(c.tombstones_dropped);
                metrics
                    .compaction_duration_seconds
                    .observe(c.worker_elapsed.as_secs_f64());
                metrics
                    .compaction_apply_duration_seconds
                    .observe(apply_start.elapsed().as_secs_f64());
            }
            tracing::info!(
                inputs = apply.remove_sstable_ids.len(),
                outputs = apply.add_metas.len(),
                bytes_read = c.bytes_read,
                bytes_written = c.bytes_written,
                elapsed_ms = c.worker_elapsed.as_millis() as u64,
                "compaction completed (background apply)"
            );
        }

        self.record_apply_duration(apply_start.elapsed());
        self.process_deferred_unlinks()?;
        self.update_gauges();
        if apply.flush_max_seq.is_some() {
            self.request_compaction();
            self.maybe_submit_compaction();
        }
        Ok(())
    }

    fn try_unlink_sstable(&mut self, id: u64) {
        let mut ps = self.pin_state.lock().expect("pin state lock");
        if ps.pin_counts.get(&id).copied().unwrap_or(0) > 0 {
            if !ps.deferred_unlinks.contains(&id) {
                ps.deferred_unlinks.push(id);
            }
            return;
        }
        drop(ps);
        let path = Self::sstable_path(&self.cfg.data_dir, id);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(error = %e, path = %path.display(), "compaction: unlink input failed");
        }
        self.block_cache.invalidate_sstable(id);
        self.reader_cache.remove(id);
    }

    fn process_deferred_unlinks(&mut self) -> EngineResult<()> {
        let pending: Vec<u64> = {
            let ps = self.pin_state.lock().expect("pin state lock");
            ps.deferred_unlinks.clone()
        };
        for id in pending {
            self.try_unlink_sstable(id);
        }
        Ok(())
    }

    fn update_compaction_gauges(&self) {
        if let Some(m) = &self.metrics {
            let depth = self.compaction_scheduler.queue_depth()
                + if self.compaction_scheduler.compaction_needed() {
                    1
                } else {
                    0
                };
            m.compaction_queue_depth.set(depth as i64);
            m.compaction_worker_busy
                .set(if self.compaction_scheduler.is_worker_busy() {
                    1
                } else {
                    0
                });
        }
    }

    /// Execute at most one compaction job synchronously (tests / explicit compact).
    /// Prefer background compaction via [`Engine::poll_compaction`].
    pub fn compact_once(&mut self) -> EngineResult<bool> {
        if self.compaction_scheduler.is_worker_busy() {
            self.poll_compaction()?;
            return Ok(false);
        }
        let Some((job, input_metas)) = self.plan_compaction_job() else {
            return Ok(false);
        };
        let drop_tombstones = self.compaction_drop_tombstones(&job);
        let result = crate::compaction_worker::execute_compaction(
            &job,
            &input_metas,
            &self.cfg.data_dir,
            &self.cfg.compaction,
            &self.block_cache,
            &self.reader_cache,
            &self.next_sstable_id_atomic,
            self.compaction_gc_watermark(),
            drop_tombstones,
        )?;
        self.submit_compaction_apply(result)?;
        self.finish_pending_applies()?;
        self.maybe_sync_data_dir()?;
        Ok(true)
    }

    /// Determine the highest WAL segment id fully covered by `max_seq` (all of
    /// its entries have seq <= max_seq). Sealed segments before the active one
    /// whose contents are entirely <= max_seq are eligible.
    ///
    /// Uses the in-memory `sealed_segment_max_seq` cache populated at seal time
    /// (and rebuilt on open from the replay scan). This is O(sealed segments)
    /// map lookups — no disk I/O — replacing what used to be a full read +
    /// decode of every sealed segment on every flush.
    fn wal_segments_covered(&self, max_seq: u64) -> EngineResult<u64> {
        let mut covered = 0u64;
        for (&id, &seg_max) in &self.sealed_segment_max_seq {
            if id >= self.active_wal_id {
                break; // BTreeMap iterates in id order; rest are >= active
            }
            if seg_max <= max_seq {
                covered = covered.max(id);
            }
        }
        Ok(covered)
    }

    fn unlink_wal_segments_up_to(&mut self, up_to_id: u64) -> EngineResult<()> {
        let segments = wal::list_segments(&self.cfg.wal_dir)?;
        for (id, path) in segments {
            if id <= up_to_id && id < self.active_wal_id {
                tracing::info!(segment = %path.display(), "unlinking covered WAL segment");
                let _ = std::fs::remove_file(&path);
                self.sealed_segment_max_seq.remove(&id);
            }
        }
        Ok(())
    }

    /// Fsync the manifest if dirty. Used by group-commit debouncing and forced
    /// durability points (shutdown, `force_flush`).
    fn sync_manifest(&mut self) -> EngineResult<()> {
        if !self.manifest_dirty {
            return Ok(());
        }
        let start = Instant::now();
        self.manifest_file
            .lock()
            .expect("manifest lock")
            .sync_all()?;
        crate::engine_fail_point!(crate::failpoints::MANIFEST_AFTER_FSYNC);
        self.manifest_dirty = false;
        self.last_manifest_sync = Instant::now();
        let elapsed = start.elapsed();
        if let Some(m) = &self.metrics {
            m.manifest_syncs_total.inc();
            m.manifest_sync_duration_seconds
                .observe(elapsed.as_secs_f64());
        }
        self.record_manifest_sync_duration(elapsed);
        Ok(())
    }

    fn estimate_pending_compaction_bytes(&self) -> u64 {
        let metas: Vec<SstableMeta> = self.sstables.iter().map(|s| s.meta.clone()).collect();
        crate::compaction::CompactionPlanner::new(&metas, &self.cfg.compaction)
            .estimate_pending_bytes()
    }

    /// Manifest fsync timing accumulated since the last drain (count, sum_ns, max_ns).
    pub fn drain_manifest_sync_window_stats(&self) -> (u64, u64, u64) {
        self.apply_scheduler.manifest_sync_window().drain()
    }

    fn record_manifest_sync_duration(&self, elapsed: std::time::Duration) {
        self.apply_scheduler.manifest_sync_window().record(elapsed);
    }

    /// Apply timing accumulated since the last drain (count, sum_ns, max_ns).
    pub fn drain_apply_window_stats(&self) -> (u64, u64, u64) {
        (
            self.apply_window_count.swap(0, Ordering::Relaxed),
            self.apply_window_sum_ns.swap(0, Ordering::Relaxed),
            self.apply_window_max_ns.swap(0, Ordering::Relaxed),
        )
    }

    fn record_apply_duration(&self, elapsed: std::time::Duration) {
        let ns = elapsed.as_nanos() as u64;
        self.apply_window_count.fetch_add(1, Ordering::Relaxed);
        self.apply_window_sum_ns.fetch_add(ns, Ordering::Relaxed);
        let mut cur = self.apply_window_max_ns.load(Ordering::Relaxed);
        while ns > cur {
            match self.apply_window_max_ns.compare_exchange_weak(
                cur,
                ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    fn maybe_sync_data_dir(&mut self) -> EngineResult<()> {
        if self.dir_fsync_pending {
            Self::fsync_dir(&self.cfg.data_dir)?;
            self.dir_fsync_pending = false;
        }
        Ok(())
    }

    /// L2 SSTable size stats for soak / fragmentation checks.
    pub fn l2_file_size_stats(&self) -> (u64, u64, u64) {
        let mut sizes: Vec<u64> = self
            .sstables
            .iter()
            .filter(|s| s.meta.level == self.cfg.compaction.max_level)
            .map(|s| s.meta.size_bytes)
            .collect();
        if sizes.is_empty() {
            return (0, 0, 0);
        }
        sizes.sort_unstable();
        let min = sizes[0];
        let max = *sizes.last().unwrap();
        let median = sizes[sizes.len() / 2];
        (min, median, max)
    }

    fn fsync_dir(dir: &Path) -> EngineResult<()> {
        let f = File::open(dir)?;
        f.sync_all()?;
        Ok(())
    }

    /// Lazy-expiry sweep over the active memtable: tombstone expired entries.
    pub fn sweep_expired(&mut self) -> EngineResult<usize> {
        let now = Self::now_ms();
        let mut expired_keys: Vec<Vec<u8>> = Vec::new();
        for (ik, entry) in self.active.iter() {
            if entry.is_expired(now) && !entry.is_tombstone() {
                expired_keys.push(ik.user_key.clone());
            }
        }
        // Deduplicate (a key may appear with multiple seqs).
        expired_keys.sort();
        expired_keys.dedup();
        let count = expired_keys.len();
        for key in expired_keys {
            // Only tombstone if the latest version is still an expired value.
            if let Some((_, entry)) = self.active.get_latest(&key) {
                if entry.is_expired(now) && !entry.is_tombstone() {
                    let seq = self.seq.next();
                    let ik = InternalKey::new(key.clone(), seq, EntryKind::Tombstone);
                    self.active.insert(ik, Entry::tombstone());
                }
            }
        }
        Ok(count)
    }

    pub fn stats(&self) -> Stats {
        let sstables = self
            .sstables
            .iter()
            .map(|s| SstableStat {
                id: s.meta.id,
                size_bytes: s.meta.size_bytes,
                min_seq: s.meta.min_seq,
                max_seq: s.meta.max_seq,
                entries: 0,
            })
            .collect();
        let wal_segments = wal::list_segments(&self.cfg.wal_dir)
            .unwrap_or_default()
            .into_iter()
            .map(|(id, path)| {
                let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                WalSegmentStat {
                    id,
                    first_seq: 0,
                    max_seq: 0,
                    size_bytes: size,
                    active: id == self.active_wal_id,
                }
            })
            .collect();
        Stats {
            uptime_s: self.started.elapsed().as_secs(),
            last_durable_seq: self.seq.peek().saturating_sub(1),
            memtable_bytes: self.active.size_bytes() as u64,
            memtable_entries: self.active.len() as u64,
            immutable_memtables: self.immutable.len() as u64,
            sstables,
            wal_segments,
        }
    }

    fn update_gauges(&self) {
        if let Some(m) = &self.metrics {
            m.memtable_size_bytes.set(self.active.size_bytes() as i64);
            m.immutable_memtable_count.set(self.immutable.len() as i64);
            m.live_sstable_count.set(self.sstables.len() as i64);
            m.last_durable_seq
                .set(self.seq.peek().saturating_sub(1) as i64);

            // Per-level live SSTable counts. Reset all observed levels each
            // time so that a level transitioning to zero is reflected.
            let mut by_level: std::collections::HashMap<u8, i64> = std::collections::HashMap::new();
            for s in &self.sstables {
                *by_level.entry(s.meta.level).or_insert(0) += 1;
            }
            // Cover levels 0..=max_level so previously-populated levels
            // show 0 instead of stale values when they drain.
            for lvl in 0..=self.cfg.compaction.max_level {
                let v = by_level.get(&lvl).copied().unwrap_or(0);
                m.live_sstables_by_level
                    .with_label_values(&[&lvl.to_string()])
                    .set(v);
            }

            // Block-cache snapshot.
            let bc = self.block_cache.stats();
            m.block_cache_hits_total
                .inc_by(bc.hits.saturating_sub(m.block_cache_hits_total.get()));
            m.block_cache_misses_total
                .inc_by(bc.misses.saturating_sub(m.block_cache_misses_total.get()));
            m.block_cache_compaction_reads_total.inc_by(
                bc.compaction_reads
                    .saturating_sub(m.block_cache_compaction_reads_total.get()),
            );
            m.block_cache_evictions_total.inc_by(
                bc.evictions
                    .saturating_sub(m.block_cache_evictions_total.get()),
            );
            m.block_cache_resident_bytes.set(bc.resident_bytes as i64);
            m.block_cache_resident_entries
                .set(bc.resident_entries as i64);
            m.disk_bytes_total.set(self.estimate_disk_bytes() as i64);
            if let Some(rc) = &self.result_cache {
                m.result_cache_resident_bytes
                    .set(rc.resident_bytes() as i64);
                m.result_cache_evictions_total.inc_by(
                    rc.evictions()
                        .saturating_sub(m.result_cache_evictions_total.get()),
                );
            }
            let seg_count = wal::list_segments(&self.cfg.wal_dir)
                .map(|v| v.len())
                .unwrap_or(0);
            m.wal_segment_count.set(seg_count as i64);
            // RPO surface: bytes in the still-active segment not yet shipped.
            let unshipped = if self.ship_dir.is_some() {
                self.active_wal_size as i64
            } else {
                0
            };
            m.wal_unshipped_bytes.set(unshipped);
            m.pending_compaction_bytes
                .set(self.estimate_pending_compaction_bytes() as i64);
        }
    }

    /// The highest write sequence assigned so far (0 if none). Cheap (no
    /// snapshot build); used by the shipping heartbeat to report the primary's
    /// current position.
    pub fn current_seq(&self) -> u64 {
        self.seq.peek().saturating_sub(1)
    }

    #[cfg(test)]
    pub fn immutable_len(&self) -> usize {
        self.immutable.len()
    }

    #[cfg(test)]
    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    #[cfg(test)]
    pub fn seq_peek(&self) -> u64 {
        self.seq.peek()
    }

    /// Test-only: force the active WAL segment to seal as if it had hit the
    /// size threshold. Mirrors the roll path in `append_wal_buffered` so the
    /// `sealed_segment_max_seq` cache and ship-out semantics get exercised
    /// without needing to actually write `WAL_SEGMENT_SIZE` bytes.
    pub fn force_roll_wal_for_test(&mut self) -> EngineResult<()> {
        if self.active_wal_size == 0 {
            return Ok(()); // mirror should_roll: empty segments don't roll
        }
        self.sync_wal()?;
        let sealed_id = self.active_wal_id;
        self.sealed_segment_max_seq
            .insert(sealed_id, self.wal_sync.buffered_seq());
        self.ship_sealed_segment(sealed_id);
        self.active_wal_id += 1;
        self.open_new_wal_segment()
    }

    pub fn sealed_segment_max_seq_snapshot(&self) -> Vec<(u64, u64)> {
        self.sealed_segment_max_seq
            .iter()
            .map(|(&a, &b)| (a, b))
            .collect()
    }
}

// Allow reading a segment's seek position cleanly in tests/util.
#[allow(dead_code)]
fn file_len(f: &mut File) -> std::io::Result<u64> {
    let pos = f.stream_position()?;
    let end = f.seek(SeekFrom::End(0))?;
    f.seek(SeekFrom::Start(pos))?;
    Ok(end)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn uk(k: &[u8]) -> Vec<u8> {
        let mut v = vec![keys::KS_USER];
        v.extend_from_slice(k);
        v
    }

    fn open_engine(dir: &TempDir) -> Engine {
        Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("data/wal"),
            ..Default::default()
        })
        .unwrap()
    }

    #[test]
    fn put_get_del_roundtrip() {
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);
        e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
        assert_eq!(e.get(&uk(b"a")).unwrap(), Some(b"1".to_vec()));
        assert!(e.del(uk(b"a")).unwrap().0);
        assert_eq!(e.get(&uk(b"a")).unwrap(), None);
    }

    #[test]
    fn reserved_keyspace_rejected() {
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);
        let sys_key = vec![keys::KS_SYSTEM, b'x'];
        assert!(e.put(sys_key, b"v".to_vec(), 0).is_err());
    }

    #[test]
    fn recovery_restores_unflushed_data() {
        let dir = TempDir::new().unwrap();
        {
            let mut e = open_engine(&dir);
            for i in 0..100u32 {
                let key = uk(format!("k{:03}", i).as_bytes());
                e.put(key, format!("v{}", i).into_bytes(), 0).unwrap();
            }
            // drop without flushing -> data only in WAL
        }
        let e = open_engine(&dir);
        for i in 0..100u32 {
            let key = uk(format!("k{:03}", i).as_bytes());
            assert_eq!(
                e.get(&key).unwrap(),
                Some(format!("v{}", i).into_bytes()),
                "key {} lost",
                i
            );
        }
    }

    #[test]
    fn shutdown_writes_marker_and_open_consumes_it() {
        let dir = TempDir::new().unwrap();
        let marker = dir.path().join("data").join(CLEAN_SHUTDOWN_MARKER);

        {
            let mut e = open_engine(&dir);
            assert!(!e.was_clean_shutdown(), "fresh dir is not a clean boot");
            e.put(uk(b"k"), b"v".to_vec(), 0).unwrap();
            e.shutdown().unwrap();
            assert!(marker.exists(), "marker must be written on shutdown");
        }

        // Reopen: marker present -> clean boot, then consumed.
        let e = open_engine(&dir);
        assert!(e.was_clean_shutdown(), "reopen after shutdown is clean");
        assert!(!marker.exists(), "marker must be consumed on open");
        assert_eq!(e.get(&uk(b"k")).unwrap(), Some(b"v".to_vec()));

        // A second reopen without a shutdown in between is NOT clean.
        drop(e);
        let e2 = open_engine(&dir);
        assert!(
            !e2.was_clean_shutdown(),
            "reopen without prior shutdown is unclean"
        );
    }

    #[test]
    fn policy_can_reject_and_record() {
        use crate::policy::WritePolicy;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // A trivial policy: reject any key whose raw suffix is "deny", and count
        // post_write calls. Proves the engine consults the hook without knowing
        // anything about what the policy enforces.
        struct TestPolicy {
            writes: AtomicUsize,
        }
        impl WritePolicy for TestPolicy {
            fn pre_write(
                &self,
                _engine: &mut Engine,
                key: &[u8],
                _value_len: usize,
                _existing: Option<usize>,
                _is_delete: bool,
            ) -> EngineResult<()> {
                if key.ends_with(b"deny") {
                    return Err(EngineError::PolicyRejected("denied by test".into()));
                }
                Ok(())
            }
            fn post_write(
                &self,
                _engine: &mut Engine,
                _key: &[u8],
                _value_len: usize,
                _existing: Option<usize>,
                _is_delete: bool,
            ) {
                self.writes.fetch_add(1, Ordering::Relaxed);
            }
        }

        let dir = TempDir::new().unwrap();
        let policy = Arc::new(TestPolicy {
            writes: AtomicUsize::new(0),
        });
        let mut e = Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("data/wal"),
            ..Default::default()
        })
        .unwrap()
        .with_write_policy(policy.clone());

        e.put(uk(b"ok"), b"1".to_vec(), 0).unwrap();
        let err = e.put(uk(b"deny"), b"2".to_vec(), 0).unwrap_err();
        assert!(matches!(err, EngineError::PolicyRejected(_)));
        // The denied write must not have persisted.
        assert_eq!(e.get(&uk(b"deny")).unwrap(), None);
        // Only the allowed put reached post_write.
        assert_eq!(policy.writes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn shutdown_ships_active_segment_bit_identical() {
        let dir = TempDir::new().unwrap();
        let ship = dir.path().join("ship");
        let mut e = Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("data/wal"),
            ..Default::default()
        })
        .unwrap()
        .with_shipping(Some(ship.clone()), crate::shipping::ShipMode::Copy);

        e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
        e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
        let active_id = e.active_wal_id;
        e.shutdown().unwrap();

        // The shipped segment must exist and be byte-identical to the live file.
        let name = wal::segment_filename(active_id);
        let live = dir.path().join("data/wal").join(&name);
        let shipped = ship.join(&name);
        assert!(
            shipped.exists(),
            "active segment must be shipped on shutdown"
        );
        assert_eq!(
            std::fs::read(&live).unwrap(),
            std::fs::read(&shipped).unwrap(),
            "shipped bytes must match live segment exactly"
        );

        // shipped.log records the segment.
        let log = std::fs::read_to_string(ship.join(crate::shipping::SHIPPED_LOG)).unwrap();
        assert!(log.contains(&format!("{} ", active_id)), "log: {}", log);
    }

    #[test]
    fn flush_then_recover_reads_from_sstable() {
        let dir = TempDir::new().unwrap();
        {
            let mut e = open_engine(&dir);
            e.put(uk(b"persist"), b"yes".to_vec(), 0).unwrap();
            e.force_flush().unwrap();
            assert_eq!(e.sstable_count(), 1);
        }
        let e = open_engine(&dir);
        assert_eq!(e.sstable_count(), 1);
        assert_eq!(e.get(&uk(b"persist")).unwrap(), Some(b"yes".to_vec()));
    }

    #[test]
    fn overwrite_after_flush_returns_new_value() {
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);
        e.put(uk(b"k"), b"old".to_vec(), 0).unwrap();
        e.force_flush().unwrap();
        e.put(uk(b"k"), b"new".to_vec(), 0).unwrap();
        assert_eq!(e.get(&uk(b"k")).unwrap(), Some(b"new".to_vec()));
    }

    #[test]
    fn delete_after_flush_returns_not_found() {
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);
        e.put(uk(b"gone"), b"here".to_vec(), 0).unwrap();
        e.force_flush().unwrap();
        e.del(uk(b"gone")).unwrap();
        assert_eq!(e.get(&uk(b"gone")).unwrap(), None);
    }

    #[test]
    fn delete_then_reflush_stays_deleted() {
        let dir = TempDir::new().unwrap();
        {
            let mut e = open_engine(&dir);
            e.put(uk(b"gone"), b"here".to_vec(), 0).unwrap();
            e.force_flush().unwrap();
            e.del(uk(b"gone")).unwrap();
            e.force_flush().unwrap();
            assert_eq!(e.get(&uk(b"gone")).unwrap(), None);
        }
        let e = open_engine(&dir);
        assert_eq!(e.get(&uk(b"gone")).unwrap(), None);
    }

    #[test]
    fn expiry_makes_key_disappear() {
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);
        let past = 1; // 1 ms after epoch, definitely expired
        e.put(uk(b"temp"), b"v".to_vec(), past).unwrap();
        assert_eq!(e.get(&uk(b"temp")).unwrap(), None);
    }

    #[test]
    fn seq_monotonic_across_restart() {
        let dir = TempDir::new().unwrap();
        let seq_after_first;
        {
            let mut e = open_engine(&dir);
            e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
            e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
            seq_after_first = e.seq_peek();
        }
        let e = open_engine(&dir);
        assert!(
            e.seq_peek() >= seq_after_first,
            "seq must not regress: {} vs {}",
            e.seq_peek(),
            seq_after_first
        );
    }

    #[test]
    fn sealed_segment_max_seq_cache_drives_truncation() {
        // Validates the wal_segments_covered fast path: writes across three
        // segments, asserts the cache holds the right max_seq per sealed
        // segment, then flushes and asserts the cache shrinks because covered
        // segments were unlinked. Previously wal_segments_covered re-read
        // every segment on every flush; this is the regression guard for that
        // bug.
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);

        // Segment 1: two writes, then force seal. The cached max_seq for the
        // sealed segment must equal the seq of the last write in it.
        e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
        e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
        let seq_after_seg1 = e.seq_peek().saturating_sub(1);
        let seg1_id = e.active_wal_id;
        e.force_roll_wal_for_test().unwrap();

        // Segment 2.
        e.put(uk(b"c"), b"3".to_vec(), 0).unwrap();
        let seq_after_seg2 = e.seq_peek().saturating_sub(1);
        let seg2_id = e.active_wal_id;
        e.force_roll_wal_for_test().unwrap();

        // Segment 3 is the new active; nothing in it yet.
        let snap = e.sealed_segment_max_seq_snapshot();
        assert_eq!(
            snap,
            vec![(seg1_id, seq_after_seg1), (seg2_id, seq_after_seg2)],
            "cache must hold max seq for both sealed segments"
        );

        // Flush: this calls wal_segments_covered, which must use the cache and
        // unlink seg1 + seg2. After unlink the cache must be empty.
        e.put(uk(b"d"), b"4".to_vec(), 0).unwrap();
        let active_before_flush = e.active_wal_id;
        e.force_flush().unwrap();
        assert_eq!(
            e.sealed_segment_max_seq_snapshot(),
            vec![],
            "covered sealed segments must be unlinked AND dropped from the cache"
        );
        // Active segment id must not change across flush; flush does not roll.
        assert_eq!(e.active_wal_id, active_before_flush);
    }

    #[test]
    fn write_batch_applies_all_ops() {
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);
        e.write_batch(vec![
            BatchOp::Put {
                key: uk(b"a"),
                value: b"1".to_vec(),
                expires_at: 0,
            },
            BatchOp::Put {
                key: uk(b"b"),
                value: b"2".to_vec(),
                expires_at: 0,
            },
        ])
        .unwrap();
        assert_eq!(e.get(&uk(b"a")).unwrap(), Some(b"1".to_vec()));
        assert_eq!(e.get(&uk(b"b")).unwrap(), Some(b"2".to_vec()));
    }

    #[test]
    fn write_batch_rejects_duplicate_keys() {
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);
        let err = e
            .write_batch(vec![
                BatchOp::Put {
                    key: uk(b"dup"),
                    value: b"1".to_vec(),
                    expires_at: 0,
                },
                BatchOp::Put {
                    key: uk(b"dup"),
                    value: b"2".to_vec(),
                    expires_at: 0,
                },
            ])
            .unwrap_err();
        assert!(matches!(err, EngineError::InvalidKey(_)));
        // Rejected before any mutation: nothing persisted.
        assert_eq!(e.get(&uk(b"dup")).unwrap(), None);
    }

    #[test]
    fn write_batch_empty_is_rejected() {
        let dir = TempDir::new().unwrap();
        let mut e = open_engine(&dir);
        assert!(e.write_batch(vec![]).is_err());
    }

    #[test]
    fn write_batch_policy_rejection_persists_nothing() {
        use crate::policy::WritePolicy;

        // Rejects any key whose raw suffix is "deny".
        struct DenyPolicy;
        impl WritePolicy for DenyPolicy {
            fn pre_write(
                &self,
                _engine: &mut Engine,
                key: &[u8],
                _value_len: usize,
                _existing: Option<usize>,
                _is_delete: bool,
            ) -> EngineResult<()> {
                if key.ends_with(b"deny") {
                    return Err(EngineError::PolicyRejected("denied".into()));
                }
                Ok(())
            }
            fn post_write(
                &self,
                _engine: &mut Engine,
                _key: &[u8],
                _value_len: usize,
                _existing: Option<usize>,
                _is_delete: bool,
            ) {
            }
        }

        let dir = TempDir::new().unwrap();
        let mut e = Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("data/wal"),
            ..Default::default()
        })
        .unwrap()
        .with_write_policy(Arc::new(DenyPolicy));

        let err = e
            .write_batch(vec![
                BatchOp::Put {
                    key: uk(b"ok"),
                    value: b"1".to_vec(),
                    expires_at: 0,
                },
                BatchOp::Put {
                    key: uk(b"deny"),
                    value: b"2".to_vec(),
                    expires_at: 0,
                },
            ])
            .unwrap_err();
        assert!(matches!(err, EngineError::PolicyRejected(_)));
        // The gate runs fully before any mutation: even the op preceding the
        // rejected one must not have persisted.
        assert_eq!(e.get(&uk(b"ok")).unwrap(), None);
        assert_eq!(e.get(&uk(b"deny")).unwrap(), None);
    }

    #[test]
    fn write_batch_clean_recovery_replays_all() {
        let dir = TempDir::new().unwrap();
        {
            let mut e = open_engine(&dir);
            e.put(uk(b"old"), b"x".to_vec(), 0).unwrap();
            e.write_batch(vec![
                BatchOp::Put {
                    key: uk(b"a"),
                    value: b"1".to_vec(),
                    expires_at: 0,
                },
                BatchOp::Put {
                    key: uk(b"b"),
                    value: b"2".to_vec(),
                    expires_at: 0,
                },
                BatchOp::Del { key: uk(b"old") },
            ])
            .unwrap();
            // drop without flush -> data only in WAL
        }
        let e = open_engine(&dir);
        assert_eq!(e.get(&uk(b"a")).unwrap(), Some(b"1".to_vec()));
        assert_eq!(e.get(&uk(b"b")).unwrap(), Some(b"2".to_vec()));
        assert_eq!(e.get(&uk(b"old")).unwrap(), None);
    }

    #[test]
    fn write_batch_torn_recovery_is_all_or_nothing() {
        let dir = TempDir::new().unwrap();
        {
            let mut e = open_engine(&dir);
            e.put(uk(b"keep"), b"v".to_vec(), 0).unwrap();
            e.write_batch(vec![
                BatchOp::Put {
                    key: uk(b"a"),
                    value: b"1".to_vec(),
                    expires_at: 0,
                },
                BatchOp::Put {
                    key: uk(b"b"),
                    value: b"2".to_vec(),
                    expires_at: 0,
                },
                BatchOp::Put {
                    key: uk(b"c"),
                    value: b"3".to_vec(),
                    expires_at: 0,
                },
            ])
            .unwrap();
            e.sync_wal().unwrap();
            // drop without flush -> committed PUT + batch live in the WAL
        }

        // Simulate a torn batch write: truncate the tail of the latest WAL
        // segment so the single batch record is incomplete. The committed PUT
        // before it stays intact.
        let wal_dir = dir.path().join("data/wal");
        let mut segs: Vec<std::path::PathBuf> = std::fs::read_dir(&wal_dir)
            .unwrap()
            .filter_map(|e| e.ok().map(|e| e.path()))
            .filter(|p| p.extension().map(|x| x == "log").unwrap_or(false))
            .collect();
        segs.sort();
        let last = segs.last().unwrap();
        let len = std::fs::metadata(last).unwrap().len();
        let f = OpenOptions::new().write(true).open(last).unwrap();
        f.set_len(len - 3).unwrap();
        f.sync_all().unwrap();

        let e = open_engine(&dir);
        // The committed single PUT survives; NO op from the torn batch replays.
        assert_eq!(e.get(&uk(b"keep")).unwrap(), Some(b"v".to_vec()));
        assert_eq!(e.get(&uk(b"a")).unwrap(), None);
        assert_eq!(e.get(&uk(b"b")).unwrap(), None);
        assert_eq!(e.get(&uk(b"c")).unwrap(), None);
    }

    #[test]
    fn recovery_rebuilds_sealed_segment_cache() {
        // Pre-create two sealed segments + one active with unflushed data,
        // crash (drop), and assert reopen rebuilds the cache from the WAL
        // scan that already happens during replay.
        let dir = TempDir::new().unwrap();
        let (sealed1, max1, sealed2, max2) = {
            let mut e = open_engine(&dir);
            e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
            let s1 = e.active_wal_id;
            let m1 = e.seq_peek().saturating_sub(1);
            e.force_roll_wal_for_test().unwrap();
            e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
            let s2 = e.active_wal_id;
            let m2 = e.seq_peek().saturating_sub(1);
            e.force_roll_wal_for_test().unwrap();
            // leave a write in the new active too, no flush
            e.put(uk(b"c"), b"3".to_vec(), 0).unwrap();
            (s1, m1, s2, m2)
        };
        let e = open_engine(&dir);
        let snap = e.sealed_segment_max_seq_snapshot();
        // The just-active segment from the prior process is now sealed too,
        // but the test only pins the two we explicitly rolled.
        assert!(
            snap.iter().any(|&(id, m)| id == sealed1 && m == max1),
            "seg1 missing from rebuilt cache: {:?}",
            snap
        );
        assert!(
            snap.iter().any(|&(id, m)| id == sealed2 && m == max2),
            "seg2 missing from rebuilt cache: {:?}",
            snap
        );
    }
}
