//! Background compaction worker.
//!
//! Compaction merge + SSTable output runs on a dedicated OS thread so the
//! write/flush path never blocks on multi-second disk work. The engine owner
//! plans jobs, submits work, and applies manifest/catalog updates via
//! [`CompactionScheduler::poll`].

use crate::block_cache::BlockCache;
use crate::compaction::{CompactionConfig, CompactionJob};
use crate::entry::Entry;
use crate::errors::EngineResult;
use crate::iter::{EntryIterator, MergeMode, MergingIterator};
use crate::keys::InternalKey;
use crate::manifest::SstableMeta;
use crate::reader_cache::ReaderCache;
use crate::sstable::{self, SstableReader};
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Result of the worker-side merge + write phase (no manifest mutation).
#[derive(Debug, Clone)]
pub struct CompactionExecuteResult {
    pub job: CompactionJob,
    pub new_metas: Vec<SstableMeta>,
    pub input_metas: Vec<SstableMeta>,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub versions_dropped: u64,
    pub tombstones_dropped: u64,
    pub worker_elapsed: Duration,
}

struct WorkItem {
    job: CompactionJob,
    input_metas: Vec<SstableMeta>,
    data_dir: PathBuf,
    cfg: CompactionConfig,
    block_cache: Arc<BlockCache>,
    reader_cache: Arc<ReaderCache>,
    next_sstable_id: Arc<AtomicU64>,
    gc_watermark: u64,
    drop_tombstones: bool,
}

enum WorkerCommand {
    Run(WorkItem),
    Shutdown,
}

/// Schedules compaction jobs on a background thread and delivers completed
/// results to the engine owner for catalog application.
pub struct CompactionScheduler {
    work_tx: Sender<WorkerCommand>,
    result_rx: Receiver<Result<CompactionExecuteResult, String>>,
    worker_busy: Arc<AtomicBool>,
    /// Set when the worker returns an error; stops [`Engine::drain_compaction`]
    /// from tight-loop resubmitting the same doomed job.
    worker_failed: Arc<AtomicBool>,
    last_error: Mutex<Option<String>>,
    compaction_needed: Arc<AtomicBool>,
    /// Best plan seen while the worker was busy (single-slot coalescing).
    pending: Mutex<Option<WorkItem>>,
    join_handle: Option<JoinHandle<()>>,
}

impl CompactionScheduler {
    pub fn new(
        _data_dir: PathBuf,
        _cfg: CompactionConfig,
        _block_cache: Arc<BlockCache>,
        _next_sstable_id: Arc<AtomicU64>,
    ) -> Self {
        let (work_tx, work_rx) = crossbeam::channel::unbounded();
        let (result_tx, result_rx) = crossbeam::channel::unbounded();
        let worker_busy = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::new(AtomicBool::new(false));
        let last_error = Mutex::new(None);
        let compaction_needed = Arc::new(AtomicBool::new(false));
        let busy_flag = worker_busy.clone();

        let join_handle = thread::Builder::new()
            .name("zydecodb-compaction".into())
            .spawn(move || compaction_worker_loop(work_rx, result_tx, busy_flag))
            .expect("spawn compaction worker");

        CompactionScheduler {
            work_tx,
            result_rx,
            worker_busy,
            worker_failed,
            last_error,
            compaction_needed,
            pending: Mutex::new(None),
            join_handle: Some(join_handle),
        }
    }

    pub fn request_compaction(&self) {
        self.compaction_needed.store(true, Ordering::Release);
    }

    pub fn is_worker_busy(&self) -> bool {
        self.worker_busy.load(Ordering::Acquire)
    }

    pub fn compaction_needed(&self) -> bool {
        self.compaction_needed.load(Ordering::Acquire)
    }

    pub fn clear_compaction_needed(&self) {
        self.compaction_needed.store(false, Ordering::Release);
    }

    pub fn note_worker_failed(&self, err: String) {
        *self.last_error.lock().expect("compaction last_error lock") = Some(err);
        self.worker_failed.store(true, Ordering::Release);
    }

    pub fn take_worker_failure(&self) -> Option<String> {
        if !self.worker_failed.swap(false, Ordering::AcqRel) {
            return None;
        }
        self.last_error
            .lock()
            .expect("compaction last_error lock")
            .take()
    }

    pub fn clear_staged(&self) {
        *self.pending.lock().expect("compaction pending lock") = None;
    }

    /// Keep the highest-priority plan seen while the worker is busy.
    #[allow(clippy::too_many_arguments)] // mirrors the full WorkItem the worker needs
    pub fn stage_if_busy(
        &self,
        job: CompactionJob,
        input_metas: Vec<SstableMeta>,
        data_dir: PathBuf,
        cfg: CompactionConfig,
        block_cache: Arc<BlockCache>,
        reader_cache: Arc<ReaderCache>,
        next_sstable_id: Arc<AtomicU64>,
        gc_watermark: u64,
        drop_tombstones: bool,
    ) {
        let item = WorkItem {
            job,
            input_metas,
            data_dir,
            cfg,
            block_cache,
            reader_cache,
            next_sstable_id,
            gc_watermark,
            drop_tombstones,
        };
        let mut pending = self.pending.lock().expect("compaction pending lock");
        let replace = match pending.as_ref() {
            Some(existing) => item.job.priority_score > existing.job.priority_score,
            None => true,
        };
        if replace {
            *pending = Some(item);
        }
    }

    #[allow(clippy::too_many_arguments)] // mirrors the full WorkItem the worker needs
    pub fn try_submit(
        &self,
        job: CompactionJob,
        input_metas: Vec<SstableMeta>,
        data_dir: PathBuf,
        cfg: CompactionConfig,
        block_cache: Arc<BlockCache>,
        reader_cache: Arc<ReaderCache>,
        next_sstable_id: Arc<AtomicU64>,
        gc_watermark: u64,
        drop_tombstones: bool,
    ) -> bool {
        if self.worker_busy.load(Ordering::Acquire) {
            self.stage_if_busy(
                job,
                input_metas,
                data_dir,
                cfg,
                block_cache,
                reader_cache,
                next_sstable_id,
                gc_watermark,
                drop_tombstones,
            );
            return false;
        }
        self.submit_work(WorkItem {
            job,
            input_metas,
            data_dir,
            cfg,
            block_cache,
            reader_cache,
            next_sstable_id,
            gc_watermark,
            drop_tombstones,
        })
    }

    /// Submit the staged plan if the worker is idle.
    pub fn try_submit_staged(&self) -> bool {
        if self.worker_busy.load(Ordering::Acquire) {
            return false;
        }
        let item = self.pending.lock().expect("compaction pending lock").take();
        item.map(|i| self.submit_work(i)).unwrap_or(false)
    }

    fn submit_work(&self, item: WorkItem) -> bool {
        self.worker_failed.store(false, Ordering::Release);
        self.worker_busy.store(true, Ordering::Release);
        self.compaction_needed.store(false, Ordering::Release);
        self.work_tx.send(WorkerCommand::Run(item)).is_ok()
    }

    pub fn poll_results(&self) -> Vec<Result<CompactionExecuteResult, String>> {
        let mut out = Vec::new();
        loop {
            match self.result_rx.try_recv() {
                Ok(r) => out.push(r),
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => break,
            }
        }
        out
    }

    pub fn queue_depth(&self) -> usize {
        let staged = self
            .pending
            .lock()
            .expect("compaction pending lock")
            .is_some();
        let busy = self.worker_busy.load(Ordering::Acquire);
        (busy as usize) + (staged as usize)
    }
}

impl Drop for CompactionScheduler {
    fn drop(&mut self) {
        let _ = self.work_tx.send(WorkerCommand::Shutdown);
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }
    }
}

fn compaction_worker_loop(
    work_rx: Receiver<WorkerCommand>,
    result_tx: Sender<Result<CompactionExecuteResult, String>>,
    worker_busy: Arc<AtomicBool>,
) {
    while let Ok(cmd) = work_rx.recv() {
        match cmd {
            WorkerCommand::Shutdown => break,
            WorkerCommand::Run(item) => {
                let start = Instant::now();
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    execute_compaction(
                        &item.job,
                        &item.input_metas,
                        &item.data_dir,
                        &item.cfg,
                        &item.block_cache,
                        &item.reader_cache,
                        &item.next_sstable_id,
                        item.gc_watermark,
                        item.drop_tombstones,
                    )
                    .map(|mut r| {
                        r.worker_elapsed = start.elapsed();
                        r
                    })
                    .map_err(|e| e.to_string())
                }));
                worker_busy.store(false, Ordering::Release);
                let result = match result {
                    Ok(r) => r,
                    Err(_) => Err("background compaction panicked".to_string()),
                };
                let _ = result_tx.send(result);
            }
        }
    }
}

fn sstable_path(data_dir: &Path, id: u64) -> PathBuf {
    data_dir.join(format!("{:08}.sst", id))
}

fn fsync_dir(path: &Path) -> EngineResult<()> {
    let f = OpenOptions::new().read(true).open(path)?;
    f.sync_all()?;
    Ok(())
}

/// Merge inputs and write output SSTables. Does not touch manifest or catalog.
#[allow(clippy::too_many_arguments)] // a compaction run needs the full input + output context
pub fn execute_compaction(
    job: &CompactionJob,
    input_metas: &[SstableMeta],
    data_dir: &Path,
    cfg: &CompactionConfig,
    block_cache: &Arc<BlockCache>,
    reader_cache: &Arc<ReaderCache>,
    next_sstable_id: &AtomicU64,
    gc_watermark: u64,
    drop_tombstones: bool,
) -> EngineResult<CompactionExecuteResult> {
    let mut bytes_read = 0u64;
    let mut input_readers: Vec<Arc<SstableReader>> = Vec::new();
    for meta in input_metas {
        bytes_read = bytes_read.saturating_add(meta.size_bytes);
        let path = sstable_path(data_dir, meta.id);
        input_readers.push(reader_cache.get_or_open(&path, meta.id, block_cache.clone())?);
    }

    let merge_sources: Vec<Box<dyn EntryIterator>> = input_readers
        .iter()
        .map(|r| Ok(Box::new(r.clone().full_iter()?) as Box<dyn EntryIterator>))
        .collect::<EngineResult<_>>()?;
    let mut merger = MergingIterator::new(merge_sources, MergeMode::Raw)?;

    let mut pending: Vec<(InternalKey, Entry)> = Vec::new();
    let mut pending_bytes: u64 = 0;
    let mut new_metas: Vec<SstableMeta> = Vec::new();
    let mut bytes_written = 0u64;
    let mut versions_dropped = 0u64;
    let mut tombstones_dropped = 0u64;
    let mut last_user_key: Option<Vec<u8>> = None;
    let mut kept_at_or_below_watermark = false;

    loop {
        let next = EntryIterator::next(&mut merger)?;
        let at_key_boundary = match (&next, &last_user_key) {
            (Some((k, _)), Some(prev)) => k.user_key != *prev,
            _ => true,
        };
        if at_key_boundary {
            kept_at_or_below_watermark = false;
        }
        let flush_this_round = match &next {
            None => !pending.is_empty(),
            Some(_) => at_key_boundary && pending_bytes >= cfg.target_file_bytes,
        };
        if flush_this_round && !pending.is_empty() {
            let meta = write_compaction_output(
                data_dir,
                &pending,
                job.output_level,
                cfg,
                next_sstable_id,
            )?;
            bytes_written = bytes_written.saturating_add(meta.size_bytes);
            new_metas.push(meta);
            pending.clear();
            pending_bytes = 0;
        }
        match next {
            None => break,
            Some((k, e)) => {
                if should_drop_at_compaction(
                    &k,
                    &e,
                    gc_watermark,
                    &mut kept_at_or_below_watermark,
                    drop_tombstones,
                ) {
                    if e.is_tombstone() {
                        tombstones_dropped += 1;
                    } else {
                        versions_dropped += 1;
                    }
                    last_user_key = Some(k.user_key.clone());
                    continue;
                }
                pending_bytes = pending_bytes
                    .saturating_add(32 + e.value_len() as u64 + k.user_key.len() as u64);
                last_user_key = Some(k.user_key.clone());
                pending.push((k, e));
            }
        }
    }
    if !pending.is_empty() {
        let meta =
            write_compaction_output(data_dir, &pending, job.output_level, cfg, next_sstable_id)?;
        bytes_written = bytes_written.saturating_add(meta.size_bytes);
        new_metas.push(meta);
    }

    let result = CompactionExecuteResult {
        job: job.clone(),
        new_metas,
        input_metas: input_metas.to_vec(),
        bytes_read,
        bytes_written,
        versions_dropped,
        tombstones_dropped,
        worker_elapsed: Duration::ZERO,
    };
    Ok(result)
}

fn should_drop_at_compaction(
    k: &InternalKey,
    e: &Entry,
    gc_watermark: u64,
    kept_at_or_below: &mut bool,
    drop_tombstones: bool,
) -> bool {
    if gc_watermark == 0 {
        return false;
    }
    if k.seq > gc_watermark {
        return false;
    }
    if drop_tombstones && e.is_tombstone() {
        return true;
    }
    if !*kept_at_or_below {
        *kept_at_or_below = true;
        return false;
    }
    true
}

fn write_compaction_output(
    data_dir: &Path,
    pairs: &[(InternalKey, Entry)],
    level: u8,
    cfg: &CompactionConfig,
    next_sstable_id: &AtomicU64,
) -> EngineResult<SstableMeta> {
    let with_bloom = !(cfg.optimize_filters_for_hits && level == cfg.max_level);
    let sst = sstable::build(pairs, with_bloom);
    let id = next_sstable_id.fetch_add(1, Ordering::AcqRel);

    let tmp_dir = data_dir.join(".tmp");
    std::fs::create_dir_all(&tmp_dir)?;
    let tmp_path = tmp_dir.join(format!("{:08}.sst.tmp", id));
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(&sst.bytes)?;
        f.sync_all()?;
    }
    crate::failpoints::failpoint_result(crate::failpoints::COMPACTION_BEFORE_RENAME)?;
    let final_path = sstable_path(data_dir, id);
    std::fs::rename(&tmp_path, &final_path)?;
    fsync_dir(data_dir)?;
    Ok(SstableMeta {
        id,
        level,
        min_key: sst.min_key,
        max_key: sst.max_key,
        min_seq: sst.min_seq,
        max_seq: sst.max_seq,
        size_bytes: sst.bytes.len() as u64,
    })
}
