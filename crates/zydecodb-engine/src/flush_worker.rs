//! Background memtable flush worker.
//!
//! SSTable build + disk publish runs on a dedicated OS thread so memtable
//! freeze on the write path never blocks on multi-second flush I/O. The engine
//! owner applies manifest/catalog updates via [`FlushScheduler::poll`].
//!
//! Phase 5b: work items carry per-tenant byte attribution for L0 token charge
//! and fair-share metrics. Immutable memtables stay FIFO on a shared LSM
//! (seq correctness); dual-queue flush reorder applies only if Fork B creates
//! independent flush domains. **No WAL capacity reservation.**

use crate::entry::Entry;
use crate::errors::EngineResult;
use crate::keys::InternalKey;
use crate::manifest::SstableMeta;
use crate::sstable;
use crate::tenant_fair::TenantId;
use crossbeam::channel::{Receiver, Sender, TryRecvError};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;

/// Result of the worker-side SSTable build + publish (no manifest mutation).
#[derive(Debug, Clone)]
pub struct FlushExecuteResult {
    pub meta: SstableMeta,
    pub max_seq: u64,
    /// Bytes per tenant in the flushed memtable (for pool release + L0 tokens).
    pub tenant_bytes: HashMap<TenantId, u64>,
    /// Hint: tenant with highest U_i/f_i among contributors (metrics / Fork B).
    pub priority_tenant: Option<TenantId>,
}

struct WorkItem {
    pairs: Vec<(InternalKey, Entry)>,
    data_dir: PathBuf,
    next_sstable_id: Arc<AtomicU64>,
    tenant_bytes: HashMap<TenantId, u64>,
    priority_tenant: Option<TenantId>,
}

enum WorkerCommand {
    Run(WorkItem),
    Shutdown,
}

/// Schedules memtable flushes on a background thread.
pub struct FlushScheduler {
    work_tx: Sender<WorkerCommand>,
    result_rx: Receiver<Result<FlushExecuteResult, String>>,
    worker_busy: Arc<AtomicBool>,
    worker_failed: Arc<AtomicBool>,
    last_error: Mutex<Option<String>>,
    /// Last tenant preferred for flush service (fairness observability).
    last_priority_tenant: Mutex<Option<TenantId>>,
    join_handle: Option<JoinHandle<()>>,
}

impl Default for FlushScheduler {
    fn default() -> Self {
        Self::new()
    }
}

impl FlushScheduler {
    pub fn new() -> Self {
        let (work_tx, work_rx) = crossbeam::channel::unbounded();
        let (result_tx, result_rx) = crossbeam::channel::unbounded();
        let worker_busy = Arc::new(AtomicBool::new(false));
        let worker_failed = Arc::new(AtomicBool::new(false));
        let last_error = Mutex::new(None);
        let busy_flag = worker_busy.clone();

        let join_handle = thread::Builder::new()
            .name("zydecodb-flush".into())
            .spawn(move || flush_worker_loop(work_rx, result_tx, busy_flag))
            .expect("spawn flush worker");

        FlushScheduler {
            work_tx,
            result_rx,
            worker_busy,
            worker_failed,
            last_error,
            last_priority_tenant: Mutex::new(None),
            join_handle: Some(join_handle),
        }
    }

    pub fn note_worker_failed(&self, err: String) {
        *self.last_error.lock().expect("flush last_error lock") = Some(err);
        self.worker_failed.store(true, Ordering::Release);
    }

    pub fn take_worker_failure(&self) -> Option<String> {
        if !self.worker_failed.swap(false, Ordering::AcqRel) {
            return None;
        }
        self.last_error
            .lock()
            .expect("flush last_error lock")
            .take()
    }

    pub fn is_worker_busy(&self) -> bool {
        self.worker_busy.load(Ordering::Acquire)
    }

    pub fn last_priority_tenant(&self) -> Option<TenantId> {
        *self
            .last_priority_tenant
            .lock()
            .expect("flush priority lock")
    }

    pub fn try_submit(
        &self,
        pairs: Vec<(InternalKey, Entry)>,
        data_dir: PathBuf,
        next_sstable_id: Arc<AtomicU64>,
        tenant_bytes: HashMap<TenantId, u64>,
        priority_tenant: Option<TenantId>,
    ) -> bool {
        if pairs.is_empty() {
            return false;
        }
        if self.worker_busy.load(Ordering::Acquire) {
            return false;
        }
        self.worker_failed.store(false, Ordering::Release);
        self.worker_busy.store(true, Ordering::Release);
        *self
            .last_priority_tenant
            .lock()
            .expect("flush priority lock") = priority_tenant;
        self.work_tx
            .send(WorkerCommand::Run(WorkItem {
                pairs,
                data_dir,
                next_sstable_id,
                tenant_bytes,
                priority_tenant,
            }))
            .is_ok()
    }

    pub fn poll_results(&self) -> Vec<Result<FlushExecuteResult, String>> {
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
        if self.worker_busy.load(Ordering::Acquire) {
            1
        } else {
            0
        }
    }
}

impl Drop for FlushScheduler {
    fn drop(&mut self) {
        let _ = self.work_tx.send(WorkerCommand::Shutdown);
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }
    }
}

fn flush_worker_loop(
    work_rx: Receiver<WorkerCommand>,
    result_tx: Sender<Result<FlushExecuteResult, String>>,
    worker_busy: Arc<AtomicBool>,
) {
    while let Ok(cmd) = work_rx.recv() {
        match cmd {
            WorkerCommand::Shutdown => break,
            WorkerCommand::Run(item) => {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    execute_flush(
                        &item.pairs,
                        &item.data_dir,
                        &item.next_sstable_id,
                        item.tenant_bytes,
                        item.priority_tenant,
                    )
                    .map_err(|e| e.to_string())
                }));
                worker_busy.store(false, Ordering::Release);
                let result = match result {
                    Ok(r) => r,
                    Err(_) => Err("background flush panicked".to_string()),
                };
                let _ = result_tx.send(result);
            }
        }
    }
}

fn fsync_dir(path: &Path) -> EngineResult<()> {
    let f = OpenOptions::new().read(true).open(path)?;
    f.sync_all()?;
    Ok(())
}

/// Build and atomically publish one SSTable. Does not touch manifest or catalog.
pub fn execute_flush(
    pairs: &[(InternalKey, Entry)],
    data_dir: &Path,
    next_sstable_id: &AtomicU64,
    tenant_bytes: HashMap<TenantId, u64>,
    priority_tenant: Option<TenantId>,
) -> EngineResult<FlushExecuteResult> {
    let max_seq = pairs.iter().map(|(k, _)| k.seq).max().unwrap_or(0);
    let sst = sstable::build(pairs, true);
    let id = next_sstable_id.fetch_add(1, Ordering::AcqRel);

    let tmp_dir = data_dir.join(".tmp");
    std::fs::create_dir_all(&tmp_dir)?;
    let tmp_path = tmp_dir.join(format!("{:08}.sst.tmp", id));
    {
        crate::failpoints::failpoint_result(crate::failpoints::SSTABLE_BEFORE_TMP_WRITE)?;
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)?;
        f.write_all(&sst.bytes)?;
        f.sync_all()?;
        crate::failpoints::failpoint_result(crate::failpoints::SSTABLE_AFTER_TMP_WRITE)?;
    }
    let final_path = data_dir.join(format!("{:08}.sst", id));
    crate::failpoints::failpoint_result(crate::failpoints::SSTABLE_BEFORE_RENAME)?;
    std::fs::rename(&tmp_path, &final_path)?;
    crate::failpoints::failpoint_result(crate::failpoints::SSTABLE_AFTER_RENAME)?;
    fsync_dir(data_dir)?;

    let meta = SstableMeta {
        id,
        level: 0,
        min_key: sst.min_key.clone(),
        max_key: sst.max_key.clone(),
        min_seq: sst.min_seq,
        max_seq: sst.max_seq,
        size_bytes: sst.bytes.len() as u64,
    };
    Ok(FlushExecuteResult {
        meta,
        max_seq,
        tenant_bytes,
        priority_tenant,
    })
}

#[allow(dead_code)]
pub fn worker_elapsed_placeholder() -> Duration {
    Duration::ZERO
}
