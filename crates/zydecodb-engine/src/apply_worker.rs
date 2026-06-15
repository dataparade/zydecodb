//! Background manifest apply for flush and compaction catalog updates.
//!
//! Worker threads produce SSTables on disk; this module appends manifest
//! records and fsyncs **before** the engine owner swaps the in-memory catalog.
//! Unlink of obsolete files stays on the owner thread after the swap.

use crate::compaction::CompactionJob;
use crate::errors::EngineResult;
use crate::manifest::{ManifestRecord, SstableMeta};
use crossbeam::channel::{Receiver, Sender};
use std::fs::File;
use std::io::Write;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

/// Per-sample-window manifest fsync stats for the soak harness.
#[derive(Debug, Default)]
pub struct ManifestSyncWindow {
    pub count: AtomicU64,
    pub sum_ns: AtomicU64,
    pub max_ns: AtomicU64,
}

impl ManifestSyncWindow {
    pub fn record(&self, elapsed: Duration) {
        let ns = elapsed.as_nanos() as u64;
        self.count.fetch_add(1, Ordering::Relaxed);
        self.sum_ns.fetch_add(ns, Ordering::Relaxed);
        let mut cur = self.max_ns.load(Ordering::Relaxed);
        while ns > cur {
            match self
                .max_ns
                .compare_exchange_weak(cur, ns, Ordering::Relaxed, Ordering::Relaxed)
            {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    pub fn drain(&self) -> (u64, u64, u64) {
        (
            self.count.swap(0, Ordering::Relaxed),
            self.sum_ns.swap(0, Ordering::Relaxed),
            self.max_ns.swap(0, Ordering::Relaxed),
        )
    }
}

/// Catalog mutation ready for the owner after durable manifest fsync.
#[derive(Debug, Clone)]
pub struct ReadyCatalogApply {
    pub manifest_records: Vec<ManifestRecord>,
    pub add_metas: Vec<SstableMeta>,
    pub remove_sstable_ids: Vec<u64>,
    pub unlink_wal_up_to: Option<u64>,
    pub flush_max_seq: Option<u64>,
    pub compaction: Option<CompactionApplyMetrics>,
}

#[derive(Debug, Clone)]
pub struct CompactionApplyMetrics {
    pub job: CompactionJob,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub versions_dropped: u64,
    pub tombstones_dropped: u64,
    pub worker_elapsed: Duration,
}

struct PendingCatalogApply {
    manifest_records: Vec<ManifestRecord>,
    add_metas: Vec<SstableMeta>,
    remove_sstable_ids: Vec<u64>,
    unlink_wal_up_to: Option<u64>,
    flush_max_seq: Option<u64>,
    compaction: Option<CompactionApplyMetrics>,
}

enum ApplyCommand {
    Run(PendingCatalogApply),
}

/// Shared manifest writer + fsync on a dedicated thread.
pub struct ApplyScheduler {
    work_tx: Option<Sender<ApplyCommand>>,
    ready: Arc<Mutex<Vec<ReadyCatalogApply>>>,
    pending_count: Arc<AtomicU64>,
    failed: Arc<AtomicBool>,
    metrics: Arc<Mutex<Option<Arc<crate::metrics::Metrics>>>>,
    manifest_sync_window: Arc<ManifestSyncWindow>,
    join_handle: Option<JoinHandle<()>>,
}

impl ApplyScheduler {
    pub fn new(
        manifest_file: Arc<Mutex<File>>,
        manifest_sync_window: Arc<ManifestSyncWindow>,
    ) -> Self {
        let (work_tx, work_rx) = crossbeam::channel::unbounded();
        let ready = Arc::new(Mutex::new(Vec::new()));
        let pending_count = Arc::new(AtomicU64::new(0));
        let failed = Arc::new(AtomicBool::new(false));
        let metrics = Arc::new(Mutex::new(None));
        let ready_out = ready.clone();
        let pending_out = pending_count.clone();
        let failed_out = failed.clone();
        let metrics_out = metrics.clone();
        let window_out = manifest_sync_window.clone();

        let join_handle = thread::Builder::new()
            .name("zydecodb-apply".into())
            .spawn(move || {
                apply_worker_loop(
                    manifest_file,
                    work_rx,
                    ready_out,
                    pending_out,
                    failed_out,
                    metrics_out,
                    window_out,
                )
            })
            .expect("spawn apply worker");

        ApplyScheduler {
            work_tx: Some(work_tx),
            ready,
            pending_count,
            failed,
            metrics,
            manifest_sync_window,
            join_handle: Some(join_handle),
        }
    }

    pub fn take_failed(&self) -> bool {
        self.failed.swap(false, Ordering::AcqRel)
    }

    pub fn set_metrics(&self, metrics: Arc<crate::metrics::Metrics>) {
        *self.metrics.lock().expect("apply metrics lock") = Some(metrics);
    }

    pub fn submit(&self, apply: ReadyCatalogApply) -> EngineResult<()> {
        if apply.compaction.is_some() {
            crate::engine_fail_point!(crate::failpoints::COMPACTION_BEFORE_MANIFEST);
        }
        let pending = PendingCatalogApply {
            manifest_records: apply.manifest_records,
            add_metas: apply.add_metas,
            remove_sstable_ids: apply.remove_sstable_ids,
            unlink_wal_up_to: apply.unlink_wal_up_to,
            flush_max_seq: apply.flush_max_seq,
            compaction: apply.compaction,
        };
        self.pending_count.fetch_add(1, Ordering::Release);
        let tx = self
            .work_tx
            .as_ref()
            .ok_or_else(|| crate::errors::EngineError::Io("apply worker shut down".into()))?;
        tx.send(ApplyCommand::Run(pending))
            .map_err(|e| crate::errors::EngineError::Io(format!("apply queue: {e}")))?;
        Ok(())
    }

    pub fn drain_ready(&self) -> Vec<ReadyCatalogApply> {
        let mut guard = self.ready.lock().expect("apply ready lock");
        std::mem::take(&mut *guard)
    }

    pub fn pending_applies(&self) -> u64 {
        self.pending_count.load(Ordering::Acquire)
    }

    pub fn ready_count(&self) -> usize {
        self.ready.lock().expect("apply ready lock").len()
    }

    pub fn manifest_sync_window(&self) -> &Arc<ManifestSyncWindow> {
        &self.manifest_sync_window
    }

    pub fn shutdown(&mut self) {
        drop(self.work_tx.take());
        if let Some(h) = self.join_handle.take() {
            let _ = h.join();
        }
    }
}

fn apply_worker_loop(
    manifest_file: Arc<Mutex<File>>,
    work_rx: Receiver<ApplyCommand>,
    ready: Arc<Mutex<Vec<ReadyCatalogApply>>>,
    pending_count: Arc<AtomicU64>,
    failed: Arc<AtomicBool>,
    metrics: Arc<Mutex<Option<Arc<crate::metrics::Metrics>>>>,
    manifest_sync_window: Arc<ManifestSyncWindow>,
) {
    while let Ok(ApplyCommand::Run(p)) = work_rx.recv() {
        let metrics_ref = metrics.lock().expect("apply metrics lock").clone();
        if let Err(e) = durable_append(
            &manifest_file,
            &p.manifest_records,
            metrics_ref.as_deref(),
            &manifest_sync_window,
        ) {
            tracing::error!(error = %e, "apply worker manifest fsync failed");
            failed.store(true, Ordering::Release);
            pending_count.fetch_sub(1, Ordering::Release);
            continue;
        }
        if let Err(e) =
            crate::failpoints::failpoint_result(crate::failpoints::APPLY_AFTER_FSYNC_BEFORE_PUBLISH)
        {
            tracing::error!(error = %e, "apply worker publish aborted at failpoint");
            failed.store(true, Ordering::Release);
            pending_count.fetch_sub(1, Ordering::Release);
            continue;
        }
        let ready_apply = ReadyCatalogApply {
            manifest_records: p.manifest_records,
            add_metas: p.add_metas,
            remove_sstable_ids: p.remove_sstable_ids,
            unlink_wal_up_to: p.unlink_wal_up_to,
            flush_max_seq: p.flush_max_seq,
            compaction: p.compaction,
        };
        ready.lock().expect("apply ready lock").push(ready_apply);
        pending_count.fetch_sub(1, Ordering::Release);
    }
}

fn durable_append(
    manifest_file: &Arc<Mutex<File>>,
    recs: &[ManifestRecord],
    metrics: Option<&crate::metrics::Metrics>,
    manifest_sync_window: &ManifestSyncWindow,
) -> EngineResult<()> {
    if recs.is_empty() {
        return Ok(());
    }
    crate::engine_fail_point!(crate::failpoints::MANIFEST_BEFORE_APPEND);
    let mut buf = Vec::new();
    for rec in recs {
        buf.extend_from_slice(&rec.encode());
    }
    let mut file = manifest_file.lock().expect("manifest lock");
    file.write_all(&buf)?;
    let start = Instant::now();
    file.sync_all()?;
    crate::engine_fail_point!(crate::failpoints::MANIFEST_AFTER_FSYNC);
    let elapsed = start.elapsed();
    if let Some(m) = metrics {
        m.manifest_syncs_total.inc();
        m.manifest_sync_duration_seconds
            .observe(elapsed.as_secs_f64());
    }
    manifest_sync_window.record(elapsed);
    Ok(())
}
