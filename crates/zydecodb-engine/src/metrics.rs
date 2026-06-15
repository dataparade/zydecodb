//! Prometheus metrics registry for the engine.
//!
//! This module exposes **only core LSM-internal counters and gauges** — WAL
//! bytes, fsyncs, SSTable flushes, memtable size, segment counts, etc. The
//! engine does not know about callers, transports, identities, or per-route
//! breakdowns; any per-request, per-route, or front-end metric is the caller's
//! responsibility.
//!
//! The engine owns the [`prometheus::Registry`] and exposes it via
//! [`Metrics::registry`]. Embedders that want to add their own counters
//! (per-route request counts, identity cache stats, etc.) construct their
//! own `IntCounterVec`/`Histogram`/... and register them into the same
//! registry. The `/metrics` text rendering ([`Metrics::render`]) then emits
//! everything as a single Prometheus scrape.

use prometheus::{
    Histogram, HistogramOpts, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts, Registry,
};
use std::sync::Arc;

#[derive(Clone)]
pub struct Metrics {
    pub registry: Registry,

    // ---- WAL ----
    pub wal_bytes_written_total: IntCounter,
    /// Fsyncs issued by the group-commit coordinator. Compare against the
    /// caller's write counters to compute the coalescing ratio.
    pub wal_group_commit_syncs_total: IntCounter,
    pub wal_fsync_duration_seconds: Histogram,
    pub wal_segment_count: IntGauge,
    /// Bytes in the active (unsealed) WAL segment not yet shipped off-box.
    /// This is the recovery-point-objective surface: data lost if the box dies
    /// before the active segment seals. 0 when shipping is disabled.
    pub wal_unshipped_bytes: IntGauge,

    // ---- SSTable / memtable ----
    pub sstable_flushes_total: IntCounter,
    pub sstable_get_duration_seconds: Histogram,
    pub bloom_false_positives_total: IntCounter,
    pub memtable_size_bytes: IntGauge,
    pub immutable_memtable_count: IntGauge,
    pub live_sstable_count: IntGauge,
    /// Per-level live SSTable counts (label `level`).
    pub live_sstables_by_level: prometheus::IntGaugeVec,

    // ---- Compaction ----
    pub compaction_jobs_total: IntCounter,
    pub compaction_bytes_read_total: IntCounter,
    pub compaction_bytes_written_total: IntCounter,
    pub compaction_duration_seconds: Histogram,
    pub compaction_queue_depth: IntGauge,
    pub compaction_worker_busy: IntGauge,
    pub compaction_versions_dropped_total: IntCounter,
    pub compaction_tombstones_dropped_total: IntCounter,
    pub compaction_repack_total: IntCounter,
    pub compaction_rejected_no_progress: IntCounter,
    /// Compaction jobs applied, labeled by `input_level` (0, 1, 2, …).
    pub compaction_jobs_by_input_level: IntCounterVec,
    pub compaction_apply_duration_seconds: Histogram,
    pub manifest_syncs_total: IntCounter,
    pub manifest_sync_duration_seconds: Histogram,
    pub pending_compaction_bytes: IntGauge,
    pub user_bytes_written_total: IntCounter,

    // ---- Block cache ----
    pub block_cache_hits_total: IntCounter,
    pub block_cache_misses_total: IntCounter,
    pub block_cache_compaction_reads_total: IntCounter,
    pub block_cache_evictions_total: IntCounter,
    pub block_cache_resident_bytes: IntGauge,
    pub block_cache_resident_entries: IntGauge,

    // ---- Result (point lookup) cache ----
    pub result_cache_hits_total: IntCounter,
    pub result_cache_misses_total: IntCounter,
    pub result_cache_evictions_total: IntCounter,
    pub result_cache_resident_bytes: IntGauge,

    // ---- Disk accounting ----
    pub disk_bytes_total: IntGauge,
    pub logical_live_bytes: IntGauge,
    pub space_amplification: prometheus::Gauge,

    // ---- Misc engine state ----
    pub last_durable_seq: IntGauge,
    /// 1 if the engine booted from a clean shutdown marker (no WAL replay
    /// needed), 0 if it recovered from an unclean stop. Set once at open.
    pub last_shutdown_clean: IntGauge,
    #[allow(dead_code)]
    errors_total: IntCounterVec,
}

impl Metrics {
    pub fn new() -> Arc<Metrics> {
        let registry = Registry::new();
        let m = Self::build(registry);
        Arc::new(m)
    }

    /// Build a `Metrics` that registers into a pre-existing registry. Useful
    /// when an embedder wants to share one registry with its own counters.
    pub fn new_in(registry: Registry) -> Arc<Metrics> {
        Arc::new(Self::build(registry))
    }

    fn build(registry: Registry) -> Metrics {
        let wal_bytes_written_total = IntCounter::with_opts(Opts::new(
            "zydecodb_wal_bytes_written_total",
            "Total bytes written to the WAL",
        ))
        .unwrap();
        let wal_group_commit_syncs_total = IntCounter::with_opts(Opts::new(
            "zydecodb_wal_group_commit_syncs_total",
            "Fsyncs issued by the group-commit coordinator (writes per fsync = coalescing)",
        ))
        .unwrap();
        let wal_fsync_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "zydecodb_wal_fsync_duration_seconds",
            "WAL fsync duration",
        ))
        .unwrap();
        let wal_segment_count = IntGauge::with_opts(Opts::new(
            "zydecodb_wal_segment_count",
            "WAL segment files on disk",
        ))
        .unwrap();
        let wal_unshipped_bytes = IntGauge::with_opts(Opts::new(
            "zydecodb_wal_unshipped_bytes",
            "Bytes in the active WAL segment not yet shipped off-box (RPO surface)",
        ))
        .unwrap();

        let sstable_flushes_total = IntCounter::with_opts(Opts::new(
            "zydecodb_sstable_flushes_total",
            "Number of memtable->SSTable flushes",
        ))
        .unwrap();
        let sstable_get_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "zydecodb_sstable_get_duration_seconds",
            "SSTable GET duration",
        ))
        .unwrap();
        let bloom_false_positives_total = IntCounter::with_opts(Opts::new(
            "zydecodb_bloom_false_positives_total",
            "Bloom filter false positives",
        ))
        .unwrap();
        let memtable_size_bytes = IntGauge::with_opts(Opts::new(
            "zydecodb_memtable_size_bytes",
            "Active memtable size in bytes",
        ))
        .unwrap();
        let immutable_memtable_count = IntGauge::with_opts(Opts::new(
            "zydecodb_immutable_memtable_count",
            "Immutable memtables awaiting flush",
        ))
        .unwrap();
        let live_sstable_count = IntGauge::with_opts(Opts::new(
            "zydecodb_live_sstable_count",
            "Live SSTable files",
        ))
        .unwrap();
        let live_sstables_by_level = IntGaugeVec::new(
            Opts::new(
                "zydecodb_live_sstables_by_level",
                "Live SSTable count per LSM level",
            ),
            &["level"],
        )
        .unwrap();

        let compaction_jobs_total = IntCounter::with_opts(Opts::new(
            "zydecodb_compaction_jobs_total",
            "Compaction jobs executed",
        ))
        .unwrap();
        let compaction_bytes_read_total = IntCounter::with_opts(Opts::new(
            "zydecodb_compaction_bytes_read_total",
            "Total input bytes read by compaction (sum of input SSTable sizes)",
        ))
        .unwrap();
        let compaction_bytes_written_total = IntCounter::with_opts(Opts::new(
            "zydecodb_compaction_bytes_written_total",
            "Total output bytes written by compaction (sum of output SSTable sizes)",
        ))
        .unwrap();
        let compaction_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "zydecodb_compaction_duration_seconds",
            "Wall-clock duration of a single compaction job",
        ))
        .unwrap();
        let compaction_queue_depth = IntGauge::with_opts(Opts::new(
            "zydecodb_compaction_queue_depth",
            "Pending compaction work (worker busy + signaled need)",
        ))
        .unwrap();
        let compaction_worker_busy = IntGauge::with_opts(Opts::new(
            "zydecodb_compaction_worker_busy",
            "1 while the background compaction worker is executing a job",
        ))
        .unwrap();
        let compaction_versions_dropped_total = IntCounter::with_opts(Opts::new(
            "zydecodb_compaction_versions_dropped_total",
            "Superseded versions dropped during compaction GC",
        ))
        .unwrap();
        let compaction_tombstones_dropped_total = IntCounter::with_opts(Opts::new(
            "zydecodb_compaction_tombstones_dropped_total",
            "Tombstones dropped during compaction GC",
        ))
        .unwrap();
        let compaction_repack_total = IntCounter::with_opts(Opts::new(
            "zydecodb_compaction_repack_total",
            "Deprecated; whole-level repack removed (always 0)",
        ))
        .unwrap();
        let compaction_rejected_no_progress = IntCounter::with_opts(Opts::new(
            "zydecodb_compaction_rejected_no_progress_total",
            "Same-level compaction jobs rejected because they cannot reduce file count",
        ))
        .unwrap();
        let compaction_jobs_by_input_level = IntCounterVec::new(
            Opts::new(
                "zydecodb_compaction_jobs_by_input_level_total",
                "Compaction jobs applied by primary input level",
            ),
            &["input_level"],
        )
        .unwrap();
        let compaction_apply_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "zydecodb_compaction_apply_duration_seconds",
            "Wall-clock duration of compaction catalog/manifest apply on engine thread",
        ))
        .unwrap();
        let manifest_syncs_total = IntCounter::with_opts(Opts::new(
            "zydecodb_manifest_syncs_total",
            "Manifest fsyncs (group-committed; may cover multiple catalog appends)",
        ))
        .unwrap();
        let manifest_sync_duration_seconds = Histogram::with_opts(HistogramOpts::new(
            "zydecodb_manifest_sync_duration_seconds",
            "Manifest fsync duration",
        ))
        .unwrap();
        let pending_compaction_bytes = IntGauge::with_opts(Opts::new(
            "zydecodb_pending_compaction_bytes",
            "Estimated bytes waiting to be compacted",
        ))
        .unwrap();
        let user_bytes_written_total = IntCounter::with_opts(Opts::new(
            "zydecodb_user_bytes_written_total",
            "User value bytes written via PUT",
        ))
        .unwrap();

        let block_cache_hits_total = IntCounter::with_opts(Opts::new(
            "zydecodb_block_cache_hits_total",
            "SSTable block-cache hits",
        ))
        .unwrap();
        let block_cache_misses_total = IntCounter::with_opts(Opts::new(
            "zydecodb_block_cache_misses_total",
            "SSTable block-cache misses (forced a disk read)",
        ))
        .unwrap();
        let block_cache_compaction_reads_total = IntCounter::with_opts(Opts::new(
            "zydecodb_block_cache_compaction_reads_total",
            "Compaction block reads that bypass the user cache",
        ))
        .unwrap();
        let block_cache_evictions_total = IntCounter::with_opts(Opts::new(
            "zydecodb_block_cache_evictions_total",
            "SSTable block-cache evictions due to capacity pressure",
        ))
        .unwrap();
        let block_cache_resident_bytes = IntGauge::with_opts(Opts::new(
            "zydecodb_block_cache_resident_bytes",
            "Bytes currently resident in the SSTable block cache",
        ))
        .unwrap();
        let block_cache_resident_entries = IntGauge::with_opts(Opts::new(
            "zydecodb_block_cache_resident_entries",
            "Block entries currently resident in the SSTable block cache",
        ))
        .unwrap();

        let result_cache_hits_total = IntCounter::with_opts(Opts::new(
            "zydecodb_result_cache_hits_total",
            "Point-lookup result cache hits",
        ))
        .unwrap();
        let result_cache_misses_total = IntCounter::with_opts(Opts::new(
            "zydecodb_result_cache_misses_total",
            "Point-lookup result cache misses",
        ))
        .unwrap();
        let result_cache_evictions_total = IntCounter::with_opts(Opts::new(
            "zydecodb_result_cache_evictions_total",
            "Point-lookup result cache evictions",
        ))
        .unwrap();
        let result_cache_resident_bytes = IntGauge::with_opts(Opts::new(
            "zydecodb_result_cache_resident_bytes",
            "Bytes resident in the point-lookup result cache",
        ))
        .unwrap();

        let disk_bytes_total = IntGauge::with_opts(Opts::new(
            "zydecodb_disk_bytes_total",
            "Live SSTable bytes on disk",
        ))
        .unwrap();
        let logical_live_bytes = IntGauge::with_opts(Opts::new(
            "zydecodb_logical_live_bytes",
            "Logical live user-key bytes",
        ))
        .unwrap();
        let space_amplification = prometheus::Gauge::with_opts(Opts::new(
            "zydecodb_space_amplification",
            "Physical disk bytes / logical live bytes",
        ))
        .unwrap();

        let last_durable_seq = IntGauge::with_opts(Opts::new(
            "zydecodb_last_durable_seq",
            "Last durable sequence number",
        ))
        .unwrap();
        let last_shutdown_clean = IntGauge::with_opts(Opts::new(
            "zydecodb_last_shutdown_clean",
            "1 if last boot followed a clean shutdown (no WAL replay), else 0",
        ))
        .unwrap();
        let errors_total = IntCounterVec::new(
            Opts::new("zydecodb_errors_total", "Errors by status code"),
            &["code"],
        )
        .unwrap();

        registry
            .register(Box::new(wal_bytes_written_total.clone()))
            .unwrap();
        registry
            .register(Box::new(wal_group_commit_syncs_total.clone()))
            .unwrap();
        registry
            .register(Box::new(wal_fsync_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(wal_segment_count.clone()))
            .unwrap();
        registry
            .register(Box::new(wal_unshipped_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(sstable_flushes_total.clone()))
            .unwrap();
        registry
            .register(Box::new(sstable_get_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(bloom_false_positives_total.clone()))
            .unwrap();
        registry
            .register(Box::new(memtable_size_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(immutable_memtable_count.clone()))
            .unwrap();
        registry
            .register(Box::new(live_sstable_count.clone()))
            .unwrap();
        registry
            .register(Box::new(live_sstables_by_level.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_jobs_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_bytes_read_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_bytes_written_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_queue_depth.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_worker_busy.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_versions_dropped_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_tombstones_dropped_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_repack_total.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_rejected_no_progress.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_jobs_by_input_level.clone()))
            .unwrap();
        registry
            .register(Box::new(compaction_apply_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(manifest_syncs_total.clone()))
            .unwrap();
        registry
            .register(Box::new(manifest_sync_duration_seconds.clone()))
            .unwrap();
        registry
            .register(Box::new(pending_compaction_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(user_bytes_written_total.clone()))
            .unwrap();
        registry
            .register(Box::new(block_cache_hits_total.clone()))
            .unwrap();
        registry
            .register(Box::new(block_cache_misses_total.clone()))
            .unwrap();
        registry
            .register(Box::new(block_cache_compaction_reads_total.clone()))
            .unwrap();
        registry
            .register(Box::new(block_cache_evictions_total.clone()))
            .unwrap();
        registry
            .register(Box::new(block_cache_resident_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(block_cache_resident_entries.clone()))
            .unwrap();
        registry
            .register(Box::new(result_cache_hits_total.clone()))
            .unwrap();
        registry
            .register(Box::new(result_cache_misses_total.clone()))
            .unwrap();
        registry
            .register(Box::new(result_cache_evictions_total.clone()))
            .unwrap();
        registry
            .register(Box::new(result_cache_resident_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(disk_bytes_total.clone()))
            .unwrap();
        registry
            .register(Box::new(logical_live_bytes.clone()))
            .unwrap();
        registry
            .register(Box::new(space_amplification.clone()))
            .unwrap();
        registry
            .register(Box::new(last_durable_seq.clone()))
            .unwrap();
        registry
            .register(Box::new(last_shutdown_clean.clone()))
            .unwrap();
        registry.register(Box::new(errors_total.clone())).unwrap();

        Metrics {
            registry,
            wal_bytes_written_total,
            wal_group_commit_syncs_total,
            wal_fsync_duration_seconds,
            wal_segment_count,
            wal_unshipped_bytes,
            sstable_flushes_total,
            sstable_get_duration_seconds,
            bloom_false_positives_total,
            memtable_size_bytes,
            immutable_memtable_count,
            live_sstable_count,
            live_sstables_by_level,
            compaction_jobs_total,
            compaction_bytes_read_total,
            compaction_bytes_written_total,
            compaction_duration_seconds,
            compaction_queue_depth,
            compaction_worker_busy,
            compaction_versions_dropped_total,
            compaction_tombstones_dropped_total,
            compaction_repack_total,
            compaction_rejected_no_progress,
            compaction_jobs_by_input_level,
            compaction_apply_duration_seconds,
            manifest_syncs_total,
            manifest_sync_duration_seconds,
            pending_compaction_bytes,
            user_bytes_written_total,
            block_cache_hits_total,
            block_cache_misses_total,
            block_cache_compaction_reads_total,
            block_cache_evictions_total,
            block_cache_resident_bytes,
            block_cache_resident_entries,
            result_cache_hits_total,
            result_cache_misses_total,
            result_cache_evictions_total,
            result_cache_resident_bytes,
            disk_bytes_total,
            logical_live_bytes,
            space_amplification,
            last_durable_seq,
            last_shutdown_clean,
            errors_total,
        }
    }

    /// Render the registry in Prometheus text format. Includes any external
    /// counters that an embedder registered into the same registry.
    pub fn render(&self) -> String {
        use prometheus::Encoder;
        let encoder = prometheus::TextEncoder::new();
        let mut buf = Vec::new();
        let families = self.registry.gather();
        encoder.encode(&families, &mut buf).unwrap();
        String::from_utf8(buf).unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_contains_core_lsm_metric_names() {
        let m = Metrics::new();
        m.wal_bytes_written_total.inc_by(42);
        let out = m.render();
        assert!(out.contains("zydecodb_wal_bytes_written_total"));
        assert!(out.contains("zydecodb_sstable_flushes_total"));
        assert!(out.contains("zydecodb_memtable_size_bytes"));
    }

    #[test]
    fn external_counters_can_share_the_registry() {
        let m = Metrics::new();
        let custom =
            IntCounter::with_opts(Opts::new("embedder_custom_total", "from outside")).unwrap();
        m.registry.register(Box::new(custom.clone())).unwrap();
        custom.inc();
        assert!(m.render().contains("embedder_custom_total"));
    }
}
