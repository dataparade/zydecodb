//! Soak harness for `zydecodb-engine`.
//!
//! Drives a configurable PUT/GET/DEL workload against a real engine for N
//! hours, emitting structured metrics every minute. This is NOT a benchmark
//! (no "ops/sec at max throughput" claim). It answers operational questions
//! that only show up over time:
//!   - Does RSS stabilize, or does it grow without bound? (memory leak)
//!   - Do open FDs stabilize? (file handle leak)
//!   - Does the immutable memtable queue sit at the cap? (flush keeps up?)
//!   - Does SSTable count grow unbounded? (no compaction is a known gap)
//!   - Do p99 latencies tail-spike periodically? (flush stalls)
//!
//! Output: one JSON object per line on stdout (or `--metrics-out FILE`). One
//! line per minute, plus a final summary line tagged `"kind":"summary"`.
//!
//! Deliberately Linux-only: RSS comes from `/proc/self/status` and FD count
//! from listing `/proc/self/fd`. Soak is a deploy-target concern; the deploy
//! target is Linux.
//!
//! There are NO assertions in this binary. It just runs and reports. The
//! `scripts/analyze-soak.py` companion script applies ceilings after a baseline
//! run has produced steady-state numbers. Picking ceilings before then would
//! produce either a rubber stamp or a constant false alarm.

use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::metrics::Metrics;

// ---------------------------------------------------------------------------
// CLI

struct Args {
    data_dir: PathBuf,
    wal_dir: PathBuf,
    hours: f64,
    ops_per_sec: u32,
    put_pct: u8,
    get_pct: u8,
    scan_pct: u8,
    // implicit: del_pct = 100 - put_pct - get_pct - scan_pct
    scan_range_keys: u32,
    snapshot_every_secs: u64,
    hot_key_count: u32,
    cold_key_count: u32,
    hot_pct: u8,
    val_min: usize,
    val_max: usize,
    seed: u64,
    metrics_out: Option<PathBuf>,
    sample_every_secs: u64,
    /// Background compaction poll interval (ms). Default 50ms.
    /// 0 = legacy per-op poll before every operation.
    poll_compaction_ms: u64,
    /// SSTable block cache capacity for this soak run (default 640 MB).
    block_cache_mb: usize,
    /// Point-lookup result cache (default 0 = disabled; opt-in for scan workloads).
    result_cache_mb: usize,
    /// SSTable reader table-cache cap (default 128; 0 = unlimited).
    max_open_readers: usize,
}

impl Args {
    fn parse() -> Args {
        // Hand-rolled arg parsing — no `clap` dep needed for this binary. Each
        // flag has a sensible default tuned for a first-pass baseline run.
        let mut data_dir: Option<PathBuf> = None;
        let mut wal_dir: Option<PathBuf> = None;
        let mut hours: f64 = 24.0;
        let mut ops_per_sec: u32 = 1000;
        let mut put_pct: u8 = 70;
        let mut get_pct: u8 = 25;
        let mut scan_pct: u8 = 0;
        let mut scan_range_keys: u32 = 100;
        let mut snapshot_every_secs: u64 = 0;
        let mut hot_key_count: u32 = 200_000;
        let mut cold_key_count: u32 = 800_000;
        let mut hot_pct: u8 = 80;
        let mut val_min: usize = 64;
        let mut val_max: usize = 1024;
        let mut seed: u64 = 0xDEAD_BEEF_CAFE_BABE;
        let mut metrics_out: Option<PathBuf> = None;
        let mut sample_every_secs: u64 = 60;
        let mut poll_compaction_ms: u64 = 50;
        let mut block_cache_mb: usize = 640;
        let mut result_cache_mb: usize = 0;
        let mut max_open_readers: usize = 128;

        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let mut val = || {
                it.next()
                    .unwrap_or_else(|| panic!("missing value for {}", arg))
            };
            match arg.as_str() {
                "--data-dir" => data_dir = Some(PathBuf::from(val())),
                "--wal-dir" => wal_dir = Some(PathBuf::from(val())),
                "--hours" => hours = val().parse().expect("--hours: float"),
                "--ops-per-sec" => ops_per_sec = val().parse().expect("--ops-per-sec: u32"),
                "--put-pct" => put_pct = val().parse().expect("--put-pct: u8 0-100"),
                "--get-pct" => get_pct = val().parse().expect("--get-pct: u8 0-100"),
                "--scan-pct" => scan_pct = val().parse().expect("--scan-pct: u8 0-100"),
                "--scan-range-keys" => {
                    scan_range_keys = val().parse().expect("--scan-range-keys: u32")
                }
                "--snapshot-every-secs" => {
                    snapshot_every_secs =
                        val().parse().expect("--snapshot-every-secs: u64 (0 = off)")
                }
                "--hot-key-count" => hot_key_count = val().parse().expect("--hot-key-count: u32"),
                "--cold-key-count" => {
                    cold_key_count = val().parse().expect("--cold-key-count: u32")
                }
                "--hot-pct" => hot_pct = val().parse().expect("--hot-pct: u8 0-100"),
                "--val-min" => val_min = val().parse().expect("--val-min: usize"),
                "--val-max" => val_max = val().parse().expect("--val-max: usize"),
                "--seed" => seed = val().parse().expect("--seed: u64"),
                "--metrics-out" => metrics_out = Some(PathBuf::from(val())),
                "--sample-every-secs" => {
                    sample_every_secs = val().parse().expect("--sample-every-secs: u64")
                }
                "--poll-compaction-ms" => {
                    poll_compaction_ms = val().parse().expect("--poll-compaction-ms: u64")
                }
                "--block-cache-mb" => {
                    block_cache_mb = val().parse().expect("--block-cache-mb: usize")
                }
                "--result-cache-mb" => {
                    result_cache_mb = val().parse().expect("--result-cache-mb: usize")
                }
                "--max-open-readers" => {
                    max_open_readers = val().parse().expect("--max-open-readers: usize")
                }
                "-h" | "--help" => {
                    print_usage();
                    std::process::exit(0);
                }
                other => {
                    eprintln!("unknown arg: {}", other);
                    print_usage();
                    std::process::exit(2);
                }
            }
        }

        let data_dir = data_dir.expect("--data-dir is required");
        let wal_dir = wal_dir.unwrap_or_else(|| data_dir.join("wal"));
        assert!(
            put_pct as u16 + get_pct as u16 + scan_pct as u16 <= 100,
            "put-pct + get-pct + scan-pct must be <= 100 (rest is delete)"
        );
        assert!(val_min <= val_max, "--val-min must be <= --val-max");
        assert!(hot_pct <= 100, "--hot-pct must be 0-100");
        assert!(hours > 0.0, "--hours must be positive");

        Args {
            data_dir,
            wal_dir,
            hours,
            ops_per_sec,
            put_pct,
            get_pct,
            scan_pct,
            scan_range_keys,
            snapshot_every_secs,
            hot_key_count,
            cold_key_count,
            hot_pct,
            val_min,
            val_max,
            seed,
            metrics_out,
            sample_every_secs,
            poll_compaction_ms,
            block_cache_mb,
            result_cache_mb,
            max_open_readers,
        }
    }
}

fn print_usage() {
    eprintln!(
        "engine-soak — long-running stability harness for zydecodb-engine

USAGE:
    engine-soak --data-dir PATH [OPTIONS]

REQUIRED:
    --data-dir PATH         Engine data directory (will be created)

OPTIONS:
    --wal-dir PATH          WAL directory (default: <data-dir>/wal)
    --hours FLOAT           How long to run (default: 24.0)
    --ops-per-sec U32       Target throughput, token-bucket limited (default: 1000)
    --put-pct U8            Percent of ops that are PUT (default: 70)
    --get-pct U8            Percent of ops that are GET (default: 25)
    --scan-pct U8           Percent of ops that are range SCAN (default: 0)
                            Remainder (after PUT/GET/SCAN) is DEL.
    --scan-range-keys U32   Approx keys spanned per scan (default: 100)
    --snapshot-every-secs U64
                            Take a long-lived snapshot every N seconds and
                            spot-check a key; 0 disables (default: 0)
    --hot-key-count U32     Hot keyset size (default: 200000)
    --cold-key-count U32    Cold keyset size (default: 800000)
    --hot-pct U8            Percent of ops that target hot keys (default: 80)
    --val-min USIZE         Min value bytes (default: 64)
    --val-max USIZE         Max value bytes (default: 1024)
    --seed U64              RNG seed (default: 0xDEADBEEFCAFEBABE)
    --metrics-out PATH      Write JSONL metrics to FILE instead of stdout
    --sample-every-secs U64 Metrics sample interval (default: 60)
    --poll-compaction-ms U64
                            Background poll_compaction interval (default: 50,
                            production default). 0 = poll before every op.
    --block-cache-mb USIZE    SSTable block cache size (default: 640)
    --result-cache-mb USIZE   Point-lookup result cache (default: 0, opt-in)
    -h, --help              Print this help
"
    );
}

// ---------------------------------------------------------------------------
// RNG (seeded LCG — same one used in the determinism test for consistency)

struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed.max(1))
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.0
    }
    fn range_u32(&mut self, n: u32) -> u32 {
        (self.next_u64() >> 32) as u32 % n
    }
    fn range_usize(&mut self, lo: usize, hi_inclusive: usize) -> usize {
        if hi_inclusive <= lo {
            return lo;
        }
        let span = (hi_inclusive - lo + 1) as u32;
        lo + self.range_u32(span) as usize
    }
}

// ---------------------------------------------------------------------------
// Workload

enum Op {
    Put { key: Vec<u8>, val: Vec<u8> },
    Get { key: Vec<u8> },
    Del { key: Vec<u8> },
    Scan { lo: Vec<u8>, hi: Vec<u8> },
}

struct Workload<'a> {
    args: &'a Args,
    rng: Lcg,
}

impl<'a> Workload<'a> {
    fn new(args: &'a Args) -> Self {
        Workload {
            rng: Lcg::new(args.seed),
            args,
        }
    }

    fn pick_key(&mut self) -> Vec<u8> {
        // Hot/cold partition. Hot keys get hot_pct of traffic.
        let hot = self.rng.range_u32(100) < self.args.hot_pct as u32;
        let id = if hot {
            self.rng.range_u32(self.args.hot_key_count)
        } else {
            self.args.hot_key_count + self.rng.range_u32(self.args.cold_key_count)
        };
        // Compose: [KS_USER][8-byte big-endian id]. 9 bytes total, deterministic.
        let mut k = Vec::with_capacity(9);
        k.push(KS_USER);
        k.extend_from_slice(&(id as u64).to_be_bytes());
        k
    }

    fn pick_value(&mut self) -> Vec<u8> {
        let len = self.rng.range_usize(self.args.val_min, self.args.val_max);
        let mut v = vec![0u8; len];
        // Fill with pseudo-random bytes so SSTable compression (when added)
        // doesn't get an unfair zero-fill freebie.
        for chunk in v.chunks_mut(8) {
            let r = self.rng.next_u64().to_be_bytes();
            let n = chunk.len();
            chunk.copy_from_slice(&r[..n]);
        }
        v
    }

    fn next(&mut self) -> Op {
        let pick = self.rng.range_u32(100);
        let put_thr = self.args.put_pct as u32;
        let get_thr = put_thr + self.args.get_pct as u32;
        let scan_thr = get_thr + self.args.scan_pct as u32;
        if pick < put_thr {
            Op::Put {
                key: self.pick_key(),
                val: self.pick_value(),
            }
        } else if pick < get_thr {
            Op::Get {
                key: self.pick_key(),
            }
        } else if pick < scan_thr {
            // Scan a small range starting from a hot or cold key.
            let lo = self.pick_key();
            // hi = lo with the last byte bumped by `scan_range_keys` worth
            // of id space — because keys are `[KS_USER][big-endian u64 id]`,
            // adding to the u64 portion bounds the scan by approx that many
            // candidate ids (most of which won't exist; the engine handles
            // sparse ranges fine).
            let mut hi = lo.clone();
            if hi.len() == 9 {
                let mut id = u64::from_be_bytes(hi[1..9].try_into().unwrap());
                id = id.saturating_add(self.args.scan_range_keys as u64);
                hi[1..9].copy_from_slice(&id.to_be_bytes());
            }
            Op::Scan { lo, hi }
        } else {
            Op::Del {
                key: self.pick_key(),
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Latency histogram (simple reservoir-style — good enough for p50/p99/p999
// signal at minute granularity; no external dep)

struct LatencySampler {
    // Fixed-capacity reservoir; older samples evict when full. At
    // 1000 ops/sec and 60s window, capacity 8192 oversamples by ~7x which is
    // sufficient for stable p99 on a per-minute basis.
    samples_us: Vec<u64>,
    cap: usize,
    next_idx: usize,
    count: u64,
}

impl LatencySampler {
    fn new(cap: usize) -> Self {
        LatencySampler {
            samples_us: Vec::with_capacity(cap),
            cap,
            next_idx: 0,
            count: 0,
        }
    }

    fn record(&mut self, d: Duration) {
        let us = d.as_micros().min(u64::MAX as u128) as u64;
        if self.samples_us.len() < self.cap {
            self.samples_us.push(us);
        } else {
            self.samples_us[self.next_idx] = us;
            self.next_idx = (self.next_idx + 1) % self.cap;
        }
        self.count += 1;
    }

    fn percentile(&self, p: f64) -> u64 {
        if self.samples_us.is_empty() {
            return 0;
        }
        let mut sorted = self.samples_us.clone();
        sorted.sort_unstable();
        let idx = ((p / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[idx.min(sorted.len() - 1)]
    }

    fn reset_window(&mut self) {
        self.samples_us.clear();
        self.next_idx = 0;
        // Do NOT reset `count` — that's the lifetime op count.
    }
}

// ---------------------------------------------------------------------------
// /proc readers (Linux-only)

fn read_rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            // "VmRSS:    123456 kB"
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

fn count_open_fds() -> Option<usize> {
    let entries = std::fs::read_dir("/proc/self/fd").ok()?;
    Some(entries.count())
}

// ---------------------------------------------------------------------------
// Metrics emitter

struct Sample {
    elapsed_secs: u64,
    wall_unix_secs: u64,
    ops_done: u64,
    ops_per_sec_observed: f64,
    rss_bytes: u64,
    open_fds: usize,
    immutable_memtable_count: i64,
    live_sstable_count: i64,
    wal_segment_count: i64,
    wal_unshipped_bytes: i64,
    last_durable_seq: i64,
    p50_us: u64,
    p99_us: u64,
    p999_us: u64,
    max_us: u64,
    // Tier 1 additions.
    sstables_l0: i64,
    sstables_l1: i64,
    sstables_l2: i64,
    block_cache_hits: u64,
    block_cache_misses: u64,
    block_cache_resident_bytes: i64,
    compaction_jobs_total: u64,
    l2_median_size_bytes: u64,
    l2_max_size_bytes: u64,
    pending_compaction_bytes: u64,
    compaction_write_amp: f64,
    compaction_repack_total: u64,
    compaction_rejected_no_progress: u64,
    block_cache_hits_window: u64,
    block_cache_misses_window: u64,
    block_cache_hit_rate_window: f64,
    block_cache_compaction_reads_window: u64,
    disk_bytes_total: u64,
    logical_live_bytes: u64,
    space_amplification: f64,
    tombstones_dropped_window: u64,
    versions_dropped_window: u64,
    result_cache_hits_window: u64,
    result_cache_misses_window: u64,
    result_cache_hit_rate_window: f64,
    poll_max_us: u64,
    poll_mean_us: u64,
    apply_count_window: u64,
    apply_max_us: u64,
    apply_mean_us: u64,
    compaction_jobs_l0_window: u64,
    compaction_jobs_l1_window: u64,
    compaction_jobs_l2_window: u64,
    flushes_window: u64,
    manifest_sync_max_us: u64,
    errors_window: u64,
}

impl Sample {
    fn to_jsonl(&self) -> String {
        // Hand-rolled JSON — no serde_json dep needed for ~15 fields.
        format!(
            "{{\
\"kind\":\"sample\",\
\"elapsed_secs\":{},\
\"wall_unix_secs\":{},\
\"ops_done\":{},\
\"ops_per_sec_observed\":{:.2},\
\"rss_bytes\":{},\
\"open_fds\":{},\
\"immutable_memtable_count\":{},\
\"live_sstable_count\":{},\
\"wal_segment_count\":{},\
\"wal_unshipped_bytes\":{},\
\"last_durable_seq\":{},\
\"p50_us\":{},\
\"p99_us\":{},\
\"p999_us\":{},\
\"max_us\":{},\
\"sstables_l0\":{},\
\"sstables_l1\":{},\
\"sstables_l2\":{},\
\"block_cache_hits\":{},\
\"block_cache_misses\":{},\
\"block_cache_resident_bytes\":{},\
\"compaction_jobs_total\":{},\
\"l2_median_size_bytes\":{},\
\"l2_max_size_bytes\":{},\
\"pending_compaction_bytes\":{},\
\"compaction_write_amp\":{:.4},\
\"compaction_repack_total\":{},\
\"compaction_rejected_no_progress\":{},\
\"block_cache_hits_window\":{},\
\"block_cache_misses_window\":{},\
\"block_cache_hit_rate_window\":{:.4},\
\"block_cache_compaction_reads_window\":{},\
\"disk_bytes_total\":{},\
\"logical_live_bytes\":{},\
\"space_amplification\":{:.4},\
\"tombstones_dropped_window\":{},\
\"versions_dropped_window\":{},\
\"result_cache_hits_window\":{},\
\"result_cache_misses_window\":{},\
\"result_cache_hit_rate_window\":{:.4},\
\"poll_max_us\":{},\
\"poll_mean_us\":{},\
\"apply_count_window\":{},\
\"apply_max_us\":{},\
\"apply_mean_us\":{},\
\"compaction_jobs_l0_window\":{},\
\"compaction_jobs_l1_window\":{},\
\"compaction_jobs_l2_window\":{},\
\"flushes_window\":{},\
\"manifest_sync_max_us\":{},\
\"errors_window\":{}\
}}",
            self.elapsed_secs,
            self.wall_unix_secs,
            self.ops_done,
            self.ops_per_sec_observed,
            self.rss_bytes,
            self.open_fds,
            self.immutable_memtable_count,
            self.live_sstable_count,
            self.wal_segment_count,
            self.wal_unshipped_bytes,
            self.last_durable_seq,
            self.p50_us,
            self.p99_us,
            self.p999_us,
            self.max_us,
            self.sstables_l0,
            self.sstables_l1,
            self.sstables_l2,
            self.block_cache_hits,
            self.block_cache_misses,
            self.block_cache_resident_bytes,
            self.compaction_jobs_total,
            self.l2_median_size_bytes,
            self.l2_max_size_bytes,
            self.pending_compaction_bytes,
            self.compaction_write_amp,
            self.compaction_repack_total,
            self.compaction_rejected_no_progress,
            self.block_cache_hits_window,
            self.block_cache_misses_window,
            self.block_cache_hit_rate_window,
            self.block_cache_compaction_reads_window,
            self.disk_bytes_total,
            self.logical_live_bytes,
            self.space_amplification,
            self.tombstones_dropped_window,
            self.versions_dropped_window,
            self.result_cache_hits_window,
            self.result_cache_misses_window,
            self.result_cache_hit_rate_window,
            self.poll_max_us,
            self.poll_mean_us,
            self.apply_count_window,
            self.apply_max_us,
            self.apply_mean_us,
            self.compaction_jobs_l0_window,
            self.compaction_jobs_l1_window,
            self.compaction_jobs_l2_window,
            self.flushes_window,
            self.manifest_sync_max_us,
            self.errors_window,
        )
    }
}

struct WindowCounters {
    jobs_l0_prev: u64,
    jobs_l1_prev: u64,
    jobs_l2_prev: u64,
    flushes_prev: u64,
    cache_hits_prev: u64,
    cache_misses_prev: u64,
    compaction_reads_prev: u64,
    tombstones_dropped_prev: u64,
    versions_dropped_prev: u64,
    result_hits_prev: u64,
    result_misses_prev: u64,
    poll_sum_ns: u64,
    poll_max_ns: u64,
    ops_in_window: u64,
    errors_window: u64,
}

impl WindowCounters {
    fn new() -> Self {
        Self {
            jobs_l0_prev: 0,
            jobs_l1_prev: 0,
            jobs_l2_prev: 0,
            flushes_prev: 0,
            cache_hits_prev: 0,
            cache_misses_prev: 0,
            compaction_reads_prev: 0,
            tombstones_dropped_prev: 0,
            versions_dropped_prev: 0,
            result_hits_prev: 0,
            result_misses_prev: 0,
            poll_sum_ns: 0,
            poll_max_ns: 0,
            ops_in_window: 0,
            errors_window: 0,
        }
    }

    fn record_error(&mut self) {
        self.errors_window += 1;
    }

    fn drain_errors(&mut self) -> u64 {
        let n = self.errors_window;
        self.errors_window = 0;
        n
    }

    fn record_poll(&mut self, elapsed_ns: u64) {
        self.poll_sum_ns = self.poll_sum_ns.saturating_add(elapsed_ns);
        self.poll_max_ns = self.poll_max_ns.max(elapsed_ns);
    }

    fn record_op(&mut self) {
        self.ops_in_window += 1;
    }
}

fn job_count_at_level(metrics: &Metrics, level: u8) -> u64 {
    metrics
        .compaction_jobs_by_input_level
        .with_label_values(&[&level.to_string()])
        .get()
}

#[allow(clippy::too_many_arguments)] // soak sampler reads many independent counters
fn collect_sample(
    started: Instant,
    ops_done: &AtomicU64,
    ops_done_prev: &mut u64,
    last_sample_at: &mut Instant,
    engine: &zydecodb_engine::engine::Engine,
    metrics: &Metrics,
    latencies: &LatencySampler,
    window: &mut WindowCounters,
    sample_index: u64,
    last_space: &mut (u64, u64, f64),
) -> Sample {
    let now = Instant::now();
    let elapsed_secs = now.duration_since(started).as_secs();
    let ops_now = ops_done.load(Ordering::Relaxed);
    let window_secs = now.duration_since(*last_sample_at).as_secs_f64().max(0.001);
    let ops_per_sec_observed = (ops_now - *ops_done_prev) as f64 / window_secs;
    *ops_done_prev = ops_now;
    *last_sample_at = now;

    let (_l2_min, l2_median, l2_max) = engine.l2_file_size_stats();
    let user_bytes = metrics.user_bytes_written_total.get().max(1);
    let compaction_written = metrics.compaction_bytes_written_total.get();
    let write_amp = compaction_written as f64 / user_bytes as f64;

    let jobs_l0 = job_count_at_level(metrics, 0);
    let jobs_l1 = job_count_at_level(metrics, 1);
    let jobs_l2 = job_count_at_level(metrics, 2);
    let flushes = metrics.sstable_flushes_total.get();
    let jobs_l0_window = jobs_l0.saturating_sub(window.jobs_l0_prev);
    let jobs_l1_window = jobs_l1.saturating_sub(window.jobs_l1_prev);
    let jobs_l2_window = jobs_l2.saturating_sub(window.jobs_l2_prev);
    let flushes_window = flushes.saturating_sub(window.flushes_prev);
    window.jobs_l0_prev = jobs_l0;
    window.jobs_l1_prev = jobs_l1;
    window.jobs_l2_prev = jobs_l2;
    window.flushes_prev = flushes;

    let cache_hits = metrics.block_cache_hits_total.get();
    let cache_misses = metrics.block_cache_misses_total.get();
    let block_cache_hits_window = cache_hits.saturating_sub(window.cache_hits_prev);
    let block_cache_misses_window = cache_misses.saturating_sub(window.cache_misses_prev);
    window.cache_hits_prev = cache_hits;
    window.cache_misses_prev = cache_misses;
    let compaction_reads = metrics.block_cache_compaction_reads_total.get();
    let block_cache_compaction_reads_window =
        compaction_reads.saturating_sub(window.compaction_reads_prev);
    window.compaction_reads_prev = compaction_reads;

    let tombstones_total = metrics.compaction_tombstones_dropped_total.get();
    let versions_total = metrics.compaction_versions_dropped_total.get();
    let tombstones_dropped_window = tombstones_total.saturating_sub(window.tombstones_dropped_prev);
    let versions_dropped_window = versions_total.saturating_sub(window.versions_dropped_prev);
    window.tombstones_dropped_prev = tombstones_total;
    window.versions_dropped_prev = versions_total;

    let result_hits = metrics.result_cache_hits_total.get();
    let result_misses = metrics.result_cache_misses_total.get();
    let result_cache_hits_window = result_hits.saturating_sub(window.result_hits_prev);
    let result_cache_misses_window = result_misses.saturating_sub(window.result_misses_prev);
    window.result_hits_prev = result_hits;
    window.result_misses_prev = result_misses;
    let result_window_total = result_cache_hits_window + result_cache_misses_window;
    let result_cache_hit_rate_window = if result_window_total > 0 {
        result_cache_hits_window as f64 / result_window_total as f64
    } else {
        0.0
    };

    if sample_index == 1 || sample_index.is_multiple_of(10) {
        engine.refresh_space_metrics();
        *last_space = (
            engine.estimate_disk_bytes(),
            engine.estimate_logical_live_bytes().unwrap_or(1),
            engine.space_amplification().unwrap_or(1.0),
        );
    }
    let (disk_bytes_total, logical_live_bytes, space_amplification) = *last_space;

    let cache_window_total = block_cache_hits_window + block_cache_misses_window;
    let block_cache_hit_rate_window = if cache_window_total > 0 {
        block_cache_hits_window as f64 / cache_window_total as f64
    } else {
        0.0
    };

    let (apply_count, apply_sum_ns, apply_max_ns) = engine.drain_apply_window_stats();
    let (_manifest_sync_count, _manifest_sync_sum_ns, manifest_sync_max_ns) =
        engine.drain_manifest_sync_window_stats();
    let apply_mean_us = if apply_count > 0 {
        apply_sum_ns / apply_count / 1000
    } else {
        0
    };
    let poll_mean_us = if window.ops_in_window > 0 {
        window.poll_sum_ns / window.ops_in_window / 1000
    } else {
        0
    };
    let poll_max_us = window.poll_max_ns / 1000;
    let apply_max_us = apply_max_ns / 1000;

    window.poll_sum_ns = 0;
    window.poll_max_ns = 0;
    window.ops_in_window = 0;

    Sample {
        elapsed_secs,
        wall_unix_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0),
        ops_done: ops_now,
        ops_per_sec_observed,
        rss_bytes: read_rss_bytes().unwrap_or(0),
        open_fds: count_open_fds().unwrap_or(0),
        immutable_memtable_count: metrics.immutable_memtable_count.get(),
        live_sstable_count: metrics.live_sstable_count.get(),
        wal_segment_count: metrics.wal_segment_count.get(),
        wal_unshipped_bytes: metrics.wal_unshipped_bytes.get(),
        last_durable_seq: metrics.last_durable_seq.get(),
        p50_us: latencies.percentile(50.0),
        p99_us: latencies.percentile(99.0),
        p999_us: latencies.percentile(99.9),
        max_us: latencies.percentile(100.0),
        sstables_l0: metrics
            .live_sstables_by_level
            .with_label_values(&["0"])
            .get(),
        sstables_l1: metrics
            .live_sstables_by_level
            .with_label_values(&["1"])
            .get(),
        sstables_l2: metrics
            .live_sstables_by_level
            .with_label_values(&["2"])
            .get(),
        block_cache_hits: metrics.block_cache_hits_total.get(),
        block_cache_misses: metrics.block_cache_misses_total.get(),
        block_cache_resident_bytes: metrics.block_cache_resident_bytes.get(),
        compaction_jobs_total: metrics.compaction_jobs_total.get(),
        l2_median_size_bytes: l2_median,
        l2_max_size_bytes: l2_max,
        pending_compaction_bytes: metrics.pending_compaction_bytes.get() as u64,
        compaction_write_amp: write_amp,
        compaction_repack_total: metrics.compaction_repack_total.get(),
        compaction_rejected_no_progress: metrics.compaction_rejected_no_progress.get(),
        block_cache_hits_window,
        block_cache_misses_window,
        block_cache_hit_rate_window,
        block_cache_compaction_reads_window,
        disk_bytes_total,
        logical_live_bytes,
        space_amplification,
        tombstones_dropped_window,
        versions_dropped_window,
        result_cache_hits_window,
        result_cache_misses_window,
        result_cache_hit_rate_window,
        poll_max_us,
        poll_mean_us,
        apply_count_window: apply_count,
        apply_max_us,
        apply_mean_us,
        compaction_jobs_l0_window: jobs_l0_window,
        compaction_jobs_l1_window: jobs_l1_window,
        compaction_jobs_l2_window: jobs_l2_window,
        flushes_window,
        manifest_sync_max_us: manifest_sync_max_ns / 1000,
        errors_window: window.drain_errors(),
    }
}

// ---------------------------------------------------------------------------
// Token-bucket rate limiter

struct RateLimiter {
    interval_ns: u64,
    next_at: Instant,
}

impl RateLimiter {
    fn new(ops_per_sec: u32) -> Self {
        let interval_ns = if ops_per_sec == 0 {
            0
        } else {
            1_000_000_000u64 / ops_per_sec as u64
        };
        RateLimiter {
            interval_ns,
            next_at: Instant::now(),
        }
    }

    fn acquire(&mut self) {
        if self.interval_ns == 0 {
            return;
        }
        let now = Instant::now();
        if now < self.next_at {
            std::thread::sleep(self.next_at - now);
        }
        self.next_at += Duration::from_nanos(self.interval_ns);
        // Don't let `next_at` drift behind real-time if we've stalled — that
        // would cause a thundering catch-up burst.
        let now2 = Instant::now();
        if self.next_at < now2 {
            self.next_at = now2;
        }
    }
}

// ---------------------------------------------------------------------------
// Main
//
// Note: no Ctrl-C handler is installed. SIGINT/SIGTERM simply kill the process;
// that's a deliberate choice — the WAL is durable through the last group-commit
// fsync, so killing the harness mid-run is equivalent to a power-loss crash on
// a deployed engine. The next open will replay the WAL, which is exactly the
// behavior the soak exercises anyway. Use a `--hours` value to set a duration.

fn main() {
    let args = Args::parse();

    let mut out: Box<dyn Write> = match &args.metrics_out {
        Some(p) => Box::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .expect("open --metrics-out"),
        ),
        None => Box::new(std::io::stdout()),
    };

    // Header line so analyze scripts know which run produced this file.
    writeln!(
        out,
        "{{\"kind\":\"header\",\"hours\":{},\"ops_per_sec_target\":{},\"put_pct\":{},\"get_pct\":{},\"hot_pct\":{},\"val_min\":{},\"val_max\":{},\"seed\":{},\"sample_every_secs\":{},\"poll_compaction_ms\":{},\"block_cache_mb\":{},\"result_cache_mb\":{},\"max_open_readers\":{}}}",
        args.hours,
        args.ops_per_sec,
        args.put_pct,
        args.get_pct,
        args.hot_pct,
        args.val_min,
        args.val_max,
        args.seed,
        args.sample_every_secs,
        args.poll_compaction_ms,
        args.block_cache_mb,
        args.result_cache_mb,
        args.max_open_readers,
    )
    .ok();
    out.flush().ok();

    let metrics = Metrics::new();
    let mut engine = Engine::open(EngineConfig {
        data_dir: args.data_dir.clone(),
        wal_dir: args.wal_dir.clone(),
        block_cache_bytes: args.block_cache_mb.saturating_mul(1024 * 1024),
        result_cache_bytes: args.result_cache_mb.saturating_mul(1024 * 1024),
        max_open_readers: args.max_open_readers,
        ..Default::default()
    })
    .expect("Engine::open")
    .with_metrics(metrics.clone());

    let mut workload = Workload::new(&args);
    let mut latencies = LatencySampler::new(8192);
    let mut rate = RateLimiter::new(args.ops_per_sec);

    let ops_done = Arc::new(AtomicU64::new(0));

    let started = Instant::now();
    let deadline = started + Duration::from_secs((args.hours * 3600.0) as u64);
    let sample_interval = Duration::from_secs(args.sample_every_secs);
    let snapshot_interval = if args.snapshot_every_secs == 0 {
        None
    } else {
        Some(Duration::from_secs(args.snapshot_every_secs))
    };
    let mut last_sample_at = started;
    let mut last_snapshot_at = started;
    let mut ops_done_prev: u64 = 0;
    let mut errors: u64 = 0;

    // Assigned only when a snapshot interval fires; left uninitialized so the
    // dead `None` does not trip the unused-assignment lint.
    let mut held_snapshot: Option<zydecodb_engine::SnapshotHandle>;
    let mut window = WindowCounters::new();
    let mut sample_index: u64 = 0;
    let mut last_space = (0u64, 1u64, 1.0f64);
    let poll_every_op = args.poll_compaction_ms == 0;
    let poll_interval = Duration::from_millis(args.poll_compaction_ms.max(1));
    let mut last_poll_at = started;
    while Instant::now() < deadline {
        rate.acquire();
        if poll_every_op || last_poll_at.elapsed() >= poll_interval {
            let poll_start = Instant::now();
            let _ = engine.poll_compaction();
            window.record_poll(poll_start.elapsed().as_nanos() as u64);
            last_poll_at = Instant::now();
        }

        let op = workload.next();
        let t0 = Instant::now();
        let res = match op {
            Op::Put { key, val } => engine.put(key, val, 0).map(|_| ()),
            Op::Get { key } => engine.get(&key).map(|_| ()),
            Op::Del { key } => engine.del(key).map(|_| ()),
            Op::Scan { lo, hi } => match engine.scan(lo, hi) {
                Ok(it) => {
                    // Drain the iterator so the cost is realistic. Bound
                    // the read so a pathological range can't dominate
                    // the harness's ops/sec budget.
                    let mut consumed = 0u32;
                    let mut last_err: Option<zydecodb_engine::errors::EngineError> = None;
                    for item in it {
                        if let Err(e) = item {
                            last_err = Some(e);
                            break;
                        }
                        consumed += 1;
                        if consumed >= 10_000 {
                            break;
                        }
                    }
                    match last_err {
                        Some(e) => Err(e),
                        None => Ok(()),
                    }
                }
                Err(e) => Err(e),
            },
        };
        latencies.record(t0.elapsed());

        if let Some(interval) = snapshot_interval {
            if last_snapshot_at.elapsed() >= interval {
                held_snapshot = Some(engine.snapshot_owned());
                if let Some(snap) = &held_snapshot {
                    let mut probe = Vec::with_capacity(9);
                    probe.push(KS_USER);
                    probe.extend_from_slice(&0u64.to_be_bytes());
                    let _ = snap.get(&probe);
                }
                last_snapshot_at = Instant::now();
            }
        }

        match res {
            Ok(()) => {
                ops_done.fetch_add(1, Ordering::Relaxed);
                window.record_op();
            }
            Err(e) => {
                errors += 1;
                window.record_error();
                // Don't spam every error to stdout — emit one per sample
                // window via the summary line below. The first 5 get logged
                // verbosely as breadcrumbs.
                if errors <= 5 {
                    eprintln!("op error #{}: {}", errors, e);
                }
            }
        }

        // Emit a sample line every `sample_every_secs`.
        if last_sample_at.elapsed() >= sample_interval {
            sample_index += 1;
            let s = collect_sample(
                started,
                &ops_done,
                &mut ops_done_prev,
                &mut last_sample_at,
                &engine,
                &metrics,
                &latencies,
                &mut window,
                sample_index,
                &mut last_space,
            );
            writeln!(out, "{}", s.to_jsonl()).ok();
            out.flush().ok();
            latencies.reset_window();
        }
    }

    // Drain any completed background work before shutdown.
    let _ = engine.poll_compaction();

    // Final shutdown + summary.
    let shutdown_start = Instant::now();
    let shutdown_result = engine.shutdown();
    let shutdown_secs = shutdown_start.elapsed().as_secs_f64();

    let total_secs = started.elapsed().as_secs_f64();
    let total_ops = ops_done.load(Ordering::Relaxed);
    let avg_ops_per_sec = total_ops as f64 / total_secs.max(0.001);

    writeln!(
        out,
        "{{\"kind\":\"summary\",\"total_secs\":{:.2},\"total_ops\":{},\"avg_ops_per_sec\":{:.2},\"errors\":{},\"shutdown_secs\":{:.3},\"shutdown_ok\":{},\"final_rss_bytes\":{},\"final_open_fds\":{},\"final_live_sstable_count\":{},\"final_wal_segment_count\":{}}}",
        total_secs,
        total_ops,
        avg_ops_per_sec,
        errors,
        shutdown_secs,
        shutdown_result.is_ok(),
        read_rss_bytes().unwrap_or(0),
        count_open_fds().unwrap_or(0),
        metrics.live_sstable_count.get(),
        metrics.wal_segment_count.get(),
    )
    .ok();
    out.flush().ok();

    if let Err(e) = shutdown_result {
        eprintln!("engine.shutdown failed: {}", e);
        std::process::exit(1);
    }
}
