//! Multi-tenant adversarial isolation soak (simulated pods).
//!
//! Two tenants on one engine (no real fleet required):
//!   V (victim): steady 4 KiB writes + point reads, **or** idle→reclaim burst
//!   N (noisy):  write flood + cache-thrash reads over a large keyspace
//!
//! Steady phases (separate data dirs):
//!   1. V solo          — baseline e2e put p99
//!   2. V|N fair=off    — shared-fate delta
//!   3. V|N fair=on     — δ-fair delta (ship bar: ≤50 ms)
//!
//! Ramp-up phases (FairDB reclaim — the hard δ):
//!   N floods while V is idle, then V bursts to reclaim ~fair-share bytes.
//!   Gate: fair-on reclaim p99 δ ≤ `--rampup-fail-delta-ms` (default 350).
//!
//! Latency is **client-visible**: victim retries `EngineBusy` until success or
//! `--retry-budget-ms`. A background poller drains flush/compaction under the
//! write lock.
//!
//! ```text
//! cargo run -p zydecodb-engine --bin tenant-isolation-soak --release -- \
//!   --data-root /tmp/zydeco-tenant-soak --mode both --seconds 20
//! ```

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::engine_handle::EngineHandle;
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::tenant_fair::FairConfig;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Steady,
    Rampup,
    Both,
}

struct Args {
    data_root: PathBuf,
    mode: Mode,
    seconds: u64,
    victim_ops_per_sec: u32,
    noisy_writers: u32,
    noisy_readers: u32,
    memtable_mb: usize,
    block_cache_mb: usize,
    retry_budget_ms: u64,
    fail_delta_ms: f64,
    ship_delta_ms: f64,
    rampup_idle_secs: u64,
    rampup_fail_delta_ms: f64,
    min_success_ratio: f64,
    skip_fair_off: bool,
    json_out: Option<PathBuf>,
}

impl Args {
    fn parse() -> Self {
        let mut data_root = PathBuf::from("/tmp/zydeco-tenant-soak");
        let mut mode = Mode::Both;
        let mut seconds = 30u64;
        let mut victim_ops_per_sec = 50u32;
        let mut noisy_writers = 1u32;
        let mut noisy_readers = 1u32;
        let mut memtable_mb = 8usize;
        let mut block_cache_mb = 16usize;
        let mut retry_budget_ms = 500u64;
        let mut fail_delta_ms = 50.0;
        let mut ship_delta_ms = 50.0;
        let mut rampup_idle_secs = 10u64;
        let mut rampup_fail_delta_ms = 350.0;
        let mut min_success_ratio = 0.85;
        let mut skip_fair_off = false;
        let mut json_out = None;
        let mut it = std::env::args().skip(1);
        while let Some(a) = it.next() {
            let mut val = || it.next().unwrap_or_else(|| panic!("missing value for {a}"));
            match a.as_str() {
                "--data-root" => data_root = PathBuf::from(val()),
                "--mode" => {
                    mode = match val().as_str() {
                        "steady" => Mode::Steady,
                        "rampup" => Mode::Rampup,
                        "both" => Mode::Both,
                        other => panic!("--mode must be steady|rampup|both, got {other}"),
                    }
                }
                "--seconds" => seconds = val().parse().expect("--seconds"),
                "--victim-ops-per-sec" => {
                    victim_ops_per_sec = val().parse().expect("--victim-ops-per-sec")
                }
                "--noisy-writers" => noisy_writers = val().parse().expect("--noisy-writers"),
                "--noisy-readers" => noisy_readers = val().parse().expect("--noisy-readers"),
                "--memtable-mb" => memtable_mb = val().parse().expect("--memtable-mb"),
                "--block-cache-mb" => block_cache_mb = val().parse().expect("--block-cache-mb"),
                "--retry-budget-ms" => retry_budget_ms = val().parse().expect("--retry-budget-ms"),
                "--fail-delta-ms" => fail_delta_ms = val().parse().expect("--fail-delta-ms"),
                "--ship-delta-ms" => ship_delta_ms = val().parse().expect("--ship-delta-ms"),
                "--rampup-idle-secs" => {
                    rampup_idle_secs = val().parse().expect("--rampup-idle-secs")
                }
                "--rampup-fail-delta-ms" => {
                    rampup_fail_delta_ms = val().parse().expect("--rampup-fail-delta-ms")
                }
                "--min-success-ratio" => {
                    min_success_ratio = val().parse().expect("--min-success-ratio")
                }
                "--skip-fair-off" => skip_fair_off = true,
                "--json-out" => json_out = Some(PathBuf::from(val())),
                "--help" | "-h" => {
                    print_help();
                    std::process::exit(0);
                }
                other => panic!("unknown arg {other}"),
            }
        }
        Args {
            data_root,
            mode,
            seconds,
            victim_ops_per_sec,
            noisy_writers,
            noisy_readers,
            memtable_mb,
            block_cache_mb,
            retry_budget_ms,
            fail_delta_ms,
            ship_delta_ms,
            rampup_idle_secs,
            rampup_fail_delta_ms,
            min_success_ratio,
            skip_fair_off,
            json_out,
        }
    }

    /// Victim reclaim batch ≈ one fair share of the write buffer (4 KiB puts).
    fn reclaim_puts(&self) -> u64 {
        let fair_share = (self.memtable_mb.saturating_mul(1024 * 1024) / 2) as u64;
        (fair_share / 4096).max(64)
    }
}

fn print_help() {
    eprintln!(
        "tenant-isolation-soak — simulated pods noisy-neighbor δ measurement\n\
         \n\
         --data-root DIR           (default /tmp/zydeco-tenant-soak)\n\
         --mode steady|rampup|both (default both)\n\
         --seconds N               steady per-phase duration (default 30)\n\
         --victim-ops-per-sec N    steady victim rate (default 50)\n\
         --noisy-writers N         (default 1)\n\
         --noisy-readers N         (default 1)\n\
         --memtable-mb N           flush threshold = fair budget (default 8)\n\
         --block-cache-mb N        (default 16)\n\
         --retry-budget-ms N       e2e wait for Busy retries (default 500)\n\
         --fail-delta-ms N         fail if steady fair-on δ exceeds (default 50)\n\
         --ship-delta-ms N         report-only steady ship target (default 50)\n\
         --rampup-idle-secs N      noisy floods while V idle (default 10)\n\
         --rampup-fail-delta-ms N  fail if ramp-up fair-on δ exceeds (default 350)\n\
         --min-success-ratio F     put success floor (default 0.85)\n\
         --skip-fair-off           only run solo + fair-on\n\
         --json-out FILE           write summary JSON"
    );
}

fn tenant_key(tenant: u8, i: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 16 + 8);
    k.push(KS_USER);
    k.extend_from_slice(&[tenant; 16]);
    k.extend_from_slice(&i.to_be_bytes());
    k
}

fn percentile_us(samples: &mut [u64], p: f64) -> f64 {
    if samples.is_empty() {
        return 0.0;
    }
    samples.sort_unstable();
    let idx = ((samples.len() as f64 - 1.0) * p).round() as usize;
    samples[idx.min(samples.len() - 1)] as f64
}

fn busy_class(err: &str) -> &'static str {
    let e = err.to_ascii_lowercase();
    if e.contains("pacing over-share") {
        "fair_pace"
    } else if e.contains("fair memtable") {
        "fair_memtable"
    } else if e.contains("flush queue") {
        "flush_queue"
    } else if e.contains("l0 compaction") {
        "l0_stall"
    } else if e.contains("compaction backlog") {
        "compaction_backlog"
    } else if e.contains("wal in-flight") {
        "wal_inflight"
    } else if e.contains("writes frozen") {
        "frozen"
    } else {
        "other"
    }
}

#[derive(Clone, Default)]
struct LatAccum {
    /// End-to-end put latency including Busy retries (or budget exhaustion).
    put_e2e_us: Vec<u64>,
    mutex_wait_us: Vec<u64>,
    put_inside_us: Vec<u64>,
    busy_retry_us: Vec<u64>,
    get_us: Vec<u64>,
    put_ok: u64,
    put_timeout: u64,
    put_attempts: u64,
    busy_reasons: HashMap<&'static str, u64>,
}

impl LatAccum {
    fn note_busy(&mut self, err: &str) {
        *self.busy_reasons.entry(busy_class(err)).or_insert(0) += 1;
    }

    fn summary(&mut self) -> PhaseSummary {
        let put_busy: u64 = self.busy_reasons.values().sum();
        PhaseSummary {
            put_p99_ms: percentile_us(&mut self.put_e2e_us, 0.99) / 1000.0,
            put_p50_ms: percentile_us(&mut self.put_e2e_us, 0.50) / 1000.0,
            mutex_p99_ms: percentile_us(&mut self.mutex_wait_us, 0.99) / 1000.0,
            put_inside_p99_ms: percentile_us(&mut self.put_inside_us, 0.99) / 1000.0,
            busy_retry_p99_ms: percentile_us(&mut self.busy_retry_us, 0.99) / 1000.0,
            get_p99_ms: percentile_us(&mut self.get_us, 0.99) / 1000.0,
            put_ok: self.put_ok,
            put_timeout: self.put_timeout,
            put_attempts: self.put_attempts,
            put_busy_events: put_busy,
            busy_reasons: self.busy_reasons.clone(),
        }
    }
}

#[derive(Clone, Debug)]
struct PhaseSummary {
    put_p99_ms: f64,
    put_p50_ms: f64,
    mutex_p99_ms: f64,
    put_inside_p99_ms: f64,
    busy_retry_p99_ms: f64,
    get_p99_ms: f64,
    put_ok: u64,
    put_timeout: u64,
    put_attempts: u64,
    put_busy_events: u64,
    busy_reasons: HashMap<&'static str, u64>,
}

impl PhaseSummary {
    fn success_ratio(&self) -> f64 {
        let n = self.put_ok + self.put_timeout;
        if n == 0 {
            0.0
        } else {
            self.put_ok as f64 / n as f64
        }
    }
}

fn fair_config(enabled: bool, memtable_bytes: u64, cache_bytes: u64) -> FairConfig {
    let mut fair = FairConfig::default();
    fair.enabled = enabled;
    fair.tenant_count = 2;
    fair.memtable_total_bytes = memtable_bytes;
    fair.cache_total_bytes = cache_bytes;
    fair.delta_buffer = Duration::from_millis(350);
    fair.delta_cache = Duration::from_millis(250);
    fair.delta_steady = Duration::from_millis(50);
    fair.ramp_up_k = 2;
    fair.flush_bandwidth_bytes_per_sec = 64 * 1024 * 1024;
    fair.l0_token_budget = 64 * 1024 * 1024;
    fair
}

fn open_handle(dir: &Path, args: &Args, fair_on: bool) -> Arc<EngineHandle> {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let memtable_bytes = args.memtable_mb.saturating_mul(1024 * 1024);
    let cache_bytes = args.block_cache_mb.saturating_mul(1024 * 1024);
    let engine = Engine::open(EngineConfig {
        data_dir: dir.join("data"),
        wal_dir: dir.join("wal"),
        memtable_flush_threshold: memtable_bytes,
        // Room for noisy freezes while the poller drains — still finite shared fate.
        max_immutable_memtables: 8,
        block_cache_bytes: cache_bytes,
        fair: fair_config(fair_on, memtable_bytes as u64, cache_bytes as u64),
        ..Default::default()
    })
    .expect("engine open");
    EngineHandle::new(engine)
}

/// Drain flush/compaction so the soak is not a pure flush-queue deadlock.
fn run_maintainer(handle: Arc<EngineHandle>, stop: Arc<AtomicBool>) {
    while !stop.load(Ordering::Relaxed) {
        {
            let mut g = handle.write();
            let _ = g.poll_compaction();
        }
        thread::sleep(Duration::from_millis(2));
    }
}

/// Point get: brief write lock for snapshot capture, then I/O off-lock.
fn snapshot_get(handle: &EngineHandle, key: &[u8]) -> bool {
    let snap = handle.write().snapshot_owned();
    snap.get(key).is_ok()
}

/// Victim: paced puts with Busy retries (e2e δ) + point gets off write I/O.
fn run_victim(
    handle: Arc<EngineHandle>,
    stop: Arc<AtomicBool>,
    ops_per_sec: u32,
    working_set: u64,
    retry_budget: Duration,
) -> LatAccum {
    let mut acc = LatAccum::default();
    let interval = Duration::from_secs_f64(1.0 / ops_per_sec.max(1) as f64);
    let mut i = 0u64;
    let mut next = Instant::now();
    while !stop.load(Ordering::Relaxed) {
        let now = Instant::now();
        if now < next {
            thread::sleep(next - now);
        }
        next += interval;

        let key = tenant_key(1, i % working_set);
        let t0 = Instant::now();
        let mut ok = false;
        let mut mutex_sum = 0u64;
        let mut inside_sum = 0u64;
        let mut busy_sum = 0u64;
        while t0.elapsed() < retry_budget {
            if stop.load(Ordering::Relaxed) {
                break;
            }
            acc.put_attempts += 1;
            let t_lock = Instant::now();
            let mut guard = handle.write();
            mutex_sum += t_lock.elapsed().as_micros() as u64;
            let t_put = Instant::now();
            let result = guard.put(key.clone(), vec![0u8; 4096], 0);
            let slowdown = guard.take_write_slowdown();
            inside_sum += t_put.elapsed().as_micros() as u64;
            drop(guard);
            if !slowdown.is_zero() {
                thread::sleep(slowdown);
            }
            match result {
                Ok(_) => {
                    ok = true;
                    break;
                }
                Err(e) => {
                    acc.note_busy(&e.to_string());
                    let t_busy = Instant::now();
                    thread::sleep(Duration::from_micros(200));
                    busy_sum += t_busy.elapsed().as_micros() as u64;
                }
            }
        }
        acc.put_e2e_us.push(t0.elapsed().as_micros() as u64);
        acc.mutex_wait_us.push(mutex_sum);
        acc.put_inside_us.push(inside_sum);
        acc.busy_retry_us.push(busy_sum);
        if ok {
            acc.put_ok += 1;
        } else {
            acc.put_timeout += 1;
        }

        let gk = tenant_key(1, (i.wrapping_mul(7)) % working_set);
        let t1 = Instant::now();
        if snapshot_get(&handle, &gk) {
            acc.get_us.push(t1.elapsed().as_micros() as u64);
        }
        i = i.wrapping_add(1);
    }
    acc
}

fn run_noisy_writer(handle: Arc<EngineHandle>, stop: Arc<AtomicBool>, start_id: u64) {
    let mut i = start_id;
    while !stop.load(Ordering::Relaxed) {
        let mut guard = handle.write();
        let result = guard.put(tenant_key(2, i), vec![0u8; 8192], 0);
        let slowdown = guard.take_write_slowdown();
        drop(guard);
        if !slowdown.is_zero() {
            thread::sleep(slowdown);
        }
        match result {
            Ok(_) => {
                i = i.wrapping_add(1);
                thread::yield_now();
            }
            Err(_) => {
                // Back off so maintainer + victim can take the write lock.
                thread::sleep(Duration::from_micros(500));
            }
        }
    }
}

fn run_noisy_reader(handle: Arc<EngineHandle>, stop: Arc<AtomicBool>, keyspace: u64, seed: u64) {
    let mut x = seed | 1;
    while !stop.load(Ordering::Relaxed) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        let k = tenant_key(2, x % keyspace.max(1));
        let _ = snapshot_get(&handle, &k);
        thread::yield_now();
    }
}

/// FairDB ramp-up: burst-reclaim ~fair-share bytes as fast as possible.
fn run_victim_reclaim(
    handle: Arc<EngineHandle>,
    reclaim_puts: u64,
    retry_budget: Duration,
) -> LatAccum {
    let mut acc = LatAccum::default();
    for i in 0..reclaim_puts {
        let key = tenant_key(1, 10_000_000 + i);
        let t0 = Instant::now();
        let mut ok = false;
        let mut mutex_sum = 0u64;
        let mut inside_sum = 0u64;
        let mut busy_sum = 0u64;
        while t0.elapsed() < retry_budget {
            acc.put_attempts += 1;
            let t_lock = Instant::now();
            let mut guard = handle.write();
            mutex_sum += t_lock.elapsed().as_micros() as u64;
            let t_put = Instant::now();
            let result = guard.put(key.clone(), vec![0u8; 4096], 0);
            let slowdown = guard.take_write_slowdown();
            inside_sum += t_put.elapsed().as_micros() as u64;
            drop(guard);
            if !slowdown.is_zero() {
                thread::sleep(slowdown);
            }
            match result {
                Ok(_) => {
                    ok = true;
                    break;
                }
                Err(e) => {
                    acc.note_busy(&e.to_string());
                    let t_busy = Instant::now();
                    thread::sleep(Duration::from_micros(200));
                    busy_sum += t_busy.elapsed().as_micros() as u64;
                }
            }
        }
        acc.put_e2e_us.push(t0.elapsed().as_micros() as u64);
        acc.mutex_wait_us.push(mutex_sum);
        acc.put_inside_us.push(inside_sum);
        acc.busy_retry_us.push(busy_sum);
        if ok {
            acc.put_ok += 1;
        } else {
            acc.put_timeout += 1;
        }
    }
    acc
}

struct PhaseResult {
    summary: PhaseSummary,
    elapsed_s: f64,
}

fn print_phase_summary(summary: &PhaseSummary) {
    let mut reasons: Vec<_> = summary.busy_reasons.iter().collect();
    reasons.sort_by_key(|(k, _)| *k);
    let reason_s = reasons
        .iter()
        .map(|(k, v)| format!("{k}={v}"))
        .collect::<Vec<_>>()
        .join(" ");
    eprintln!(
        "  put_p99={:.2}ms put_p50={:.2}ms mutex_p99={:.2}ms inside_p99={:.2}ms busy_retry_p99={:.2}ms get_p99={:.2}ms ok={} timeout={} success={:.1}% busy_events={} {}",
        summary.put_p99_ms,
        summary.put_p50_ms,
        summary.mutex_p99_ms,
        summary.put_inside_p99_ms,
        summary.busy_retry_p99_ms,
        summary.get_p99_ms,
        summary.put_ok,
        summary.put_timeout,
        summary.success_ratio() * 100.0,
        summary.put_busy_events,
        reason_s
    );
}

struct RampupPhaseResult {
    summary: PhaseSummary,
    /// Wall time for the entire reclaim batch (FairDB-style reclaim delay).
    reclaim_wall_ms: f64,
    elapsed_s: f64,
}

/// Drive noisy puts until we see EngineBusy (write buffer / stall pressure),
/// so reclaim does not start against an empty pool.
fn fill_until_busy(handle: &EngineHandle, max_attempts: u64) -> bool {
    for i in 0..max_attempts {
        match handle
            .write()
            .put(tenant_key(2, 50_000_000 + i), vec![0u8; 8192], 0)
        {
            Ok(_) => {}
            Err(_) => return true,
        }
    }
    false
}

/// Idle while noisy floods, force buffer pressure, then victim reclaim burst.
fn run_rampup_phase(
    name: &str,
    dir: &Path,
    args: &Args,
    fair_on: bool,
    with_noise: bool,
) -> RampupPhaseResult {
    let reclaim_puts = args.reclaim_puts();
    eprintln!(
        "=== rampup {name} fair={} noise={} idle={}s reclaim_puts={} (≈fair share) ===",
        fair_on, with_noise, args.rampup_idle_secs, reclaim_puts
    );
    let handle = open_handle(dir, args, fair_on);

    if with_noise {
        let mut eng = handle.write();
        for i in 0..1_000u64 {
            let _ = eng.put(tenant_key(2, i), vec![0u8; 8192], 0);
        }
        let _ = eng.force_flush();
    }

    let stop = Arc::new(AtomicBool::new(false));
    // Maintainer starts only after pressure fill for noisy phases, so idle
    // flooding can actually pin the write buffer (FairDB-style).
    let start_maintainer = Arc::new(AtomicBool::new(!with_noise));
    let n_workers = 1 // maintainer gate thread
        + if with_noise {
            (args.noisy_writers + args.noisy_readers) as usize
        } else {
            0
        };
    let barrier = Arc::new(Barrier::new(n_workers.max(1)));
    let mut joins = Vec::new();

    {
        let h = Arc::clone(&handle);
        let s = Arc::clone(&stop);
        let b = Arc::clone(&barrier);
        let ready = Arc::clone(&start_maintainer);
        joins.push(thread::spawn(move || {
            b.wait();
            while !ready.load(Ordering::Relaxed) && !s.load(Ordering::Relaxed) {
                thread::sleep(Duration::from_millis(1));
            }
            if !s.load(Ordering::Relaxed) {
                run_maintainer(h, s);
            }
        }));
    }

    if with_noise {
        for w in 0..args.noisy_writers {
            let h = Arc::clone(&handle);
            let s = Arc::clone(&stop);
            let b = Arc::clone(&barrier);
            let start = (w as u64 + 1) * 2_000_000;
            joins.push(thread::spawn(move || {
                b.wait();
                run_noisy_writer(h, s, start);
            }));
        }
        for r in 0..args.noisy_readers {
            let h = Arc::clone(&handle);
            let s = Arc::clone(&stop);
            let b = Arc::clone(&barrier);
            let seed = 0xBEEF + r as u64;
            joins.push(thread::spawn(move || {
                b.wait();
                run_noisy_reader(h, s, 200_000, seed);
            }));
        }
    }

    let t0 = Instant::now();
    // Noisy floods while victim stays idle (maintainer still off under noise).
    thread::sleep(Duration::from_secs(args.rampup_idle_secs));

    let mut saw_pressure = false;
    if with_noise {
        saw_pressure = fill_until_busy(&handle, 50_000);
        eprintln!("  prefill_pressure_busy={saw_pressure}");
        // Allow refill during reclaim (δ is delay-to-reclaim under drain).
        start_maintainer.store(true, Ordering::Relaxed);
    }

    let budget = Duration::from_millis(args.retry_budget_ms.max(1_000));
    let t_reclaim = Instant::now();
    let mut acc = run_victim_reclaim(Arc::clone(&handle), reclaim_puts, budget);
    let reclaim_wall_ms = t_reclaim.elapsed().as_secs_f64() * 1000.0;

    stop.store(true, Ordering::Relaxed);
    start_maintainer.store(true, Ordering::Relaxed);
    for j in joins {
        let _ = j.join();
    }
    let elapsed_s = t0.elapsed().as_secs_f64();

    {
        let mut g = handle.write();
        let _ = g.poll_compaction();
        let _ = g.shutdown();
    }

    let summary = acc.summary();
    print_phase_summary(&summary);
    eprintln!(
        "  reclaim_wall={:.2}ms ({:.2}ms/put avg) pressure={}",
        reclaim_wall_ms,
        reclaim_wall_ms / reclaim_puts.max(1) as f64,
        saw_pressure
    );
    RampupPhaseResult {
        summary,
        reclaim_wall_ms,
        elapsed_s,
    }
}

fn run_phase(name: &str, dir: &Path, args: &Args, fair_on: bool, with_noise: bool) -> PhaseResult {
    eprintln!(
        "=== phase {name} fair={} noise={} seconds={} ===",
        fair_on, with_noise, args.seconds
    );
    let handle = open_handle(dir, args, fair_on);

    // Seed a modest noisy working set, then flush so the phase starts with
    // cold SST pages for thrash reads — not a pre-filled fair/global pool.
    if with_noise {
        {
            let mut eng = handle.write();
            for i in 0..1_000u64 {
                let _ = eng.put(tenant_key(2, i), vec![0u8; 8192], 0);
            }
            let _ = eng.force_flush();
        }
    }

    let stop = Arc::new(AtomicBool::new(false));
    let n_workers = 1 // victim
        + 1 // maintainer
        + if with_noise {
            (args.noisy_writers + args.noisy_readers) as usize
        } else {
            0
        };
    let barrier = Arc::new(Barrier::new(n_workers));

    let mut joins = Vec::new();
    {
        let h = Arc::clone(&handle);
        let s = Arc::clone(&stop);
        let b = Arc::clone(&barrier);
        joins.push(thread::spawn(move || {
            b.wait();
            run_maintainer(h, s);
        }));
    }

    let victim_acc = Arc::new(Mutex::new(None));
    {
        let h = Arc::clone(&handle);
        let s = Arc::clone(&stop);
        let b = Arc::clone(&barrier);
        let out = Arc::clone(&victim_acc);
        let ops = args.victim_ops_per_sec;
        let budget = Duration::from_millis(args.retry_budget_ms);
        joins.push(thread::spawn(move || {
            b.wait();
            // ~256 × 4 KiB ≈ 1 MiB — under a 2-tenant fair share of 8 MiB.
            let acc = run_victim(h, s, ops, 256, budget);
            *out.lock().unwrap() = Some(acc);
        }));
    }

    if with_noise {
        for w in 0..args.noisy_writers {
            let h = Arc::clone(&handle);
            let s = Arc::clone(&stop);
            let b = Arc::clone(&barrier);
            let start = (w as u64 + 1) * 1_000_000;
            joins.push(thread::spawn(move || {
                b.wait();
                run_noisy_writer(h, s, start);
            }));
        }
        for r in 0..args.noisy_readers {
            let h = Arc::clone(&handle);
            let s = Arc::clone(&stop);
            let b = Arc::clone(&barrier);
            let seed = 0xC0FFEE + r as u64;
            joins.push(thread::spawn(move || {
                b.wait();
                run_noisy_reader(h, s, 200_000, seed);
            }));
        }
    }

    let t0 = Instant::now();
    thread::sleep(Duration::from_secs(args.seconds));
    stop.store(true, Ordering::Relaxed);
    for j in joins {
        let _ = j.join();
    }
    let elapsed_s = t0.elapsed().as_secs_f64();

    {
        let mut g = handle.write();
        let _ = g.poll_compaction();
        let _ = g.shutdown();
    }

    let mut acc = victim_acc.lock().unwrap().take().unwrap_or_default();
    let summary = acc.summary();
    print_phase_summary(&summary);
    PhaseResult { summary, elapsed_s }
}

fn run_steady_suite(args: &Args) -> (serde_json::Value, bool, bool, bool) {
    let solo = run_phase("solo", &args.data_root.join("solo"), args, false, false);

    let fair_off = if args.skip_fair_off {
        None
    } else {
        Some(run_phase(
            "noise_fair_off",
            &args.data_root.join("fair_off"),
            args,
            false,
            true,
        ))
    };

    let fair_on = run_phase(
        "noise_fair_on",
        &args.data_root.join("fair_on"),
        args,
        true,
        true,
    );

    let delta_off = fair_off
        .as_ref()
        .map(|p| p.summary.put_p99_ms - solo.summary.put_p99_ms);
    let delta_on = fair_on.summary.put_p99_ms - solo.summary.put_p99_ms;
    let get_delta_on = fair_on.summary.get_p99_ms - solo.summary.get_p99_ms;
    let success_on = fair_on.summary.success_ratio();

    eprintln!();
    eprintln!("======== STEADY SOAK SUMMARY ========");
    eprintln!(
        "solo put_p99={:.2}ms get_p99={:.2}ms (e2e incl. Busy retries, budget={}ms)",
        solo.summary.put_p99_ms, solo.summary.get_p99_ms, args.retry_budget_ms
    );
    if let (Some(off), Some(d_off)) = (&fair_off, delta_off) {
        eprintln!(
            "fair=OFF put_p99={:.2}ms delta={:.2}ms success={:.1}% busy_events={}",
            off.summary.put_p99_ms,
            d_off,
            off.summary.success_ratio() * 100.0,
            off.summary.put_busy_events
        );
    }
    eprintln!(
        "fair=ON  put_p99={:.2}ms delta={:.2}ms get_delta={:.2}ms success={:.1}% busy_events={} ship_δ={:.0}",
        fair_on.summary.put_p99_ms,
        delta_on,
        get_delta_on,
        success_on * 100.0,
        fair_on.summary.put_busy_events,
        args.ship_delta_ms
    );
    if let Some(d_off) = delta_off {
        eprintln!(
            "improvement (off−on)={:.2}ms  fair_on_beats_off={}",
            d_off - delta_on,
            delta_on < d_off
        );
    }
    let meets_ship = delta_on <= args.fail_delta_ms && delta_on <= args.ship_delta_ms;
    let meets_success = success_on >= args.min_success_ratio;
    eprintln!(
        "verdict steady: ship(≤{:.0}ms)={}  success(≥{:.0}%)={}",
        args.fail_delta_ms,
        meets_ship,
        args.min_success_ratio * 100.0,
        meets_success
    );

    let busy_json: serde_json::Map<String, serde_json::Value> = fair_on
        .summary
        .busy_reasons
        .iter()
        .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
        .collect();

    let json = serde_json::json!({
        "solo_put_p99_ms": solo.summary.put_p99_ms,
        "solo_get_p99_ms": solo.summary.get_p99_ms,
        "fair_off_put_p99_ms": fair_off.as_ref().map(|p| p.summary.put_p99_ms),
        "fair_off_delta_ms": delta_off,
        "fair_off_success_ratio": fair_off.as_ref().map(|p| p.summary.success_ratio()),
        "fair_on_put_p99_ms": fair_on.summary.put_p99_ms,
        "fair_on_put_p50_ms": fair_on.summary.put_p50_ms,
        "fair_on_mutex_p99_ms": fair_on.summary.mutex_p99_ms,
        "fair_on_put_inside_p99_ms": fair_on.summary.put_inside_p99_ms,
        "fair_on_busy_retry_p99_ms": fair_on.summary.busy_retry_p99_ms,
        "fair_on_get_p99_ms": fair_on.summary.get_p99_ms,
        "fair_on_delta_ms": delta_on,
        "fair_on_get_delta_ms": get_delta_on,
        "fair_on_success_ratio": success_on,
        "fair_on_put_ok": fair_on.summary.put_ok,
        "fair_on_put_timeout": fair_on.summary.put_timeout,
        "fair_on_put_attempts": fair_on.summary.put_attempts,
        "fair_on_busy_reasons": busy_json,
        "meets_ship": meets_ship,
        "meets_success": meets_success,
        "solo_elapsed_s": solo.elapsed_s,
        "fair_on_elapsed_s": fair_on.elapsed_s,
    });
    (
        json,
        meets_ship,
        meets_success,
        delta_on > args.fail_delta_ms,
    )
}

fn run_rampup_suite(args: &Args) -> (serde_json::Value, bool, bool, bool) {
    let solo = run_rampup_phase(
        "solo",
        &args.data_root.join("rampup_solo"),
        args,
        false,
        false,
    );

    let fair_off = if args.skip_fair_off {
        None
    } else {
        Some(run_rampup_phase(
            "noise_fair_off",
            &args.data_root.join("rampup_fair_off"),
            args,
            false,
            true,
        ))
    };

    let fair_on = run_rampup_phase(
        "noise_fair_on",
        &args.data_root.join("rampup_fair_on"),
        args,
        true,
        true,
    );

    let delta_off = fair_off
        .as_ref()
        .map(|p| p.summary.put_p99_ms - solo.summary.put_p99_ms);
    let delta_on = fair_on.summary.put_p99_ms - solo.summary.put_p99_ms;
    let wall_delta_on = fair_on.reclaim_wall_ms - solo.reclaim_wall_ms;
    let success_on = fair_on.summary.success_ratio();

    eprintln!();
    eprintln!("======== RAMP-UP RECLAIM SUMMARY ========");
    eprintln!(
        "solo reclaim put_p99={:.2}ms wall={:.2}ms puts={} idle={}s",
        solo.summary.put_p99_ms,
        solo.reclaim_wall_ms,
        args.reclaim_puts(),
        args.rampup_idle_secs
    );
    if let (Some(off), Some(d_off)) = (&fair_off, delta_off) {
        eprintln!(
            "fair=OFF reclaim put_p99={:.2}ms delta={:.2}ms wall={:.2}ms success={:.1}% busy_events={}",
            off.summary.put_p99_ms,
            d_off,
            off.reclaim_wall_ms,
            off.summary.success_ratio() * 100.0,
            off.summary.put_busy_events
        );
    }
    eprintln!(
        "fair=ON  reclaim put_p99={:.2}ms delta={:.2}ms wall={:.2}ms wall_delta={:.2}ms success={:.1}% busy_events={} rampup_δ={:.0}",
        fair_on.summary.put_p99_ms,
        delta_on,
        fair_on.reclaim_wall_ms,
        wall_delta_on,
        success_on * 100.0,
        fair_on.summary.put_busy_events,
        args.rampup_fail_delta_ms
    );
    if let Some(d_off) = delta_off {
        eprintln!(
            "improvement p99 (off−on)={:.2}ms  fair_on_beats_off={}",
            d_off - delta_on,
            delta_on < d_off
        );
    }
    // Gate on the worse of per-put p99 δ and batch wall δ (reclaim delay).
    let gate_delta = delta_on.max(wall_delta_on);
    let meets_rampup = gate_delta <= args.rampup_fail_delta_ms;
    let meets_success = success_on >= args.min_success_ratio;
    eprintln!(
        "verdict ramp-up: gate_δ={:.2}ms (≤{:.0}ms)={}  success(≥{:.0}%)={}",
        gate_delta,
        args.rampup_fail_delta_ms,
        meets_rampup,
        args.min_success_ratio * 100.0,
        meets_success
    );

    let busy_json: serde_json::Map<String, serde_json::Value> = fair_on
        .summary
        .busy_reasons
        .iter()
        .map(|(k, v)| (k.to_string(), serde_json::json!(v)))
        .collect();

    let json = serde_json::json!({
        "solo_put_p99_ms": solo.summary.put_p99_ms,
        "solo_reclaim_wall_ms": solo.reclaim_wall_ms,
        "fair_off_put_p99_ms": fair_off.as_ref().map(|p| p.summary.put_p99_ms),
        "fair_off_delta_ms": delta_off,
        "fair_off_reclaim_wall_ms": fair_off.as_ref().map(|p| p.reclaim_wall_ms),
        "fair_on_put_p99_ms": fair_on.summary.put_p99_ms,
        "fair_on_mutex_p99_ms": fair_on.summary.mutex_p99_ms,
        "fair_on_busy_retry_p99_ms": fair_on.summary.busy_retry_p99_ms,
        "fair_on_delta_ms": delta_on,
        "fair_on_reclaim_wall_ms": fair_on.reclaim_wall_ms,
        "fair_on_wall_delta_ms": wall_delta_on,
        "fair_on_gate_delta_ms": gate_delta,
        "fair_on_success_ratio": success_on,
        "fair_on_put_ok": fair_on.summary.put_ok,
        "fair_on_put_timeout": fair_on.summary.put_timeout,
        "fair_on_busy_reasons": busy_json,
        "reclaim_puts": args.reclaim_puts(),
        "idle_secs": args.rampup_idle_secs,
        "rampup_fail_delta_ms": args.rampup_fail_delta_ms,
        "meets_rampup": meets_rampup,
        "meets_success": meets_success,
        "elapsed_s": fair_on.elapsed_s,
    });
    (
        json,
        meets_rampup,
        meets_success,
        gate_delta > args.rampup_fail_delta_ms,
    )
}

fn main() {
    let args = Args::parse();
    std::fs::create_dir_all(&args.data_root).expect("data-root");

    let mut exit_code = 0i32;
    let mut steady_json = serde_json::Value::Null;
    let mut rampup_json = serde_json::Value::Null;

    if matches!(args.mode, Mode::Steady | Mode::Both) {
        let (j, meets_ship, meets_success, fail_delta) = run_steady_suite(&args);
        steady_json = j;
        if !meets_success {
            eprintln!("FAIL: steady fair-on victim success below floor");
            exit_code = exit_code.max(2);
        }
        if fail_delta || !meets_ship {
            eprintln!("FAIL: steady fair-on put p99 delta exceeds ship bar");
            exit_code = exit_code.max(1);
        }
    }

    if matches!(args.mode, Mode::Rampup | Mode::Both) {
        let (j, meets_rampup, meets_success, fail_delta) = run_rampup_suite(&args);
        rampup_json = j;
        if !meets_success {
            eprintln!("FAIL: ramp-up fair-on victim success below floor");
            exit_code = exit_code.max(2);
        }
        if fail_delta || !meets_rampup {
            eprintln!(
                "FAIL: ramp-up fair-on reclaim p99 delta exceeds {:.0}ms bar",
                args.rampup_fail_delta_ms
            );
            exit_code = exit_code.max(3);
        }
    }

    let summary = serde_json::json!({
        "kind": "tenant_isolation_soak_summary",
        "mode": match args.mode {
            Mode::Steady => "steady",
            Mode::Rampup => "rampup",
            Mode::Both => "both",
        },
        "seconds_per_phase": args.seconds,
        "memtable_mb": args.memtable_mb,
        "block_cache_mb": args.block_cache_mb,
        "victim_ops_per_sec": args.victim_ops_per_sec,
        "retry_budget_ms": args.retry_budget_ms,
        "fail_delta_ms": args.fail_delta_ms,
        "ship_delta_ms": args.ship_delta_ms,
        "rampup_fail_delta_ms": args.rampup_fail_delta_ms,
        "min_success_ratio": args.min_success_ratio,
        "steady": steady_json,
        "rampup": rampup_json,
    });
    println!("{}", summary);
    if let Some(path) = &args.json_out {
        let mut f = std::fs::File::create(path).expect("json-out");
        writeln!(f, "{summary}").unwrap();
    }

    if exit_code != 0 {
        std::process::exit(exit_code);
    }
}
