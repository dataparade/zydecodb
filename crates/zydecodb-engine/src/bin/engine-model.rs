//! Model-based differential tester for `zydecodb-engine`.
//!
//! Drives a deterministic sequence of operations at a real engine and checks
//! every observable result against a trivially-correct reference model — a
//! per-key version history in memory. The model IS the specification; any
//! divergence is an engine bug (or a harness bug, which is also worth having).
//!
//! Op set v1: Put, Delete, Get, Scan (full ordered-range comparison), Flush,
//! Compact, Snapshot (owned snapshot reads must equal the model at the
//! snapshot's seq ceiling), Crash (drop without shutdown + reopen; recovered
//! state must equal the model at exactly the replayed seq, and must not have
//! lost fsynced writes), plus periodic full-keyspace checkpoint diffs.
//!
//! Determinism: a single seeded Lcg (same constants as the determinism
//! tests and engine-soak) drives every choice. No wall-clock input. Replaying
//! `--seed S --steps N` reproduces the exact op sequence, so a failing seed
//! IS the regression artifact.
//!
//! Output: one JSON object per line (header, progress, divergences, summary).
//! Exit 0 = no divergence; exit 1 = divergence found (line printed with the
//! seed and step needed to reproduce).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

#[path = "../soak_util.rs"]
mod soak_common;
use soak_common::Lcg;

// ---------------------------------------------------------------------------
// CLI

struct Args {
    data_dir: PathBuf,
    wal_dir: PathBuf,
    seed: u64,
    steps: u64,
    keyspace: u64,
    val_min: usize,
    val_max: usize,
    checkpoint_every: u64,
    /// Memtable flush threshold (bytes). Small on purpose: the point is to
    /// force constant flush/compaction churn, not to soak.
    flush_threshold: usize,
    out: Option<PathBuf>,
}

impl Args {
    fn parse() -> Args {
        let mut data_dir: Option<PathBuf> = None;
        let mut wal_dir: Option<PathBuf> = None;
        let mut seed: u64 = 1;
        let mut steps: u64 = 100_000;
        let mut keyspace: u64 = 50_000;
        let mut val_min: usize = 8;
        let mut val_max: usize = 64;
        let mut checkpoint_every: u64 = 1_000;
        let mut flush_threshold: usize = 1024 * 1024;
        let mut out: Option<PathBuf> = None;

        let mut it = std::env::args().skip(1);
        while let Some(arg) = it.next() {
            let mut val = || {
                it.next()
                    .unwrap_or_else(|| panic!("missing value for {}", arg))
            };
            match arg.as_str() {
                "--data-dir" => data_dir = Some(PathBuf::from(val())),
                "--wal-dir" => wal_dir = Some(PathBuf::from(val())),
                "--seed" => seed = val().parse().expect("--seed: u64"),
                "--steps" => steps = val().parse().expect("--steps: u64"),
                "--keyspace" => keyspace = val().parse().expect("--keyspace: u64"),
                "--val-min" => val_min = val().parse().expect("--val-min: usize"),
                "--val-max" => val_max = val().parse().expect("--val-max: usize"),
                "--checkpoint-every" => {
                    checkpoint_every = val().parse().expect("--checkpoint-every: u64")
                }
                "--flush-threshold" => {
                    flush_threshold = val().parse().expect("--flush-threshold: usize")
                }
                "--out" => out = Some(PathBuf::from(val())),
                other => panic!("unknown arg: {}", other),
            }
        }
        Args {
            data_dir: data_dir.expect("--data-dir required"),
            wal_dir: wal_dir.expect("--wal-dir required"),
            seed,
            steps,
            keyspace,
            val_min,
            val_max,
            checkpoint_every,
            flush_threshold,
            out,
        }
    }
}

// ---------------------------------------------------------------------------
// Reference model: per-key version history. `None` value = tombstone.

#[derive(Default)]
struct Model {
    hist: BTreeMap<Vec<u8>, BTreeMap<u64, Option<Vec<u8>>>>,
}

impl Model {
    fn put(&mut self, key: Vec<u8>, seq: u64, val: Vec<u8>) {
        self.hist.entry(key).or_default().insert(seq, Some(val));
    }

    fn del(&mut self, key: Vec<u8>, seq: u64) {
        self.hist.entry(key).or_default().insert(seq, None);
    }

    /// Value visible at seq ceiling `s`: newest entry at or below `s`,
    /// suppressing tombstones.
    fn value_at(&self, key: &[u8], s: u64) -> Option<Vec<u8>> {
        self.hist
            .get(key)
            .and_then(|h| h.range(..=s).next_back())
            .and_then(|(_, v)| v.clone())
    }

    /// Full live keyspace at seq ceiling `s`, in key order.
    fn live_at(&self, s: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut out = BTreeMap::new();
        for (k, h) in &self.hist {
            if let Some((_, Some(v))) = h.range(..=s).next_back() {
                out.insert(k.clone(), v.clone());
            }
        }
        out
    }
}

// ---------------------------------------------------------------------------
// Harness

fn key_of(id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(9);
    k.push(KS_USER);
    k.extend_from_slice(&id.to_be_bytes());
    k
}

struct Harness {
    /// `Option` so `crash()` can drop the engine (simulated kill) before
    /// reopening — two live engines on the same dirs is never legal.
    engine: Option<Engine>,
    model: Model,
    rng: Lcg,
    args: Args,
    out: Box<dyn Write>,
    crashes: u64,
    checkpoints: u64,
    snapshots_checked: u64,
}

impl Harness {
    fn log(&mut self, line: &str) {
        writeln!(self.out, "{}", line).ok();
        self.out.flush().ok();
    }

    fn divergence(&mut self, step: u64, op: &str, detail: &str) -> ! {
        let line = format!(
            "{{\"kind\":\"divergence\",\"seed\":{},\"step\":{},\"op\":\"{}\",\"detail\":{}}}",
            self.args.seed,
            step,
            op,
            serde_json_string(detail)
        );
        self.log(&line);
        eprintln!("DIVERGENCE at step {} (seed {}): {} — {}", step, self.args.seed, op, detail);
        std::process::exit(1);
    }

    fn eng(&self) -> &Engine {
        self.engine.as_ref().expect("engine live outside crash window")
    }

    fn eng_mut(&mut self) -> &mut Engine {
        self.engine.as_mut().expect("engine live outside crash window")
    }

    /// Retry wrapper for backpressure: paced model runs must not die on
    /// EngineBusy; they must wait for the background pipeline like a real
    /// well-behaved client would.
    fn put_checked(&mut self, key: Vec<u8>, val: Vec<u8>) -> u64 {
        for _ in 0..10_000 {
            match self.eng_mut().put(key.clone(), val.clone(), 0) {
                Ok(seq) => return seq,
                Err(_) => {
                    let _ = self.eng_mut().poll_compaction();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
        panic!("put blocked by backpressure for 10k retries");
    }

    fn del_checked(&mut self, key: Vec<u8>) -> u64 {
        for _ in 0..10_000 {
            match self.eng_mut().del(key.clone()) {
                Ok((_existed, seq)) => return seq,
                Err(_) => {
                    let _ = self.eng_mut().poll_compaction();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
        panic!("del blocked by backpressure for 10k retries");
    }

    /// Collect a full-keyspace scan into an owned map, releasing the
    /// iterator's engine borrow before any divergence reporting.
    fn scan_all(&self) -> Result<BTreeMap<Vec<u8>, Vec<u8>>, String> {
        let iter = self
            .eng()
            .scan(vec![KS_USER], vec![KS_USER + 1])
            .map_err(|e| format!("scan error: {}", e))?;
        let mut got = BTreeMap::new();
        for item in iter {
            let (k, v) = item.map_err(|e| format!("iter error: {}", e))?;
            got.insert(k, v);
        }
        Ok(got)
    }

    /// Full-keyspace diff: engine scan vs model at the engine's current seq.
    fn checkpoint(&mut self, step: u64) {
        let ack = self.eng().current_seq();
        let expected = self.model.live_at(ack);
        let got = match self.scan_all() {
            Ok(g) => g,
            Err(e) => self.divergence(step, "checkpoint", &e),
        };
        if got != expected {
            let detail = diff_summary(&expected, &got);
            self.divergence(step, "checkpoint", &detail);
        }
        self.checkpoints += 1;
    }

    /// Crash: drop without shutdown, reopen, verify prefix-consistent replay.
    fn crash(&mut self, step: u64) {
        let ack = self.eng().current_seq();
        let durable = self.eng().last_synced_seq();
        // Drop the engine with no shutdown: equivalent to a process kill.
        // Only after the drop may a new engine open the same dirs.
        drop(self.engine.take());
        self.engine = Some(Engine::open(self.engine_config()).expect("reopen after crash"));

        let recovered = self.eng().current_seq();
        if recovered > ack {
            self.divergence(
                step,
                "crash",
                &format!("replay fabricated writes: recovered seq {} > ack {}", recovered, ack),
            );
        }
        if recovered < durable {
            self.divergence(
                step,
                "crash",
                &format!(
                    "replay lost fsynced writes: recovered seq {} < durable {}",
                    recovered, durable
                ),
            );
        }
        // Replay is prefix-consistent, so recovered state must equal the model
        // at exactly `recovered`.
        let expected = self.model.live_at(recovered);
        let got = match self.scan_all() {
            Ok(g) => g,
            Err(e) => self.divergence(step, "crash", &e),
        };
        if got != expected {
            let detail = diff_summary(&expected, &got);
            self.divergence(step, "crash", &format!("state != model at seq {}: {}", recovered, detail));
        }
        self.crashes += 1;
    }

    fn engine_config(&self) -> EngineConfig {
        EngineConfig {
            data_dir: self.args.data_dir.clone(),
            wal_dir: self.args.wal_dir.clone(),
            block_cache_bytes: 32 * 1024 * 1024,
            memtable_flush_threshold: self.args.flush_threshold,
            ..Default::default()
        }
    }

    fn step(&mut self, step: u64) {
        let roll = self.rng.range_u32(100);
        match roll {
            // Put (35%)
            0..=34 => {
                let id = self.rng.next_u64() % self.args.keyspace;
                let len = self.rng.range_usize(self.args.val_min, self.args.val_max);
                let mut val = format!("s{}:", step).into_bytes();
                while val.len() < len {
                    val.push((self.rng.next_u64() & 0xFF) as u8);
                }
                val.truncate(len);
                let seq = self.put_checked(key_of(id), val.clone());
                self.model.put(key_of(id), seq, val);
            }
            // Delete (15%)
            35..=49 => {
                let id = self.rng.next_u64() % self.args.keyspace;
                let seq = self.del_checked(key_of(id));
                self.model.del(key_of(id), seq);
            }
            // Get (25%)
            50..=74 => {
                let id = self.rng.next_u64() % self.args.keyspace;
                let key = key_of(id);
                let ack = self.eng().current_seq();
                let expected = self.model.value_at(&key, ack);
                match self.eng().get(&key) {
                    Ok(got) => {
                        if got != expected {
                            self.divergence(
                                step,
                                "get",
                                &format!(
                                    "key id {}: expected {}, got {}",
                                    id,
                                    describe(&expected),
                                    describe(&got)
                                ),
                            );
                        }
                    }
                    Err(e) => self.divergence(step, "get", &format!("error: {}", e)),
                }
            }
            // Scan (10%): half-open [key(a), key(b)), full ordered comparison.
            75..=84 => {
                let a = self.rng.next_u64() % self.args.keyspace;
                let mut b = self.rng.next_u64() % self.args.keyspace;
                if b <= a {
                    b = (a + 1) % self.args.keyspace;
                }
                let (lo, hi) = (key_of(a.min(b)), key_of(a.max(b)));
                let ack = self.eng().current_seq();
                let expected: Vec<(Vec<u8>, Vec<u8>)> = self
                    .model
                    .live_at(ack)
                    .into_iter()
                    .filter(|(k, _)| k.as_slice() >= lo.as_slice() && k.as_slice() < hi.as_slice())
                    .collect();
                let scan_result: Result<Vec<(Vec<u8>, Vec<u8>)>, String> = (|| {
                    let iter = self
                        .eng()
                        .scan(lo.clone(), hi.clone())
                        .map_err(|e| format!("error: {}", e))?;
                    iter.collect::<Result<Vec<_>, _>>()
                        .map_err(|e| format!("iter error: {}", e))
                })();
                match scan_result {
                    Ok(got) => {
                        if got != expected {
                            self.divergence(
                                step,
                                "scan",
                                &format!(
                                    "range [{:?}..{:?}): expected {} keys, got {} keys",
                                    lo,
                                    hi,
                                    expected.len(),
                                    got.len()
                                ),
                            );
                        }
                    }
                    Err(e) => self.divergence(step, "scan", &e),
                }
            }
            // Flush (5%)
            85..=89 => {
                if let Err(e) = self.eng_mut().force_flush() {
                    self.divergence(step, "flush", &format!("error: {}", e));
                }
            }
            // Compact (5%)
            90..=94 => {
                if let Err(e) = self.eng_mut().drain_compaction() {
                    self.divergence(step, "compact", &format!("error: {}", e));
                }
            }
            // Snapshot (4%): owned snapshot reads must equal model at ceiling.
            95..=98 => {
                let ceiling = self.eng().current_seq();
                let snap = self.eng().snapshot_owned();
                for _ in 0..8 {
                    let id = self.rng.next_u64() % self.args.keyspace;
                    let key = key_of(id);
                    let expected = self.model.value_at(&key, ceiling);
                    match snap.get(&key) {
                        Ok(got) => {
                            if got != expected {
                                self.divergence(
                                    step,
                                    "snapshot",
                                    &format!(
                                        "key id {} at seq {}: expected {}, got {}",
                                        id,
                                        ceiling,
                                        describe(&expected),
                                        describe(&got)
                                    ),
                                );
                            }
                        }
                        Err(e) => self.divergence(step, "snapshot", &format!("error: {}", e)),
                    }
                }
                self.snapshots_checked += 1;
            }
            // Crash (1%)
            _ => {
                self.crash(step);
            }
        }
    }
}

fn describe(v: &Option<Vec<u8>>) -> String {
    match v {
        None => "None".into(),
        Some(b) => format!("Some({} bytes, prefix {:?})", b.len(), &b[..b.len().min(12)]),
    }
}

/// First mismatch summary between two ordered keyspaces, for the divergence line.
fn diff_summary(expected: &BTreeMap<Vec<u8>, Vec<u8>>, got: &BTreeMap<Vec<u8>, Vec<u8>>) -> String {
    if expected.len() != got.len() {
        return format!("key count: expected {}, got {}", expected.len(), got.len());
    }
    for (k, v) in expected {
        match got.get(k) {
            None => return format!("missing key {:?}", k),
            Some(gv) if gv != v => {
                return format!("key {:?}: expected {} bytes, got {} bytes", k, v.len(), gv.len())
            }
            _ => {}
        }
    }
    for k in got.keys() {
        if !expected.contains_key(k) {
            return format!("unexpected key {:?}", k);
        }
    }
    "unknown diff".into()
}

/// Minimal JSON string escaping for the detail field (no serde dep here).
fn serde_json_string(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 2);
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

fn main() {
    let args = Args::parse();
    let mut out: Box<dyn Write> = match &args.out {
        Some(p) => Box::new(
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .expect("open --out"),
        ),
        None => Box::new(std::io::stdout()),
    };
    writeln!(
        out,
        "{{\"kind\":\"header\",\"seed\":{},\"steps\":{},\"keyspace\":{},\"val_min\":{},\"val_max\":{},\"checkpoint_every\":{},\"flush_threshold\":{}}}",
        args.seed, args.steps, args.keyspace, args.val_min, args.val_max, args.checkpoint_every, args.flush_threshold
    )
    .ok();
    out.flush().ok();

    std::fs::create_dir_all(&args.data_dir).expect("create data dir");
    std::fs::create_dir_all(&args.wal_dir).expect("create wal dir");

    let engine = Engine::open(EngineConfig {
        data_dir: args.data_dir.clone(),
        wal_dir: args.wal_dir.clone(),
        block_cache_bytes: 32 * 1024 * 1024,
        memtable_flush_threshold: args.flush_threshold,
        ..Default::default()
    })
    .expect("Engine::open");

    let mut h = Harness {
        engine: Some(engine),
        model: Model::default(),
        rng: Lcg::new(args.seed),
        crashes: 0,
        checkpoints: 0,
        snapshots_checked: 0,
        out,
        args,
    };

    for step in 0..h.args.steps {
        h.step(step);
        if step % h.args.checkpoint_every == 0 {
            h.checkpoint(step);
        }
        if step % 10_000 == 0 {
            let line = format!(
                "{{\"kind\":\"progress\",\"step\":{},\"crashes\":{},\"checkpoints\":{}}}",
                step, h.crashes, h.checkpoints
            );
            h.log(&line);
        }
    }

    // Final checkpoint at the end of the run, then clean shutdown.
    h.checkpoint(h.args.steps);
    if let Err(e) = h.eng_mut().shutdown() {
        h.divergence(h.args.steps, "shutdown", &format!("error: {}", e));
    }
    let line = format!(
        "{{\"kind\":\"summary\",\"seed\":{},\"steps\":{},\"crashes\":{},\"checkpoints\":{},\"snapshots_checked\":{},\"divergences\":0}}",
        h.args.seed, h.args.steps, h.crashes, h.checkpoints, h.snapshots_checked
    );
    h.log(&line);
}
