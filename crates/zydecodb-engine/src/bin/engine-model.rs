//! Model-based differential tester for `zydecodb-engine`.
//!
//! Drives a deterministic sequence of operations at a real engine and checks
//! every observable result against a trivially-correct reference model — a
//! per-key version history plus a global op log in memory. The model IS the
//! specification; any divergence is an engine bug (or a harness bug, which is
//! also worth having).
//!
//! Op set v1: Put, Delete, Get, Scan (full ordered-range comparison), Flush,
//! Compact, Snapshot (owned snapshot reads must equal the model at the
//! snapshot's seq ceiling), Crash (drop without shutdown + reopen; recovered
//! state must equal the model at exactly the replayed seq, and must not have
//! lost fsynced writes), plus periodic full-keyspace checkpoint diffs.
//!
//! Op set v2: Batch (write_batch atomicity — all ops share one seq, so crash
//! prefix-consistency checks cover torn batches), TTL (short-expiry puts must
//! be invisible everywhere after expiry, including historical snapshot reads),
//! SnapshotAt (historical seq-ceiling reads against model history), and
//! ChangeStream (the archived+active WAL change stream after a resume token
//! must equal the model op log exactly — including across crashes).
//!
//! Keys use the document-body layout `[0x01]['d'][collection u32][id u64]` so
//! the change-log stream (which filters to doc-body keys) covers every write.
//!
//! Determinism: a single seeded Lcg (same constants as the determinism
//! tests and engine-soak) drives every choice. TTL introduces wall-clock
//! dependence by nature; it is made replay-safe by sleeping past expiry
//! inside the op, so every later observation is time-stable.
//!
//! Output: one JSON object per line (header, progress, divergences, summary).
//! Exit 0 = no divergence; exit 1 = divergence found (line printed with the
//! seed and step needed to reproduce).

use std::collections::BTreeMap;
use std::io::Write;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};
use zydecodb_engine::change_log::{ChangeLogConfig, LogicalChangeKind};
use zydecodb_engine::engine::{BatchOp, Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

#[path = "../soak_util.rs"]
mod soak_common;
use soak_common::Lcg;

/// Change streams filter to doc-body keys under one (tenant, collection).
/// The harness uses a single fixed collection so every write is streamed.
const COLLECTION_ID: u32 = 7;
const TENANT_PREFIX: [u8; 1] = [KS_USER];

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// CLI

struct Args {
    data_dir: PathBuf,
    wal_dir: PathBuf,
    changelog_dir: PathBuf,
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
        let mut changelog_dir: Option<PathBuf> = None;
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
                "--changelog-dir" => changelog_dir = Some(PathBuf::from(val())),
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
        let data_dir = data_dir.expect("--data-dir required");
        let wal_dir = wal_dir.expect("--wal-dir required");
        Args {
            changelog_dir: changelog_dir.unwrap_or_else(|| data_dir.join("../changelog")),
            data_dir,
            wal_dir,
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
// Reference model: per-key version history plus a global op log.

/// A stored value. `Expiring` becomes invisible (everywhere, including
/// historical seq-ceiling reads) once wall-clock passes the expiry — matching
/// the engine's read-time expiry evaluation.
#[derive(Clone)]
enum ModelVal {
    Plain(Vec<u8>),
    Expiring(Vec<u8>, u64),
}

#[derive(Default)]
struct Model {
    hist: BTreeMap<Vec<u8>, BTreeMap<u64, Option<ModelVal>>>,
    /// Global op log for change-stream diffs: (seq, op ordinal, key, value).
    /// `None` value = delete. Values are the exact bytes handed to the engine.
    oplog: Vec<(u64, u32, Vec<u8>, Option<Vec<u8>>)>,
}

impl Model {
    fn put(&mut self, key: Vec<u8>, seq: u64, ord: u32, val: Vec<u8>) {
        self.hist
            .entry(key.clone())
            .or_default()
            .insert(seq, Some(ModelVal::Plain(val.clone())));
        self.oplog.push((seq, ord, key, Some(val)));
    }

    fn put_expiring(&mut self, key: Vec<u8>, seq: u64, ord: u32, val: Vec<u8>, expiry_ms: u64) {
        self.hist
            .entry(key.clone())
            .or_default()
            .insert(seq, Some(ModelVal::Expiring(val.clone(), expiry_ms)));
        self.oplog.push((seq, ord, key, Some(val)));
    }

    fn del(&mut self, key: Vec<u8>, seq: u64, ord: u32) {
        self.hist.entry(key.clone()).or_default().insert(seq, None);
        self.oplog.push((seq, ord, key, None));
    }

    /// Value visible at seq ceiling `s`: newest entry at or below `s`,
    /// suppressing tombstones and (by wall clock) expired values.
    fn value_at(&self, key: &[u8], s: u64) -> Option<Vec<u8>> {
        let (_, v) = self.hist.get(key)?.range(..=s).next_back()?;
        match v {
            None => None,
            Some(ModelVal::Plain(val)) => Some(val.clone()),
            Some(ModelVal::Expiring(val, exp)) => {
                if now_ms() >= *exp {
                    None
                } else {
                    Some(val.clone())
                }
            }
        }
    }

    /// Full live keyspace at seq ceiling `s`, in key order.
    fn live_at(&self, s: u64) -> BTreeMap<Vec<u8>, Vec<u8>> {
        let mut out = BTreeMap::new();
        for k in self.hist.keys() {
            if let Some(v) = self.value_at(k, s) {
                out.insert(k.clone(), v);
            }
        }
        out
    }

    /// Op-log entries strictly after `(seq, ord)`, in stream order.
    fn changes_after(&self, seq: u64, ord: u32) -> Vec<(u64, u32, Vec<u8>, Option<Vec<u8>>)> {
        self.oplog
            .iter()
            .filter(|(s, o, _, _)| *s > seq || (*s == seq && *o > ord))
            .cloned()
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Harness

fn key_of(id: u64) -> Vec<u8> {
    let mut k = Vec::with_capacity(14);
    k.push(KS_USER);
    k.push(b'd');
    k.extend_from_slice(&COLLECTION_ID.to_be_bytes());
    k.extend_from_slice(&id.to_be_bytes());
    k
}

/// Inverse of `key_of`: the 8-byte id from a full doc-body key.
fn id_of(key: &[u8]) -> u64 {
    u64::from_be_bytes(key[6..14].try_into().expect("doc-body key"))
}

/// The stream's `doc_id` is the key with the doc-body header (tenant prefix,
/// 'd', collection id) already stripped — i.e. exactly the 8-byte id.
fn id_of_doc(doc_id: &[u8]) -> u64 {
    u64::from_be_bytes(doc_id[..8].try_into().expect("doc id"))
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
    streams_checked: u64,
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

    fn open_engine(&self) -> Engine {
        Engine::open(self.engine_config())
            .expect("Engine::open")
            .with_change_log(ChangeLogConfig {
                archive_dir: self.args.changelog_dir.clone(),
                // Generous retention: nothing pruned mid-run, so the stream
                // check can diff full history. Prune semantics have their own
                // dedicated tests.
                retention_secs: 30 * 24 * 3600,
                retention_bytes: 1 << 40,
            })
            .expect("with_change_log")
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

    /// Retry wrapper for backpressure: paced model runs must not die on
    /// EngineBusy; they must wait for the background pipeline like a real
    /// well-behaved client would.
    fn put_checked(&mut self, key: Vec<u8>, val: Vec<u8>, expires_at: u64) -> u64 {
        for _ in 0..10_000 {
            match self.eng_mut().put(key.clone(), val.clone(), expires_at) {
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

    fn batch_checked(&mut self, ops: Vec<BatchOp>) -> u64 {
        for _ in 0..10_000 {
            match self.eng_mut().write_batch(ops.clone()) {
                Ok(seq) => return seq,
                Err(_) => {
                    let _ = self.eng_mut().poll_compaction();
                    std::thread::sleep(std::time::Duration::from_millis(1));
                }
            }
        }
        panic!("batch blocked by backpressure for 10k retries");
    }

    fn make_val(&mut self, step: u64) -> Vec<u8> {
        let len = self.rng.range_usize(self.args.val_min, self.args.val_max);
        let mut val = format!("s{}:", step).into_bytes();
        while val.len() < len {
            val.push((self.rng.next_u64() & 0xFF) as u8);
        }
        val.truncate(len);
        val
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

    /// Diff the change stream after `(t_seq, t_ord)` against the model op log.
    /// `up_to_seq` caps the expected entries (post-crash streams only contain
    /// what replay recovered); `u64::MAX` expects the full log.
    fn check_stream(&mut self, step: u64, t_seq: u64, t_ord: u32, up_to_seq: u64) {
        let cfg = match self.eng().change_log_config() {
            Some(c) => c.clone(),
            None => self.divergence(step, "stream", "change log not enabled"),
        };
        let manifest = match self.eng().change_log_manifest() {
            Some(m) => m.clone(),
            None => self.divergence(step, "stream", "change log manifest missing"),
        };
        let active = self.eng().active_wal_path();
        let got = match zydecodb_engine::change_log::iter_logical_changes_after(
            &cfg,
            &manifest,
            Some(&active),
            &TENANT_PREFIX,
            COLLECTION_ID,
            t_seq,
            t_ord,
        ) {
            Ok(g) => g,
            Err(e) => self.divergence(step, "stream", &format!("iter error: {}", e)),
        };
        let expected: Vec<_> = self
            .model
            .changes_after(t_seq, t_ord)
            .into_iter()
            .filter(|(s, _, _, _)| *s <= up_to_seq)
            .collect();
        if got.len() != expected.len() {
            self.divergence(
                step,
                "stream",
                &format!(
                    "after ({}, {}): expected {} changes, got {}",
                    t_seq,
                    t_ord,
                    expected.len(),
                    got.len()
                ),
            );
        }
        for (g, (es, eo, ek, ev)) in got.iter().zip(expected.iter()) {
            let g_id = id_of_doc(&g.doc_id);
            let e_id = id_of(ek);
            let g_kind = g.kind;
            let e_kind = if ev.is_some() {
                LogicalChangeKind::Upsert
            } else {
                LogicalChangeKind::Delete
            };
            if g.seq != *es || g.op_ordinal != *eo || g_id != e_id || g_kind != e_kind {
                self.divergence(
                    step,
                    "stream",
                    &format!(
                        "entry mismatch: got (seq {}, ord {}, id {}, {:?}), expected (seq {}, ord {}, id {}, {:?})",
                        g.seq, g.op_ordinal, g_id, g_kind, es, eo, e_id, e_kind
                    ),
                );
            }
            if let (Some(v), LogicalChangeKind::Upsert) = (ev, g_kind) {
                if g.stored_value != *v {
                    self.divergence(
                        step,
                        "stream",
                        &format!("value mismatch at seq {} id {}", es, e_id),
                    );
                }
            }
        }
        self.streams_checked += 1;
    }

    /// Crash: drop without shutdown, reopen, verify prefix-consistent replay
    /// and change-stream continuity across the crash boundary.
    fn crash(&mut self, step: u64) {
        let ack = self.eng().current_seq();
        let durable = self.eng().last_synced_seq();
        // Resume-token position for the post-crash stream check: a few ops
        // back from ack so the stream spans the crash boundary.
        let (t_seq, t_ord) = self
            .model
            .oplog
            .iter()
            .rev()
            .nth(5)
            .map(|(s, o, _, _)| (*s, *o))
            .unwrap_or((0, 0));

        // Drop the engine with no shutdown: equivalent to a process kill.
        // Only after the drop may a new engine open the same dirs.
        drop(self.engine.take());
        self.engine = Some(self.open_engine());

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
        // A resume token taken before the crash must still work after it:
        // the stream must contain exactly the recovered ops after the token.
        self.check_stream(step, t_seq, t_ord, recovered);
        self.crashes += 1;
    }

    fn step(&mut self, step: u64) {
        let roll = self.rng.range_u32(100);
        match roll {
            // Put (30%)
            0..=29 => {
                let id = self.rng.next_u64() % self.args.keyspace;
                let val = self.make_val(step);
                let seq = self.put_checked(key_of(id), val.clone(), 0);
                self.model.put(key_of(id), seq, 0, val);
            }
            // Delete (12%)
            30..=41 => {
                let id = self.rng.next_u64() % self.args.keyspace;
                let seq = self.del_checked(key_of(id));
                self.model.del(key_of(id), seq, 0);
            }
            // Get (22%)
            42..=63 => {
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
            // Scan (8%): half-open [key(a), key(b)), full ordered comparison.
            64..=71 => {
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
            // Batch (4%): 2-5 ops on distinct keys, one shared seq.
            72..=75 => {
                let n = self.rng.range_usize(2, 5);
                let mut ids: Vec<u64> = Vec::with_capacity(n);
                while ids.len() < n {
                    let id = self.rng.next_u64() % self.args.keyspace;
                    if !ids.contains(&id) {
                        ids.push(id);
                    }
                }
                let mut ops = Vec::with_capacity(n);
                let mut vals: Vec<Option<Vec<u8>>> = Vec::with_capacity(n);
                for &id in &ids {
                    if self.rng.range_u32(100) < 70 {
                        let v = self.make_val(step);
                        ops.push(BatchOp::Put {
                            key: key_of(id),
                            value: v.clone(),
                            expires_at: 0,
                        });
                        vals.push(Some(v));
                    } else {
                        ops.push(BatchOp::Del { key: key_of(id) });
                        vals.push(None);
                    }
                }
                let seq = self.batch_checked(ops);
                for (ord, (&id, v)) in ids.iter().zip(vals.into_iter()).enumerate() {
                    match v {
                        Some(v) => self.model.put(key_of(id), seq, ord as u32, v),
                        None => self.model.del(key_of(id), seq, ord as u32),
                    }
                }
            }
            // TTL (3%): short-expiry put, then sleep past expiry inside the op
            // so every later observation (reads, snapshots, crash replay) is
            // time-stable: the key must be invisible everywhere.
            76..=78 => {
                let id = self.rng.next_u64() % self.args.keyspace;
                let val = self.make_val(step);
                let expiry = now_ms() + 60;
                let seq = self.put_checked(key_of(id), val.clone(), expiry);
                self.model.put_expiring(key_of(id), seq, 0, val, expiry);
                std::thread::sleep(std::time::Duration::from_millis(80));
            }
            // Flush (5%)
            79..=83 => {
                if let Err(e) = self.eng_mut().force_flush() {
                    self.divergence(step, "flush", &format!("error: {}", e));
                }
            }
            // Compact (5%)
            84..=88 => {
                if let Err(e) = self.eng_mut().drain_compaction() {
                    self.divergence(step, "compact", &format!("error: {}", e));
                }
            }
            // Snapshot (4%): owned snapshot reads must equal model at ceiling.
            89..=92 => {
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
            // SnapshotAt (3%): historical seq-ceiling reads against model
            // history — exercises the bounded-snapshot SSTable path.
            //
            // The engine's documented GC contract: versions are retained for
            // LIVE snapshots only, so a historical read at an unpinned ceiling
            // may find an overwritten old version GC'd (returns None), but it
            // must NEVER return a value that was not the value-at-ceiling
            // (no newer writes, no resurrected pre-tombstone values), and a
            // key unchanged since the ceiling must always read its value
            // (the newest version is never GC'd).
            93..=95 => {
                let ack = self.eng().current_seq();
                let ceiling = if ack == 0 { 0 } else { self.rng.next_u64() % (ack + 1) };
                let snap = self.eng().snapshot_at(ceiling);
                for _ in 0..8 {
                    let id = self.rng.next_u64() % self.args.keyspace;
                    let key = key_of(id);
                    let at_ceiling = self.model.value_at(&key, ceiling);
                    let at_ack = self.model.value_at(&key, ack);
                    match snap.get(&key) {
                        Ok(got) => {
                            let ok = got == at_ceiling
                                || (got.is_none()
                                    && at_ceiling.is_some()
                                    && at_ceiling != at_ack);
                            if !ok {
                                self.divergence(
                                    step,
                                    "snapshot_at",
                                    &format!(
                                        "key id {} at seq {}/{}: at-ceiling {}, at-ack {}, got {}",
                                        id,
                                        ceiling,
                                        ack,
                                        describe(&at_ceiling),
                                        describe(&at_ack),
                                        describe(&got)
                                    ),
                                );
                            }
                        }
                        Err(e) => self.divergence(step, "snapshot_at", &format!("error: {}", e)),
                    }
                }
                self.snapshots_checked += 1;
            }
            // ChangeStream (3%): sync the WAL, then the stream after a random
            // past token must equal the model op log exactly.
            96..=98 => {
                if let Err(e) = self.eng_mut().sync_wal() {
                    self.divergence(step, "stream", &format!("sync error: {}", e));
                }
                let (t_seq, t_ord) = if self.model.oplog.is_empty() {
                    (0, 0)
                } else {
                    let idx = self.rng.range_usize(0, self.model.oplog.len() - 1);
                    let (s, o, _, _) = self.model.oplog[idx];
                    (s, o)
                };
                self.check_stream(step, t_seq, t_ord, u64::MAX);
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
        "{{\"kind\":\"header\",\"seed\":{},\"steps\":{},\"keyspace\":{},\"val_min\":{},\"val_max\":{},\"checkpoint_every\":{},\"flush_threshold\":{},\"v\":2}}",
        args.seed, args.steps, args.keyspace, args.val_min, args.val_max, args.checkpoint_every, args.flush_threshold
    )
    .ok();
    out.flush().ok();

    std::fs::create_dir_all(&args.data_dir).expect("create data dir");
    std::fs::create_dir_all(&args.wal_dir).expect("create wal dir");
    std::fs::create_dir_all(&args.changelog_dir).expect("create changelog dir");

    let mut h = Harness {
        engine: None,
        model: Model::default(),
        rng: Lcg::new(args.seed),
        crashes: 0,
        checkpoints: 0,
        snapshots_checked: 0,
        streams_checked: 0,
        out,
        args,
    };
    h.engine = Some(h.open_engine());

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
        "{{\"kind\":\"summary\",\"seed\":{},\"steps\":{},\"crashes\":{},\"checkpoints\":{},\"snapshots_checked\":{},\"streams_checked\":{},\"divergences\":0}}",
        h.args.seed, h.args.steps, h.crashes, h.checkpoints, h.snapshots_checked, h.streams_checked
    );
    h.log(&line);
}
