//! Recovery determinism.
//!
//! Two runs that execute the *same* seeded op sequence, perform the *same*
//! drop-and-reopen (the v1 stand-in for a forced crash), and probe the *same*
//! keys must produce byte-identical observable state. That state is:
//!   - the value returned by `get` for every key the test ever wrote.
//!
//! (Internal counters like `seq_peek` and `sstable_count` would also be
//! interesting to compare but they are `#[cfg(test)]`-only in this engine, so
//! they are not reachable from integration tests. The visible-via-public-API
//! contract is what production callers actually depend on.)
//!
//! The canonical bug this guards against is non-deterministic recovery caused
//! by iterating a `HashMap` during manifest replay or memtable flush. If two
//! runs disagree on any of the above, recovery isn't deterministic and the
//! engine cannot be reasoned about in production.
//!
//! Note: this is the "deterministic-given-same-history" property. The harsher
//! "deterministic-given-same-failpoint" form needs the failpoints feature plus
//! the crash matrix scaffolding; this baseline test runs on the default build
//! so every PR gets it for free.

use std::collections::BTreeMap;
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

/// Tiny seeded RNG so determinism doesn't depend on a stdlib RNG.
struct Lcg(u64);
impl Lcg {
    fn new(seed: u64) -> Self {
        Lcg(seed.max(1))
    }
    fn next_u32(&mut self) -> u32 {
        self.0 = self.0.wrapping_mul(6364136223846793005).wrapping_add(1);
        (self.0 >> 33) as u32
    }
    fn range(&mut self, n: u32) -> u32 {
        self.next_u32() % n
    }
}

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Del(Vec<u8>),
    Flush,
    Reopen,
}

fn uk(raw: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(raw.len() + 1);
    v.push(KS_USER);
    v.extend_from_slice(raw);
    v
}

/// Generate a deterministic op sequence from a seed. The same seed always
/// yields the same Vec<Op>, including key/value bytes — this is the contract
/// that lets the determinism test compare two independent runs.
fn generate_ops(seed: u64, count: usize) -> Vec<Op> {
    let mut rng = Lcg::new(seed);
    let mut ops = Vec::with_capacity(count);
    // Small key alphabet so writes collide and exercise overwrite/tombstone.
    for _ in 0..count {
        let pick = rng.range(100);
        if pick < 70 {
            let klen = 1 + rng.range(6) as usize;
            let key: Vec<u8> = (0..klen).map(|_| b'a' + rng.range(8) as u8).collect();
            let vlen = rng.range(16) as usize;
            let val: Vec<u8> = (0..vlen).map(|_| rng.next_u32() as u8).collect();
            ops.push(Op::Put(uk(&key), val));
        } else if pick < 85 {
            let klen = 1 + rng.range(6) as usize;
            let key: Vec<u8> = (0..klen).map(|_| b'a' + rng.range(8) as u8).collect();
            ops.push(Op::Del(uk(&key)));
        } else if pick < 95 {
            ops.push(Op::Flush);
        } else {
            ops.push(Op::Reopen);
        }
    }
    ops
}

fn open(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .expect("engine open")
}

/// Drive `ops` against a fresh engine in `dir`, then drop and reopen one final
/// time to force a recovery pass. Return the observable map of all touched
/// keys.
fn run_and_recover(dir: &TempDir, ops: &[Op]) -> BTreeMap<Vec<u8>, Option<Vec<u8>>> {
    let mut probed_keys = BTreeMap::<Vec<u8>, ()>::new();
    {
        let mut e = open(dir);
        for op in ops {
            match op {
                Op::Put(k, v) => {
                    e.put(k.clone(), v.clone(), 0).expect("put");
                    probed_keys.insert(k.clone(), ());
                }
                Op::Del(k) => {
                    e.del(k.clone()).expect("del");
                    probed_keys.insert(k.clone(), ());
                }
                Op::Flush => e.force_flush().expect("flush"),
                Op::Reopen => {
                    drop(e);
                    e = open(dir);
                }
            }
        }
    }

    // Final crash + recover.
    let e = open(dir);
    let mut snapshot = BTreeMap::new();
    for k in probed_keys.keys() {
        let v = e.get(k).expect("get");
        snapshot.insert(k.clone(), v);
    }
    snapshot
}

#[test]
fn same_seed_same_recovered_state() {
    let ops = generate_ops(0xDEAD_BEEF_CAFE_BABE, 200);
    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let state_a = run_and_recover(&dir_a, &ops);
    let state_b = run_and_recover(&dir_b, &ops);
    assert_eq!(
        state_a, state_b,
        "recovered key/value state diverged between two runs of the same seeded sequence"
    );
}

#[test]
fn same_seed_same_recovered_state_high_flush_pressure() {
    // A sequence biased toward flushes exercises the manifest-replay path —
    // the canonical hazard for non-determinism (HashMap iteration order).
    let mut ops = generate_ops(0xCAFEBABE, 60);
    let mut padded = Vec::with_capacity(ops.len() * 2);
    for (i, op) in ops.drain(..).enumerate() {
        padded.push(op);
        if i % 5 == 0 {
            padded.push(Op::Flush);
        }
    }
    padded.push(Op::Flush);

    let dir_a = TempDir::new().unwrap();
    let dir_b = TempDir::new().unwrap();
    let state_a = run_and_recover(&dir_a, &padded);
    let state_b = run_and_recover(&dir_b, &padded);
    assert_eq!(state_a, state_b);
}

#[test]
fn double_recovery_is_a_fixed_point() {
    // Recovering twice in a row from a clean state must produce the same
    // observable result. A bug that mutates persistent state during recovery
    // (e.g. rewriting the manifest in a non-idempotent way) would show up here.
    let ops = generate_ops(0x1234_5678_9ABC_DEF0, 120);
    let dir = TempDir::new().unwrap();
    let snap_1 = run_and_recover(&dir, &ops);
    // run_and_recover already drops the engine; do one more recover-only pass.
    let e = open(&dir);
    let mut snap_2 = BTreeMap::new();
    for k in snap_1.keys() {
        snap_2.insert(k.clone(), e.get(k).expect("get"));
    }
    assert_eq!(snap_1, snap_2, "second recovery diverged from first");
}
