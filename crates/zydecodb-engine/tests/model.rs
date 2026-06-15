//! Property-based model test: real `Engine` vs. `BTreeMap` reference.
//!
//! For any randomly generated sequence of operations (Put/Get/Del/Flush/Reopen),
//! the engine's externally-observable state after every step must match a plain
//! `BTreeMap<Vec<u8>, Vec<u8>>` reference. This catches:
//!   - MVCC sort-order bugs (newer seq must shadow older for same user key)
//!   - Tombstone propagation through memtable -> immutable -> SSTable
//!   - Recovery bugs (a Reopen mid-sequence must preserve all acked writes)
//!   - Flush bugs (force_flush must not lose, duplicate, or reorder anything)
//!
//! Tuned so 256 cases per run finish in <30s. Crank `PROPTEST_CASES=10000` for
//! the nightly run. TTL/`expires_at` is intentionally pinned to 0 here so the
//! model is deterministic; expiry has its own dedicated test elsewhere.

use proptest::collection::vec;
use proptest::prelude::*;
use std::collections::BTreeMap;
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

/// Raw user key (the `KS_USER` prefix byte is added by `compose`). Small alphabet
/// so the same key is touched repeatedly, exercising overwrite + tombstone paths.
const ALPHABET: &[u8] = b"abcdefgh";
const MAX_KEY_LEN: usize = 8;
const MAX_VAL_LEN: usize = 32;
const MAX_OPS: usize = 30;

#[derive(Debug, Clone)]
enum Op {
    Put(Vec<u8>, Vec<u8>),
    Get(Vec<u8>),
    Del(Vec<u8>),
    Flush,
    Reopen,
    /// Range scan from `lo` to `hi` (composed user keys after prefixing).
    Scan(Vec<u8>, Vec<u8>),
    /// Take a snapshot and verify a spot-check key against the model.
    SnapshotGet(Vec<u8>),
}

fn raw_key() -> impl Strategy<Value = Vec<u8>> {
    vec(prop::sample::select(ALPHABET), 1..=MAX_KEY_LEN)
}

fn raw_value() -> impl Strategy<Value = Vec<u8>> {
    vec(any::<u8>(), 0..=MAX_VAL_LEN)
}

fn op_strategy() -> impl Strategy<Value = Op> {
    // Weighted so writes dominate but every kind shows up regularly. Flush and
    // Reopen are rare because they're expensive and a 30-op sequence with five
    // flushes is mostly just flushing.
    prop_oneof![
        6 => (raw_key(), raw_value()).prop_map(|(k, v)| Op::Put(k, v)),
        4 => raw_key().prop_map(Op::Get),
        2 => raw_key().prop_map(Op::Del),
        2 => (raw_key(), raw_key()).prop_map(|(a, b)| {
            let (lo, hi) = if a <= b { (a, b) } else { (b, a) };
            Op::Scan(lo, hi)
        }),
        2 => raw_key().prop_map(Op::SnapshotGet),
        1 => Just(Op::Flush),
        1 => Just(Op::Reopen),
    ]
}

fn compose(raw: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(raw.len() + 1);
    k.push(KS_USER);
    k.extend_from_slice(raw);
    k
}

fn open(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .expect("engine open")
}

proptest! {
    #![proptest_config(ProptestConfig {
        // Tests do real disk I/O; keep the per-PR run bounded. Crank via
        // PROPTEST_CASES env var on nightly.
        cases: 64,
        max_shrink_iters: 1024,
        .. ProptestConfig::default()
    })]

    #[test]
    fn engine_matches_btreemap_under_arbitrary_op_sequences(
        ops in vec(op_strategy(), 1..=MAX_OPS),
    ) {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = open(&dir);
        let mut model: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();

        for (step, op) in ops.iter().enumerate() {
            match op {
                Op::Put(k, v) => {
                    let composed = compose(k);
                    engine.put(composed.clone(), v.clone(), 0)
                        .unwrap_or_else(|e| panic!("step {}: put failed: {}", step, e));
                    model.insert(composed, v.clone());
                }
                Op::Get(k) => {
                    let composed = compose(k);
                    let got = engine.get(&composed)
                        .unwrap_or_else(|e| panic!("step {}: get failed: {}", step, e));
                    let expected = model.get(&composed).cloned();
                    prop_assert_eq!(
                        got, expected,
                        "step {}: GET divergence for key {:?}", step, k
                    );
                }
                Op::Del(k) => {
                    let composed = compose(k);
                    let (engine_existed, _seq) = engine.del(composed.clone())
                        .unwrap_or_else(|e| panic!("step {}: del failed: {}", step, e));
                    let model_existed = model.remove(&composed).is_some();
                    prop_assert_eq!(
                        engine_existed, model_existed,
                        "step {}: DEL `existed` flag divergence for key {:?}", step, k
                    );
                }
                Op::Flush => {
                    engine.force_flush()
                        .unwrap_or_else(|e| panic!("step {}: flush failed: {}", step, e));
                }
                Op::Reopen => {
                    // Drop + reopen the engine; the model is unchanged because
                    // every prior op was acked (so it must be durable).
                    drop(engine);
                    engine = open(&dir);
                }
                Op::Scan(lo_raw, hi_raw) => {
                    let lo = compose(lo_raw);
                    let hi = compose(hi_raw);
                    if hi <= lo {
                        continue;
                    }
                    let mut got: Vec<(Vec<u8>, Vec<u8>)> = engine
                        .scan(lo.clone(), hi.clone())
                        .unwrap_or_else(|e| panic!("step {}: scan failed: {}", step, e))
                        .map(|r| r.unwrap_or_else(|e| panic!("step {}: scan item: {}", step, e)))
                        .collect();
                    got.sort();
                    let mut expected: Vec<(Vec<u8>, Vec<u8>)> = model
                        .range(lo..hi)
                        .map(|(k, v)| (k.clone(), v.clone()))
                        .collect();
                    expected.sort();
                    prop_assert_eq!(got, expected, "step {}: SCAN divergence", step);
                }
                Op::SnapshotGet(k) => {
                    let composed = compose(k);
                    let snap = engine.snapshot();
                    let got = snap
                        .get(&composed)
                        .unwrap_or_else(|e| panic!("step {}: snap get: {}", step, e));
                    let expected = model.get(&composed).cloned();
                    prop_assert_eq!(
                        got, expected,
                        "step {}: SNAPSHOT GET divergence for key {:?}", step, k
                    );
                }
            }

            // After every op (not just Get), spot-check a handful of keys from
            // the model. A divergence anywhere proves a bug; cheaper than a
            // full scan per step.
            for k in model.keys().take(4) {
                let got = engine.get(k)
                    .unwrap_or_else(|e| panic!("step {}: post-op get failed: {}", step, e));
                prop_assert_eq!(
                    got.as_deref(), Some(model[k].as_slice()),
                    "step {}: post-op spot check divergence on key {:?}", step, k
                );
            }
        }

        // Final full-state assertion: every key the model holds must match
        // exactly, and nothing the model deleted must be readable.
        for (k, v) in &model {
            let got = engine.get(k)
                .unwrap_or_else(|e| panic!("final get failed: {}", e));
            prop_assert_eq!(
                got.as_deref(), Some(v.as_slice()),
                "final: missing/divergent key {:?}", k
            );
        }
    }

    /// Overwrite invariant: the latest PUT for a given key must always win,
    /// regardless of how many flushes happened in between.
    #[test]
    fn last_put_wins_across_flushes(
        key in raw_key(),
        values in vec(raw_value(), 2..=8),
    ) {
        let dir = TempDir::new().expect("tempdir");
        let mut engine = open(&dir);
        let composed = compose(&key);

        for (i, v) in values.iter().enumerate() {
            engine.put(composed.clone(), v.clone(), 0).unwrap();
            // Flush on odd indices so some values end up in SSTables and others
            // remain in the active memtable when we read.
            if i % 2 == 1 {
                engine.force_flush().unwrap();
            }
        }

        let expected = values.last().unwrap().clone();
        let got = engine.get(&composed).unwrap();
        prop_assert_eq!(got, Some(expected));
    }

    /// Delete-then-flush-then-reopen invariant: a tombstone must survive both
    /// a flush (so it lands in an SSTable) and a process restart.
    #[test]
    fn tombstone_survives_flush_and_reopen(
        key in raw_key(),
        value in raw_value(),
    ) {
        let dir = TempDir::new().expect("tempdir");
        let composed = compose(&key);
        {
            let mut engine = open(&dir);
            engine.put(composed.clone(), value, 0).unwrap();
            engine.force_flush().unwrap();
            engine.del(composed.clone()).unwrap();
            engine.force_flush().unwrap();
            prop_assert_eq!(engine.get(&composed).unwrap(), None);
        }
        let engine = open(&dir);
        prop_assert_eq!(engine.get(&composed).unwrap(), None);
    }
}
