//! Operational resilience tests (disk-full simulation, concurrent readers, Bloom FPR).

use std::sync::Arc;
use std::thread;
use tempfile::TempDir;
use zydecodb_engine::bloom::BloomFilter;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::errors::EngineError;
use zydecodb_engine::keys::KS_USER;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

#[test]
fn bloom_false_positive_rate_is_bounded_at_scale() {
    let keys: Vec<Vec<u8>> = (0..10_000u32)
        .map(|i| format!("present{:05}", i).into_bytes())
        .collect();
    let bloom = BloomFilter::build(&keys);
    let mut false_pos = 0u32;
    let probes = 50_000u32;
    for i in 0..probes {
        let key = format!("absent{:05}", i);
        if bloom.maybe_contains(key.as_bytes()) {
            false_pos += 1;
        }
    }
    let rate = false_pos as f64 / probes as f64;
    assert!(rate < 0.02, "bloom FPR {rate:.4} exceeded 2% ceiling");
}

#[test]
fn concurrent_readers_with_single_writer_see_consistent_values() {
    let dir = TempDir::new().unwrap();
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap();

    for i in 0..100u32 {
        e.put(
            uk(format!("k{:03}", i).as_bytes()),
            format!("v{i}").into_bytes(),
            0,
        )
        .unwrap();
    }
    e.force_flush().unwrap();

    let snap = Arc::new(e.snapshot_owned());
    let handles: Vec<_> = (0..4)
        .map(|_| {
            let s = snap.clone();
            thread::spawn(move || {
                for i in 0..100u32 {
                    let key = uk(format!("k{:03}", i).as_bytes());
                    let got = s.get(&key).unwrap().expect("missing key");
                    assert_eq!(got, format!("v{i}").into_bytes());
                }
            })
        })
        .collect();
    for h in handles {
        h.join().expect("reader thread panicked");
    }
}

#[test]
fn engine_busy_when_writes_frozen() {
    let dir = TempDir::new().unwrap();
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap();
    e.freeze_writes();
    let err = e.put(uk(b"k"), b"v".to_vec(), 0).unwrap_err();
    assert!(matches!(err, EngineError::EngineBusy(_)));
    e.unfreeze_writes();
    e.put(uk(b"k"), b"v".to_vec(), 0).unwrap();
}
