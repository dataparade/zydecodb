//! Concurrent readers must not serialize behind a writer for snapshot capture.
//!
//! Run:
//! `cargo test -p zydecodb-engine --test read_concurrency -- --nocapture`

use std::sync::{Arc, Barrier, Mutex};
use std::thread;
use std::time::Instant;
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::engine_handle::EngineHandle;
use zydecodb_engine::keys::KS_USER;

fn uk(i: u64) -> Vec<u8> {
    let mut k = vec![KS_USER];
    k.extend_from_slice(b"rc/");
    k.extend_from_slice(&i.to_be_bytes());
    k
}

fn p50_p99_us(samples: &mut [u128]) -> (f64, f64) {
    samples.sort_unstable();
    let p = |q: f64| {
        let idx = ((samples.len() as f64) * q).floor() as usize;
        samples[idx.min(samples.len() - 1)] as f64
    };
    (p(0.50), p(0.99))
}

/// N reader threads capture snapshots via `read()` and point-get while one
/// writer keeps putting. Prints read latency percentiles for the PR description.
#[test]
fn concurrent_readers_alongside_writer() {
    let dir = TempDir::new().unwrap();
    let handle = EngineHandle::new(
        Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("wal"),
            ..Default::default()
        })
        .expect("open"),
    );

    // Seed a working set so gets hit memtable (and later SST after freeze).
    {
        let mut e = handle.write();
        for i in 0..2_000u64 {
            e.put(uk(i), format!("v{i}").into_bytes(), 0).unwrap();
        }
    }

    const READERS: usize = 8;
    const READS_PER: usize = 2_000;
    let barrier = Arc::new(Barrier::new(READERS + 1));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let writer = {
        let handle = Arc::clone(&handle);
        let barrier = Arc::clone(&barrier);
        let stop = Arc::clone(&stop);
        thread::spawn(move || {
            barrier.wait();
            let mut i = 10_000u64;
            while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                let mut e = handle.write();
                e.put(uk(i), b"w".to_vec(), 0).unwrap();
                i += 1;
                drop(e);
                thread::yield_now();
            }
        })
    };

    let mut joins = Vec::new();
    let samples: Arc<Mutex<Vec<u128>>> =
        Arc::new(Mutex::new(Vec::with_capacity(READERS * READS_PER)));
    for t in 0..READERS {
        let handle = Arc::clone(&handle);
        let barrier = Arc::clone(&barrier);
        let samples = Arc::clone(&samples);
        joins.push(thread::spawn(move || {
            barrier.wait();
            let mut local = Vec::with_capacity(READS_PER);
            for n in 0..READS_PER {
                let key = uk((n as u64 + t as u64 * 97) % 2_000);
                let start = Instant::now();
                let snap = handle.read().snapshot_owned();
                let v = snap.get(&key).unwrap();
                local.push(start.elapsed().as_micros());
                assert!(v.is_some(), "seeded key missing");
            }
            samples.lock().unwrap().extend(local);
        }));
    }

    for j in joins {
        j.join().unwrap();
    }
    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    writer.join().unwrap();

    let mut all = samples.lock().unwrap().clone();
    let (p50, p99) = p50_p99_us(&mut all);
    eprintln!(
        "read_concurrency: {} samples, p50={p50:.1}µs p99={p99:.1}µs (readers={READERS})",
        all.len()
    );
    // Soft gate: a healthy shared-read path stays well under a millisecond p99
    // for memtable hits on a quiet laptop. Fail only on pathological stalls.
    assert!(
        p99 < 5_000.0,
        "read p99 too high ({p99:.1}µs) — readers may still be serialized behind writers"
    );
    assert!(all.len() == READERS * READS_PER);
}
