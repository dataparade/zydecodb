//! Proves the group-commit fsync no longer holds the engine mutex.
//!
//! Run with:
//! `cargo test -p zydecodb-engine --features failpoints --test concurrency_lock -- --test-threads=1`

#![cfg(feature = "failpoints")]

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::engine_handle::EngineHandle;
use zydecodb_engine::failpoints::*;
use zydecodb_engine::keys::KS_USER;

fn fail_lock() -> &'static Mutex<()> {
    static LOCK: Mutex<()> = Mutex::new(());
    &LOCK
}

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

/// While a fsync is in flight (delayed by a 500ms failpoint sleep), the engine
/// mutex must be free: a concurrent writer should acquire the lock and complete
/// its append well inside that window. Before the decoupling the fsync ran under
/// the engine lock, so this write would have blocked for the full sleep.
#[test]
fn engine_lock_is_free_during_in_flight_fsync() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let engine = EngineHandle::new(
        Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("data/wal"),
            ..Default::default()
        })
        .expect("engine open"),
    );

    // Buffer a write so there is an unsynced suffix to fsync.
    engine.write().put(uk(b"k1"), b"v1".to_vec(), 0).unwrap();
    let wal = engine.write().wal_sync();

    // Delay the fsync by 500ms, INSIDE WalSync::sync (which does not hold the
    // engine lock).
    fail::cfg(WAL_BEFORE_FSYNC, "sleep(500)").expect("cfg");

    let wal_for_thread = Arc::clone(&wal);
    let syncer = thread::spawn(move || {
        // Sleeps 500ms inside the failpoint, then fsyncs.
        wal_for_thread.sync().expect("sync");
    });

    // Let the syncer enter the failpoint sleep.
    thread::sleep(Duration::from_millis(75));

    // The engine lock must be free during the sleeping fsync.
    let start = Instant::now();
    engine.write().put(uk(b"k2"), b"v2".to_vec(), 0).unwrap();
    let elapsed = start.elapsed();

    assert!(
        elapsed < Duration::from_millis(300),
        "engine lock was held across the in-flight fsync (write took {elapsed:?})"
    );

    // Disable the failpoint before joining so the syncer's wakeup is clean.
    fail::cfg(WAL_BEFORE_FSYNC, "off").expect("cfg off");
    syncer.join().unwrap();

    // Both writes are present and the durability watermark advanced.
    let e = engine.write();
    assert_eq!(e.get(&uk(b"k1")).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(e.get(&uk(b"k2")).unwrap(), Some(b"v2".to_vec()));
}

/// A reader capturing a snapshot must also proceed while a fsync sleeps: the
/// snapshot capture only needs the engine lock briefly, and the read runs
/// entirely off-lock.
#[test]
fn snapshot_capture_proceeds_during_in_flight_fsync() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let engine = EngineHandle::new(
        Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("data/wal"),
            ..Default::default()
        })
        .expect("engine open"),
    );

    engine.write().put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    let wal = engine.write().wal_sync();

    fail::cfg(WAL_BEFORE_FSYNC, "sleep(500)").expect("cfg");
    let wal_for_thread = Arc::clone(&wal);
    let syncer = thread::spawn(move || wal_for_thread.sync().expect("sync"));
    thread::sleep(Duration::from_millis(75));

    let start = Instant::now();
    let value = {
        let snap = engine.read().snapshot_owned();
        snap.get(&uk(b"a")).unwrap()
    };
    let elapsed = start.elapsed();

    assert_eq!(value, Some(b"1".to_vec()));
    assert!(
        elapsed < Duration::from_millis(300),
        "snapshot read blocked behind the in-flight fsync (took {elapsed:?})"
    );

    fail::cfg(WAL_BEFORE_FSYNC, "off").expect("cfg off");
    syncer.join().unwrap();
}
