//! Disk-full and fsync-lie resilience via injected failpoints.
//!
//! Run with: `cargo test -p zydecodb-engine --features failpoints --test resilience_failpoints -- --test-threads=1`

#![cfg(feature = "failpoints")]
// Tests build CompactionConfig::default() then tweak a couple of fields.
#![allow(clippy::field_reassign_with_default)]

use std::sync::Mutex;
use tempfile::TempDir;
use zydecodb_engine::compaction::CompactionConfig;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::errors::EngineError;
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

fn open(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .expect("engine open")
}

fn open_for_compaction(dir: &TempDir) -> Engine {
    let mut cfg = CompactionConfig::default();
    cfg.l0_trigger = 2;
    cfg.target_file_bytes = 4 * 1024;
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        compaction: cfg,
        ..Default::default()
    })
    .expect("engine open")
}

fn assert_baseline_visible(e: &Engine) {
    assert_eq!(
        e.get(&uk(b"pre_a")).unwrap().as_deref(),
        Some(b"1".as_ref())
    );
    assert_eq!(
        e.get(&uk(b"pre_b")).unwrap().as_deref(),
        Some(b"2".as_ref())
    );
}

#[test]
fn disk_full_on_wal_append_surfaces_io_error() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    fail::cfg(WAL_BEFORE_APPEND, "return").expect("cfg");

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let err = e.put(uk(b"k"), b"v".to_vec(), 0).unwrap_err();
    assert!(matches!(err, EngineError::Io(_)));
}

#[test]
fn disk_full_on_sstable_flush_surfaces_io_error() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    fail::cfg(SSTABLE_BEFORE_TMP_WRITE, "return").expect("cfg");

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    e.put(uk(b"k"), b"v".to_vec(), 0).unwrap();
    let err = e.force_flush().unwrap_err();
    assert!(matches!(err, EngineError::Io(_)));
}

#[test]
fn disk_full_on_wal_append_recovers_baseline() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    {
        let mut e = open(&dir);
        e.put(uk(b"pre_a"), b"1".to_vec(), 0).unwrap();
        e.put(uk(b"pre_b"), b"2".to_vec(), 0).unwrap();
        e.sync_wal().unwrap();

        fail::cfg(WAL_BEFORE_APPEND, "return").expect("cfg");
        let err = e.put(uk(b"crash"), b"x".to_vec(), 0).unwrap_err();
        assert!(matches!(err, EngineError::Io(_)));
    }

    let e = open(&dir);
    assert_baseline_visible(&e);
    assert!(e.get(&uk(b"crash")).unwrap().is_none());
}

#[test]
fn disk_full_on_flush_recovers_baseline() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    {
        let mut e = open(&dir);
        e.put(uk(b"pre_a"), b"1".to_vec(), 0).unwrap();
        e.put(uk(b"pre_b"), b"2".to_vec(), 0).unwrap();
        e.sync_wal().unwrap();
        e.put(uk(b"buffered"), b"3".to_vec(), 0).unwrap();

        fail::cfg(SSTABLE_BEFORE_TMP_WRITE, "return").expect("cfg");
        let err = e.force_flush().unwrap_err();
        assert!(matches!(err, EngineError::Io(_)));
    }

    let e = open(&dir);
    assert_baseline_visible(&e);
    assert_eq!(
        e.get(&uk(b"buffered")).unwrap().as_deref(),
        Some(b"3".as_ref())
    );
}

#[test]
fn disk_full_on_manifest_append_recovers_baseline() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    {
        let mut e = open(&dir);
        e.put(uk(b"pre_a"), b"1".to_vec(), 0).unwrap();
        e.put(uk(b"pre_b"), b"2".to_vec(), 0).unwrap();
        e.sync_wal().unwrap();
        e.put(uk(b"flush_me"), b"3".to_vec(), 0).unwrap();

        fail::cfg(MANIFEST_BEFORE_APPEND, "return").expect("cfg");
        let err = e.force_flush().unwrap_err();
        assert!(matches!(err, EngineError::Io(_)));
    }

    let e = open(&dir);
    assert_baseline_visible(&e);
    assert_eq!(
        e.get(&uk(b"flush_me")).unwrap().as_deref(),
        Some(b"3".as_ref())
    );
}

#[test]
fn disk_full_on_compaction_rename_recovers_baseline() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    {
        let mut e = open_for_compaction(&dir);
        e.put(uk(b"pre_a"), b"1".to_vec(), 0).unwrap();
        e.put(uk(b"pre_b"), b"2".to_vec(), 0).unwrap();
        e.sync_wal().unwrap();
        for i in 0..40u32 {
            e.put(uk(format!("k{i:02}").as_bytes()), b"v".to_vec(), 0)
                .unwrap();
        }
        e.force_flush().unwrap();

        // Arm before scheduling compaction (same pattern as crash_matrix Trigger::Compaction).
        fail::cfg(COMPACTION_BEFORE_RENAME, "return").expect("cfg");
        // l0_trigger=2: one more flush creates a second L0 file and schedules a job.
        e.put(uk(b"cflush_0"), b"v".to_vec(), 0).unwrap();
        let err = match e.force_flush() {
            Err(err) => err,
            Ok(()) => e.drain_compaction().unwrap_err(),
        };
        assert!(matches!(err, EngineError::Io(_)));
    }

    let mut e = open_for_compaction(&dir);
    assert_baseline_visible(&e);
    e.put(uk(b"post"), b"ok".to_vec(), 0).unwrap();
    assert_eq!(
        e.get(&uk(b"post")).unwrap().as_deref(),
        Some(b"ok".as_ref())
    );
}

#[test]
fn fsync_lie_advances_watermark_without_real_fsync() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    e.put(uk(b"pre"), b"ok".to_vec(), 0).unwrap();
    e.sync_wal().unwrap();

    fail::cfg(WAL_LIE_FSYNC, "return").expect("cfg");
    e.put(uk(b"buffered"), b"x".to_vec(), 0).unwrap();
    let buffered = e.last_buffered_seq();
    let synced = e.sync_wal().unwrap();
    assert_eq!(synced, buffered);
    assert!(synced > 1);
}
