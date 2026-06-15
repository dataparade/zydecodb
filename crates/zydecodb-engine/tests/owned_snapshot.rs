//! Long-lived owned snapshot semantics.

// Tests build EngineConfig::default() then tweak a couple of fields.
#![allow(clippy::field_reassign_with_default)]

use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

#[test]
fn owned_snapshot_survives_writes_and_compaction() {
    let dir = TempDir::new().unwrap();
    let mut cfg = zydecodb_engine::compaction::CompactionConfig::default();
    cfg.l0_trigger = 2;
    cfg.target_file_bytes = 4096;
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        compaction: cfg,
        ..Default::default()
    })
    .unwrap();

    e.put(uk(b"k1"), b"v1".to_vec(), 0).unwrap();
    e.force_flush().unwrap();
    let snap = e.snapshot_owned();
    assert_eq!(
        snap.get(&uk(b"k1")).unwrap().as_deref(),
        Some(b"v1".as_ref())
    );

    e.put(uk(b"k1"), b"v2".to_vec(), 0).unwrap();
    e.put(uk(b"k2"), b"x".to_vec(), 0).unwrap();
    e.force_flush().unwrap();

    assert_eq!(
        snap.get(&uk(b"k1")).unwrap().as_deref(),
        Some(b"v1".as_ref())
    );
    assert_eq!(e.get(&uk(b"k1")).unwrap().as_deref(), Some(b"v2".as_ref()));
}

#[test]
fn pinned_snapshot_retains_deleted_key_until_dropped() {
    let dir = TempDir::new().unwrap();
    let mut cfg = zydecodb_engine::compaction::CompactionConfig::default();
    cfg.l0_trigger = 2;
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        compaction: cfg,
        ..Default::default()
    })
    .unwrap();

    e.put(uk(b"k"), b"v1".to_vec(), 0).unwrap();
    e.force_flush().unwrap();
    let snap = e.snapshot_owned();
    e.del(uk(b"k")).unwrap();
    e.force_flush().unwrap();
    assert!(snap.get(&uk(b"k")).unwrap().is_some());
    assert!(e.get(&uk(b"k")).unwrap().is_none());
}

#[test]
fn tombstone_gc_reclaims_space_without_live_snapshot() {
    let dir = TempDir::new().unwrap();
    let mut cfg = zydecodb_engine::compaction::CompactionConfig::default();
    cfg.l0_trigger = 2;
    cfg.target_file_bytes = 512;
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        compaction: cfg,
        ..Default::default()
    })
    .unwrap();

    for i in 0..20u32 {
        e.put(uk(format!("k{i:02}").as_bytes()), vec![0u8; 128], 0)
            .unwrap();
    }
    e.force_flush().unwrap();
    e.drain_compaction().unwrap();
    let before = e.estimate_disk_bytes();

    for i in 0..20u32 {
        e.del(uk(format!("k{i:02}").as_bytes())).unwrap();
    }
    e.force_flush().unwrap();
    e.drain_compaction().unwrap();
    let after = e.estimate_disk_bytes();
    assert!(
        after < before,
        "expected tombstone GC to shrink on-disk bytes: before={before} after={after}"
    );
}
