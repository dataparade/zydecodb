//! Tests for the multi-tenant pods primitives: the data_dir lock, prefix delete
//! (tenant offboarding), base snapshots, and WAL-replay-ceiling PITR.

use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::errors::EngineError;
use zydecodb_engine::keys::KS_USER;

fn cfg(dir: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_dir: dir.join("data"),
        wal_dir: dir.join("wal"),
        block_cache_bytes: 8 * 1024 * 1024,
        max_open_readers: 16,
        ..Default::default()
    }
}

/// A user-keyspace key (engine writes require the `KS_USER` prefix byte).
fn uk(parts: &[u8]) -> Vec<u8> {
    let mut k = vec![KS_USER];
    k.extend_from_slice(parts);
    k
}

#[test]
fn data_dir_lock_excludes_second_open() {
    let tmp = tempfile::TempDir::new().unwrap();
    let engine = Engine::open(cfg(tmp.path())).unwrap();

    // A second open of the same data_dir while the first is alive must fail.
    match Engine::open(cfg(tmp.path())) {
        Err(EngineError::Locked(_)) => {}
        Err(e) => panic!("expected Locked, got {e:?}"),
        Ok(_) => panic!("expected the second open to be rejected by the lock"),
    }

    // After dropping the first, the lock is released and open succeeds again.
    drop(engine);
    let _reopened = Engine::open(cfg(tmp.path())).unwrap();
}

#[test]
fn delete_prefix_removes_only_matching_keys() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut engine = Engine::open(cfg(tmp.path())).unwrap();

    engine.put(uk(b"a:1"), b"x".to_vec(), 0).unwrap();
    engine.put(uk(b"a:2"), b"y".to_vec(), 0).unwrap();
    engine.put(uk(b"b:1"), b"z".to_vec(), 0).unwrap();

    let deleted = engine.delete_prefix(uk(b"a:")).unwrap();
    assert_eq!(deleted, 2);

    assert_eq!(engine.get(&uk(b"a:1")).unwrap(), None);
    assert_eq!(engine.get(&uk(b"a:2")).unwrap(), None);
    assert_eq!(engine.get(&uk(b"b:1")).unwrap(), Some(b"z".to_vec()));
}

#[test]
fn snapshot_is_a_consistent_readable_base() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut engine = Engine::open(cfg(tmp.path())).unwrap();
    engine.put(uk(b"k1"), b"v1".to_vec(), 0).unwrap();
    engine.put(uk(b"k2"), b"v2".to_vec(), 0).unwrap();

    let snap = tmp.path().join("snap");
    let snapshot_seq = engine.snapshot_to(&snap).unwrap();
    assert_eq!(snapshot_seq, engine.current_seq());
    drop(engine);

    // Open the snapshot directory directly as a data_dir (with a fresh, empty
    // WAL): the captured state must be fully readable at the snapshot sequence.
    let restored = Engine::open(EngineConfig {
        data_dir: snap,
        wal_dir: tmp.path().join("snap-wal"),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(restored.get(&uk(b"k1")).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(restored.get(&uk(b"k2")).unwrap(), Some(b"v2".to_vec()));
    assert_eq!(restored.current_seq(), snapshot_seq);
}

#[test]
fn wal_replay_ceiling_restores_to_a_point_in_time() {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut base = cfg(tmp.path());
    // Inline fsync so each write is durable in the WAL before we drop the engine.
    let mut engine = Engine::open(base.clone()).unwrap().with_group_commit(false);
    let _s1 = engine.put(uk(b"k1"), b"v1".to_vec(), 0).unwrap();
    let s2 = engine.put(uk(b"k2"), b"v2".to_vec(), 0).unwrap();
    let _s3 = engine.put(uk(b"k3"), b"v3".to_vec(), 0).unwrap();
    // Drop without a clean shutdown so the WAL is replayed on the next open.
    drop(engine);

    // Reopen capped at s2: k1 and k2 replay, k3 (seq > s2) is dropped.
    base.wal_replay_max_seq = Some(s2);
    let restored = Engine::open(base).unwrap();
    assert_eq!(restored.get(&uk(b"k1")).unwrap(), Some(b"v1".to_vec()));
    assert_eq!(restored.get(&uk(b"k2")).unwrap(), Some(b"v2".to_vec()));
    assert_eq!(restored.get(&uk(b"k3")).unwrap(), None);
    assert_eq!(restored.current_seq(), s2);
}
