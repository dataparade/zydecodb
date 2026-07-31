//! PITR via admin restore: exact `--to-seq` and best-effort `--to-time`.
//!
//! Base snapshot is taken *before* the post-snapshot WAL history so the replay
//! ceiling (not already-flushed SSTables) determines restored visibility.

use tempfile::TempDir;
use zydecodb::admin;
use zydecodb_engine::engine::{Engine, EngineConfig, RESTORE_IN_PROGRESS_MARKER};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::shipping::{self, ShipMode};

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn open_ship(dir: &TempDir, ship: &std::path::Path) -> Engine {
    std::fs::create_dir_all(ship).unwrap();
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
    .with_shipping(Some(ship.to_path_buf()), ShipMode::Copy)
    .with_group_commit(false)
}

#[test]
fn restore_to_seq_lands_on_exact_sequence() {
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let snap = dir.path().join("snap");
    let out = dir.path().join("restored");

    let mut e = open_ship(&dir, &ship);
    e.put(uk(b"seed"), b"0".to_vec(), 0).unwrap();
    e.sync_wal().unwrap();
    e.force_flush().unwrap();
    e.snapshot_to(&snap).unwrap();

    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    let s_a = e.current_seq();
    e.force_roll_wal_for_test().unwrap();

    e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
    let s_b = e.current_seq();
    e.force_roll_wal_for_test().unwrap();

    e.put(uk(b"c"), b"3".to_vec(), 0).unwrap();
    let _s_c = e.current_seq();
    e.force_roll_wal_for_test().unwrap();
    e.shutdown().unwrap();

    admin::restore(&snap, &ship, Some(s_b), None, &out).expect("restore to-seq");

    let restored = Engine::open(EngineConfig {
        data_dir: out.clone(),
        wal_dir: out.join("wal"),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(restored.current_seq(), s_b, "exact to-seq must land on s_b");
    assert_eq!(restored.get(&uk(b"seed")).unwrap(), Some(b"0".to_vec()));
    assert_eq!(restored.get(&uk(b"a")).unwrap(), Some(b"1".to_vec()));
    assert_eq!(restored.get(&uk(b"b")).unwrap(), Some(b"2".to_vec()));
    assert_eq!(
        restored.get(&uk(b"c")).unwrap(),
        None,
        "seq > to_seq must not appear"
    );
    assert!(s_a < s_b);
}

#[test]
fn restore_clears_marker_and_wipes_ceiling_wal() {
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let snap = dir.path().join("snap");
    let out = dir.path().join("restored");

    let mut e = open_ship(&dir, &ship);
    e.put(uk(b"seed"), b"0".to_vec(), 0).unwrap();
    e.sync_wal().unwrap();
    e.force_flush().unwrap();
    e.snapshot_to(&snap).unwrap();

    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    e.force_roll_wal_for_test().unwrap();
    e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
    let s_b = e.current_seq();
    e.force_roll_wal_for_test().unwrap();
    e.shutdown().unwrap();

    // Stale junk in the output WAL dir must be wiped, not silently kept.
    std::fs::create_dir_all(out.join("wal")).unwrap();
    std::fs::write(out.join("wal").join("wal-00000099.log"), b"stale").unwrap();

    admin::restore(&snap, &ship, Some(s_b), None, &out).expect("restore");

    assert!(
        !out.join(RESTORE_IN_PROGRESS_MARKER).exists(),
        "completed restore must remove the in-progress marker"
    );
    let leftovers: Vec<_> = std::fs::read_dir(out.join("wal"))
        .unwrap()
        .flatten()
        .filter(|e| e.file_name().to_string_lossy().starts_with("wal-"))
        .collect();
    assert!(
        leftovers.is_empty(),
        "ceiling restore must wipe installed WAL segments: {leftovers:?}"
    );

    let restored = Engine::open(EngineConfig {
        data_dir: out.clone(),
        wal_dir: out.join("wal"),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(restored.current_seq(), s_b);
}

#[test]
fn restore_to_time_is_best_effort_via_timeindex() {
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let snap = dir.path().join("snap");
    let out = dir.path().join("restored");
    let t0 = 1_700_000_000_000u64;

    let mut e = open_ship(&dir, &ship);
    e.put(uk(b"seed"), b"0".to_vec(), 0).unwrap();
    e.sync_wal().unwrap();
    e.force_flush().unwrap();
    e.snapshot_to(&snap).unwrap();

    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    let s_a = e.current_seq();
    e.force_roll_wal_for_test().unwrap();
    shipping::append_timeindex(&ship, t0, s_a).unwrap();

    e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
    let s_b = e.current_seq();
    e.force_roll_wal_for_test().unwrap();
    shipping::append_timeindex(&ship, t0 + 5_000, s_b).unwrap();

    e.put(uk(b"c"), b"3".to_vec(), 0).unwrap();
    let s_c = e.current_seq();
    e.force_roll_wal_for_test().unwrap();
    shipping::append_timeindex(&ship, t0 + 10_000, s_c).unwrap();
    e.shutdown().unwrap();

    let resolved = shipping::resolve_seq_at_or_before(&ship, t0 + 6_000)
        .unwrap()
        .expect("timeindex sample");
    assert_eq!(resolved, s_b);

    admin::restore(&snap, &ship, None, Some(t0 + 6_000), &out).expect("restore to-time");

    let restored = Engine::open(EngineConfig {
        data_dir: out.clone(),
        wal_dir: out.join("wal"),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(restored.current_seq(), s_b);
    assert!(restored.get(&uk(b"b")).unwrap().is_some());
    assert!(restored.get(&uk(b"c")).unwrap().is_none());
}
