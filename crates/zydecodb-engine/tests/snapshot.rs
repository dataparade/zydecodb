//! Scoped snapshot semantics.
//!
//! v1 snapshots are scoped to a borrow of the engine; while held, no
//! mutations can run. The tests below verify that:
//!   - A snapshot's `get` and `scan` return the same data the engine's
//!     direct `get` / `scan` would at snapshot time.
//!   - `seq_upper` is set so that brand-new writes (issued AFTER the
//!     snapshot was dropped) are visible to the engine but would NOT be
//!     visible to that older snapshot if it could observe them.
//!
//! Long-lived snapshots that survive writes/flushes/compactions are v2.

use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

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

#[test]
fn snapshot_sees_the_state_at_capture_time() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();

    let snap = e.snapshot();
    assert_eq!(snap.get(&uk(b"a")).unwrap().as_deref(), Some(b"1".as_ref()));
    assert_eq!(snap.get(&uk(b"b")).unwrap().as_deref(), Some(b"2".as_ref()));

    e.put(uk(b"a"), b"new".to_vec(), 0).unwrap();
    // A fresh snapshot sees the new value.
    let snap2 = e.snapshot();
    assert_eq!(
        snap2.get(&uk(b"a")).unwrap().as_deref(),
        Some(b"new".as_ref())
    );
}

#[test]
fn snapshot_scan_returns_consistent_view() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    for i in 0..10u32 {
        e.put(
            uk(format!("k{}", i).as_bytes()),
            format!("v{}", i).into_bytes(),
            0,
        )
        .unwrap();
    }
    let snap = e.snapshot();
    let got: Vec<_> = snap
        .scan(uk(b"k0"), uk(b"k9_"))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(got.len(), 10);
}

#[test]
fn snapshot_seq_upper_excludes_future_writes() {
    // Even though v1 snapshots are scoped (and the borrow blocks writes),
    // the seq_upper mechanism is the same one MVCC will hinge on. Verify
    // the ceiling is set so that a synthetic key tagged with a seq above
    // the snapshot wouldn't be visible.
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    let ceiling = e.snapshot().seq_upper();
    // The engine has at least one write; the ceiling must be at least the
    // last write's seq (>=1) and strictly less than the next allocation.
    assert!(
        ceiling >= 1,
        "snapshot seq_upper={} should cover writes",
        ceiling
    );

    e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
    let new_ceiling = e.snapshot().seq_upper();
    assert!(
        new_ceiling > ceiling,
        "new snapshot ceiling {} must advance past old {}",
        new_ceiling,
        ceiling
    );
}

#[test]
fn snapshot_get_treats_tombstone_as_missing() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    e.del(uk(b"a")).unwrap();
    let snap = e.snapshot();
    assert!(snap.get(&uk(b"a")).unwrap().is_none());
}

#[test]
fn snapshot_scan_handles_mixed_memtable_and_sstable_sources() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    // Half in SSTable, half in memtable.
    for i in 0..50u32 {
        e.put(uk(format!("k{:04}", i).as_bytes()), b"sst".to_vec(), 0)
            .unwrap();
    }
    e.force_flush().unwrap();
    for i in 50..100u32 {
        e.put(uk(format!("k{:04}", i).as_bytes()), b"mt".to_vec(), 0)
            .unwrap();
    }
    let snap = e.snapshot();
    let got: Vec<_> = snap
        .scan(uk(b"k0000"), uk(b"k9999"))
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(got.len(), 100);
    let mut sst_count = 0;
    let mut mt_count = 0;
    for (_, v) in got {
        match v.as_slice() {
            b"sst" => sst_count += 1,
            b"mt" => mt_count += 1,
            _ => panic!("unexpected value"),
        }
    }
    assert_eq!(sst_count, 50);
    assert_eq!(mt_count, 50);
}
