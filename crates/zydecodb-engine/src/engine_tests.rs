use super::*;
use tempfile::TempDir;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![keys::KS_USER];
    v.extend_from_slice(k);
    v
}

fn open_engine(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
}

#[test]
fn put_get_del_roundtrip() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);
    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    assert_eq!(e.get(&uk(b"a")).unwrap(), Some(b"1".to_vec()));
    assert!(e.del(uk(b"a")).unwrap().0);
    assert_eq!(e.get(&uk(b"a")).unwrap(), None);
}

#[test]
fn reserved_keyspace_rejected() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);
    let sys_key = vec![keys::KS_SYSTEM, b'x'];
    assert!(e.put(sys_key, b"v".to_vec(), 0).is_err());
}

#[test]
fn recovery_restores_unflushed_data() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = open_engine(&dir);
        for i in 0..100u32 {
            let key = uk(format!("k{:03}", i).as_bytes());
            e.put(key, format!("v{}", i).into_bytes(), 0).unwrap();
        }
        // drop without flushing -> data only in WAL
    }
    let e = open_engine(&dir);
    for i in 0..100u32 {
        let key = uk(format!("k{:03}", i).as_bytes());
        assert_eq!(
            e.get(&key).unwrap(),
            Some(format!("v{}", i).into_bytes()),
            "key {} lost",
            i
        );
    }
}

#[test]
fn shutdown_writes_marker_and_open_consumes_it() {
    let dir = TempDir::new().unwrap();
    let marker = dir.path().join("data").join(CLEAN_SHUTDOWN_MARKER);

    {
        let mut e = open_engine(&dir);
        assert!(!e.was_clean_shutdown(), "fresh dir is not a clean boot");
        e.put(uk(b"k"), b"v".to_vec(), 0).unwrap();
        e.shutdown().unwrap();
        assert!(marker.exists(), "marker must be written on shutdown");
    }

    // Reopen: marker present -> clean boot, then consumed.
    let e = open_engine(&dir);
    assert!(e.was_clean_shutdown(), "reopen after shutdown is clean");
    assert!(!marker.exists(), "marker must be consumed on open");
    assert_eq!(e.get(&uk(b"k")).unwrap(), Some(b"v".to_vec()));

    // A second reopen without a shutdown in between is NOT clean.
    drop(e);
    let e2 = open_engine(&dir);
    assert!(
        !e2.was_clean_shutdown(),
        "reopen without prior shutdown is unclean"
    );
}

#[test]
fn policy_can_reject_and_record() {
    use crate::policy::WritePolicy;
    use std::sync::atomic::{AtomicUsize, Ordering};

    // A trivial policy: reject any key whose raw suffix is "deny", and count
    // post_write calls. Proves the engine consults the hook without knowing
    // anything about what the policy enforces.
    struct TestPolicy {
        writes: AtomicUsize,
    }
    impl WritePolicy for TestPolicy {
        fn pre_write(
            &self,
            _engine: &mut Engine,
            key: &[u8],
            _value_len: usize,
            _existing: Option<usize>,
            _is_delete: bool,
        ) -> EngineResult<()> {
            if key.ends_with(b"deny") {
                return Err(EngineError::PolicyRejected("denied by test".into()));
            }
            Ok(())
        }
        fn post_write(
            &self,
            _engine: &mut Engine,
            _key: &[u8],
            _value_len: usize,
            _existing: Option<usize>,
            _is_delete: bool,
        ) {
            self.writes.fetch_add(1, Ordering::Relaxed);
        }
    }

    let dir = TempDir::new().unwrap();
    let policy = Arc::new(TestPolicy {
        writes: AtomicUsize::new(0),
    });
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
    .with_write_policy(policy.clone());

    e.put(uk(b"ok"), b"1".to_vec(), 0).unwrap();
    let err = e.put(uk(b"deny"), b"2".to_vec(), 0).unwrap_err();
    assert!(matches!(err, EngineError::PolicyRejected(_)));
    // The denied write must not have persisted.
    assert_eq!(e.get(&uk(b"deny")).unwrap(), None);
    // Only the allowed put reached post_write.
    assert_eq!(policy.writes.load(Ordering::Relaxed), 1);
}

#[test]
fn shutdown_ships_active_segment_bit_identical() {
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
    .with_shipping(Some(ship.clone()), crate::shipping::ShipMode::Copy);

    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
    let active_id = e.active_wal_id;
    e.shutdown().unwrap();

    // The shipped segment must exist and be byte-identical to the live file.
    let name = wal::segment_filename(active_id);
    let live = dir.path().join("data/wal").join(&name);
    let shipped = ship.join(&name);
    assert!(
        shipped.exists(),
        "active segment must be shipped on shutdown"
    );
    assert_eq!(
        std::fs::read(&live).unwrap(),
        std::fs::read(&shipped).unwrap(),
        "shipped bytes must match live segment exactly"
    );

    // shipped.log records the segment.
    let log = std::fs::read_to_string(ship.join(crate::shipping::SHIPPED_LOG)).unwrap();
    assert!(log.contains(&format!("{} ", active_id)), "log: {}", log);
}

#[test]
fn flush_then_recover_reads_from_sstable() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = open_engine(&dir);
        e.put(uk(b"persist"), b"yes".to_vec(), 0).unwrap();
        e.force_flush().unwrap();
        assert_eq!(e.sstable_count(), 1);
    }
    let e = open_engine(&dir);
    assert_eq!(e.sstable_count(), 1);
    assert_eq!(e.get(&uk(b"persist")).unwrap(), Some(b"yes".to_vec()));
}

#[test]
fn overwrite_after_flush_returns_new_value() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);
    e.put(uk(b"k"), b"old".to_vec(), 0).unwrap();
    e.force_flush().unwrap();
    e.put(uk(b"k"), b"new".to_vec(), 0).unwrap();
    assert_eq!(e.get(&uk(b"k")).unwrap(), Some(b"new".to_vec()));
}

#[test]
fn delete_after_flush_returns_not_found() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);
    e.put(uk(b"gone"), b"here".to_vec(), 0).unwrap();
    e.force_flush().unwrap();
    e.del(uk(b"gone")).unwrap();
    assert_eq!(e.get(&uk(b"gone")).unwrap(), None);
}

#[test]
fn delete_then_reflush_stays_deleted() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = open_engine(&dir);
        e.put(uk(b"gone"), b"here".to_vec(), 0).unwrap();
        e.force_flush().unwrap();
        e.del(uk(b"gone")).unwrap();
        e.force_flush().unwrap();
        assert_eq!(e.get(&uk(b"gone")).unwrap(), None);
    }
    let e = open_engine(&dir);
    assert_eq!(e.get(&uk(b"gone")).unwrap(), None);
}

#[test]
fn expiry_makes_key_disappear() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);
    let past = 1; // 1 ms after epoch, definitely expired
    e.put(uk(b"temp"), b"v".to_vec(), past).unwrap();
    assert_eq!(e.get(&uk(b"temp")).unwrap(), None);
}

#[test]
fn seq_monotonic_across_restart() {
    let dir = TempDir::new().unwrap();
    let seq_after_first;
    {
        let mut e = open_engine(&dir);
        e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
        e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
        seq_after_first = e.seq_peek();
    }
    let e = open_engine(&dir);
    assert!(
        e.seq_peek() >= seq_after_first,
        "seq must not regress: {} vs {}",
        e.seq_peek(),
        seq_after_first
    );
}

#[test]
fn sealed_segment_max_seq_cache_drives_truncation() {
    // Validates the wal_segments_covered fast path: writes across three
    // segments, asserts the cache holds the right max_seq per sealed
    // segment, then flushes and asserts the cache shrinks because covered
    // segments were unlinked. Previously wal_segments_covered re-read
    // every segment on every flush; this is the regression guard for that
    // bug.
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);

    // Segment 1: two writes, then force seal. The cached max_seq for the
    // sealed segment must equal the seq of the last write in it.
    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
    let seq_after_seg1 = e.seq_peek().saturating_sub(1);
    let seg1_id = e.active_wal_id;
    e.force_roll_wal_for_test().unwrap();

    // Segment 2.
    e.put(uk(b"c"), b"3".to_vec(), 0).unwrap();
    let seq_after_seg2 = e.seq_peek().saturating_sub(1);
    let seg2_id = e.active_wal_id;
    e.force_roll_wal_for_test().unwrap();

    // Segment 3 is the new active; nothing in it yet.
    let snap = e.sealed_segment_max_seq_snapshot();
    assert_eq!(
        snap,
        vec![(seg1_id, seq_after_seg1), (seg2_id, seq_after_seg2)],
        "cache must hold max seq for both sealed segments"
    );

    // Flush: this calls wal_segments_covered, which must use the cache and
    // unlink seg1 + seg2. After unlink the cache must be empty.
    e.put(uk(b"d"), b"4".to_vec(), 0).unwrap();
    let active_before_flush = e.active_wal_id;
    e.force_flush().unwrap();
    assert_eq!(
        e.sealed_segment_max_seq_snapshot(),
        vec![],
        "covered sealed segments must be unlinked AND dropped from the cache"
    );
    // Active segment id must not change across flush; flush does not roll.
    assert_eq!(e.active_wal_id, active_before_flush);
}

#[test]
fn write_batch_applies_all_ops() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);
    e.write_batch(vec![
        BatchOp::Put {
            key: uk(b"a"),
            value: b"1".to_vec(),
            expires_at: 0,
        },
        BatchOp::Put {
            key: uk(b"b"),
            value: b"2".to_vec(),
            expires_at: 0,
        },
    ])
    .unwrap();
    assert_eq!(e.get(&uk(b"a")).unwrap(), Some(b"1".to_vec()));
    assert_eq!(e.get(&uk(b"b")).unwrap(), Some(b"2".to_vec()));
}

#[test]
fn write_batch_rejects_duplicate_keys() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);
    let err = e
        .write_batch(vec![
            BatchOp::Put {
                key: uk(b"dup"),
                value: b"1".to_vec(),
                expires_at: 0,
            },
            BatchOp::Put {
                key: uk(b"dup"),
                value: b"2".to_vec(),
                expires_at: 0,
            },
        ])
        .unwrap_err();
    assert!(matches!(err, EngineError::InvalidKey(_)));
    // Rejected before any mutation: nothing persisted.
    assert_eq!(e.get(&uk(b"dup")).unwrap(), None);
}

#[test]
fn write_batch_empty_is_rejected() {
    let dir = TempDir::new().unwrap();
    let mut e = open_engine(&dir);
    assert!(e.write_batch(vec![]).is_err());
}

#[test]
fn write_batch_policy_rejection_persists_nothing() {
    use crate::policy::WritePolicy;

    // Rejects any key whose raw suffix is "deny".
    struct DenyPolicy;
    impl WritePolicy for DenyPolicy {
        fn pre_write(
            &self,
            _engine: &mut Engine,
            key: &[u8],
            _value_len: usize,
            _existing: Option<usize>,
            _is_delete: bool,
        ) -> EngineResult<()> {
            if key.ends_with(b"deny") {
                return Err(EngineError::PolicyRejected("denied".into()));
            }
            Ok(())
        }
        fn post_write(
            &self,
            _engine: &mut Engine,
            _key: &[u8],
            _value_len: usize,
            _existing: Option<usize>,
            _is_delete: bool,
        ) {
        }
    }

    let dir = TempDir::new().unwrap();
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
    .with_write_policy(Arc::new(DenyPolicy));

    let err = e
        .write_batch(vec![
            BatchOp::Put {
                key: uk(b"ok"),
                value: b"1".to_vec(),
                expires_at: 0,
            },
            BatchOp::Put {
                key: uk(b"deny"),
                value: b"2".to_vec(),
                expires_at: 0,
            },
        ])
        .unwrap_err();
    assert!(matches!(err, EngineError::PolicyRejected(_)));
    // The gate runs fully before any mutation: even the op preceding the
    // rejected one must not have persisted.
    assert_eq!(e.get(&uk(b"ok")).unwrap(), None);
    assert_eq!(e.get(&uk(b"deny")).unwrap(), None);
}

#[test]
fn write_batch_clean_recovery_replays_all() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = open_engine(&dir);
        e.put(uk(b"old"), b"x".to_vec(), 0).unwrap();
        e.write_batch(vec![
            BatchOp::Put {
                key: uk(b"a"),
                value: b"1".to_vec(),
                expires_at: 0,
            },
            BatchOp::Put {
                key: uk(b"b"),
                value: b"2".to_vec(),
                expires_at: 0,
            },
            BatchOp::Del { key: uk(b"old") },
        ])
        .unwrap();
        // drop without flush -> data only in WAL
    }
    let e = open_engine(&dir);
    assert_eq!(e.get(&uk(b"a")).unwrap(), Some(b"1".to_vec()));
    assert_eq!(e.get(&uk(b"b")).unwrap(), Some(b"2".to_vec()));
    assert_eq!(e.get(&uk(b"old")).unwrap(), None);
}

#[test]
fn write_batch_torn_recovery_is_all_or_nothing() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = open_engine(&dir);
        e.put(uk(b"keep"), b"v".to_vec(), 0).unwrap();
        e.write_batch(vec![
            BatchOp::Put {
                key: uk(b"a"),
                value: b"1".to_vec(),
                expires_at: 0,
            },
            BatchOp::Put {
                key: uk(b"b"),
                value: b"2".to_vec(),
                expires_at: 0,
            },
            BatchOp::Put {
                key: uk(b"c"),
                value: b"3".to_vec(),
                expires_at: 0,
            },
        ])
        .unwrap();
        e.sync_wal().unwrap();
        // drop without flush -> committed PUT + batch live in the WAL
    }

    // Simulate a torn batch write: truncate the tail of the latest WAL
    // segment so the single batch record is incomplete. The committed PUT
    // before it stays intact.
    let wal_dir = dir.path().join("data/wal");
    let mut segs: Vec<std::path::PathBuf> = std::fs::read_dir(&wal_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "log").unwrap_or(false))
        .collect();
    segs.sort();
    let last = segs.last().unwrap();
    let len = std::fs::metadata(last).unwrap().len();
    let f = OpenOptions::new().write(true).open(last).unwrap();
    f.set_len(len - 3).unwrap();
    f.sync_all().unwrap();

    let e = open_engine(&dir);
    // The committed single PUT survives; NO op from the torn batch replays.
    assert_eq!(e.get(&uk(b"keep")).unwrap(), Some(b"v".to_vec()));
    assert_eq!(e.get(&uk(b"a")).unwrap(), None);
    assert_eq!(e.get(&uk(b"b")).unwrap(), None);
    assert_eq!(e.get(&uk(b"c")).unwrap(), None);
}

#[test]
fn recovery_rebuilds_sealed_segment_cache() {
    // Pre-create two sealed segments + one active with unflushed data,
    // crash (drop), and assert reopen rebuilds the cache from the WAL
    // scan that already happens during replay.
    let dir = TempDir::new().unwrap();
    let (sealed1, max1, sealed2, max2) = {
        let mut e = open_engine(&dir);
        e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
        let s1 = e.active_wal_id;
        let m1 = e.seq_peek().saturating_sub(1);
        e.force_roll_wal_for_test().unwrap();
        e.put(uk(b"b"), b"2".to_vec(), 0).unwrap();
        let s2 = e.active_wal_id;
        let m2 = e.seq_peek().saturating_sub(1);
        e.force_roll_wal_for_test().unwrap();
        // leave a write in the new active too, no flush
        e.put(uk(b"c"), b"3".to_vec(), 0).unwrap();
        (s1, m1, s2, m2)
    };
    let e = open_engine(&dir);
    let snap = e.sealed_segment_max_seq_snapshot();
    // The just-active segment from the prior process is now sealed too,
    // but the test only pins the two we explicitly rolled.
    assert!(
        snap.iter().any(|&(id, m)| id == sealed1 && m == max1),
        "seg1 missing from rebuilt cache: {:?}",
        snap
    );
    assert!(
        snap.iter().any(|&(id, m)| id == sealed2 && m == max2),
        "seg2 missing from rebuilt cache: {:?}",
        snap
    );
}
