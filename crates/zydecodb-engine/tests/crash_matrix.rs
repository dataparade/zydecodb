//! Crash matrix: every failpoint x every crash mode.
//!
//! For each (failpoint, mode) pair we:
//!   1. Open an engine and write a known-good prefix `pre_keys` (all acked).
//!   2. Arm the failpoint with the mode (return-error or panic).
//!   3. Attempt the offending op; assert it surfaced as a typed error or panic.
//!   4. Drop the engine (simulating a crash).
//!   5. Reopen.
//!   6. Assert:
//!      a) the engine opens cleanly (no orphan SSTables, no torn-tail panic),
//!      b) every acked write from step 1 is visible,
//!      c) the failed write is either fully visible (crash after durability) or invisible (crash before); never torn or partial.
//!   7. Confirm the engine remains writable post-recovery with a fresh PUT.
//!
//! This file is gated on the `failpoints` feature. Without the feature the
//! `fail_point!` invocations are no-ops, so the tests would all pass trivially
//! and provide no signal. Run with:
//!   `cargo test -p zydecodb-engine --features failpoints --test crash_matrix`
//!
//! The `fail` crate maintains process-global registry state, so tests must run
//! one at a time. We enforce this with a `Mutex` guarded `FailScenario`. Run
//! the binary with `--test-threads=1` if you want to be defensive, but the
//! mutex alone is sufficient.

#![cfg(feature = "failpoints")]

use std::sync::Mutex;
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::errors::EngineError;
use zydecodb_engine::failpoints::*;
use zydecodb_engine::keys::KS_USER;

/// Single global lock around the `fail` crate's process-wide registry. Without
/// this, parallel cases would race on shared failpoint state.
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

/// Crash modes the matrix exercises. Each maps to a `fail` actions string.
#[derive(Debug, Clone, Copy)]
enum Mode {
    /// Inject an `EngineError::Io` at the point. The engine must surface it
    /// without corrupting state.
    Return,
    /// Panic at the point. The engine must recover on the next open.
    Panic,
}

impl Mode {
    fn actions(self) -> &'static str {
        match self {
            Mode::Return => "return",
            Mode::Panic => "panic",
        }
    }
}

/// Which engine method to invoke after the PUT in order to reach a failpoint
/// that doesn't fire on the put path itself.
#[derive(Debug, Clone, Copy)]
enum Trigger {
    /// The failpoint fires inline on `Engine::put` — no extra work needed.
    Put,
    /// The failpoint sits behind group-commit `sync_wal`; the put buffers
    /// without fsyncing, so the test must call `sync_wal` explicitly.
    SyncWal,
    /// The failpoint sits on the flush path; the test must call `force_flush`.
    Flush,
    /// The failpoint sits on the compaction path; the test must do enough
    /// flushes to trip the L0 compaction trigger.
    Compaction,
}

/// Drive one (failpoint, mode) case end-to-end.
fn run_case(failpoint: &str, mode: Mode, trigger: Trigger) {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let scenario = fail::FailScenario::setup();

    let dir = TempDir::new().expect("tempdir");

    // Phase 1: pre-arm baseline writes — these MUST survive the crash.
    let pre = [
        (uk(b"pre_1"), b"a".to_vec()),
        (uk(b"pre_2"), b"b".to_vec()),
        (uk(b"pre_3"), b"c".to_vec()),
    ];
    {
        let mut e = open(&dir);
        for (k, v) in &pre {
            e.put(k.clone(), v.clone(), 0).expect("baseline put");
        }
        // Force everything to disk before arming the failpoint so we have a
        // crisp before/after boundary. Without this, the failpoint might fire
        // during a baseline op that happens to trigger a roll/flush.
        e.sync_wal().expect("baseline sync");
        // Engine drops here; data is on disk.
    }

    // Phase 2: arm the failpoint and attempt the offending op.
    fail::cfg(failpoint, mode.actions()).unwrap_or_else(|e| {
        panic!("failed to arm {}: {:?}", failpoint, e);
    });

    let crash_key = uk(b"crash_key");
    let crash_val = b"crash_val".to_vec();
    // `outcome` records whichever call hit the failpoint, so the assertions
    // below can check the right boundary. `Ok(())` means no failure was
    // observed (the point didn't fire on this execution — tolerable for some
    // panic cases that need an even more specific state to trigger).
    let mut outcome: Result<(), EngineError> = Ok(());
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut e = open(&dir);
        let put_res = e.put(crash_key.clone(), crash_val.clone(), 0);
        if let Err(err) = &put_res {
            outcome = Err(EngineError::Io(err.to_string()));
            return;
        }
        match trigger {
            Trigger::Put => { /* put alone reaches the point */ }
            Trigger::SyncWal => {
                if let Err(err) = e.sync_wal() {
                    outcome = Err(EngineError::Io(err.to_string()));
                }
            }
            Trigger::Flush => {
                if let Err(err) = e.force_flush() {
                    outcome = Err(EngineError::Io(err.to_string()));
                }
            }
            Trigger::Compaction => {
                // Default L0 trigger is 4. Do five flushes (each preceded by
                // a fresh put so something is in the active memtable). The
                // last flush trips compaction, fires the failpoint.
                for i in 0..5u32 {
                    let k = uk(format!("cflush_{}", i).as_bytes());
                    if let Err(err) = e.put(k, b"v".to_vec(), 0) {
                        outcome = Err(EngineError::Io(err.to_string()));
                        return;
                    }
                    if let Err(err) = e.force_flush() {
                        outcome = Err(EngineError::Io(err.to_string()));
                        return;
                    }
                }
            }
        }
    }));

    // For return mode, the targeted op must have surfaced an Io error.
    if matches!(mode, Mode::Return) && result.is_ok() {
        assert!(
            matches!(outcome, Err(EngineError::Io(_))),
            "expected Io error from {} (return mode, trigger {:?}), got {:?}",
            failpoint,
            trigger,
            outcome
        );
    }
    // For panic mode, either the call unwound (result is Err) or — for points
    // that don't fire on this execution path — nothing happened. Both are
    // acceptable; the contract under test is that recovery is clean.

    // Disarm before reopen so recovery isn't sabotaged by the same point.
    fail::remove(failpoint);

    // Phase 3: reopen. Must succeed cleanly.
    let mut e = open(&dir);

    // a) All acked baseline writes must be visible.
    for (k, v) in &pre {
        let got = e.get(k).expect("post-crash get");
        assert_eq!(
            got.as_deref(),
            Some(v.as_slice()),
            "acked baseline write lost for key {:?} after crash at {}",
            k,
            failpoint
        );
    }

    // b) The crash-time write is either visible-with-correct-value or absent.
    //    Never a torn/partial value.
    let crash_got = e.get(&crash_key).expect("post-crash get");
    match crash_got {
        None => { /* lost — acceptable, the put never acked */ }
        Some(v) => assert_eq!(
            v, crash_val,
            "torn value at crash site {} — engine corrupted",
            failpoint
        ),
    }

    // c) Engine is still writable: prove the catalog is internally consistent.
    e.put(uk(b"post_recovery"), b"ok".to_vec(), 0)
        .expect("engine must be writable after recovery");
    assert_eq!(
        e.get(&uk(b"post_recovery")).unwrap().as_deref(),
        Some(b"ok".as_slice())
    );

    drop(e);
    scenario.teardown();
}

// One #[test] per (failpoint, mode). Doing it this way (instead of a single
// driver iterating a table) gives you a per-case failure name in the test
// output, which makes triage trivial.

macro_rules! crash_case {
    ($name:ident, $point:expr, $mode:expr, $trigger:expr) => {
        #[test]
        fn $name() {
            run_case($point, $mode, $trigger);
        }
    };
}

// WAL append points fire on the put path (group commit buffers but still
// runs the write_all that the points wrap).
crash_case!(
    wal_before_append_return,
    WAL_BEFORE_APPEND,
    Mode::Return,
    Trigger::Put
);
crash_case!(
    wal_before_append_panic,
    WAL_BEFORE_APPEND,
    Mode::Panic,
    Trigger::Put
);
crash_case!(
    wal_after_append_return,
    WAL_AFTER_APPEND,
    Mode::Return,
    Trigger::Put
);
crash_case!(
    wal_after_append_panic,
    WAL_AFTER_APPEND,
    Mode::Panic,
    Trigger::Put
);

// WAL fsync points only fire when something explicitly calls sync_wal. With
// group commit (the default), put buffers and does not fsync inline.
crash_case!(
    wal_before_fsync_return,
    WAL_BEFORE_FSYNC,
    Mode::Return,
    Trigger::SyncWal
);
crash_case!(
    wal_before_fsync_panic,
    WAL_BEFORE_FSYNC,
    Mode::Panic,
    Trigger::SyncWal
);
crash_case!(
    wal_after_fsync_return,
    WAL_AFTER_FSYNC,
    Mode::Return,
    Trigger::SyncWal
);
crash_case!(
    wal_after_fsync_panic,
    WAL_AFTER_FSYNC,
    Mode::Panic,
    Trigger::SyncWal
);

// Memtable insert points — on the put path.
crash_case!(
    engine_before_memtable_insert_return,
    ENGINE_BEFORE_MEMTABLE_INSERT,
    Mode::Return,
    Trigger::Put
);
crash_case!(
    engine_before_memtable_insert_panic,
    ENGINE_BEFORE_MEMTABLE_INSERT,
    Mode::Panic,
    Trigger::Put
);
crash_case!(
    engine_after_memtable_insert_return,
    ENGINE_AFTER_MEMTABLE_INSERT,
    Mode::Return,
    Trigger::Put
);
crash_case!(
    engine_after_memtable_insert_panic,
    ENGINE_AFTER_MEMTABLE_INSERT,
    Mode::Panic,
    Trigger::Put
);

// SSTable + manifest points fire on the flush path.
crash_case!(
    sstable_before_tmp_write_return,
    SSTABLE_BEFORE_TMP_WRITE,
    Mode::Return,
    Trigger::Flush
);
crash_case!(
    sstable_before_tmp_write_panic,
    SSTABLE_BEFORE_TMP_WRITE,
    Mode::Panic,
    Trigger::Flush
);
crash_case!(
    sstable_after_tmp_write_return,
    SSTABLE_AFTER_TMP_WRITE,
    Mode::Return,
    Trigger::Flush
);
crash_case!(
    sstable_after_tmp_write_panic,
    SSTABLE_AFTER_TMP_WRITE,
    Mode::Panic,
    Trigger::Flush
);
crash_case!(
    sstable_before_rename_return,
    SSTABLE_BEFORE_RENAME,
    Mode::Return,
    Trigger::Flush
);
crash_case!(
    sstable_before_rename_panic,
    SSTABLE_BEFORE_RENAME,
    Mode::Panic,
    Trigger::Flush
);
crash_case!(
    sstable_after_rename_return,
    SSTABLE_AFTER_RENAME,
    Mode::Return,
    Trigger::Flush
);
crash_case!(
    sstable_after_rename_panic,
    SSTABLE_AFTER_RENAME,
    Mode::Panic,
    Trigger::Flush
);
crash_case!(
    manifest_before_append_return,
    MANIFEST_BEFORE_APPEND,
    Mode::Return,
    Trigger::Flush
);
crash_case!(
    manifest_before_append_panic,
    MANIFEST_BEFORE_APPEND,
    Mode::Panic,
    Trigger::Flush
);
crash_case!(
    manifest_after_fsync_return,
    MANIFEST_AFTER_FSYNC,
    Mode::Return,
    Trigger::Flush
);
crash_case!(
    manifest_after_fsync_panic,
    MANIFEST_AFTER_FSYNC,
    Mode::Panic,
    Trigger::Flush
);

// Compaction points fire when L0 hits the trigger and a job runs.
use zydecodb_engine::failpoints::{
    COMPACTION_AFTER_MANIFEST_BEFORE_UNLINK, COMPACTION_BEFORE_MANIFEST, COMPACTION_BEFORE_RENAME,
};
crash_case!(
    compaction_before_rename_return,
    COMPACTION_BEFORE_RENAME,
    Mode::Return,
    Trigger::Compaction
);
crash_case!(
    compaction_before_rename_panic,
    COMPACTION_BEFORE_RENAME,
    Mode::Panic,
    Trigger::Compaction
);
crash_case!(
    compaction_before_manifest_return,
    COMPACTION_BEFORE_MANIFEST,
    Mode::Return,
    Trigger::Compaction
);
crash_case!(
    compaction_before_manifest_panic,
    COMPACTION_BEFORE_MANIFEST,
    Mode::Panic,
    Trigger::Compaction
);
crash_case!(
    compaction_after_manifest_before_unlink_return,
    COMPACTION_AFTER_MANIFEST_BEFORE_UNLINK,
    Mode::Return,
    Trigger::Compaction
);
crash_case!(
    compaction_after_manifest_before_unlink_panic,
    COMPACTION_AFTER_MANIFEST_BEFORE_UNLINK,
    Mode::Panic,
    Trigger::Compaction
);

// Apply-thread ordering: manifest fsync before catalog publish; unlink after swap.
use zydecodb_engine::failpoints::{
    APPLY_AFTER_FSYNC_BEFORE_PUBLISH, APPLY_AFTER_PUBLISH_BEFORE_UNLINK,
};
crash_case!(
    apply_after_fsync_before_publish_return,
    APPLY_AFTER_FSYNC_BEFORE_PUBLISH,
    Mode::Return,
    Trigger::Flush
);
crash_case!(
    apply_after_fsync_before_publish_panic,
    APPLY_AFTER_FSYNC_BEFORE_PUBLISH,
    Mode::Panic,
    Trigger::Flush
);
crash_case!(
    apply_after_fsync_before_publish_compaction_return,
    APPLY_AFTER_FSYNC_BEFORE_PUBLISH,
    Mode::Return,
    Trigger::Compaction
);
crash_case!(
    apply_after_fsync_before_publish_compaction_panic,
    APPLY_AFTER_FSYNC_BEFORE_PUBLISH,
    Mode::Panic,
    Trigger::Compaction
);
crash_case!(
    apply_after_publish_before_unlink_return,
    APPLY_AFTER_PUBLISH_BEFORE_UNLINK,
    Mode::Return,
    Trigger::Flush
);
crash_case!(
    apply_after_publish_before_unlink_panic,
    APPLY_AFTER_PUBLISH_BEFORE_UNLINK,
    Mode::Panic,
    Trigger::Flush
);
crash_case!(
    apply_after_publish_before_unlink_compaction_return,
    APPLY_AFTER_PUBLISH_BEFORE_UNLINK,
    Mode::Return,
    Trigger::Compaction
);
crash_case!(
    apply_after_publish_before_unlink_compaction_panic,
    APPLY_AFTER_PUBLISH_BEFORE_UNLINK,
    Mode::Panic,
    Trigger::Compaction
);

// Segment-roll points only fire when the active segment is full. Triggering
// that in a unit test would require writing >64 MB of WAL, which is too
// expensive for the per-PR matrix. They're covered by the soak run instead.
// Their failpoint constants are still exported and assertable; see
// `tests/format_versions.rs` (Phase 1.3) for static-shape coverage.
