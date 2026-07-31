//! Crash during an atomic batch (tx commit equivalent) with change-log archive:
//! the committed batch appears all-or-nothing in logical change iteration.

#![cfg(feature = "failpoints")]

use std::sync::Mutex;
use tempfile::TempDir;
use zydecodb_engine::change_log::{self, ChangeLogConfig};
use zydecodb_engine::engine::{BatchOp, Engine, EngineConfig};
use zydecodb_engine::failpoints::{WAL_AFTER_APPEND, WAL_BEFORE_APPEND};
use zydecodb_engine::keys::KS_USER;

fn fail_lock() -> &'static Mutex<()> {
    static LOCK: Mutex<()> = Mutex::new(());
    &LOCK
}

fn prefix() -> Vec<u8> {
    vec![KS_USER]
}

/// Document-body key: prefix | 'd' | collection_id | doc_id
fn doc_key(collection_id: u32, doc_id: &[u8]) -> Vec<u8> {
    let mut k = prefix();
    k.push(b'd');
    k.extend_from_slice(&collection_id.to_be_bytes());
    k.extend_from_slice(doc_id);
    k
}

fn open(dir: &TempDir) -> Engine {
    let archive = dir.path().join("change_log");
    std::fs::create_dir_all(&archive).unwrap();
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
    .with_change_log(ChangeLogConfig {
        archive_dir: archive,
        retention_secs: 3600,
        retention_bytes: 64 * 1024 * 1024,
    })
    .unwrap()
}

fn logical_docs(e: &Engine) -> Vec<Vec<u8>> {
    let cfg = e.change_log_config().unwrap();
    let manifest = e.change_log_manifest().unwrap();
    let active = e.active_wal_path();
    let changes =
        change_log::iter_logical_changes_after(cfg, manifest, Some(&active), &prefix(), 1, 0, 0)
            .unwrap();
    changes.into_iter().map(|c| c.doc_id).collect()
}

#[test]
fn batch_crash_before_wal_append_is_invisible() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();

    {
        let mut e = open(&dir);
        e.put(doc_key(1, b"seed"), b"s".to_vec(), 0).unwrap();
        e.sync_wal().unwrap();
    }

    fail::cfg(WAL_BEFORE_APPEND, "1*return").unwrap();
    {
        let mut e = open(&dir);
        let ops = vec![
            BatchOp::Put {
                key: doc_key(1, b"a"),
                value: b"1".to_vec(),
                expires_at: 0,
            },
            BatchOp::Put {
                key: doc_key(1, b"b"),
                value: b"2".to_vec(),
                expires_at: 0,
            },
        ];
        assert!(e.write_batch(ops).is_err());
    }
    fail::remove(WAL_BEFORE_APPEND);

    let e = open(&dir);
    let ids = logical_docs(&e);
    assert!(
        !ids.iter().any(|id| id == b"a" || id == b"b"),
        "failed batch must not appear partially in change log: {ids:?}"
    );
    assert!(e.get(&doc_key(1, b"a")).unwrap().is_none());
    assert!(e.get(&doc_key(1, b"b")).unwrap().is_none());
}

#[test]
fn batch_crash_after_wal_append_is_all_or_nothing() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();

    {
        let mut e = open(&dir);
        e.put(doc_key(1, b"seed"), b"s".to_vec(), 0).unwrap();
        e.sync_wal().unwrap();
    }

    fail::cfg(WAL_AFTER_APPEND, "1*return").unwrap();
    let batch_err = {
        let mut e = open(&dir);
        let ops = vec![
            BatchOp::Put {
                key: doc_key(1, b"a"),
                value: b"1".to_vec(),
                expires_at: 0,
            },
            BatchOp::Put {
                key: doc_key(1, b"b"),
                value: b"2".to_vec(),
                expires_at: 0,
            },
        ];
        e.write_batch(ops)
    };
    fail::remove(WAL_AFTER_APPEND);
    // After-append may surface as error after durability; either way reopen is clean.
    let _ = batch_err;

    let mut e = open(&dir);
    e.force_roll_wal_for_test().unwrap();
    let ids = logical_docs(&e);
    let has_a = ids.iter().any(|id| id.as_slice() == b"a");
    let has_b = ids.iter().any(|id| id.as_slice() == b"b");
    assert_eq!(
        has_a, has_b,
        "batch must be all-or-nothing in change log (a={has_a} b={has_b} ids={ids:?})"
    );
    let got_a = e.get(&doc_key(1, b"a")).unwrap().is_some();
    let got_b = e.get(&doc_key(1, b"b")).unwrap().is_some();
    assert_eq!(got_a, got_b, "engine visibility must match batch atomicity");
}
