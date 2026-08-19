//! Admin snapshot / restore mid-flight durability:
//! - half-written snapshot (no SNAPMETA) is unrestorable
//! - killed restore leaves `RESTORE_IN_PROGRESS` so `Engine::open` refuses

#![cfg(feature = "failpoints")]

use std::sync::Mutex;
use tempfile::TempDir;
use zydecodb::admin;
use zydecodb_engine::engine::{Engine, EngineConfig, RESTORE_IN_PROGRESS_MARKER};
use zydecodb_engine::failpoints::{SNAPSHOT_AFTER_PUBLISH, SNAPSHOT_BEFORE_PUBLISH};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::shipping::ShipMode;

fn fail_lock() -> &'static Mutex<()> {
    static LOCK: Mutex<()> = Mutex::new(());
    &LOCK
}

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn seed_engine(dir: &TempDir) -> Engine {
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap();
    e.put(uk(b"k"), b"v".to_vec(), 0).unwrap();
    e.sync_wal().unwrap();
    e.force_flush().unwrap();
    e
}

#[test]
fn half_written_snapshot_missing_snapmeta_is_unrestorable() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();
    let snap = dir.path().join("snap");
    let ship = dir.path().join("ship");
    let out = dir.path().join("restore-out");
    std::fs::create_dir_all(&ship).unwrap();

    {
        let mut e = seed_engine(&dir).with_shipping(Some(ship.clone()), ShipMode::Copy);
        fail::cfg(SNAPSHOT_BEFORE_PUBLISH, "1*return").unwrap();
        assert!(e.snapshot_to(&snap).is_err());
        fail::remove(SNAPSHOT_BEFORE_PUBLISH);
    }

    assert!(
        !snap.join("SNAPMETA").exists(),
        "kill before publish must leave SNAPMETA unpublished"
    );

    // Ordering contract: at the BEFORE_PUBLISH point the MANIFEST copy is
    // already on disk and byte-complete (and fsynced). Only SNAPMETA — the
    // restorability gate — may be missing.
    let copied =
        std::fs::read(snap.join("MANIFEST")).expect("MANIFEST must be fully copied pre-publish");
    let source = std::fs::read(dir.path().join("data").join("MANIFEST")).unwrap();
    assert_eq!(
        copied, source,
        "copied MANIFEST must match the source bytes"
    );

    let err = admin::restore(&snap, &ship, None, None, &out);
    assert!(err.is_err(), "half-written snapshot must not restore");
    let msg = err.unwrap_err();
    assert!(
        msg.contains("SNAPMETA") || msg.contains("not restorable"),
        "error must name unrestorable snapshot; got {msg}"
    );
}

#[test]
fn published_snapshot_restores_cleanly() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();
    let snap = dir.path().join("snap");
    let ship = dir.path().join("ship");
    let out = dir.path().join("restore-out");
    std::fs::create_dir_all(&ship).unwrap();

    {
        let mut e = seed_engine(&dir).with_shipping(Some(ship.clone()), ShipMode::Copy);
        fail::cfg(SNAPSHOT_AFTER_PUBLISH, "1*return").unwrap();
        // After publish may still error, but SNAPMETA is durable.
        let _ = e.snapshot_to(&snap);
        fail::remove(SNAPSHOT_AFTER_PUBLISH);
    }
    assert!(snap.join("SNAPMETA").is_file());

    admin::restore(&snap, &ship, None, None, &out).expect("restore");
    let e = Engine::open(EngineConfig {
        data_dir: out.clone(),
        wal_dir: out.join("wal"),
        ..Default::default()
    })
    .expect("restored engine opens");
    assert!(e.get(&uk(b"k")).unwrap().is_some());
}

#[test]
fn killed_restore_marker_refuses_engine_open() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("partial");
    std::fs::create_dir_all(&data).unwrap();
    std::fs::create_dir_all(data.join("wal")).unwrap();
    std::fs::write(data.join(RESTORE_IN_PROGRESS_MARKER), b"1").unwrap();

    let err = Engine::open(EngineConfig {
        data_dir: data,
        wal_dir: dir.path().join("partial/wal"),
        ..Default::default()
    });
    let e = match err {
        Ok(_) => panic!("incomplete restore must refuse open"),
        Err(e) => e,
    };
    let msg = e.to_string();
    assert!(
        msg.contains("RESTORE_IN_PROGRESS") || msg.contains("incomplete"),
        "got {msg}"
    );
}
