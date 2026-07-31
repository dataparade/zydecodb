//! Crash during WAL seal + shipping handoff: `shipped.log` never references a
//! segment whose bytes are missing or whose HMAC does not verify.

#![cfg(feature = "failpoints")]

use std::sync::Mutex;
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::failpoints::{
    SHIP_AFTER_LOG_APPEND, SHIP_AFTER_SEGMENT, SHIP_BEFORE_LOG_APPEND, SHIP_BEFORE_SEGMENT,
};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::shipping::{self, ShipMode};

fn fail_lock() -> &'static Mutex<()> {
    static LOCK: Mutex<()> = Mutex::new(());
    &LOCK
}

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn open(dir: &TempDir, hmac: &[u8]) -> Engine {
    let ship = dir.path().join("ship");
    std::fs::create_dir_all(&ship).unwrap();
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
    .with_shipping(Some(ship), ShipMode::Copy)
    .with_shipping_hmac_key(Some(hmac.to_vec()))
}

fn assert_shipped_log_consistent(ship_dir: &std::path::Path, hmac: &[u8]) {
    let entries = shipping::read_shipped_log(ship_dir).unwrap();
    for entry in entries {
        let path = ship_dir.join(zydecodb_engine::wal::segment_filename(entry.segment_id));
        assert!(
            path.is_file(),
            "shipped.log references missing segment {}",
            entry.segment_id
        );
        assert!(
            shipping::verify_entry(&path, &entry, Some(hmac)).unwrap(),
            "HMAC/sha256 must verify for shipped segment {}",
            entry.segment_id
        );
    }
}

fn run_ship_failpoint(fp: &str) {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();
    let hmac = b"ship-seal-hmac-key-32-bytes!!!!!!";

    {
        let mut e = open(&dir, hmac);
        e.put(uk(b"pre"), b"1".to_vec(), 0).unwrap();
        e.sync_wal().unwrap();
    }

    fail::cfg(fp, "1*return").unwrap();
    {
        let mut e = open(&dir, hmac);
        e.put(uk(b"mid"), b"2".to_vec(), 0).unwrap();
        let _ = e.force_roll_wal_for_test();
    }
    fail::remove(fp);

    // Recovery open + optional retry seal must leave shipped.log consistent.
    {
        let mut e = open(&dir, hmac);
        e.put(uk(b"post"), b"3".to_vec(), 0).unwrap();
        let _ = e.force_roll_wal_for_test();
    }

    assert_shipped_log_consistent(&dir.path().join("ship"), hmac);
}

#[test]
fn ship_before_segment_crash_keeps_log_consistent() {
    run_ship_failpoint(SHIP_BEFORE_SEGMENT);
}

#[test]
fn ship_after_segment_before_log_crash_keeps_log_consistent() {
    run_ship_failpoint(SHIP_BEFORE_LOG_APPEND);
}

#[test]
fn ship_after_segment_crash_keeps_log_consistent() {
    run_ship_failpoint(SHIP_AFTER_SEGMENT);
}

#[test]
fn ship_after_log_append_crash_keeps_log_consistent() {
    run_ship_failpoint(SHIP_AFTER_LOG_APPEND);
}

#[test]
fn double_ship_same_segment_appends_once() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let wal = dir.path().join("wal");
    std::fs::create_dir_all(&wal).unwrap();
    let hmac = b"ship-seal-hmac-key-32-bytes!!!!!!";
    let src = wal.join(zydecodb_engine::wal::segment_filename(7));
    std::fs::write(&src, b"sealed-segment-bytes").unwrap();

    shipping::ship_segment(&src, &ship, 7, 42, ShipMode::Copy, Some(hmac)).unwrap();
    // Retry (e.g. crash after append, or Engine::shutdown re-ship): no second line.
    shipping::ship_segment(&src, &ship, 7, 42, ShipMode::Copy, Some(hmac)).unwrap();

    let entries = shipping::read_shipped_log(&ship).unwrap();
    assert_eq!(entries.len(), 1, "idempotent ship must not duplicate the log line");
    assert_shipped_log_consistent(&ship, hmac);
}

#[test]
fn reship_with_different_content_is_rejected() {
    let _g = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let wal = dir.path().join("wal");
    std::fs::create_dir_all(&wal).unwrap();
    let hmac = b"ship-seal-hmac-key-32-bytes!!!!!!";
    let src = wal.join(zydecodb_engine::wal::segment_filename(7));
    std::fs::write(&src, b"version-a").unwrap();

    shipping::ship_segment(&src, &ship, 7, 42, ShipMode::Copy, Some(hmac)).unwrap();
    std::fs::write(&src, b"version-b-tampered").unwrap();
    let err = shipping::ship_segment(&src, &ship, 7, 42, ShipMode::Copy, Some(hmac));
    assert!(
        err.is_err(),
        "same segment id with different bytes must be a hard error"
    );
}

#[test]
fn torn_final_line_and_legacy_duplicate_are_tolerated() {
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let wal = dir.path().join("wal");
    std::fs::create_dir_all(&wal).unwrap();
    let src = wal.join(zydecodb_engine::wal::segment_filename(1));
    std::fs::write(&src, b"bytes").unwrap();
    shipping::ship_segment(&src, &ship, 1, 10, ShipMode::Copy, None).unwrap();

    // A crash mid-append leaves a partial final line: ignored, not fatal.
    let log = ship.join(shipping::SHIPPED_LOG);
    let good = std::fs::read_to_string(&log).unwrap();
    std::fs::write(&log, format!("{good}2 99 abcdef")).unwrap();
    let entries = shipping::read_shipped_log(&ship).unwrap();
    assert_eq!(entries.len(), 1, "torn tail must be ignored");

    // A pre-fix double append of the identical line: skipped, not fatal.
    std::fs::write(&log, format!("{good}{good}")).unwrap();
    let entries = shipping::read_shipped_log(&ship).unwrap();
    assert_eq!(entries.len(), 1, "exact duplicate line must be skipped");

    // Garbage in the MIDDLE of the log is still corruption, not a torn tail.
    std::fs::write(&log, format!("garbage-not-a-line\n{good}")).unwrap();
    assert!(shipping::read_shipped_log(&ship).is_err());
}
