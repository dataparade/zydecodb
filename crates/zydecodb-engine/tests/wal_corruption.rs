//! WAL recovery refuses mid-stream corruption instead of silently truncating.
//!
//! Only the active (highest-id) segment at crash time may end with a torn tail.
//! A sealed segment was fsynced complete before the roll, so any damage there is
//! bit-rot; and even in the active segment, a damaged record with intact records
//! after it means corruption ate committed data. Both cases must make
//! `Engine::open` fail loudly rather than open with a silently shortened log.

use std::path::{Path, PathBuf};
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::errors::EngineError;
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::wal::{self, WalRecord};

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn open(dir: &TempDir) -> Result<Engine, EngineError> {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
}

/// Write a WAL segment file by hand: `[first_seq][version]` header followed by
/// the already-encoded record bytes (so a test can pre-corrupt one record).
fn write_segment(wal_dir: &Path, id: u64, first_seq: u64, records: &[Vec<u8>]) -> PathBuf {
    let mut buf = Vec::new();
    buf.extend_from_slice(&first_seq.to_be_bytes());
    buf.push(wal::WAL_FORMAT_VERSION);
    for r in records {
        buf.extend_from_slice(r);
    }
    let path = wal_dir.join(wal::segment_filename(id));
    std::fs::write(&path, buf).expect("write segment");
    path
}

fn wal_dir(dir: &TempDir) -> PathBuf {
    let p = dir.path().join("data/wal");
    std::fs::create_dir_all(&p).expect("create wal dir");
    p
}

fn rec(seq: u64, key: &[u8], val: &[u8]) -> Vec<u8> {
    WalRecord::put(seq, 0, uk(key), val.to_vec()).encode()
}

#[test]
fn sealed_segment_midstream_corruption_refuses_open() {
    let dir = TempDir::new().unwrap();
    let wd = wal_dir(&dir);

    // Segment 1 is sealed (a higher-id active segment exists). A sealed segment
    // must be intact end-to-end, so even a torn tail (a truncated trailing
    // record) is treated as corruption — a sealed file should never end mid-
    // record.
    let r1 = rec(1, b"a", b"1");
    let r2_full = rec(2, b"b", b"2");
    let mut seg1 = r1;
    seg1.extend_from_slice(&r2_full[..r2_full.len() - 4]); // truncated record 2
    write_segment(&wd, 1, 1, &[seg1]);
    // Segment 2 is the active one and is perfectly valid.
    write_segment(&wd, 2, 4, &[rec(4, b"d", b"4"), rec(5, b"e", b"5")]);

    let err = match open(&dir) {
        Ok(_) => panic!("must refuse to open with a corrupt sealed segment"),
        Err(e) => e,
    };
    let EngineError::Io(msg) = &err else {
        panic!("expected EngineError::Io, got {err:?}");
    };
    assert!(
        msg.contains("corruption detected in sealed segment"),
        "must name the sealed-segment corruption contract; got: {msg}"
    );
}

#[test]
fn active_segment_midstream_corruption_refuses_open() {
    let dir = TempDir::new().unwrap();
    let wd = wal_dir(&dir);

    // Single (active) segment: a damaged middle record with an intact record
    // after it is corruption, not a torn tail.
    let r1 = rec(1, b"a", b"1");
    let mut r2 = rec(2, b"b", b"2");
    let r3 = rec(3, b"c", b"3");
    *r2.last_mut().unwrap() ^= 0xFF;
    write_segment(&wd, 1, 1, &[r1, r2, r3]);

    let err = match open(&dir) {
        Ok(_) => panic!("must refuse to open with mid-stream corruption"),
        Err(e) => e,
    };
    let EngineError::Io(msg) = &err else {
        panic!("expected EngineError::Io, got {err:?}");
    };
    assert!(
        msg.contains("corruption detected in segment"),
        "must name the corruption contract; got: {msg}"
    );
}

#[test]
fn active_segment_torn_tail_truncates_and_opens() {
    let dir = TempDir::new().unwrap();
    let wd = wal_dir(&dir);

    // Two complete records followed by a partial trailing fragment: the classic
    // crash-mid-append tail. Recovery truncates it and opens cleanly.
    let mut body = Vec::new();
    body.extend_from_slice(&rec(1, b"a", b"1"));
    body.extend_from_slice(&rec(2, b"b", b"2"));
    write_segment(&wd, 1, 1, &[std::mem::take(&mut body)]);
    // Append a partial record fragment to the active segment.
    let path = wd.join(wal::segment_filename(1));
    let mut existing = std::fs::read(&path).unwrap();
    existing.extend_from_slice(&rec(3, b"c", b"3")[..6]); // truncated record
    std::fs::write(&path, existing).unwrap();

    let e = open(&dir).expect("torn tail must truncate and open");
    assert_eq!(e.get(&uk(b"a")).unwrap().as_deref(), Some(b"1".as_ref()));
    assert_eq!(e.get(&uk(b"b")).unwrap().as_deref(), Some(b"2".as_ref()));
    assert!(e.get(&uk(b"c")).unwrap().is_none());
}

#[test]
fn clean_multi_segment_opens_and_replays_all() {
    let dir = TempDir::new().unwrap();
    let wd = wal_dir(&dir);

    write_segment(&wd, 1, 1, &[rec(1, b"a", b"1"), rec(2, b"b", b"2")]);
    write_segment(&wd, 2, 3, &[rec(3, b"c", b"3")]);

    let e = open(&dir).expect("clean segments open");
    for (k, v) in [(b"a", b"1"), (b"b", b"2"), (b"c", b"3")] {
        assert_eq!(e.get(&uk(k)).unwrap().as_deref(), Some(v.as_ref()));
    }
}
