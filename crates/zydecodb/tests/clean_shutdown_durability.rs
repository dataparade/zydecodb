//! F5: a clean shutdown must not acknowledge a write before that write is
//! durable. With the WAL fsync parked at a failpoint, a `Put` followed by the
//! shutdown flag (what SIGTERM does) must produce NO response until the final
//! shutdown fsync completes — and the acknowledged write must survive a
//! restart. Pre-fix the waiter was released by `commit.stop()` with a false
//! `Ok` before any fsync ran.

#![cfg(feature = "failpoints")]

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use zydecodb_engine::errors::Status;
use zydecodb_engine::failpoints::WAL_BEFORE_FSYNC;
use zydecodb_engine::frame::{Command, KeyPayload, PutPayload, RequestEnvelope};

fn put_req(key: &[u8], value: &[u8]) -> RequestEnvelope {
    RequestEnvelope::new(
        Command::Put,
        PutPayload {
            routing_key: [0u8; 16],
            txid: 0,
            expires_at: 0,
            key: key.to_vec(),
            value: value.to_vec(),
        }
        .encode(),
    )
}

fn get_req(key: &[u8]) -> RequestEnvelope {
    RequestEnvelope::new(
        Command::Get,
        KeyPayload {
            routing_key: [0u8; 16],
            snapshot_seq: 0,
            key: key.to_vec(),
        }
        .encode(),
    )
}

#[test]
fn sigterm_does_not_ack_write_before_final_fsync() {
    let _scenario = fail::FailScenario::setup();
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let cfg = base_config(&tmp, addr);
    let data_dir = cfg.data_dir.clone();
    let wal_dir = cfg.wal_dir.clone();
    let (shutdown, handle) = spawn_server(cfg);

    // Park every WAL fsync at the failpoint.
    fail::cfg(WAL_BEFORE_FSYNC, "pause").unwrap();

    // The writer sends a Put and reports when (and with what status) the
    // server answers.
    let (tx, rx) = mpsc::channel();
    let writer = thread::spawn(move || {
        let mut s = wait_connect(addr);
        write_request(&mut s, &put_req(b"k1", b"v1"));
        let start = Instant::now();
        let resp = read_response(&mut s);
        let _ = tx.send((start.elapsed(), resp.status));
    });

    // Let the writer buffer its append and the coordinator park at the
    // failpoint (mirrors the synchronization style in rawkv_concurrency.rs).
    thread::sleep(Duration::from_millis(300));

    // SIGTERM-equivalent: flip the shutdown flag.
    *shutdown.lock().unwrap() = true;

    // The write must NOT be acknowledged before its fsync completes. Capture
    // the outcome rather than asserting here so cleanup (releasing the
    // failpoint) always runs.
    let early_ack = rx.recv_timeout(Duration::from_millis(500)).ok();

    // Release the fsync: the shutdown path's final sync makes the write
    // durable, and only then may the ack land.
    fail::cfg(WAL_BEFORE_FSYNC, "off").unwrap();

    assert!(
        early_ack.is_none(),
        "write acknowledged during shutdown before its fsync completed: {early_ack:?}"
    );
    let (_elapsed, status) = rx
        .recv_timeout(Duration::from_secs(15))
        .expect("writer never got a response after the failpoint was released");
    assert_eq!(status, Status::Ok);
    writer.join().unwrap();
    handle.join().unwrap();

    // Restart on the same directories: the acknowledged write survived.
    let addr2 = free_addr();
    let mut cfg2 = base_config(&tmp, addr2);
    cfg2.data_dir = data_dir;
    cfg2.wal_dir = wal_dir;
    let (shutdown2, handle2) = spawn_server(cfg2);
    let mut s = wait_connect(addr2);
    write_request(&mut s, &get_req(b"k1"));
    let resp = read_response(&mut s);
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(resp.payload, b"v1");
    shutdown_join(&shutdown2, handle2);
}
