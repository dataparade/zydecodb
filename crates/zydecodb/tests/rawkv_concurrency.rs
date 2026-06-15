//! Proves a raw-KV GET no longer serializes behind a long write path.
//!
//! Run with:
//! `cargo test -p zydecodb --features failpoints --test rawkv_concurrency -- --test-threads=1`

#![cfg(feature = "failpoints")]

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use zydecodb::commit::{CommitCoordinator, DurabilityMode};
use zydecodb::dispatch::handle_request;
use zydecodb::security::{SecurityRuntime, SessionState};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::failpoints::WAL_BEFORE_FSYNC;
use zydecodb_engine::frame::{Command, KeyPayload, PutPayload, RequestEnvelope};

fn fail_lock() -> &'static Mutex<()> {
    static LOCK: Mutex<()> = Mutex::new(());
    &LOCK
}

fn put_req(key: &[u8], value: &[u8]) -> RequestEnvelope {
    let p = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: key.to_vec(),
        value: value.to_vec(),
    };
    RequestEnvelope::new(Command::Put, p.encode())
}

fn get_req(key: &[u8]) -> RequestEnvelope {
    let p = KeyPayload {
        routing_key: [0u8; 16],
        snapshot_seq: 0,
        key: key.to_vec(),
    };
    RequestEnvelope::new(Command::Get, p.encode())
}

/// One connection issues a durable write whose fsync is delayed 500ms. While
/// that write blocks awaiting durability (fsync in flight, engine lock free),
/// a second connection's GET must complete promptly. Before the lock decoupling
/// the GET held the engine lock across its whole dispatch and the fsync held the
/// engine lock for its full duration, so the GET would have blocked ~500ms.
#[test]
fn raw_kv_get_proceeds_during_in_flight_write_fsync() {
    let _guard = fail_lock().lock().unwrap_or_else(|p| p.into_inner());
    let _scenario = fail::FailScenario::setup();

    let dir = TempDir::new().unwrap();
    let engine = Arc::new(Mutex::new(
        Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("wal"),
            ..Default::default()
        })
        .unwrap(),
    ));

    let commit = CommitCoordinator::new(Arc::clone(&engine), DurabilityMode::Sync);
    let commit_thread = commit.spawn().unwrap();
    let security = SecurityRuntime::default();

    // Seed "a"="1" durably while no failpoint is active.
    let out = handle_request(
        &engine,
        put_req(b"a", b"1"),
        SessionState::anonymous(),
        &security,
    );
    commit.commit(out.commit_seq.unwrap(), false);

    // Delay the NEXT fsync by 500ms (the in-flight write's durability wait).
    fail::cfg(WAL_BEFORE_FSYNC, "sleep(500)").unwrap();

    let engine_w = Arc::clone(&engine);
    let commit_w = Arc::clone(&commit);
    let writer = thread::spawn(move || {
        let security = SecurityRuntime::default();
        let out = handle_request(
            &engine_w,
            put_req(b"b", b"2"),
            SessionState::anonymous(),
            &security,
        );
        // Blocks ~500ms in await_durable while the coordinator's fsync sleeps.
        commit_w.commit(out.commit_seq.unwrap(), false);
    });

    // Let the writer buffer its append and the coordinator enter the fsync sleep.
    thread::sleep(Duration::from_millis(100));

    let start = Instant::now();
    let out = handle_request(&engine, get_req(b"a"), SessionState::anonymous(), &security);
    let elapsed = start.elapsed();

    assert_eq!(out.response.payload, b"1");
    assert!(
        elapsed < Duration::from_millis(300),
        "raw-KV GET serialized behind the in-flight write fsync (took {elapsed:?})"
    );

    fail::cfg(WAL_BEFORE_FSYNC, "off").unwrap();
    writer.join().unwrap();
    commit.stop();
    commit_thread.join().unwrap();
}
