//! A WAL fsync error must fail closed: the blocked writer gets an error (not a
//! hang), every later write is refused before it reaches the memtable, reads
//! keep working, and `/readyz` reports 503 until restart.
//!
//! Run with:
//! `cargo test -p zydecodb --features failpoints --test fsync_fail_closed`

#![cfg(feature = "failpoints")]

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use zydecodb::config::MetricsConfig;
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

fn http_get(addr: SocketAddr, path: &str) -> (u16, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut buf = String::new();
    s.read_to_string(&mut buf).unwrap();
    let code: u16 = buf
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    (code, buf)
}

#[test]
fn wal_fsync_failure_fails_closed() {
    let _scenario = fail::FailScenario::setup();
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let metrics_addr = free_addr();
    let mut cfg = base_config(&tmp, addr);
    cfg.metrics = MetricsConfig {
        listen: Some(metrics_addr),
        per_tenant: false,
        allow_remote: false,
        token: None,
    };
    let (shutdown, handle) = spawn_server(cfg);
    wait_tcp_up(metrics_addr);
    // wait_connect installs a 5s socket read timeout: if the server hangs the
    // writer, read_response panics on the timeout, which is the bound we want.
    let mut s = wait_connect(addr);

    // Healthy: a durable write is acknowledged and the node is ready.
    write_request(&mut s, &put_req(b"a", b"1"));
    assert_eq!(read_response(&mut s).status, Status::Ok);
    assert_eq!(http_get(metrics_addr, "/readyz").0, 200);

    // Every fsync from here on fails.
    fail::cfg(WAL_BEFORE_FSYNC, "return").unwrap();

    // The write whose fsync fails is refused, promptly, with IoError.
    let start = Instant::now();
    write_request(&mut s, &put_req(b"b", b"2"));
    let resp = read_response(&mut s);
    assert!(
        start.elapsed() < Duration::from_secs(5),
        "writer hung after fsync failure"
    );
    assert_eq!(resp.status, Status::IoError, "{resp:?}");
    let msg = String::from_utf8_lossy(&resp.payload);
    assert!(msg.contains("writes refused until restart"), "{msg}");

    // Later writes of every kind are refused up front (raw KV, document, and
    // transaction begin), so nothing new enters the memtable.
    write_request(&mut s, &put_req(b"c", b"3"));
    assert_eq!(read_response(&mut s).status, Status::IoError);
    let doc = zydecodb_document::wire::DocPutPayload {
        collection: "c".into(),
        doc_id: b"d1".to_vec(),
        body: br#"{"n":1}"#.to_vec(),
        relaxed: true,
        expires_at: 0,
    };
    write_request(&mut s, &RequestEnvelope::new(Command::DocPut, doc.encode()));
    assert_eq!(read_response(&mut s).status, Status::IoError);
    write_request(&mut s, &RequestEnvelope::new(Command::Begin, vec![]));
    assert_eq!(read_response(&mut s).status, Status::IoError);

    // Reads keep serving, on this connection and on a fresh one.
    write_request(&mut s, &get_req(b"a"));
    let resp = read_response(&mut s);
    assert_eq!(resp.status, Status::Ok);
    assert_eq!(resp.payload, b"1");
    let mut s2 = wait_connect(addr);
    write_request(&mut s2, &get_req(b"a"));
    assert_eq!(read_response(&mut s2).status, Status::Ok);
    write_request(&mut s2, &put_req(b"d", b"4"));
    assert_eq!(read_response(&mut s2).status, Status::IoError);

    // Alive but not ready.
    assert_eq!(http_get(metrics_addr, "/healthz").0, 200);
    let (code, body) = http_get(metrics_addr, "/readyz");
    assert_eq!(code, 503, "{body}");
    assert!(body.contains("WAL fsync failed"), "{body}");

    fail::cfg(WAL_BEFORE_FSYNC, "off").unwrap();
    shutdown_join(&shutdown, handle);
}
