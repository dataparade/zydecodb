//! An idle connection must stay open (so pooled clients can reuse it) and a
//! `Ping` keepalive must succeed after an idle gap longer than the socket's
//! internal read-poll interval.

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

const CMD_PING: u8 = 0xF0;
const PROTO: u8 = 0x01;

fn ping(stream: &mut TcpStream) -> u8 {
    let frame = [PROTO, CMD_PING, 0, 0, 0, 0];
    stream.write_all(&frame).unwrap();
    let mut header = [0u8; 6];
    stream.read_exact(&mut header).unwrap();
    assert_eq!(header[0], PROTO);
    let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    if len > 0 {
        let mut body = vec![0u8; len];
        stream.read_exact(&mut body).unwrap();
    }
    header[1] // status byte
}

#[test]
fn idle_connection_survives_and_ping_keepalive_works() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let addr = free_addr();
    let config = zydecodb::config::Config {
        listen: addr,
        data_dir,
        wal_dir,
        block_cache_mb: 64,
        max_open_readers: 32,
        poll_compaction_ms: 50,
        durability: Default::default(),
        fsync_interval_ms: 100,
        shipping: Default::default(),
        metrics: Default::default(),
        replica: Default::default(),
        security: zydecodb::config::SecurityConfig {
            require_auth: zydecodb::config::RequireAuth::False,
            idle_timeout_secs: 60,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
        fair: Default::default(),
    };

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = None;
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(addr) {
            stream = Some(s);
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let mut stream = stream.expect("connect");
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    // First ping works immediately.
    assert_eq!(ping(&mut stream), 0x00);

    // Stay idle well past the server's 200ms internal read-poll interval; the
    // connection must NOT be dropped.
    thread::sleep(Duration::from_millis(700));

    // The same connection still serves a keepalive ping.
    assert_eq!(ping(&mut stream), 0x00);

    *shutdown.lock().unwrap() = true;
    let _ = handle.join();
}
