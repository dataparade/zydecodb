//! Shared helpers for zydecodb integration tests.
//!
//! Each integration test binary is a separate crate; pull this in with:
//! ```ignore
//! #[path = "common/mod.rs"]
//! mod common;
//! use common::*;
//! ```

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tempfile::TempDir;
use zydecodb::config::{Config, RequireAuth, SecurityConfig};
use zydecodb::server::Server;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

pub fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

pub fn ensure_dirs(tmp: &TempDir) -> (PathBuf, PathBuf) {
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    (data_dir, wal_dir)
}

/// Minimal server config (auth off) on `listen` with temp data/wal dirs.
pub fn base_config(tmp: &TempDir, listen: SocketAddr) -> Config {
    let (data_dir, wal_dir) = ensure_dirs(tmp);
    Config {
        listen,
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
        security: SecurityConfig {
            require_auth: RequireAuth::False,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
        fair: Default::default(),
    }
}

/// Like [`base_config`] with `require_auth = true` and the given keys file.
pub fn auth_config(tmp: &TempDir, listen: SocketAddr, keys_file: PathBuf) -> Config {
    let mut cfg = base_config(tmp, listen);
    cfg.security = SecurityConfig {
        require_auth: RequireAuth::True,
        keys_file,
        ..Default::default()
    };
    cfg
}

pub fn write_request(stream: &mut impl Write, req: &RequestEnvelope) {
    stream.write_all(&req.encode()).unwrap();
    stream.flush().unwrap();
}

pub fn read_response(stream: &mut impl Read) -> ResponseEnvelope {
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    stream.read_exact(&mut header).unwrap();
    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).unwrap();
    }
    ResponseEnvelope::new(status, payload)
}

pub fn wait_connect(addr: SocketAddr) -> TcpStream {
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(addr) {
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
            return s;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("failed to connect to {addr}");
}

pub fn wait_tcp_up(addr: SocketAddr) {
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("listener at {addr} never came up");
}

pub fn session_init(stream: &mut (impl Write + Read), secret: &str) -> Status {
    write_request(
        stream,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    read_response(stream).status
}

pub fn session_init_ok(stream: &mut (impl Write + Read), secret: &str) {
    assert_eq!(session_init(stream, secret), Status::Ok);
}

/// Spawn `Server::run` on a background thread. Caller owns `config` dirs lifetime.
pub fn spawn_server(config: Config) -> (Arc<Mutex<bool>>, JoinHandle<()>) {
    let server = Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());
    (shutdown, handle)
}

pub fn shutdown_join(shutdown: &Arc<Mutex<bool>>, handle: JoinHandle<()>) {
    *shutdown.lock().unwrap() = true;
    let _ = handle.join();
}

/// Ephemeral server: owns a `TempDir` inside the server thread (auth off).
pub fn spawn_ephemeral_server() -> (SocketAddr, Arc<Mutex<bool>>, JoinHandle<()>) {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let config = base_config(&tmp, addr);
    let server = Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || {
        let _tmp = tmp;
        server.run(config).unwrap();
    });
    wait_tcp_up(addr);
    (addr, shutdown, handle)
}
