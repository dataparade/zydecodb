//! Zero-config `serve`: `Config::local_default_with_home` must resolve state
//! under `~/.zydecodb/`, keep auth optional on loopback, and boot a server
//! that answers unauthenticated requests.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use zydecodb::config::Config;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn wait_connect(addr: SocketAddr) -> TcpStream {
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

fn roundtrip(stream: &mut TcpStream, req: &RequestEnvelope) -> ResponseEnvelope {
    stream.write_all(&req.encode()).unwrap();
    stream.flush().unwrap();
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    stream.read_exact(&mut header).unwrap();
    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).unwrap();
    }
    ResponseEnvelope::new(status, payload)
}

#[test]
fn local_default_paths_live_under_home() {
    let home = TempDir::new().unwrap();
    let cfg = Config::local_default_with_home(home.path());

    let base = home.path().join(".zydecodb");
    assert_eq!(cfg.data_dir, base.join("data"));
    assert_eq!(cfg.wal_dir, base.join("wal"));
    assert_eq!(cfg.security.keys_file, base.join("keys.toml"));

    assert_eq!(cfg.listen.to_string(), "127.0.0.1:9470");
    // Loopback + RequireAuth::Auto => no key required for the first-run path.
    assert!(!cfg.effective_require_auth());
}

#[test]
fn local_default_server_serves_unauthenticated_on_loopback() {
    let home = TempDir::new().unwrap();
    let mut cfg = Config::local_default_with_home(home.path());
    // Keep the well-known port free for developers running the suite alongside
    // a real local server; loopback is what matters for the auth policy.
    cfg.listen = free_addr();
    let addr = cfg.listen;

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(cfg).unwrap());

    let mut stream = wait_connect(addr);

    let ping = roundtrip(&mut stream, &RequestEnvelope::new(Command::Ping, vec![]));
    assert_eq!(ping.status, Status::Ok);

    // A write must succeed with no SessionInit (auth resolves off on loopback).
    let put = zydecodb_engine::frame::PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: b"smoke".to_vec(),
        value: b"ok".to_vec(),
    };
    let resp = roundtrip(
        &mut stream,
        &RequestEnvelope::new(Command::Put, put.encode()),
    );
    assert_eq!(resp.status, Status::Ok);

    // State must have landed under the temp home, not /var/lib.
    assert!(home.path().join(".zydecodb").join("wal").exists());
    assert!(home.path().join(".zydecodb").join("data").exists());

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
