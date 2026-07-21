use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use zydecodb::config::{Config, RequireAuth, SecurityConfig};
use zydecodb::security::keys::{KeyRole, KeyStore};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn base_config(tmp: &TempDir, listen: SocketAddr, keys_file: PathBuf) -> Config {
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
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
            require_auth: RequireAuth::True,
            keys_file,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    }
}

fn write_request(stream: &mut TcpStream, req: &RequestEnvelope) {
    stream.write_all(&req.encode()).unwrap();
    stream.flush().unwrap();
}

fn read_response(stream: &mut TcpStream) -> ResponseEnvelope {
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    stream.read_exact(&mut header).unwrap();
    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).unwrap();
    }
    ResponseEnvelope::new(status, payload)
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

#[test]
fn test_keystore_load_io_error_fallback() {
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    
    // Initial key
    let secret = KeyStore::create_key(
        &keys_file,
        "test_key",
        KeyRole::ReadWrite,
        "00000000000000000000000000000000",
        vec![],
    )
    .unwrap();

    let addr = free_addr();
    let config = base_config(&tmp, addr, keys_file.clone());
    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    // Wait for server to start
    let _ = wait_connect(addr);

    // Verify key works
    let mut stream = wait_connect(addr);
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    // Enable failpoint to simulate I/O error on reload
    fail::cfg("keystore_load_io_error", "return").unwrap();

    // Trigger reload
    std::process::Command::new("kill")
        .arg("-HUP")
        .arg(std::process::id().to_string())
        .status()
        .unwrap();
    
    // Wait for reload to process
    thread::sleep(Duration::from_millis(500));

    // Verify the server DID NOT crash, and the OLD key still works
    let mut stream2 = wait_connect(addr);
    write_request(
        &mut stream2,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(read_response(&mut stream2).status, Status::Ok, "Server should retain old keys on I/O error");

    // Also verify it didn't fail open (anonymous access should still be denied)
    let mut stream3 = wait_connect(addr);
    write_request(
        &mut stream3,
        &RequestEnvelope::new(Command::Stats, vec![]),
    );
    assert_eq!(read_response(&mut stream3).status, Status::Unauthorized, "Server should not fail open");

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
