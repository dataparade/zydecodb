use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use zydecodb::config::{Config, RequireAuth, SecurityConfig};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn base_config(tmp: &TempDir, listen: SocketAddr) -> Config {
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
            require_auth: RequireAuth::False,
            max_connections: 128,
            idle_timeout_secs: 2,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
        fair: Default::default(),
    }
}

#[test]
fn test_slowloris_connection_starvation() {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let config = base_config(&tmp, addr);

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    // Wait for server to start
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    let mut connections = vec![];
    for _ in 0..128 {
        let s = TcpStream::connect(addr).expect("Failed to open connection up to max_connections");
        s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
        s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
        connections.push(s);
    }

    // Try to open the 129th connection. It should be rejected or dropped instantly.
    let mut extra = TcpStream::connect(addr).unwrap();
    extra
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();

    // If we try to read from it, it should return 0 bytes (EOF) or error quickly.
    let mut buf = [0u8; 1];
    let res = extra.read(&mut buf);
    assert!(
        res.is_err() || res.unwrap() == 0,
        "VULNERABILITY SURFACED: 129th connection was accepted and kept open despite max_connections=128!"
    );

    // Now test idle timeout on the 128 connections.
    // We send 1 byte every 1 second (idle_timeout is 2s).
    for _ in 0..3 {
        for s in &mut connections {
            let res = s.write_all(&[0]);
            assert!(res.is_ok(), "Connection was closed prematurely!");
        }
        thread::sleep(Duration::from_secs(1));
    }

    // Now stop sending bytes. They should be closed after 2 seconds.
    let start = Instant::now();
    for s in &mut connections {
        s.set_read_timeout(Some(Duration::from_secs(4))).unwrap();
        let mut buf = [0u8; 1];
        let res = s.read(&mut buf); // This will block until closed or timeout
        if let Err(e) = res {
            if e.kind() == std::io::ErrorKind::WouldBlock
                || e.kind() == std::io::ErrorKind::TimedOut
            {
                panic!("VULNERABILITY SURFACED: Connections were not closed at the correct idle timeout!");
            }
        }
    }
    let elapsed = start.elapsed();

    assert!(
        elapsed >= Duration::from_secs(0) && elapsed < Duration::from_secs(3),
        "Connections were not closed at the correct idle timeout! Elapsed: {:?}",
        elapsed
    );

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn test_json_bomb_oom() {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let config = base_config(&tmp, addr);

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    // Wait for server to start
    let mut stream = loop {
        if let Ok(s) = TcpStream::connect(addr) {
            break s;
        }
        thread::sleep(Duration::from_millis(20));
    };

    // Create a deeply nested JSON array: [[[[...]]]]
    let depth = 10000;
    let mut json = String::with_capacity(depth * 2);
    for _ in 0..depth {
        json.push('[');
    }
    for _ in 0..depth {
        json.push(']');
    }

    let payload = zydecodb_engine::frame::PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: b"bomb".to_vec(),
        value: json.into_bytes(),
    };

    let req = RequestEnvelope::new(Command::DocPut, payload.encode());
    stream.write_all(&req.encode()).unwrap();
    stream.flush().unwrap();

    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    let res = stream.read_exact(&mut header);

    // If the server crashed (stack overflow), read_exact will return an error (Connection reset by peer).
    // If it handled it securely, it should return a valid response (likely a ProtocolError or similar).
    if res.is_err() {
        panic!(
            "VULNERABILITY SURFACED: Server crashed (likely Stack Overflow) on deeply nested JSON!"
        );
    }

    let (status, _) = ResponseEnvelope::parse_header(&header).unwrap();
    assert_eq!(
        status,
        Status::ProtocolError,
        "Expected ProtocolError for overly deep JSON"
    );

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
