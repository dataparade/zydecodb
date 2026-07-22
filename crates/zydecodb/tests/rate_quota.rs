use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use zydecodb::config::{Config, QuotasConfig, RequireAuth, SecurityConfig};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, PutPayload, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN,
};

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

fn wait_connect(addr: std::net::SocketAddr) -> TcpStream {
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(addr) {
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
            return s;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("failed to connect");
}

#[test]
fn rate_limit_returns_engine_busy() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let config = Config {
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
        security: SecurityConfig {
            require_auth: RequireAuth::False,
            rate_limit_rps: 2,
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

    let mut stream = wait_connect(addr);
    let ping = RequestEnvelope::new(Command::Ping, vec![]);
    let mut saw_busy = false;
    for _ in 0..20 {
        write_request(&mut stream, &ping);
        let resp = read_response(&mut stream);
        if resp.status == Status::EngineBusy {
            saw_busy = true;
            break;
        }
    }
    assert!(saw_busy, "expected rate limiter to return EngineBusy");

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn tenant_quota_rejects_oversized_write() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

    let config = Config {
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
        security: SecurityConfig {
            require_auth: RequireAuth::False,
            legacy_single_tenant: false,
            quotas: QuotasConfig {
                max_bytes_per_tenant: 64,
            },
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

    let mut stream = wait_connect(addr);
    let put = |key: &[u8], value: &[u8]| PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: key.to_vec(),
        value: value.to_vec(),
    };

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Put, put(b"a", &[0u8; 40]).encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Put, put(b"b", &[0u8; 40]).encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::PolicyRejected);

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
