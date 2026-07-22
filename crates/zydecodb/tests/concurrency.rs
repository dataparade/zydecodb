use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN,
};

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

fn connect(addr: SocketAddr) -> TcpStream {
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(addr) {
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
            return s;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not come up");
}

/// Two connections hammer the server concurrently. With the thread-per-connection
/// model neither connection can monopolize the engine, both must make progress,
/// and shutdown must join cleanly. This is the regression guard for the
/// EngineHandle write-domain concurrency.
#[test]
fn concurrent_connections_make_progress() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = probe.local_addr().unwrap();
    drop(probe);

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

    let mut workers = Vec::new();
    for w in 0..2u8 {
        workers.push(thread::spawn(move || {
            let mut s = connect(addr);
            for i in 0..50u32 {
                let key = format!("conn{}:{}", w, i).into_bytes();
                let val = format!("v{}", i).into_bytes();
                let resp = roundtrip(&mut s, &put_req(&key, &val));
                assert_eq!(resp.status, Status::Ok, "put failed w{} i{}", w, i);
            }
            for i in 0..50u32 {
                let key = format!("conn{}:{}", w, i).into_bytes();
                let resp = roundtrip(&mut s, &get_req(&key));
                assert_eq!(resp.status, Status::Ok, "get failed w{} i{}", w, i);
                assert_eq!(resp.payload, format!("v{}", i).into_bytes());
            }
        }));
    }
    for h in workers {
        h.join().unwrap();
    }

    // A fresh connection sees data written by both connections.
    let mut s = connect(addr);
    assert_eq!(roundtrip(&mut s, &get_req(b"conn0:0")).status, Status::Ok);
    assert_eq!(roundtrip(&mut s, &get_req(b"conn1:49")).status, Status::Ok);
    drop(s);

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
