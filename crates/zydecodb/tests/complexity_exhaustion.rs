use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
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
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
        fair: Default::default(),
    }
}

use zydecodb_document::wire::{DocPutPayload, FindPayload, IndexDefPayload, WireProjection};

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
fn test_filter_complexity_exhaustion() {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let config = base_config(&tmp, addr);

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);

    // Create collection via index definition
    let idx = IndexDefPayload {
        collection: "test".to_string(),
        index_name: "idx_a".to_string(),
        fields: vec!["a".to_string()],
        unique: false,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::IndexDef, idx.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    // Insert a document
    let put = DocPutPayload {
        collection: "test".to_string(),
        doc_id: b"doc1".to_vec(),
        body: b"{\"a\": 1}".to_vec(),
        relaxed: false,
        expires_at: 0,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::DocPut, put.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    // Build a deeply nested $or / $and filter
    // e.g. {"$or": [{"$and": [{"a": 1}, {"$or": [...]}]}]}
    let depth = 1000;
    let mut filter_json = String::from("{\"a\": 1}");
    for i in 0..depth {
        if i % 2 == 0 {
            filter_json = format!("{{\"$or\": [{}, {{\"a\": 2}}]}}", filter_json);
        } else {
            filter_json = format!("{{\"$and\": [{}, {{\"a\": 1}}]}}", filter_json);
        }
    }

    let find = FindPayload {
        collection: "test".to_string(),
        filter: filter_json.into_bytes(),
        sort: vec![],
        projection: WireProjection::None,
        skip: 0,
        limit: 10,
        cursor: vec![],
    };

    let req = RequestEnvelope::new(Command::Find, find.encode());

    // We send the request and expect either a quick error (if complexity is limited)
    // or a quick success (if it handles it efficiently). It should NOT hang.
    write_request(&mut stream, &req);

    let start = Instant::now();
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    let res = stream.read_exact(&mut header);
    let elapsed = start.elapsed();

    // If it took too long, it's a vulnerability
    assert!(
        elapsed < Duration::from_secs(2),
        "VULNERABILITY SURFACED: Deeply nested filter pinned the thread for {:?}",
        elapsed
    );

    if res.is_ok() {
        let (status, _) = ResponseEnvelope::parse_header(&header).unwrap();
        // It might be ProtocolError (if we reject deep nesting) or Ok (if we evaluate it fast)
        assert!(
            status == Status::ProtocolError
                || status == Status::Ok
                || status == Status::Error
                || status == Status::InvalidValue,
            "Unexpected status: {:?}",
            status
        );
    } else {
        // If it crashed (stack overflow), that's also a vulnerability
        panic!("VULNERABILITY SURFACED: Server crashed on deeply nested filter (likely Stack Overflow)");
    }

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
