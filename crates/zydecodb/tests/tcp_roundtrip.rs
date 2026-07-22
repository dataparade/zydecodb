use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN,
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

#[test]
fn tcp_put_get_del_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();

    let data_dir2 = data_dir.clone();
    let wal_dir2 = wal_dir.clone();
    thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let engine = zydecodb_engine::engine_handle::EngineHandle::new(
            Engine::open(EngineConfig {
                data_dir: data_dir2,
                wal_dir: wal_dir2,
                block_cache_bytes: 64 * 1024 * 1024,
                max_open_readers: 32,
                ..Default::default()
            })
            .unwrap(),
        );

        let security = zydecodb::security::SecurityRuntime::default();
        let mut session = zydecodb::security::SessionState::anonymous();
        for _ in 0..3 {
            let req = zydecodb::dispatch::read_request(&mut stream).unwrap();
            let outcome = zydecodb::dispatch::handle_request(&engine, req, session, &security);
            session = outcome.session;
            zydecodb::dispatch::write_response(&mut stream, &outcome.response).unwrap();
            let _ = engine.write().poll_compaction();
        }
        let _ = engine.write().shutdown();
    });

    let mut stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
        .set_write_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let put = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: b"hello".to_vec(),
        value: b"world".to_vec(),
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Put, put.encode()),
    );
    let put_resp = read_response(&mut stream);
    assert_eq!(put_resp.status, Status::Ok);

    let get = KeyPayload {
        routing_key: [0u8; 16],
        snapshot_seq: 0,
        key: b"hello".to_vec(),
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Get, get.encode()),
    );
    let get_resp = read_response(&mut stream);
    assert_eq!(get_resp.status, Status::Ok);
    assert_eq!(get_resp.payload, b"world");

    let del = KeyPayload {
        routing_key: [0u8; 16],
        snapshot_seq: 0,
        key: b"hello".to_vec(),
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Del, del.encode()),
    );
    let del_resp = read_response(&mut stream);
    assert_eq!(del_resp.status, Status::Ok);
    assert_eq!(del_resp.payload[0], 1);
}

#[test]
fn server_process_start_stop() {
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

    let mut stream = None;
    for _ in 0..50 {
        if let Ok(s) = TcpStream::connect(addr) {
            stream = Some(s);
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    assert!(stream.is_some());

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
