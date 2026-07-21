use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use zydecodb::config::{Config, RequireAuth, SecurityConfig};
use zydecodb::security::keys::{KeyRole, KeyStore};
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
fn auth_required_rejects_anonymous_put() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    let keys_file = tmp.path().join("keys.toml");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let secret = KeyStore::create_key(
        &keys_file,
        "test",
        KeyRole::ReadWrite,
        "00000000000000000000000000000000",
        vec![],
    )
    .unwrap();

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
            require_auth: RequireAuth::True,
            keys_file: keys_file.clone(),
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    };

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);

    let put = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: b"secret".to_vec(),
        value: b"data".to_vec(),
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Put, put.encode()),
    );
    let resp = read_response(&mut stream);
    assert_eq!(resp.status, Status::Unauthorized);

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Put, put.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn read_only_key_cannot_put() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    let keys_file = tmp.path().join("keys.toml");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let secret = KeyStore::create_key(
        &keys_file,
        "ro",
        KeyRole::ReadOnly,
        "00000000000000000000000000000000",
        vec![],
    )
    .unwrap();

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
            require_auth: RequireAuth::True,
            keys_file,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    };

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    let put = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: b"x".to_vec(),
        value: b"y".to_vec(),
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Put, put.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Forbidden);

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn tenant_isolation() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    let keys_file = tmp.path().join("keys.toml");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let secret_a = KeyStore::create_key(
        &keys_file,
        "tenant_a",
        KeyRole::ReadWrite,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        vec![],
    )
    .unwrap();
    let secret_b = KeyStore::create_key(
        &keys_file,
        "tenant_b",
        KeyRole::ReadWrite,
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        vec![],
    )
    .unwrap();

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
            require_auth: RequireAuth::True,
            keys_file,
            legacy_single_tenant: false,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    };

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream_a = wait_connect(addr);
    session_init(&mut stream_a, &secret_a);
    put_key(&mut stream_a, b"shared-name", b"from-a");
    drop(stream_a);

    let mut stream_b = wait_connect(addr);
    session_init(&mut stream_b, &secret_b);
    let get = KeyPayload {
        routing_key: [0u8; 16],
        snapshot_seq: 0,
        key: b"shared-name".to_vec(),
    };
    write_request(
        &mut stream_b,
        &RequestEnvelope::new(Command::Get, get.encode()),
    );
    assert_eq!(read_response(&mut stream_b).status, Status::NotFound);

    drop(stream_b);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn prefix_acl_denies_out_of_scope_keys() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    let keys_file = tmp.path().join("keys.toml");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let secret = KeyStore::create_key(
        &keys_file,
        "analytics",
        KeyRole::ReadWrite,
        "00000000000000000000000000000000",
        vec!["events:".to_string()],
    )
    .unwrap();

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
            require_auth: RequireAuth::True,
            keys_file,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    };

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);
    session_init(&mut stream, &secret);
    put_key(&mut stream, b"events:click", b"1");

    let get = KeyPayload {
        routing_key: [0u8; 16],
        snapshot_seq: 0,
        key: b"events:click".to_vec(),
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Get, get.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    let denied = KeyPayload {
        routing_key: [0u8; 16],
        snapshot_seq: 0,
        key: b"users:1".to_vec(),
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Get, denied.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Forbidden);

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn prefix_acl_applies_to_document_collections() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    let keys_file = tmp.path().join("keys.toml");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let secret = KeyStore::create_key(
        &keys_file,
        "analytics",
        KeyRole::ReadWrite,
        "00000000000000000000000000000000",
        vec!["events:".to_string()],
    )
    .unwrap();

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
            require_auth: RequireAuth::True,
            keys_file,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    };

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);
    session_init(&mut stream, &secret);

    // KV-style prefix "events:" allows collection "events".
    let idx = zydecodb_document::wire::IndexDefPayload {
        collection: "events".into(),
        index_name: "by_n".into(),
        fields: vec!["n".into()],
        unique: false,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::IndexDef, idx.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    let allowed = zydecodb_document::wire::DocPutPayload {
        collection: "events".into(),
        doc_id: b"1".to_vec(),
        body: br#"{"n":1}"#.to_vec(),
        relaxed: false,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::DocPut, allowed.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    let denied = zydecodb_document::wire::DocPutPayload {
        collection: "users".into(),
        doc_id: b"1".to_vec(),
        body: br#"{"n":1}"#.to_vec(),
        relaxed: false,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::DocPut, denied.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Forbidden);

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
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

fn session_init(stream: &mut TcpStream, secret: &str) {
    write_request(
        stream,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(read_response(stream).status, Status::Ok);
}

fn put_key(stream: &mut TcpStream, key: &[u8], value: &[u8]) {
    let put = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: key.to_vec(),
        value: value.to_vec(),
    };
    write_request(stream, &RequestEnvelope::new(Command::Put, put.encode()));
    assert_eq!(read_response(stream).status, Status::Ok);
}
