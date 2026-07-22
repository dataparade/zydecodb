//! Security-only integration suite: locks in every fail-closed guard and
//! authenticated-plane hardening so a refactor cannot silently reopen them.
//!
//! Run with: `cargo test -p zydecodb --test security_suite`

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use zydecodb::config::{
    Config, MetricsConfig, ReplicaConfig, RequireAuth, SecurityConfig, ShippingConfig,
};
use zydecodb::security::keys::{KeyRole, KeyStore};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN,
};

/// Serializes tests that read or mutate `ZYDECODB_BOOTSTRAP_KEY`: the keystore
/// reads the env at load, so a bootstrap set by one test must never leak into
/// another test's startup guard.
static ENV_LOCK: Mutex<()> = Mutex::new(());

const ZERO_TENANT: &str = "00000000000000000000000000000000";

// ---------- harness ----------

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
        fair: Default::default(),
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

fn session_init(stream: &mut TcpStream, secret: &str) -> Status {
    write_request(
        stream,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    read_response(stream).status
}

fn http_get(addr: SocketAddr, path: &str, bearer: Option<&str>) -> (u16, String) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    let auth = match bearer {
        Some(t) => format!("Authorization: Bearer {t}\r\n"),
        None => String::new(),
    };
    let req = format!("GET {path} HTTP/1.1\r\nHost: localhost\r\n{auth}Connection: close\r\n\r\n");
    s.write_all(req.as_bytes()).unwrap();
    let mut buf = String::new();
    s.read_to_string(&mut buf).unwrap();
    let code: u16 = buf
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|c| c.parse().ok())
        .expect("status line");
    (code, buf)
}

fn wait_tcp_up(addr: SocketAddr) {
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("listener at {addr} never came up");
}

// ---------- auth ----------

/// Multiple keys resolve via the sha256 lookup index: each secret maps to its
/// own record and a wrong secret is rejected (proves the O(1) path cannot
/// cross-match records).
#[test]
fn auth_lookup_single_argon_path() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    let s1 =
        KeyStore::create_key(&keys_file, "a", KeyRole::ReadWrite, ZERO_TENANT, vec![]).unwrap();
    let s2 = KeyStore::create_key(&keys_file, "b", KeyRole::ReadOnly, ZERO_TENANT, vec![]).unwrap();

    let addr = free_addr();
    let config = base_config(&tmp, addr, keys_file);
    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut c1 = wait_connect(addr);
    assert_eq!(session_init(&mut c1, &s1), Status::Ok);
    drop(c1);

    let mut c2 = wait_connect(addr);
    assert_eq!(session_init(&mut c2, &s2), Status::Ok);
    // The read-only key really is record "b": a PUT must be Forbidden.
    let put = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    };
    write_request(&mut c2, &RequestEnvelope::new(Command::Put, put.encode()));
    assert_eq!(read_response(&mut c2).status, Status::Forbidden);
    drop(c2);

    let mut c3 = wait_connect(addr);
    assert_eq!(
        session_init(&mut c3, "zdk_definitely_not_a_key"),
        Status::Unauthorized
    );
    drop(c3);

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

/// Auth required + zero keys + no bootstrap: the server must refuse to start
/// instead of running a service nobody can ever authenticate to.
#[test]
fn require_auth_empty_keys_refuses_start() {
    let _env = ENV_LOCK.lock().unwrap();
    assert!(
        std::env::var_os("ZYDECODB_BOOTSTRAP_KEY").is_none(),
        "test env must not carry a bootstrap key"
    );
    let tmp = TempDir::new().unwrap();
    let config = base_config(&tmp, free_addr(), tmp.path().join("empty-keys.toml"));
    let server = zydecodb::server::Server::new();
    let err = server.run(config).expect_err("must refuse to start");
    assert!(
        err.to_string().contains("no keys"),
        "expected empty-keys refusal, got: {err}"
    );
}

/// Bootstrap env key + non-loopback listen: refused at startup.
#[test]
fn bootstrap_non_loopback_refuses_start() {
    let _env = ENV_LOCK.lock().unwrap();
    std::env::set_var("ZYDECODB_BOOTSTRAP_KEY", "zdk_dev_bootstrap");
    let tmp = TempDir::new().unwrap();
    let mut config = base_config(&tmp, free_addr(), tmp.path().join("keys.toml"));
    config.listen = "0.0.0.0:9470".parse().unwrap();
    let server = zydecodb::server::Server::new();
    let result = server.run(config);
    std::env::remove_var("ZYDECODB_BOOTSTRAP_KEY");
    let err = result.expect_err("bootstrap on non-loopback must refuse to start");
    assert!(
        err.to_string().contains("ZYDECODB_BOOTSTRAP_KEY"),
        "expected bootstrap refusal, got: {err}"
    );
}

/// legacy_single_tenant + a non-zero-tenant key is an ambiguous keyspace:
/// refused at startup.
#[test]
fn legacy_mixed_tenants_refuses_start() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    KeyStore::create_key(
        &keys_file,
        "tenant_x",
        KeyRole::ReadWrite,
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        vec![],
    )
    .unwrap();

    let mut config = base_config(&tmp, free_addr(), keys_file);
    config.security.legacy_single_tenant = true;
    let server = zydecodb::server::Server::new();
    let err = server.run(config).expect_err("mixed layout must refuse");
    assert!(
        err.to_string().contains("legacy_single_tenant"),
        "expected legacy/mixed-tenant refusal, got: {err}"
    );
}

// ---------- metrics plane ----------

/// Non-loopback metrics bind without allow_remote: refused at startup.
#[test]
fn metrics_non_loopback_without_allow_refuses() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    KeyStore::create_key(&keys_file, "k", KeyRole::ReadWrite, ZERO_TENANT, vec![]).unwrap();

    let mut config = base_config(&tmp, free_addr(), keys_file);
    config.metrics = MetricsConfig {
        listen: Some("0.0.0.0:9471".parse().unwrap()),
        per_tenant: false,
        allow_remote: false,
        token: None,
    };
    let server = zydecodb::server::Server::new();
    let err = server.run(config).expect_err("remote metrics must refuse");
    assert!(
        err.to_string().contains("allow_remote"),
        "expected metrics bind refusal, got: {err}"
    );
}

/// allow_remote without a token is also refused (token is the whole point).
#[test]
fn metrics_remote_without_token_refuses() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    KeyStore::create_key(&keys_file, "k", KeyRole::ReadWrite, ZERO_TENANT, vec![]).unwrap();

    let mut config = base_config(&tmp, free_addr(), keys_file);
    config.metrics = MetricsConfig {
        listen: Some("0.0.0.0:9471".parse().unwrap()),
        per_tenant: false,
        allow_remote: true,
        token: None,
    };
    let server = zydecodb::server::Server::new();
    let err = server
        .run(config)
        .expect_err("tokenless remote must refuse");
    assert!(
        err.to_string().contains("token"),
        "expected token requirement, got: {err}"
    );
}

/// With a token configured, /metrics needs the bearer; probes stay open.
#[test]
fn metrics_token_required_for_scrape() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    KeyStore::create_key(&keys_file, "k", KeyRole::ReadWrite, ZERO_TENANT, vec![]).unwrap();

    let metrics_addr = free_addr();
    let mut config = base_config(&tmp, free_addr(), keys_file);
    config.metrics = MetricsConfig {
        listen: Some(metrics_addr),
        per_tenant: false,
        allow_remote: false,
        token: Some("scrape-secret".into()),
    };
    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());
    wait_tcp_up(metrics_addr);

    let (code, _) = http_get(metrics_addr, "/metrics", None);
    assert_eq!(code, 401, "no bearer -> 401");
    let (code, _) = http_get(metrics_addr, "/metrics", Some("wrong-token"));
    assert_eq!(code, 401, "wrong bearer -> 401");
    let (code, body) = http_get(metrics_addr, "/metrics", Some("scrape-secret"));
    assert_eq!(code, 200, "correct bearer -> 200");
    assert!(body.contains("# HELP") || body.contains("# TYPE"));
    // Probes stay unauthenticated.
    let (code, _) = http_get(metrics_addr, "/healthz", None);
    assert_eq!(code, 200);
    let (code, _) = http_get(metrics_addr, "/readyz", None);
    assert_eq!(code, 200);

    *shutdown.lock().unwrap() = true;
    let _ = handle.join();
}

// ---------- WAL shipping HMAC ----------

/// Shipping enabled without an HMAC key file: refused at startup.
#[test]
fn shipping_without_hmac_refuses_when_enabled() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    KeyStore::create_key(&keys_file, "k", KeyRole::ReadWrite, ZERO_TENANT, vec![]).unwrap();

    let mut config = base_config(&tmp, free_addr(), keys_file);
    config.shipping = ShippingConfig {
        ship_dir: Some(tmp.path().join("ship")),
        mode: "copy".into(),
        heartbeat_ms: 0,
        hmac_key_file: None,
    };
    let server = zydecodb::server::Server::new();
    let err = server
        .run(config)
        .expect_err("keyless shipping must refuse");
    assert!(
        err.to_string().contains("hmac_key_file"),
        "expected HMAC requirement, got: {err}"
    );
}

/// Replica configured without the shared HMAC key: refused at startup.
#[test]
fn replica_without_hmac_refuses() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    KeyStore::create_key(&keys_file, "k", KeyRole::ReadWrite, ZERO_TENANT, vec![]).unwrap();

    let mut config = base_config(&tmp, free_addr(), keys_file);
    config.replica = ReplicaConfig {
        from: Some(tmp.path().join("ship")),
        poll_ms: 100,
        hmac_key_file: None,
    };
    let server = zydecodb::server::Server::new();
    let err = server.run(config).expect_err("keyless replica must refuse");
    assert!(
        err.to_string().contains("hmac_key_file"),
        "expected HMAC requirement, got: {err}"
    );
}

/// End-to-end HMAC verification on the shipped stream: a valid entry installs;
/// tampered segment bytes and a forged (HMAC-less) manifest line are refused.
#[test]
fn shipping_hmac_roundtrip_rejects_tampering() {
    use zydecodb_engine::shipping::{self, ShipMode};
    use zydecodb_engine::wal;

    let key = b"suite-hmac-key".to_vec();
    let tmp = TempDir::new().unwrap();
    let wal_src = tmp.path().join("src_wal");
    let ship = tmp.path().join("ship");
    std::fs::create_dir_all(&wal_src).unwrap();

    let seg = wal_src.join(wal::segment_filename(1));
    std::fs::write(&seg, b"authentic-segment-bytes").unwrap();
    shipping::ship_segment(&seg, &ship, 1, 10, ShipMode::Copy, Some(&key)).unwrap();

    // Log line carries 4 fields (id, seq, sha256, hmac).
    let log = std::fs::read_to_string(ship.join(shipping::SHIPPED_LOG)).unwrap();
    assert_eq!(
        log.trim().split_whitespace().count(),
        4,
        "expected HMAC field on shipped.log line: {log}"
    );

    // Valid stream installs.
    let replica_wal = tmp.path().join("replica_wal_ok");
    let mut rep = zydecodb::replica::Replica::new(ship.clone(), replica_wal.clone())
        .with_hmac_key(Some(key.clone()));
    let out = rep.sync().unwrap();
    assert_eq!(out.installed, vec![1], "authentic segment must install");

    // Tampered segment bytes are refused.
    let tampered_dir = tmp.path().join("ship_tampered");
    copy_dir(&ship, &tampered_dir);
    std::fs::write(tampered_dir.join(wal::segment_filename(1)), b"evil-bytes!").unwrap();
    let mut rep =
        zydecodb::replica::Replica::new(tampered_dir, tmp.path().join("replica_wal_tampered"))
            .with_hmac_key(Some(key.clone()));
    let err = rep.sync().unwrap_err().to_string();
    assert!(
        err.contains("hash mismatch") || err.contains("corrupt") || err.contains("hmac"),
        "tampered segment must not install; got: {err}"
    );

    // A forged manifest: attacker rewrites the segment AND the sha256 in the
    // log, but cannot produce the HMAC. Refused.
    let forged_dir = tmp.path().join("ship_forged");
    std::fs::create_dir_all(&forged_dir).unwrap();
    let evil = b"forged-segment";
    std::fs::write(forged_dir.join(wal::segment_filename(1)), evil).unwrap();
    let evil_sha = shipping::sha256_file(&forged_dir.join(wal::segment_filename(1))).unwrap();
    std::fs::write(
        forged_dir.join(shipping::SHIPPED_LOG),
        format!("1 10 {evil_sha}\n"),
    )
    .unwrap();
    let mut rep =
        zydecodb::replica::Replica::new(forged_dir, tmp.path().join("replica_wal_forged"))
            .with_hmac_key(Some(key));
    let err = rep.sync().unwrap_err().to_string();
    assert!(
        err.contains("hmac") || err.contains("missing"),
        "forged manifest without a valid HMAC must not install; got: {err}"
    );
}

fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        std::fs::copy(entry.path(), dst.join(entry.file_name())).unwrap();
    }
}

// ---------- UDS ----------

/// The Unix socket is chmod'd to 0600 at bind, regardless of umask.
#[test]
fn uds_socket_mode_0600() {
    use std::os::unix::fs::PermissionsExt;

    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    KeyStore::create_key(&keys_file, "k", KeyRole::ReadWrite, ZERO_TENANT, vec![]).unwrap();

    let addr = free_addr();
    let sock = tmp.path().join("zydecodb.sock");
    let mut config = base_config(&tmp, addr, keys_file);
    config.listen_unix = Some(sock.clone());

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());
    wait_tcp_up(addr);

    let mode = std::fs::metadata(&sock).unwrap().permissions().mode();
    assert_eq!(
        mode & 0o777,
        0o600,
        "unix socket must be 0600, got {:o}",
        mode & 0o777
    );

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

// ---------- prefix ACLs (KV + documents) ----------

/// One server, one scoped key: prefix ACL denies out-of-scope raw-KV keys AND
/// out-of-scope document collections.
#[test]
fn doc_and_kv_prefix_acl() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    let secret = KeyStore::create_key(
        &keys_file,
        "scoped",
        KeyRole::ReadWrite,
        ZERO_TENANT,
        vec!["events:".to_string()],
    )
    .unwrap();

    let addr = free_addr();
    let config = base_config(&tmp, addr, keys_file);
    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);
    assert_eq!(session_init(&mut stream, &secret), Status::Ok);

    // KV: in-scope allowed, out-of-scope Forbidden.
    let put = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: b"events:click".to_vec(),
        value: b"1".to_vec(),
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Put, put.encode()),
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

    // Documents: collection "events" allowed, "users" Forbidden.
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

    let denied_doc = zydecodb_document::wire::DocPutPayload {
        collection: "users".into(),
        doc_id: b"1".to_vec(),
        body: br#"{"n":1}"#.to_vec(),
        relaxed: false,
        expires_at: 0,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::DocPut, denied_doc.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Forbidden);

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

// ---------- DoS bound: sort buffer ----------

/// A small configured max_sort_buffer rejects an oversized sorted result
/// instead of buffering it (memory-DoS bound on the authenticated plane).
#[test]
fn sort_buffer_cap_configurable() {
    let _env = ENV_LOCK.lock().unwrap();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    let secret =
        KeyStore::create_key(&keys_file, "k", KeyRole::ReadWrite, ZERO_TENANT, vec![]).unwrap();

    let addr = free_addr();
    let mut config = base_config(&tmp, addr, keys_file);
    config.security.max_sort_buffer = 2;

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);
    assert_eq!(session_init(&mut stream, &secret), Status::Ok);

    // Define the collection, insert 3 docs (over the cap of 2).
    let idx = zydecodb_document::wire::IndexDefPayload {
        collection: "items".into(),
        index_name: "by_n".into(),
        fields: vec!["n".into()],
        unique: false,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::IndexDef, idx.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    for i in 0..3u8 {
        let doc = zydecodb_document::wire::DocPutPayload {
            collection: "items".into(),
            doc_id: vec![b'0' + i],
            body: format!("{{\"n\":{i},\"m\":{}}}", 10 - i).into_bytes(),
            relaxed: false,
            expires_at: 0,
        };
        write_request(
            &mut stream,
            &RequestEnvelope::new(Command::DocPut, doc.encode()),
        );
        assert_eq!(read_response(&mut stream).status, Status::Ok);
    }

    // A sort on an unindexed field forces the buffered-sort path; 3 matches
    // exceed the cap of 2 and must be rejected, not buffered.
    let find = zydecodb_document::wire::FindPayload {
        collection: "items".into(),
        filter: b"{}".to_vec(),
        sort: vec![("m".into(), true)],
        projection: zydecodb_document::wire::WireProjection::None,
        skip: 0,
        limit: 10,
        cursor: vec![],
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Find, find.encode()),
    );
    let resp = read_response(&mut stream);
    assert_ne!(
        resp.status,
        Status::Ok,
        "oversized sorted result must be rejected"
    );
    let msg = String::from_utf8_lossy(&resp.payload);
    assert!(
        msg.contains("sorted result exceeds 2"),
        "expected sort-buffer rejection, got: {msg}"
    );

    drop(stream);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
