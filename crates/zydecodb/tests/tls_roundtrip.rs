use rcgen::{CertificateParams, KeyPair};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use zydecodb::config::{Config, RequireAuth, SecurityConfig, TlsConfig};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

#[test]
fn tls_ping_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    let cert_path = tmp.path().join("cert.pem");
    let key_path = tmp.path().join("key.pem");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();

    let key_pair = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("localhost".into()),
    );
    let cert = params.self_signed(&key_pair).unwrap();
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key_pair.serialize_pem()).unwrap();

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
            ..Default::default()
        },
        tls: TlsConfig {
            enabled: true,
            cert: Some(cert_path.clone()),
            key: Some(key_path),
        },
        listen_unix: None,
        runtime: Default::default(),
        fair: Default::default(),
    };

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    for _ in 0..50 {
        if TcpStream::connect(addr).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }

    let stream = TcpStream::connect(addr).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let server_name = rustls::pki_types::ServerName::try_from("localhost").unwrap();
    let mut root_store = rustls::RootCertStore::empty();
    let cert_der = rustls_pemfile::certs(&mut std::io::BufReader::new(
        std::fs::File::open(&cert_path).unwrap(),
    ))
    .next()
    .unwrap()
    .unwrap();
    root_store.add(cert_der).unwrap();
    let tls_config = rustls::ClientConfig::builder()
        .with_root_certificates(root_store)
        .with_no_client_auth();
    let session = rustls::ClientConnection::new(Arc::new(tls_config), server_name).unwrap();
    let mut tls = rustls::StreamOwned::new(session, stream);

    let ping = RequestEnvelope::new(Command::Ping, vec![]);
    tls.write_all(&ping.encode()).unwrap();
    tls.flush().unwrap();

    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    tls.read_exact(&mut header).unwrap();
    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
    assert_eq!(status, Status::Ok);
    let mut payload = vec![0u8; len];
    if len > 0 {
        tls.read_exact(&mut payload).unwrap();
    }

    drop(tls);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
