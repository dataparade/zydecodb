#[path = "common/mod.rs"]
mod common;
use common::*;

use rcgen::{CertificateParams, KeyPair};
use std::io::Read;
use std::sync::Arc;
use tempfile::TempDir;
use zydecodb::config::TlsConfig;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

#[test]
fn tls_ping_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let cert_path = tmp.path().join("cert.pem");
    let key_path = tmp.path().join("key.pem");

    let key_pair = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["localhost".to_string()]).unwrap();
    params.distinguished_name.push(
        rcgen::DnType::CommonName,
        rcgen::DnValue::Utf8String("localhost".into()),
    );
    let cert = params.self_signed(&key_pair).unwrap();
    std::fs::write(&cert_path, cert.pem()).unwrap();
    std::fs::write(&key_path, key_pair.serialize_pem()).unwrap();

    let addr = free_addr();
    let mut config = base_config(&tmp, addr);
    config.tls = TlsConfig {
        enabled: true,
        cert: Some(cert_path.clone()),
        key: Some(key_path),
    };

    let (shutdown, handle) = spawn_server(config);
    wait_tcp_up(addr);

    let stream = wait_connect(addr);

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
    write_request(&mut tls, &ping);

    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    tls.read_exact(&mut header).unwrap();
    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
    assert_eq!(status, Status::Ok);
    let mut payload = vec![0u8; len];
    if len > 0 {
        tls.read_exact(&mut payload).unwrap();
    }

    drop(tls);
    shutdown_join(&shutdown, handle);
}
