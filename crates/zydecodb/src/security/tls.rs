use rustls::pki_types::CertificateDer;
use rustls::{ServerConfig, ServerConnection};
use std::fs::File;
use std::io;
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;

pub type TlsStream = rustls::StreamOwned<ServerConnection, TcpStream>;

pub fn load_server_config(cert_path: &Path, key_path: &Path) -> Result<Arc<ServerConfig>, String> {
    let certfile = File::open(cert_path).map_err(|e| e.to_string())?;
    let mut reader = std::io::BufReader::new(certfile);
    let certs: Vec<CertificateDer<'static>> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if certs.is_empty() {
        return Err("no certificates found in cert file".into());
    }

    let keyfile = File::open(key_path).map_err(|e| e.to_string())?;
    let mut reader = std::io::BufReader::new(keyfile);
    let key = rustls_pemfile::private_key(&mut reader)
        .map_err(|e| e.to_string())?
        .ok_or_else(|| "no private key found in key file".to_string())?;

    let config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| e.to_string())?;
    Ok(Arc::new(config))
}

pub fn accept(tcp: TcpStream, config: &Arc<ServerConfig>) -> Result<TlsStream, String> {
    let session = ServerConnection::new(Arc::clone(config)).map_err(|e| e.to_string())?;
    let mut tls = rustls::StreamOwned::new(session, tcp);
    while tls.conn.is_handshaking() {
        if tls.conn.wants_write() {
            tls.conn
                .write_tls(&mut tls.sock)
                .map_err(|e| e.to_string())?;
        }
        if tls.conn.wants_read() {
            match tls.conn.read_tls(&mut tls.sock) {
                Ok(0) => return Err("tls handshake eof".into()),
                Ok(_) => {
                    tls.conn.process_new_packets().map_err(|e| e.to_string())?;
                }
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Err(e.to_string()),
            }
        }
    }
    Ok(tls)
}
