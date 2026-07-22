//! End-to-end round-trip over a Unix-domain socket: the server should serve the
//! same wire protocol on a UDS path as it does over TCP.

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::os::unix::net::UnixStream;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, KeyPayload, PutPayload, RequestEnvelope};

#[test]
fn uds_put_get_roundtrip() {
    let tmp = TempDir::new().unwrap();
    let sock = tmp.path().join("zydeco.sock");
    let addr = free_addr();
    let mut config = base_config(&tmp, addr);
    config.listen_unix = Some(sock.clone());

    let (shutdown, handle) = spawn_server(config);

    // Wait for the socket to come up.
    let mut stream = None;
    for _ in 0..100 {
        if let Ok(s) = UnixStream::connect(&sock) {
            stream = Some(s);
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    let mut stream = stream.expect("server did not bind unix socket");
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
    assert_eq!(read_response(&mut stream).status, Status::Ok);

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

    drop(stream);
    shutdown_join(&shutdown, handle);
}
