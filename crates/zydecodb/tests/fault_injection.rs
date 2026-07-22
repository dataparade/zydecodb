#[path = "common/mod.rs"]
mod common;
use common::*;

use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use zydecodb::security::keys::{KeyRole, KeyStore};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope};

#[test]
fn test_keystore_load_io_error_fallback() {
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");

    // Initial key
    let secret = KeyStore::create_key(
        &keys_file,
        "test_key",
        KeyRole::ReadWrite,
        "00000000000000000000000000000000",
        vec![],
    )
    .unwrap();

    let addr = free_addr();
    let config = auth_config(&tmp, addr, keys_file.clone());
    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    // Wait for server to start
    let _ = wait_connect(addr);

    // Verify key works
    let mut stream = wait_connect(addr);
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    // Enable failpoint to simulate I/O error on reload
    fail::cfg("keystore_load_io_error", "return").unwrap();

    // Trigger reload
    std::process::Command::new("kill")
        .arg("-HUP")
        .arg(std::process::id().to_string())
        .status()
        .unwrap();

    // Wait for reload to process
    thread::sleep(Duration::from_millis(500));

    // Verify the server DID NOT crash, and the OLD key still works
    let mut stream2 = wait_connect(addr);
    write_request(
        &mut stream2,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(
        read_response(&mut stream2).status,
        Status::Ok,
        "Server should retain old keys on I/O error"
    );

    // Also verify it didn't fail open (anonymous access should still be denied)
    let mut stream3 = wait_connect(addr);
    write_request(&mut stream3, &RequestEnvelope::new(Command::Stats, vec![]));
    assert_eq!(
        read_response(&mut stream3).status,
        Status::Unauthorized,
        "Server should not fail open"
    );

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
