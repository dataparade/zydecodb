#[path = "common/mod.rs"]
mod common;
use common::*;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use zydecodb::security::keys::{KeyRole, KeyStore};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, PutPayload, RequestEnvelope};

fn kv_put(stream: &mut impl std::io::Write, key: &[u8]) {
    let put = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: key.to_vec(),
        value: b"val".to_vec(),
    };
    write_request(stream, &RequestEnvelope::new(Command::Put, put.encode()));
}

#[test]
fn test_auth_concurrency_revocation() {
    let _ = tracing_subscriber::fmt::try_init();
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");

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
    let runner = server.clone();
    let handle = thread::spawn(move || runner.run(config).unwrap());
    let _ = wait_connect(addr);

    // Dedicated live session — this is the connection the assertion uses.
    let mut live = wait_connect(addr);
    session_init_ok(&mut live, &secret);
    kv_put(&mut live, b"live-before");
    assert_eq!(read_response(&mut live).status, Status::Ok);

    let running = Arc::new(AtomicBool::new(true));
    let mut threads = vec![];
    for i in 0..20 {
        let running = Arc::clone(&running);
        let secret = secret.clone();
        threads.push(thread::spawn(move || {
            let mut stream = wait_connect(addr);
            session_init_ok(&mut stream, &secret);
            while running.load(Ordering::SeqCst) {
                kv_put(&mut stream, format!("key_{i}").as_bytes());
                let _ = read_response(&mut stream);
            }
        }));
    }

    thread::sleep(Duration::from_millis(300));
    KeyStore::revoke_key(&keys_file, "test_key").unwrap();
    server.reload_keys();

    let mut probe = wait_connect(addr);
    write_request(
        &mut probe,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(
        read_response(&mut probe).status,
        Status::Unauthorized,
        "reloaded keystore still accepted the revoked key"
    );

    kv_put(&mut live, b"live-after");
    assert_eq!(
        read_response(&mut live).status,
        Status::Unauthorized,
        "VULNERABILITY SURFACED: Existing connections remained authenticated after their key was revoked!"
    );

    running.store(false, Ordering::SeqCst);
    for t in threads {
        t.join().unwrap();
    }

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
