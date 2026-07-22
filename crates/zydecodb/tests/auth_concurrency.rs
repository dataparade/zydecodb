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
use zydecodb_engine::frame::{Command, RequestEnvelope};

#[test]
fn test_auth_concurrency_revocation() {
    let _ = tracing_subscriber::fmt::try_init();
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

    let running = Arc::new(AtomicBool::new(true));
    let mut threads = vec![];

    // Spawn 20 client threads
    for i in 0..20 {
        let running = Arc::clone(&running);
        let secret = secret.clone();
        threads.push(thread::spawn(move || {
            let mut stream = wait_connect(addr);

            // SessionInit
            write_request(
                &mut stream,
                &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
            );
            let resp = read_response(&mut stream);
            assert_eq!(resp.status, Status::Ok);

            let mut successes = 0;
            let mut unauthorized = 0;

            while running.load(Ordering::SeqCst) {
                let put = zydecodb_engine::frame::PutPayload {
                    routing_key: [0u8; 16],
                    txid: 0,
                    expires_at: 0,
                    key: format!("key_{}", i).into_bytes(),
                    value: b"val".to_vec(),
                };

                write_request(
                    &mut stream,
                    &RequestEnvelope::new(Command::Put, put.encode()),
                );
                let resp = read_response(&mut stream);

                if resp.status == Status::Ok {
                    successes += 1;
                } else if resp.status == Status::Unauthorized || resp.status == Status::Forbidden {
                    unauthorized += 1;
                    break; // Exit the thread once we see the revocation
                } else {
                    // println!("Thread {} got {:?}", i, resp.status);
                }
            }
            (successes, unauthorized)
        }));
    }

    // Let them run for a bit
    thread::sleep(Duration::from_millis(500));

    // Revoke the key
    KeyStore::revoke_key(&keys_file, "test_key").unwrap();

    // Trigger reload
    std::process::Command::new("kill")
        .arg("-HUP")
        .arg(std::process::id().to_string())
        .status()
        .unwrap();

    // Wait for reload to process
    thread::sleep(Duration::from_millis(500));

    // Now, any request should be rejected if the key was revoked.
    // Let's make one more request on a NEW connection to verify the key is gone.
    let mut stream = wait_connect(addr);
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::SessionInit, secret.as_bytes().to_vec()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Unauthorized);

    // But what about the EXISTING connections?
    // Let's wait a bit for the threads to hit their next request and get Unauthorized.
    thread::sleep(Duration::from_millis(500));
    running.store(false, Ordering::SeqCst);

    let mut any_unauthorized = false;
    for t in threads {
        let (_succ, unauth) = t.join().unwrap();
        if unauth > 0 {
            any_unauthorized = true;
        }
    }

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();

    assert!(
        any_unauthorized,
        "VULNERABILITY SURFACED: Existing connections remained authenticated after their key was revoked!"
    );
}
