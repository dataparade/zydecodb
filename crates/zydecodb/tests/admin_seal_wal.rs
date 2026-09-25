//! End-to-end `AdminSealWal` (0x43): a live seal rotates the active WAL
//! segment, ships it, and reports the outcome as JSON. A second seal with no
//! intervening writes is a no-op (`sealed:false`), so backup orchestration can
//! poll safely.

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::net::TcpStream;
use tempfile::TempDir;
use zydecodb::security::keys::{KeyRole, KeyStore};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, PutPayload, RequestEnvelope};

fn put(stream: &mut TcpStream, key: &[u8]) {
    let p = PutPayload {
        routing_key: [0u8; 16],
        txid: 0,
        expires_at: 0,
        key: key.to_vec(),
        value: b"v".to_vec(),
    };
    write_request(stream, &RequestEnvelope::new(Command::Put, p.encode()));
    let resp = read_response(stream);
    assert_eq!(resp.status, Status::Ok, "Put {key:?}: {resp:?}");
}

fn seal(stream: &mut TcpStream) -> (Status, String) {
    write_request(stream, &RequestEnvelope::new(Command::AdminSealWal, vec![]));
    let resp = read_response(stream);
    (
        resp.status,
        String::from_utf8_lossy(&resp.payload).into_owned(),
    )
}

#[test]
fn admin_seal_wal_rotates_ships_and_noops_when_empty() {
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    let admin = KeyStore::create_key(
        &keys_file,
        "admin",
        KeyRole::Admin,
        "00000000000000000000000000000000",
        vec![],
    )
    .unwrap();

    let ship_dir = tmp.path().join("ship");
    let hmac_file = tmp.path().join("ship.hmac");
    std::fs::write(&hmac_file, [0x5Au8; 32]).unwrap();
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&hmac_file, std::fs::Permissions::from_mode(0o600)).unwrap();
    }

    let addr = free_addr();
    let mut cfg = auth_config(&tmp, addr, keys_file);
    cfg.shipping.ship_dir = Some(ship_dir.clone());
    cfg.shipping.hmac_key_file = Some(hmac_file);
    let (shutdown, handle) = spawn_server(cfg);

    let mut s = wait_connect(addr);
    session_init_ok(&mut s, &admin);

    // Empty active segment: seal is a no-op and says so.
    let (status, body) = seal(&mut s);
    assert_eq!(status, Status::Ok);
    assert!(
        body.contains("\"sealed\":false"),
        "empty seal should report sealed:false, got {body}"
    );

    for i in 0..8u32 {
        put(&mut s, format!("k{i}").as_bytes());
    }

    let (status, body) = seal(&mut s);
    assert_eq!(status, Status::Ok, "seal failed: {body}");
    assert!(
        body.contains("\"sealed\":true"),
        "expected sealed:true, got {body}"
    );
    assert!(
        body.contains("\"shipped\":true"),
        "expected shipped:true, got {body}"
    );

    // The shipped log entry must verify against the shipped segment bytes.
    let entries = zydecodb_engine::shipping::read_shipped_log(&ship_dir).unwrap();
    assert_eq!(entries.len(), 1, "expected exactly one shipped entry");
    let entry = &entries[0];
    let seg_path = ship_dir.join(format!("wal-{:08}.log", entry.segment_id));
    assert!(seg_path.exists(), "missing shipped segment {seg_path:?}");
    let hmac = std::fs::read(tmp.path().join("ship.hmac")).unwrap();
    assert!(
        zydecodb_engine::shipping::verify_entry(&seg_path, entry, Some(&hmac)).unwrap(),
        "shipped entry failed verification"
    );

    // No writes since the seal: another seal is a no-op.
    let (status, body) = seal(&mut s);
    assert_eq!(status, Status::Ok);
    assert!(
        body.contains("\"sealed\":false"),
        "second seal should be a no-op, got {body}"
    );
    let entries = zydecodb_engine::shipping::read_shipped_log(&ship_dir).unwrap();
    assert_eq!(entries.len(), 1, "no-op seal must not append shipped.log");

    shutdown_join(&shutdown, handle);
}
