//! `Engine::tenant_usage_bytes` seeds per-tenant quota accounting at startup.
//! The scan must reflect data written by a *previous* process lifetime so
//! byte caps survive restarts (previously usage reset to zero on every boot).

use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

fn cfg(dir: &TempDir) -> EngineConfig {
    EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("wal"),
        ..Default::default()
    }
}

fn tenant_key(tenant: u8, client: &[u8]) -> Vec<u8> {
    let mut k = vec![KS_USER];
    k.extend_from_slice(&[tenant; 16]);
    k.extend_from_slice(client);
    k
}

#[test]
fn usage_scan_sums_live_values_per_tenant() {
    let dir = TempDir::new().unwrap();
    let mut e = Engine::open(cfg(&dir)).unwrap();

    e.put(tenant_key(1, b"a"), b"12345".to_vec(), 0).unwrap(); // 5 bytes
    e.put(tenant_key(1, b"b"), b"123".to_vec(), 0).unwrap(); // 3 bytes
    e.put(tenant_key(2, b"a"), b"1234567".to_vec(), 0).unwrap(); // 7 bytes
    // Legacy un-prefixed single-tenant layout (no 16-byte tenant): skipped,
    // matching the write-path policy's own tenant extraction.
    e.put(vec![KS_USER, b'x'], b"999".to_vec(), 0).unwrap();
    // Overwrite credits the old value; delete frees it.
    e.put(tenant_key(1, b"a"), b"12".to_vec(), 0).unwrap(); // now 2 bytes
    e.put(tenant_key(1, b"gone"), b"1234".to_vec(), 0).unwrap();
    e.del(tenant_key(1, b"gone")).unwrap();

    let usage = e.tenant_usage_bytes().unwrap();
    assert_eq!(usage.get(&[1u8; 16]), Some(&(2 + 3)));
    assert_eq!(usage.get(&[2u8; 16]), Some(&7));
    assert_eq!(usage.len(), 2);
}

#[test]
fn usage_scan_reflects_pre_restart_state() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = Engine::open(cfg(&dir)).unwrap();
        e.put(tenant_key(7, b"a"), b"ten bytes!".to_vec(), 0).unwrap();
        e.put(tenant_key(7, b"b"), b"five".to_vec(), 0).unwrap();
        // Dirty shutdown: no Engine::shutdown, so the next open replays the WAL.
        drop(e);
    }
    let reopened = Engine::open(cfg(&dir)).unwrap();
    let usage = reopened.tenant_usage_bytes().unwrap();
    assert_eq!(
        usage.get(&[7u8; 16]),
        Some(&(10 + 4)),
        "a fresh process must see the previous lifetime's bytes"
    );
}
