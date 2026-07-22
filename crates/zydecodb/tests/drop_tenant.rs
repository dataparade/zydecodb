//! Tenant offboarding: `admin drop-tenant` must remove all of one tenant's keys
//! (and reclaim space with `--compact`) while leaving other tenants untouched.

use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

/// A user key under `tenant`: `KS_USER || <16-byte tenant> || suffix`.
fn tenant_key(tenant: &[u8; 16], suffix: &[u8]) -> Vec<u8> {
    let mut k = Vec::with_capacity(1 + 16 + suffix.len());
    k.push(KS_USER);
    k.extend_from_slice(tenant);
    k.extend_from_slice(suffix);
    k
}

fn engine_cfg(tmp: &std::path::Path) -> EngineConfig {
    EngineConfig {
        data_dir: tmp.join("data"),
        wal_dir: tmp.join("wal"),
        ..Default::default()
    }
}

#[test]
fn drop_tenant_removes_only_that_tenant() {
    let tmp = TempDir::new().unwrap();
    let tenant_a = 1u128.to_be_bytes();
    let tenant_b = 2u128.to_be_bytes();

    // Seed two tenants' data, then release the lock before the CLI opens it.
    {
        let mut engine = Engine::open(engine_cfg(tmp.path())).unwrap();
        engine
            .put(tenant_key(&tenant_a, b":k1"), b"a1".to_vec(), 0)
            .unwrap();
        engine
            .put(tenant_key(&tenant_a, b":k2"), b"a2".to_vec(), 0)
            .unwrap();
        engine
            .put(tenant_key(&tenant_b, b":k1"), b"b1".to_vec(), 0)
            .unwrap();
        engine.shutdown().unwrap();
    }

    // Minimal config pointing the CLI at the same data/wal dirs.
    let config_path = tmp.path().join("zydeco.toml");
    let toml = format!(
        "listen = \"127.0.0.1:0\"\n\
         data_dir = {data:?}\n\
         wal_dir = {wal:?}\n\
         [security]\n\
         require_auth = \"false\"\n",
        data = tmp.path().join("data"),
        wal = tmp.path().join("wal"),
    );
    std::fs::write(&config_path, toml).unwrap();

    let tenant_a_hex = format!("{:032x}", 1u128);
    zydecodb::admin::drop_tenant(&config_path, &tenant_a_hex, true).unwrap();

    // Reopen: tenant A is gone, tenant B is intact.
    let engine = Engine::open(engine_cfg(tmp.path())).unwrap();
    assert_eq!(engine.get(&tenant_key(&tenant_a, b":k1")).unwrap(), None);
    assert_eq!(engine.get(&tenant_key(&tenant_a, b":k2")).unwrap(), None);
    assert_eq!(
        engine.get(&tenant_key(&tenant_b, b":k1")).unwrap(),
        Some(b"b1".to_vec())
    );
}

#[test]
fn drop_tenant_on_engine_live_path_leaves_other_tenant() {
    use zydecodb_document::catalog::Catalog;

    let tmp = TempDir::new().unwrap();
    let tenant_a = 1u128.to_be_bytes();
    let tenant_b = 2u128.to_be_bytes();
    let mut engine = Engine::open(engine_cfg(tmp.path())).unwrap();
    engine
        .put(tenant_key(&tenant_a, b":k1"), b"a1".to_vec(), 0)
        .unwrap();
    engine
        .put(tenant_key(&tenant_b, b":k1"), b"b1".to_vec(), 0)
        .unwrap();
    let mut catalog = Catalog::load(&engine).unwrap();

    // Simulate live drop while the engine remains open (server holds data_dir).
    let result =
        zydecodb::admin::drop_tenant_on_engine(&mut engine, &mut catalog, &tenant_a, false)
            .unwrap();
    assert_eq!(result.deleted_keys, 1);
    assert_eq!(engine.get(&tenant_key(&tenant_a, b":k1")).unwrap(), None);
    assert_eq!(
        engine.get(&tenant_key(&tenant_b, b":k1")).unwrap(),
        Some(b"b1".to_vec())
    );
    // Concurrent-style write from the surviving tenant still works.
    engine
        .put(tenant_key(&tenant_b, b":k2"), b"b2".to_vec(), 0)
        .unwrap();
    assert_eq!(
        engine.get(&tenant_key(&tenant_b, b":k2")).unwrap(),
        Some(b"b2".to_vec())
    );
}
