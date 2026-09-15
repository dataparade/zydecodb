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
    // A collection per tenant, each with a counted document, so the drop has
    // catalog entries and counter records to clean up.
    let prefix_a: Vec<u8> = [&[zydecodb_engine::keys::KS_USER][..], &tenant_a[..]].concat();
    let prefix_b: Vec<u8> = [&[zydecodb_engine::keys::KS_USER][..], &tenant_b[..]].concat();
    catalog.ensure_collection(&prefix_a, "docs");
    catalog.ensure_collection(&prefix_b, "docs");
    catalog.persist(&mut engine).unwrap();
    for prefix in [&prefix_a, &prefix_b] {
        zydecodb_document::store::upsert(
            &mut engine,
            &mut catalog,
            prefix,
            "docs",
            b"d1",
            br#"{"n":1}"#,
            false,
        )
        .unwrap();
    }
    let coll_a = catalog.collection(&prefix_a, "docs").unwrap().id;
    let coll_b = catalog.collection(&prefix_b, "docs").unwrap().id;
    let counter_key = zydecodb_document::catalog::counter_sys_key;
    assert!(engine.sys_get(&counter_key(coll_a)).unwrap().is_some());

    // Simulate live drop while the engine remains open (server holds data_dir).
    let result =
        zydecodb::admin::drop_tenant_on_engine(&mut engine, &mut catalog, &tenant_a, false)
            .unwrap();
    // Raw key + document key for tenant A.
    assert_eq!(result.deleted_keys, 2);
    assert_eq!(result.removed_collections, 1);
    assert!(catalog.collection(&prefix_a, "docs").is_none());
    assert_eq!(catalog.collection(&prefix_b, "docs").unwrap().doc_count, 1);
    // Tenant A's counter record is gone; tenant B's survives.
    assert!(engine.sys_get(&counter_key(coll_a)).unwrap().is_none());
    assert!(engine.sys_get(&counter_key(coll_b)).unwrap().is_some());
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
