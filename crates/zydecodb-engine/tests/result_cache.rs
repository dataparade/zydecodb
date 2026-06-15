//! Integration tests for the point-lookup result cache.

// Tests build EngineConfig::default() then tweak a couple of fields.
#![allow(clippy::field_reassign_with_default)]

use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::metrics::Metrics;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

#[test]
fn result_cache_survives_compaction_invalidation_of_block_cache() {
    let dir = TempDir::new().unwrap();
    let metrics = Metrics::new();
    let mut cfg = zydecodb_engine::compaction::CompactionConfig::default();
    cfg.l0_trigger = 2;
    cfg.target_file_bytes = 4 * 1024;
    cfg.l1_target_bytes = 8 * 1024;

    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        block_cache_bytes: 64 * 1024,
        result_cache_bytes: 4 * 1024 * 1024,
        compaction: cfg,
        ..Default::default()
    })
    .expect("engine open")
    .with_metrics(metrics.clone());

    let hot = uk(b"hot");
    e.put(hot.clone(), b"payload".to_vec(), 0).unwrap();
    e.force_flush().unwrap();

    for _ in 0..500 {
        assert_eq!(e.get(&hot).unwrap().as_deref(), Some(b"payload".as_ref()));
    }
    let hits_before = metrics.result_cache_hits_total.get();

    for _ in 0..4 {
        for i in 0..40u32 {
            e.put(uk(format!("k{:04}", i).as_bytes()), b"v".to_vec(), 0)
                .unwrap();
        }
        e.force_flush().unwrap();
    }
    e.drain_compaction().unwrap();

    for _ in 0..200 {
        assert_eq!(e.get(&hot).unwrap().as_deref(), Some(b"payload".as_ref()));
    }
    assert!(
        metrics.result_cache_hits_total.get() > hits_before,
        "hot key should keep hitting the result cache across compaction"
    );
}
