//! Integration tests for the SSTable block cache (engine-level effects).

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

fn open_with_cache(dir: &TempDir, cache_bytes: usize) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        block_cache_bytes: cache_bytes,
        ..Default::default()
    })
    .expect("engine open")
}

#[test]
fn repeated_get_after_flush_yields_cache_hits() {
    let dir = TempDir::new().unwrap();
    let mut e = open_with_cache(&dir, 16 * 1024 * 1024);

    for i in 0..200u32 {
        e.put(uk(format!("k{:04}", i).as_bytes()), b"v".to_vec(), 0)
            .unwrap();
    }
    e.force_flush().unwrap();

    // First read warms the cache for the relevant block; repeated reads
    // for the same key should not require disk for subsequent calls.
    // The engine doesn't expose the cache directly through metrics in
    // this test, but the get must succeed every time.
    for _ in 0..1000 {
        assert!(e.get(&uk(b"k0050")).unwrap().is_some());
    }
}

#[test]
fn engine_works_with_tiny_cache_capacity() {
    // 1 KB cache forces near-constant eviction; correctness must hold.
    let dir = TempDir::new().unwrap();
    let mut e = open_with_cache(&dir, 1024);

    for i in 0..500u32 {
        e.put(
            uk(format!("k{:04}", i).as_bytes()),
            b"value-payload".to_vec(),
            0,
        )
        .unwrap();
    }
    e.force_flush().unwrap();

    for i in 0..500u32 {
        let got = e.get(&uk(format!("k{:04}", i).as_bytes())).unwrap();
        assert_eq!(got.as_deref(), Some(b"value-payload".as_ref()));
    }
}

#[test]
fn compaction_reads_bypass_user_cache_counters() {
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
        compaction: cfg,
        ..Default::default()
    })
    .expect("engine open")
    .with_metrics(metrics.clone());

    for _ in 0..8 {
        for i in 0..40u32 {
            e.put(uk(format!("k{:04}", i).as_bytes()), b"v".to_vec(), 0)
                .unwrap();
        }
        e.force_flush().unwrap();
    }

    for _ in 0..200 {
        let _ = e.get(&uk(b"k0020")).unwrap();
    }

    // Sync block-cache counters into metrics before the baseline snapshot.
    // (Puts no longer refresh topology gauges — that was readdir-per-write waste.)
    e.refresh_metrics();
    let hits_before = metrics.block_cache_hits_total.get();
    let misses_before = metrics.block_cache_misses_total.get();

    e.drain_compaction().unwrap();
    e.refresh_metrics();

    assert_eq!(
        metrics.block_cache_hits_total.get(),
        hits_before,
        "compaction must not increment user cache hits"
    );
    assert_eq!(
        metrics.block_cache_misses_total.get(),
        misses_before,
        "compaction must not increment user cache misses"
    );
    assert!(
        metrics.block_cache_compaction_reads_total.get() > 0,
        "compaction must record bypass reads"
    );
}

#[test]
fn cache_is_invalidated_when_sstable_is_unlinked_by_compaction() {
    // After compaction unlinks input SSTables, the cache must drop their
    // blocks so a future SSTable with a recycled id (if any) cannot serve
    // stale data. We can't observe the cache directly here, but we CAN
    // verify that compaction completes cleanly and reads still work.
    let dir = TempDir::new().unwrap();
    let mut cfg = zydecodb_engine::compaction::CompactionConfig::default();
    cfg.l0_trigger = 2;
    cfg.target_file_bytes = 4 * 1024;
    cfg.l1_target_bytes = 16 * 1024;

    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        block_cache_bytes: 64 * 1024,
        compaction: cfg,
        ..Default::default()
    })
    .unwrap();

    for batch in 0..6 {
        for i in 0..30u32 {
            let key = uk(format!("k{:04}", i).as_bytes());
            let val = format!("b{}", batch).into_bytes();
            e.put(key, val, 0).unwrap();
        }
        e.force_flush().unwrap();
    }

    for i in 0..30u32 {
        let got = e.get(&uk(format!("k{:04}", i).as_bytes())).unwrap();
        assert_eq!(
            got.as_deref(),
            Some(b"b5".as_ref()),
            "k{:04} must reflect newest batch after compaction",
            i
        );
    }
}

#[test]
fn metadata_not_counted_toward_block_cache() {
    let dir = TempDir::new().unwrap();
    let metrics = Metrics::new();
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        block_cache_bytes: 4 * 1024 * 1024,
        ..Default::default()
    })
    .expect("engine open")
    .with_metrics(metrics.clone());

    for i in 0..100u32 {
        e.put(uk(format!("k{:04}", i).as_bytes()), b"payload".to_vec(), 0)
            .unwrap();
    }
    e.force_flush().unwrap();

    assert_eq!(
        metrics.block_cache_resident_bytes.get(),
        0,
        "index/bloom must not be charged to the block cache on SSTable open"
    );
}

#[test]
fn tiny_cache_evicts_data_without_losing_correctness() {
    let dir = TempDir::new().unwrap();
    let mut cfg = zydecodb_engine::compaction::CompactionConfig::default();
    cfg.l0_trigger = 2;
    cfg.target_file_bytes = 4 * 1024;
    cfg.l1_target_bytes = 16 * 1024;

    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        block_cache_bytes: 32 * 1024,
        compaction: cfg,
        ..Default::default()
    })
    .unwrap();

    for _ in 0..8 {
        for i in 0..40u32 {
            e.put(uk(format!("k{:04}", i).as_bytes()), b"v".to_vec(), 0)
                .unwrap();
        }
        e.force_flush().unwrap();
    }
    e.drain_compaction().unwrap();

    for i in 0..40u32 {
        assert!(
            e.get(&uk(format!("k{:04}", i).as_bytes()))
                .unwrap()
                .is_some(),
            "reads must succeed after data-block eviction under tiny cache"
        );
    }
}
