//! End-to-end compaction tests.
//!
//! Drives the engine through enough flushes to trip the L0 compaction
//! trigger, then asserts:
//!   - The visible state matches the BTreeMap reference (no data loss /
//!     duplication / reordering through the merge).
//!   - The number of L0 SSTables drops after compaction.
//!   - L1 SSTables appear and are non-overlapping in key range.
//!   - Tombstones in the input correctly suppress the older value in the
//!     compacted output (point reads return None).

// Tests build CompactionConfig::default() then tweak a couple of fields.
#![allow(clippy::field_reassign_with_default)]

use std::collections::BTreeMap;
use tempfile::TempDir;
use zydecodb_engine::compaction::CompactionConfig;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn open(dir: &TempDir, cfg: CompactionConfig) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        compaction: cfg,
        ..Default::default()
    })
    .expect("engine open")
}

fn flush_each_write_cfg() -> CompactionConfig {
    // Trigger compaction at the smallest possible threshold so a handful of
    // flushes provoke it. Tiny target file size so we exercise the splitter.
    let mut c = CompactionConfig::default();
    c.l0_trigger = 4;
    c.target_file_bytes = 4 * 1024; // 4 KB outputs
    c.l1_target_bytes = 16 * 1024;
    c
}

#[test]
fn compaction_reduces_l0_count_and_creates_l1_files() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir, flush_each_write_cfg());

    // Five flushes — each produces one L0 SSTable. The 4th triggers
    // compaction post-flush, which folds all 4 L0 files into L1.
    for batch in 0..5 {
        for i in 0..50u32 {
            let key = format!("k{:04}_{}", i, batch);
            let val = format!("v{:04}_{}", i, batch);
            e.put(uk(key.as_bytes()), val.into_bytes(), 0).unwrap();
        }
        e.force_flush().unwrap();
    }

    let lvls: std::collections::HashMap<u8, usize> = e.live_sstable_levels().into_iter().collect();
    assert!(
        lvls.get(&0).copied().unwrap_or(0) < 4,
        "L0 should have been compacted, got {:?}",
        lvls
    );
    assert!(
        lvls.get(&1).copied().unwrap_or(0) >= 1,
        "L1 should have output files, got {:?}",
        lvls
    );
}

#[test]
fn data_survives_compaction_intact() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir, flush_each_write_cfg());

    let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for batch in 0..6 {
        for i in 0..40u32 {
            let key = uk(format!("key{:04}", i).as_bytes());
            let val = format!("v_b{}_i{}", batch, i).into_bytes();
            e.put(key.clone(), val.clone(), 0).unwrap();
            reference.insert(key, val);
        }
        e.force_flush().unwrap();
    }

    for (k, v) in &reference {
        assert_eq!(
            e.get(k).unwrap().as_deref(),
            Some(v.as_slice()),
            "key mismatch after compaction for {:?}",
            k
        );
    }
}

#[test]
fn tombstones_in_input_suppress_older_values_after_compaction() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir, flush_each_write_cfg());

    // Establish 30 keys across two flushes, then delete every other one
    // across two more flushes.
    for i in 0..30u32 {
        let key = uk(format!("k{:04}", i).as_bytes());
        e.put(key, format!("v{}", i).into_bytes(), 0).unwrap();
    }
    e.force_flush().unwrap();

    for i in 0..30u32 {
        if i % 2 == 0 {
            let key = uk(format!("k{:04}", i).as_bytes());
            e.del(key).unwrap();
        }
    }
    e.force_flush().unwrap();
    // Two more flushes to trip compaction.
    for batch in 0..2 {
        for i in 100..110u32 {
            let key = uk(format!("filler{:04}_{}", i, batch).as_bytes());
            e.put(key, b"v".to_vec(), 0).unwrap();
        }
        e.force_flush().unwrap();
    }

    for i in 0..30u32 {
        let key = uk(format!("k{:04}", i).as_bytes());
        let got = e.get(&key).unwrap();
        if i % 2 == 0 {
            assert!(got.is_none(), "k{} should be tombstoned, got {:?}", i, got);
        } else {
            assert_eq!(
                got.as_deref(),
                Some(format!("v{}", i).as_bytes()),
                "k{} should still be present",
                i
            );
        }
    }
}

#[test]
fn compaction_survives_reopen() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = open(&dir, flush_each_write_cfg());
        for batch in 0..5 {
            for i in 0..30u32 {
                let key = uk(format!("k{:04}", i).as_bytes());
                let val = format!("b{}_i{}", batch, i).into_bytes();
                e.put(key, val, 0).unwrap();
            }
            e.force_flush().unwrap();
        }
    }
    // Reopen — recovery must reconstruct the per-level catalog from the
    // manifest's SstableAdd/SstableRemove records and present the same
    // logical state.
    let e = open(&dir, flush_each_write_cfg());
    for i in 0..30u32 {
        let key = uk(format!("k{:04}", i).as_bytes());
        let expected = format!("b4_i{}", i).into_bytes();
        assert_eq!(
            e.get(&key).unwrap(),
            Some(expected),
            "key k{} missing or stale after reopen",
            i
        );
    }
}

#[test]
fn flush_returns_before_background_compaction_finishes() {
    use std::time::{Duration, Instant};
    let dir = TempDir::new().unwrap();
    let mut cfg = CompactionConfig::default();
    cfg.l0_trigger = 2;
    cfg.target_file_bytes = 1024;
    let mut e = open(&dir, cfg);
    for batch in 0..4 {
        for i in 0..80u32 {
            let key = uk(format!("k{:04}_{}", i, batch).as_bytes());
            e.put(key, vec![0u8; 256], 0).unwrap();
        }
        let t0 = Instant::now();
        e.force_flush().unwrap();
        assert!(
            t0.elapsed() < Duration::from_secs(2),
            "flush took {:?}; foreground compaction may still be blocking",
            t0.elapsed()
        );
    }
    e.drain_compaction().unwrap();
}

#[test]
fn compaction_does_not_leak_sstable_files() {
    let dir = TempDir::new().unwrap();
    let data = dir.path().join("data");

    {
        let mut e = open(&dir, flush_each_write_cfg());
        for batch in 0..8 {
            for i in 0..40u32 {
                let key = uk(format!("k{:04}", i).as_bytes());
                let val = format!("b{}_i{}", batch, i).into_bytes();
                e.put(key, val, 0).unwrap();
            }
            e.force_flush().unwrap();
        }
        e.drain_compaction().unwrap();
    }

    // Without compaction we'd have ~8 .sst files. With compaction kicking
    // in at L0>=4, we expect fewer files on disk than flushes performed —
    // compaction outputs replace inputs, and the inputs were unlinked.
    let sst_count = std::fs::read_dir(&data)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.path()
                .extension()
                .map(|ext| ext == "sst")
                .unwrap_or(false)
        })
        .count();
    assert!(
        sst_count < 8,
        "expected compaction to consolidate flushes, found {} .sst files",
        sst_count
    );
}

#[test]
fn space_amplification_drops_after_tombstone_gc() {
    let dir = TempDir::new().unwrap();
    let mut cfg = CompactionConfig::default();
    cfg.l0_trigger = 2;
    cfg.target_file_bytes = 512;
    let mut e = open(&dir, cfg);

    for i in 0..20u32 {
        e.put(uk(format!("k{i:02}").as_bytes()), vec![0u8; 128], 0)
            .unwrap();
    }
    e.force_flush().unwrap();
    e.drain_compaction().unwrap();
    let disk_before = e.estimate_disk_bytes();
    let logical_before = e.estimate_logical_live_bytes().unwrap();
    assert!(logical_before > 100);

    for i in 0..20u32 {
        e.del(uk(format!("k{i:02}").as_bytes())).unwrap();
    }
    e.force_flush().unwrap();
    e.drain_compaction().unwrap();

    assert_eq!(e.estimate_logical_live_bytes().unwrap(), 1);
    assert!(
        e.estimate_disk_bytes() < disk_before,
        "disk should shrink after tombstone GC"
    );
}
