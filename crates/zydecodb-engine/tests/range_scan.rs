//! Range and prefix scan correctness across memtable + L0 + L1 boundaries.

use std::collections::BTreeMap;
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn open(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .expect("engine open")
}

fn collect_scan(e: &Engine, lo: Vec<u8>, hi: Vec<u8>) -> Vec<(Vec<u8>, Vec<u8>)> {
    e.scan(lo, hi).unwrap().map(|r| r.unwrap()).collect()
}

#[test]
fn scan_yields_keys_in_user_key_order() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut reference: BTreeMap<Vec<u8>, Vec<u8>> = BTreeMap::new();
    for i in 0..200u32 {
        let key = uk(format!("k{:04}", i).as_bytes());
        let val = format!("v{}", i).into_bytes();
        e.put(key.clone(), val.clone(), 0).unwrap();
        reference.insert(key, val);
    }

    let got = collect_scan(&e, uk(b"k0050"), uk(b"k0100"));
    let expected: Vec<_> = reference
        .range(uk(b"k0050")..uk(b"k0100"))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    assert_eq!(got, expected);
}

#[test]
fn scan_merges_memtable_and_sstable_entries_with_newest_wins() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);

    // Write some keys, flush so they're in an SSTable, then overwrite a
    // subset in the memtable.
    for i in 0..20u32 {
        e.put(uk(format!("k{:02}", i).as_bytes()), b"old".to_vec(), 0)
            .unwrap();
    }
    e.force_flush().unwrap();
    for i in 0..20u32 {
        if i % 2 == 0 {
            e.put(uk(format!("k{:02}", i).as_bytes()), b"new".to_vec(), 0)
                .unwrap();
        }
    }
    // Some keys are now in the memtable (overwrites), the rest only in the
    // SSTable. Scan must pick the newer version for the overwritten ones.

    let got = collect_scan(&e, uk(b"k00"), uk(b"k99"));
    assert_eq!(got.len(), 20);
    for (k, v) in got {
        let suffix = &k[k.len() - 2..];
        let idx: u32 = std::str::from_utf8(suffix).unwrap().parse().unwrap();
        if idx.is_multiple_of(2) {
            assert_eq!(v, b"new", "k{:02} should be the new value", idx);
        } else {
            assert_eq!(v, b"old", "k{:02} should be the old value", idx);
        }
    }
}

#[test]
fn scan_suppresses_tombstones_across_levels() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);

    for i in 0..10u32 {
        e.put(uk(format!("k{}", i).as_bytes()), b"v".to_vec(), 0)
            .unwrap();
    }
    e.force_flush().unwrap();
    // Delete a couple via tombstone in the memtable.
    e.del(uk(b"k3")).unwrap();
    e.del(uk(b"k7")).unwrap();

    let got = collect_scan(&e, uk(b"k0"), uk(b"k9_"));
    let keys: Vec<&[u8]> = got.iter().map(|(k, _)| k.as_slice()).collect();
    assert!(!keys.contains(&uk(b"k3").as_slice()));
    assert!(!keys.contains(&uk(b"k7").as_slice()));
    assert_eq!(got.len(), 8);
}

#[test]
fn prefix_scan_includes_all_keys_with_prefix() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);

    let prefix = uk(b"user:42:");
    for kind in &["name", "email", "age", "city"] {
        let mut key = prefix.clone();
        key.extend_from_slice(kind.as_bytes());
        e.put(key, kind.as_bytes().to_vec(), 0).unwrap();
    }
    // Decoy in a different prefix.
    e.put(uk(b"user:43:name"), b"name".to_vec(), 0).unwrap();
    e.force_flush().unwrap();

    let got: Vec<_> = e
        .prefix_scan(prefix.clone())
        .unwrap()
        .map(|r| r.unwrap())
        .collect();
    assert_eq!(got.len(), 4, "prefix scan must not leak across prefixes");
    for (k, _) in got {
        assert!(k.starts_with(&prefix));
    }
}

#[test]
fn empty_range_yields_no_entries() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    e.put(uk(b"a"), b"1".to_vec(), 0).unwrap();
    e.put(uk(b"z"), b"2".to_vec(), 0).unwrap();
    let got = collect_scan(&e, uk(b"m"), uk(b"q"));
    assert!(got.is_empty());
}
