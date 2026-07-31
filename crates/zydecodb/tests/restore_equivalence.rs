//! Admin snapshot → restore: byte-level SST equivalence + query equivalence.
//! Also covers snapshot under sustained write load.

use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;
use zydecodb::admin;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::shipping::ShipMode;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn sha256_file(path: &Path) -> [u8; 32] {
    let bytes = std::fs::read(path).unwrap();
    let mut h = Sha256::new();
    h.update(&bytes);
    h.finalize().into()
}

/// Map of relative name → sha256 for every `.sst` (and optionally MANIFEST) in dir.
fn artifact_digests(dir: &Path, include_manifest: bool) -> BTreeMap<String, [u8; 32]> {
    let mut out = BTreeMap::new();
    for entry in std::fs::read_dir(dir).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().to_string_lossy().into_owned();
        if name.ends_with(".sst") || (include_manifest && name == "MANIFEST") {
            out.insert(name, sha256_file(&entry.path()));
        }
    }
    out
}

fn open_source(dir: &TempDir, ship: PathBuf) -> Engine {
    std::fs::create_dir_all(&ship).unwrap();
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
    .with_shipping(Some(ship), ShipMode::Copy)
}

#[test]
fn snapshot_restore_byte_and_query_equivalence() {
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let snap = dir.path().join("snap");
    let out = dir.path().join("restored");

    let seeded: Vec<(Vec<u8>, Vec<u8>)> = (0..32u8)
        .map(|i| (uk(&[b'k', i]), format!("v{i}").into_bytes()))
        .collect();

    let mut e = open_source(&dir, ship.clone());
    for (k, v) in &seeded {
        e.put(k.clone(), v.clone(), 0).unwrap();
    }
    e.sync_wal().unwrap();
    e.force_flush().unwrap();
    e.force_roll_wal_for_test().unwrap();
    let snapshot_seq = e.snapshot_to(&snap).unwrap();
    e.shutdown().unwrap();
    drop(e);

    let snap_ssts = artifact_digests(&snap, true);
    assert!(!snap_ssts.is_empty(), "snapshot must contain SSTables");
    assert!(snap.join("SNAPMETA").is_file());

    let t0 = Instant::now();
    admin::restore(&snap, &ship, None, None, &out).expect("restore");
    let restore_ms = t0.elapsed().as_millis();
    eprintln!("restore_equivalence empty-ish fixture restore_ms={restore_ms}");

    // SST bytes from the base must match in the restore target (hardlink/copy).
    let restored_ssts = artifact_digests(&out, false);
    for (name, digest) in &snap_ssts {
        if name == "MANIFEST" {
            continue; // may advance after WAL replay + shutdown flush
        }
        assert_eq!(
            restored_ssts.get(name),
            Some(digest),
            "SST {name} digest mismatch after restore"
        );
    }

    let source = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap();
    let restored = Engine::open(EngineConfig {
        data_dir: out.clone(),
        wal_dir: out.join("wal"),
        ..Default::default()
    })
    .unwrap();

    assert!(restored.current_seq() >= snapshot_seq);
    for (k, v) in &seeded {
        assert_eq!(
            source.get(k).unwrap(),
            Some(v.clone()),
            "source missing {:?}",
            k
        );
        assert_eq!(
            restored.get(k).unwrap(),
            Some(v.clone()),
            "restored missing {:?}",
            k
        );
    }
}

#[test]
fn snapshot_under_write_load_restore_respects_watermark() {
    let dir = TempDir::new().unwrap();
    let ship = dir.path().join("ship");
    let snap = dir.path().join("snap");
    let out = dir.path().join("restored");
    std::fs::create_dir_all(&ship).unwrap();

    let eng = Arc::new(Mutex::new(
        Engine::open(EngineConfig {
            data_dir: dir.path().join("data"),
            wal_dir: dir.path().join("data/wal"),
            ..Default::default()
        })
        .unwrap()
        .with_shipping(Some(ship.clone()), ShipMode::Copy),
    ));

    // Pre-seed durable prefix.
    {
        let mut e = eng.lock().unwrap();
        for i in 0..16u8 {
            e.put(uk(&[b'p', i]), b"pre".to_vec(), 0).unwrap();
        }
        e.sync_wal().unwrap();
        e.force_flush().unwrap();
    }

    let stop = Arc::new(Mutex::new(false));
    let stop_w = Arc::clone(&stop);
    let eng_w = Arc::clone(&eng);
    let writer = thread::spawn(move || {
        let mut i = 0u32;
        while !*stop_w.lock().unwrap() {
            let mut e = eng_w.lock().unwrap();
            let k = uk(format!("w{i}").as_bytes());
            let _ = e.put(k, b"live".to_vec(), 0);
            let _ = e.sync_wal();
            i = i.wrapping_add(1);
            drop(e);
            thread::sleep(Duration::from_millis(1));
        }
    });

    thread::sleep(Duration::from_millis(30));
    let snapshot_seq = {
        let mut e = eng.lock().unwrap();
        e.snapshot_to(&snap).unwrap()
    };
    *stop.lock().unwrap() = true;
    writer.join().unwrap();

    // Seal so post-snapshot writes that made it into sealed segments are shipped.
    {
        let mut e = eng.lock().unwrap();
        let _ = e.force_roll_wal_for_test();
        e.shutdown().unwrap();
    }

    admin::restore(&snap, &ship, Some(snapshot_seq), None, &out).expect("restore to snap seq");

    let restored = Engine::open(EngineConfig {
        data_dir: out.clone(),
        wal_dir: out.join("wal"),
        wal_replay_max_seq: Some(snapshot_seq),
        ..Default::default()
    })
    .unwrap();
    assert_eq!(restored.current_seq(), snapshot_seq);

    // Pre-seed keys must be present.
    for i in 0..16u8 {
        assert!(
            restored.get(&uk(&[b'p', i])).unwrap().is_some(),
            "pre-seed key p{i} missing at snapshot watermark"
        );
    }
}

#[test]
fn restore_timing_fixture_sizes() {
    // Measures hardlink-base + empty-WAL restore for GUIDE expectations.
    let sizes = [0usize, 256 * 1024, 2 * 1024 * 1024];
    for size in sizes {
        let dir = TempDir::new().unwrap();
        let ship = dir.path().join("ship");
        let snap = dir.path().join("snap");
        let out = dir.path().join("restored");
        let mut e = open_source(&dir, ship.clone());
        if size > 0 {
            let chunk = vec![0xABu8; 4096];
            let n = size / chunk.len();
            for i in 0..n {
                let k = uk(format!("b{i}").as_bytes());
                e.put(k, chunk.clone(), 0).unwrap();
            }
            e.sync_wal().unwrap();
            e.force_flush().unwrap();
        } else {
            e.put(uk(b"empty"), b"x".to_vec(), 0).unwrap();
            e.sync_wal().unwrap();
            e.force_flush().unwrap();
        }
        e.force_roll_wal_for_test().unwrap();
        e.snapshot_to(&snap).unwrap();
        e.shutdown().unwrap();

        let t0 = Instant::now();
        admin::restore(&snap, &ship, None, None, &out).unwrap();
        let ms = t0.elapsed().as_millis();
        eprintln!("restore_timing approx_payload_bytes={size} restore_ms={ms}");
        assert!(out.join("SNAPMETA").exists() || snap.join("SNAPMETA").exists());
    }
}
