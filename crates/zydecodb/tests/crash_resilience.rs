use tempfile::TempDir;

use zydecodb_engine::engine::{BatchOp, Engine, EngineConfig};

fn base_config(tmp: &TempDir) -> EngineConfig {
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    EngineConfig {
        data_dir, wal_dir, ..Default::default()
    }
}

#[test]
fn test_crash_resilience_torn_write() {
    let tmp = TempDir::new().unwrap();
    let config = base_config(&tmp);

    // 1. Start engine and write some keys
    let mut engine = Engine::open(config.clone()).unwrap();

    for i in 0..100 {
        let mut key = vec![0x01];
        key.extend_from_slice(format!("k{}", i).as_bytes());
        let value = format!("v{}", i).into_bytes();
        engine
            .write_batch(vec![BatchOp::Put {
                key,
                value,
                expires_at: 0,
            }])
            .unwrap();
    }

    // 2. Inject a simulated crash during a write
    // We'll simulate a torn write by manually truncating the active WAL file.
    // First, let's write a batch that we will corrupt.
    let mut torn_key = vec![0x01];
    torn_key.extend_from_slice(b"torn_key");
    engine
        .write_batch(vec![BatchOp::Put {
            key: torn_key.clone(),
            value: b"torn_value".to_vec(),
            expires_at: 0,
        }])
        .unwrap();
    engine.sync_wal().unwrap();

    // Close the engine cleanly first so we can manipulate the file
    drop(engine);

    // 3. Truncate the last 2 bytes of the active WAL segment
    let wal_dir = config.wal_dir.clone();
    println!(
        "wal_dir contents: {:?}",
        std::fs::read_dir(&wal_dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .collect::<Vec<_>>()
    );
    let mut active_wal = None;
    for entry in std::fs::read_dir(&wal_dir).unwrap() {
        let entry = entry.unwrap();
        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) == Some("log") {
            // Find the highest numbered WAL file
            if active_wal.is_none() || path > active_wal.clone().unwrap() {
                active_wal = Some(path);
            }
        }
    }
    let active_wal = active_wal.unwrap();
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&active_wal)
        .unwrap();
    let len = file.metadata().unwrap().len();
    assert!(len > 2);
    file.set_len(len - 2).unwrap(); // Truncate the last 2 bytes (CRC32 footer)
    drop(file);

    // 4. Restart the server (re-open the Engine)
    // It should recover successfully, discarding the torn write, but keeping the first 100.
    let engine_res = Engine::open(config);
    assert!(
        engine_res.is_ok(),
        "VULNERABILITY SURFACED: Engine failed to recover from torn write!"
    );
    let engine = engine_res.unwrap();

    // Verify the first 100 keys exist
    for i in 0..100 {
        let mut key = vec![0x01];
        key.extend_from_slice(format!("k{}", i).as_bytes());
        let val = engine.get(&key).unwrap();
        assert!(val.is_some(), "Key {} missing after recovery", i);
    }

    // Verify the torn key does NOT exist
    let torn_val = engine.get(&torn_key).unwrap();
    assert!(
        torn_val.is_none(),
        "Torn key should not have been recovered!"
    );
}
