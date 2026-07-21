use std::path::PathBuf;
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use zydecodb::config::{Config, ReplicaConfig, SecurityConfig, ShippingConfig, RequireAuth};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::shipping::ShipMode;
use zydecodb::replica::Replica;

fn base_config(tmp: &TempDir, port: u16, is_replica: bool) -> Config {
    let base = tmp.path().join(if is_replica { "replica" } else { "primary" });
    let data_dir = base.join("data");
    let wal_dir = base.join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    
    let ship_dir = tmp.path().join("ship");
    
    let mut config = Config {
        listen: format!("127.0.0.1:{}", port).parse().unwrap(),
        data_dir,
        wal_dir,
        block_cache_mb: 64,
        max_open_readers: 32,
        poll_compaction_ms: 50,
        durability: Default::default(),
        fsync_interval_ms: 100,
        shipping: Default::default(),
        metrics: Default::default(),
        replica: Default::default(),
        security: SecurityConfig {
            require_auth: RequireAuth::False,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
    };

    if is_replica {
        config.replica = ReplicaConfig {
            from: Some(ship_dir),
            poll_ms: 10,
            hmac_key_file: None,
        };
    } else {
        config.shipping = ShippingConfig {
            ship_dir: Some(ship_dir),
            mode: "copy".to_string(),
            heartbeat_ms: 1000,
            hmac_key_file: None,
        };
    }
    
    config
}

#[test]
fn test_shipping_abuse_corrupted_segment() {
    let tmp = TempDir::new().unwrap();
    let primary_cfg = base_config(&tmp, 0, false);
    let replica_cfg = base_config(&tmp, 0, true);

    // 1. Start primary and write some data
    let mut primary_engine = Engine::open(EngineConfig {
        data_dir: primary_cfg.data_dir.clone(),
        wal_dir: primary_cfg.wal_dir.clone(),
        ..Default::default()
    }).unwrap().with_shipping(
        primary_cfg.shipping.ship_dir.clone(),
        ShipMode::Copy,
    ).with_group_commit(false);

    // Write segment 1
    primary_engine.write_batch(vec![zydecodb_engine::engine::BatchOp::Put {
        key: vec![0x01, b'k', b'1'],
        value: b"v1".to_vec(),
        expires_at: 0,
    }]).unwrap();
    primary_engine.force_roll_wal_for_test().unwrap(); // Forces segment rotation and shipping

    // Write segment 2
    primary_engine.write_batch(vec![zydecodb_engine::engine::BatchOp::Put {
        key: vec![0x01, b'k', b'2'],
        value: b"v2".to_vec(),
        expires_at: 0,
    }]).unwrap();
    primary_engine.force_roll_wal_for_test().unwrap();

    // 2. Adversary corrupts segment 2
    let ship_dir = primary_cfg.shipping.ship_dir.as_ref().unwrap();
    println!("ship_dir contents: {:?}", std::fs::read_dir(ship_dir).unwrap().map(|e| e.unwrap().path()).collect::<Vec<_>>());
    let seg2_path = ship_dir.join("wal-00000002.log");
    assert!(seg2_path.exists());
    
    // Corrupt the bytes of segment 2
    let mut bytes = std::fs::read(&seg2_path).unwrap();
    if bytes.len() > 10 {
        bytes[10] ^= 0xFF; // Flip some bits
    }
    std::fs::write(&seg2_path, bytes).unwrap();

    // 3. Start replica and attempt to sync
    let mut replica = Replica::new(
        replica_cfg.replica.from.clone().unwrap(),
        replica_cfg.wal_dir.clone(),
    );

    let res = replica.sync();
    assert!(res.is_err(), "VULNERABILITY SURFACED: Replica accepted corrupted segment!");
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("hash mismatch") || err_str.contains("corrupt"),
        "Expected hash mismatch error, got: {}", err_str
    );
}

#[test]
fn test_shipping_abuse_out_of_order_manifest() {
    let tmp = TempDir::new().unwrap();
    let primary_cfg = base_config(&tmp, 0, false);
    let replica_cfg = base_config(&tmp, 0, true);

    let mut primary_engine = Engine::open(EngineConfig {
        data_dir: primary_cfg.data_dir.clone(),
        wal_dir: primary_cfg.wal_dir.clone(),
        ..Default::default()
    }).unwrap().with_shipping(
        primary_cfg.shipping.ship_dir.clone(),
        ShipMode::Copy,
    ).with_group_commit(false);

    primary_engine.write_batch(vec![zydecodb_engine::engine::BatchOp::Put {
        key: vec![0x01, b'k', b'1'], value: b"v1".to_vec(), expires_at: 0,
    }]).unwrap();
    primary_engine.force_roll_wal_for_test().unwrap(); // seg 1

    primary_engine.write_batch(vec![zydecodb_engine::engine::BatchOp::Put {
        key: vec![0x01, b'k', b'2'], value: b"v2".to_vec(), expires_at: 0,
    }]).unwrap();
    primary_engine.force_roll_wal_for_test().unwrap(); // seg 2

    // Adversary modifies shipped.log to list segment 2 before segment 1
    let ship_dir = &primary_cfg.shipping.ship_dir;
    let log_path = ship_dir.as_ref().unwrap().join("shipped.log");
    let log_contents = std::fs::read_to_string(&log_path).unwrap();
    
    let mut lines: Vec<&str> = log_contents.lines().collect();
    if lines.len() >= 2 {
        lines.swap(0, 1);
    }
    std::fs::write(&log_path, lines.join("\n") + "\n").unwrap();

    let mut replica = Replica::new(
        replica_cfg.replica.from.clone().unwrap(),
        replica_cfg.wal_dir.clone(),
    );

    let res = replica.sync();
    assert!(res.is_err(), "VULNERABILITY SURFACED: Replica accepted out-of-order manifest!");
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("out of order") || err_str.contains("expected seq"),
        "Expected sequence error, got: {}", err_str
    );
}