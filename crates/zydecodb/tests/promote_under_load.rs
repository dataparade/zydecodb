//! Promote-under-load + retention-gap + promotion timing drills.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::path::PathBuf;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use zydecodb::config::{Config, ShippingConfig};
use zydecodb::replica::{self, Replica};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::shipping::{self, ShipMode};
use zydecodb_engine::wal;

#[path = "common/mod.rs"]
mod common;
use common::{free_addr, write_secret_file};

const PROTO: u8 = 0x01;
const CMD_PUT: u8 = 0x01;
const CMD_GET: u8 = 0x02;
const STATUS_OK: u8 = 0x00;

fn named_base_config(dir: &TempDir, name: &str, listen: SocketAddr) -> Config {
    let root = dir.path().join(name);
    let data_dir = root.join("data");
    let wal_dir = root.join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    Config {
        listen,
        data_dir,
        wal_dir,
        block_cache_mb: 64,
        max_open_readers: 32,
        poll_compaction_ms: 50,
        durability: Default::default(),
        fsync_interval_ms: 50,
        shipping: Default::default(),
        metrics: Default::default(),
        replica: Default::default(),
        security: zydecodb::config::SecurityConfig {
            require_auth: zydecodb::config::RequireAuth::False,
            keys_file: PathBuf::from("/nonexistent"),
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
        fair: Default::default(),
        aggregation: Default::default(),
        change_streams: Default::default(),
    }
}

fn frame(cmd: u8, payload: &[u8]) -> Vec<u8> {
    let mut f = vec![PROTO, cmd];
    f.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    f.extend_from_slice(payload);
    f
}

fn put_frame(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&[0u8; 16]);
    p.extend_from_slice(&0u64.to_be_bytes());
    p.extend_from_slice(&0u64.to_be_bytes());
    p.extend_from_slice(&(key.len() as u32).to_be_bytes());
    p.extend_from_slice(&(value.len() as u32).to_be_bytes());
    p.extend_from_slice(key);
    p.extend_from_slice(value);
    frame(CMD_PUT, &p)
}

fn get_frame(key: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&[0u8; 16]);
    p.extend_from_slice(&0u64.to_be_bytes());
    p.extend_from_slice(&(key.len() as u32).to_be_bytes());
    p.extend_from_slice(key);
    frame(CMD_GET, &p)
}

fn request(addr: SocketAddr, frame: &[u8]) -> (u8, Vec<u8>) {
    let mut s = TcpStream::connect(addr).unwrap();
    s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    s.write_all(frame).unwrap();
    let mut header = [0u8; 6];
    s.read_exact(&mut header).unwrap();
    let len = u32::from_be_bytes([header[2], header[3], header[4], header[5]]) as usize;
    let mut body = vec![0u8; len];
    if len > 0 {
        s.read_exact(&mut body).unwrap();
    }
    (header[1], body)
}

fn wait_listening(addr: SocketAddr) {
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server at {addr} never came up");
}

#[test]
fn promote_under_load_keeps_shipped_acks() {
    let tmp = TempDir::new().unwrap();
    let ship_dir = tmp.path().join("ship");
    let hmac_key = tmp.path().join("ship.hmac");
    write_secret_file(&hmac_key, b"promote-under-load-hmac-key-material!!!");

    let primary_addr = free_addr();
    let mut primary_cfg = named_base_config(&tmp, "primary", primary_addr);
    primary_cfg.shipping = ShippingConfig {
        ship_dir: Some(ship_dir.clone()),
        mode: "copy".into(),
        heartbeat_ms: 100,
        hmac_key_file: Some(hmac_key.clone()),
    };
    let primary = zydecodb::server::Server::new();
    let primary_shutdown = primary.shutdown_flag();
    let primary_handle = thread::spawn(move || primary.run(primary_cfg).unwrap());
    wait_listening(primary_addr);

    let mut acked: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    for i in 0..20u8 {
        let key = vec![b'k', i];
        let val = vec![b'v', i];
        let (st, _) = request(primary_addr, &put_frame(&key, &val));
        assert_eq!(st, STATUS_OK);
        acked.push((key, val));
    }

    // Clean stop seals+ships the active segment (acked window).
    *primary_shutdown.lock().unwrap() = true;
    primary_handle.join().unwrap();
    assert!(ship_dir.join("shipped.log").exists());

    // Replica catch-up + promote.
    let replica_data = tmp.path().join("replica/data");
    let replica_wal = tmp.path().join("replica/wal");
    std::fs::create_dir_all(&replica_data).unwrap();
    std::fs::create_dir_all(&replica_wal).unwrap();
    let mut rep = Replica::new(ship_dir.clone(), replica_wal.clone())
        .with_hmac_key(Some(std::fs::read(&hmac_key).unwrap()));
    rep.sync().unwrap();
    // Materialize shipped WAL into SSTables before promote.
    {
        let mut eng = Engine::open(EngineConfig {
            data_dir: replica_data.clone(),
            wal_dir: replica_wal.clone(),
            ..Default::default()
        })
        .unwrap();
        eng.shutdown().unwrap();
    }

    let t0 = Instant::now();
    let out = replica::promote(&ship_dir, &replica_wal, &replica_data).unwrap();
    let promote_ms = t0.elapsed().as_millis();
    eprintln!(
        "promote_under_load promote_ms={promote_ms} new_epoch={}",
        out.new_epoch
    );
    assert!(out.new_epoch > out.previous_epoch);

    // Serve promoted node and verify acked keys.
    let promo_addr = free_addr();
    let mut promo_cfg = named_base_config(&tmp, "promoted", promo_addr);
    promo_cfg.data_dir = replica_data;
    promo_cfg.wal_dir = replica_wal;
    promo_cfg.shipping = ShippingConfig {
        ship_dir: Some(ship_dir.clone()),
        mode: "copy".into(),
        heartbeat_ms: 0,
        hmac_key_file: Some(hmac_key.clone()),
    };
    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(promo_cfg).unwrap());
    wait_listening(promo_addr);

    for (k, v) in &acked {
        let (st, body) = request(promo_addr, &get_frame(k));
        assert_eq!(st, STATUS_OK, "acked key must survive promote");
        assert_eq!(&body, v);
    }

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn retention_gap_halts_loudly() {
    let tmp = TempDir::new().unwrap();
    let ship = tmp.path().join("ship");
    let wal = tmp.path().join("replica_wal");
    std::fs::create_dir_all(&ship).unwrap();
    std::fs::create_dir_all(&wal).unwrap();

    // Build three fake sealed segments + shipped.log entries.
    for id in 1..=3u64 {
        let name = wal::segment_filename(id);
        let path = ship.join(&name);
        let mut body = Vec::new();
        body.extend_from_slice(&id.to_be_bytes());
        body.push(zydecodb_engine::wal::WAL_FORMAT_VERSION);
        // Minimal empty body is fine for install+verify hash path — use ship_segment.
        std::fs::write(&path, &body).unwrap();
        shipping::ship_segment(&path, &ship, id, id * 10, ShipMode::Copy, None).unwrap();
    }

    // Delete segment 2 while 3 remains → permanent gap for a fresh replica.
    let seg2 = ship.join(wal::segment_filename(2));
    assert!(seg2.exists());
    std::fs::remove_file(&seg2).unwrap();
    assert!(ship.join(wal::segment_filename(3)).exists());

    let mut rep = Replica::new(ship, wal);
    let err = rep.sync().expect_err("must hard-fail on retention gap");
    let msg = err.to_string();
    assert!(
        msg.contains("retention gap") || msg.contains("missing"),
        "got {msg}"
    );
}

#[test]
fn fenced_old_primary_refuses_after_promote() {
    // Covered in replica_e2e; keep a focused re-check with timing note.
    let tmp = TempDir::new().unwrap();
    let ship = tmp.path().join("ship");
    std::fs::create_dir_all(&ship).unwrap();
    replica::write_fence(&ship, 1).unwrap();
    let data = tmp.path().join("data");
    let wal = tmp.path().join("wal");
    let t0 = Instant::now();
    let out = replica::promote(&ship, &wal, &data).unwrap();
    eprintln!("promote_timing_ms={}", t0.elapsed().as_millis());
    replica::write_fence(&ship, out.new_epoch).unwrap();

    let hmac = tmp.path().join("hmac");
    write_secret_file(&hmac, b"fence-hmac-key-material-padded-32b!");
    let addr = free_addr();
    let mut cfg = named_base_config(&tmp, "old", addr);
    cfg.shipping = ShippingConfig {
        ship_dir: Some(ship),
        mode: "copy".into(),
        heartbeat_ms: 0,
        hmac_key_file: Some(hmac),
    };
    let err = zydecodb::server::Server::new()
        .run(cfg)
        .expect_err("fenced primary must refuse");
    assert!(err.to_string().contains("fence"));
}
