//! End-to-end: a primary ships its WAL, a read replica ingests the
//! sha256-verified segments, serves the replicated data, and refuses writes.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

use zydecodb::config::{Config, ReplicaConfig, ShippingConfig};

const PROTO: u8 = 0x01;
const CMD_PUT: u8 = 0x01;
const CMD_GET: u8 = 0x02;
const STATUS_OK: u8 = 0x00;
const STATUS_FORBIDDEN: u8 = 0x0C;

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn base_config(dir: &TempDir, name: &str, listen: SocketAddr) -> Config {
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
        fsync_interval_ms: 100,
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
    }
}

fn put_frame(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&[0u8; 16]); // routing key
    p.extend_from_slice(&0u64.to_be_bytes()); // txid
    p.extend_from_slice(&0u64.to_be_bytes()); // expires_at
    p.extend_from_slice(&(key.len() as u32).to_be_bytes());
    p.extend_from_slice(&(value.len() as u32).to_be_bytes());
    p.extend_from_slice(key);
    p.extend_from_slice(value);
    frame(CMD_PUT, &p)
}

fn get_frame(key: &[u8]) -> Vec<u8> {
    let mut p = Vec::new();
    p.extend_from_slice(&[0u8; 16]); // routing key
    p.extend_from_slice(&0u64.to_be_bytes()); // snapshot_seq
    p.extend_from_slice(&(key.len() as u32).to_be_bytes());
    p.extend_from_slice(key);
    frame(CMD_GET, &p)
}

fn frame(cmd: u8, payload: &[u8]) -> Vec<u8> {
    let mut f = vec![PROTO, cmd];
    f.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    f.extend_from_slice(payload);
    f
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
fn replica_ingests_shipped_wal_and_is_read_only() {
    let tmp = TempDir::new().unwrap();
    let ship_dir = tmp.path().join("ship");

    // --- Primary: ship WAL into ship_dir. ---
    let primary_addr = free_addr();
    let mut primary_cfg = base_config(&tmp, "primary", primary_addr);
    primary_cfg.shipping = ShippingConfig {
        ship_dir: Some(ship_dir.clone()),
        mode: "copy".into(),
        heartbeat_ms: 200,
    };
    let primary = zydecodb::server::Server::new();
    let primary_shutdown = primary.shutdown_flag();
    let primary_handle = thread::spawn(move || primary.run(primary_cfg).unwrap());
    wait_listening(primary_addr);

    let (st, body) = request(primary_addr, &put_frame(b"greeting", b"bonjour"));
    assert_eq!(st, STATUS_OK, "primary PUT should succeed");
    assert_eq!(body.len(), 8, "PUT returns an 8-byte seq");

    // Clean-stop the primary: this flushes and ships the active WAL segment.
    *primary_shutdown.lock().unwrap() = true;
    primary_handle.join().unwrap();

    assert!(
        ship_dir.join("shipped.log").exists(),
        "primary must have shipped a segment"
    );

    // --- Replica: ingest the shipped WAL, serve read-only. ---
    let replica_addr = free_addr();
    let mut replica_cfg = base_config(&tmp, "replica", replica_addr);
    replica_cfg.replica = ReplicaConfig {
        from: Some(ship_dir.clone()),
        poll_ms: 100,
    };
    let replica = zydecodb::server::Server::new();
    let replica_shutdown = replica.shutdown_flag();
    let replica_handle = thread::spawn(move || replica.run(replica_cfg).unwrap());
    wait_listening(replica_addr);

    // The replicated key is readable on the replica.
    let (st, body) = request(replica_addr, &get_frame(b"greeting"));
    assert_eq!(st, STATUS_OK, "replicated key should be readable");
    assert_eq!(body, b"bonjour");

    // Writes are refused.
    let (st, _) = request(replica_addr, &put_frame(b"x", b"y"));
    assert_eq!(st, STATUS_FORBIDDEN, "replica must reject writes");

    *replica_shutdown.lock().unwrap() = true;
    replica_handle.join().unwrap();
}

/// Promotion bumps the epoch past the shipped-stream fence, and a stale old
/// primary that tries to re-attach to the same stream is refused at startup.
#[test]
fn promotion_bumps_epoch_and_fences_old_primary() {
    use zydecodb::replica;

    let tmp = TempDir::new().unwrap();
    let ship = tmp.path().join("ship");
    std::fs::create_dir_all(&ship).unwrap();

    // An old primary has been running and stamped the stream fence at epoch 1.
    replica::write_fence(&ship, 1).unwrap();

    // Promote a replica against this stream: epoch must advance to 2.
    let replica_data = tmp.path().join("replica_data");
    let replica_wal = tmp.path().join("replica_wal");
    let out = replica::promote(&ship, &replica_wal, &replica_data).unwrap();
    assert_eq!(out.previous_epoch, 1);
    assert_eq!(out.new_epoch, 2);
    assert_eq!(replica::read_epoch(&replica_data), 2);

    // The promoted node starts as primary against the SAME stream and stamps 2
    // (this is what serve does on a clean start; simulated here).
    replica::write_fence(&ship, out.new_epoch).unwrap();

    // The OLD primary (epoch 1, no EPOCH file) tries to restart against the same
    // stream. serve must refuse before binding a socket to avoid split-brain.
    let old_addr = free_addr();
    let mut old_cfg = base_config(&tmp, "old_primary", old_addr);
    old_cfg.shipping = ShippingConfig {
        ship_dir: Some(ship.clone()),
        mode: "copy".into(),
        heartbeat_ms: 0,
    };
    let server = zydecodb::server::Server::new();
    let err = server
        .run(old_cfg)
        .expect_err("fenced old primary must refuse to start");
    assert!(
        err.to_string().contains("fence"),
        "expected a fence rejection, got: {err}"
    );
}
