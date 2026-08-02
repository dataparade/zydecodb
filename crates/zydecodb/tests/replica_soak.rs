//! Replication soak: hours-long failover cycling under continuous write load.
//!
//! Loop per cycle: primary serves writes and ships WAL segments; a replica
//! syncs at randomized intervals (lag/catch-up); the primary stops; the
//! replica promotes; every acked write from every prior cycle must read back
//! from the newly serving node (sampled across history, exhaustive for the
//! last cycle). Then the promoted node becomes the next cycle's primary and
//! a fresh replica dir syncs from the same ship dir.
//!
//! Not part of normal CI: gated behind `#[ignore]` and an env duration.
//!
//! Run:
//!   REPLICA_SOAK_MINUTES=120 cargo test --release --test replica_soak -- --ignored --nocapture

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use zydecodb::config::{Config, ShippingConfig};
use zydecodb::replica::{self, Replica};
use zydecodb_engine::engine::{Engine, EngineConfig};

#[path = "common/mod.rs"]
mod common;
use common::{free_addr, write_secret_file};

const PROTO: u8 = 0x01;
const CMD_PUT: u8 = 0x01;
const CMD_GET: u8 = 0x02;
const STATUS_OK: u8 = 0x00;

fn base_config(dir: &std::path::Path, name: &str, listen: SocketAddr) -> Config {
    let root = dir.join(name);
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
            keys_file: std::path::PathBuf::from("/nonexistent"),
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
    let mut out = vec![PROTO, cmd];
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
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

/// One request; `Err` on any I/O failure (server down mid-failover) so the
/// writer can retry like a real client.
fn request(addr: SocketAddr, f: &[u8]) -> Result<(u8, Vec<u8>), std::io::Error> {
    let mut s = TcpStream::connect(addr)?;
    s.set_read_timeout(Some(Duration::from_secs(5)))?;
    s.write_all(f)?;
    let mut head = [0u8; 6];
    s.read_exact(&mut head)?;
    let status = head[1];
    let len = u32::from_be_bytes([head[2], head[3], head[4], head[5]]) as usize;
    let mut body = vec![0u8; len];
    if len > 0 {
        s.read_exact(&mut body)?;
    }
    Ok((status, body))
}

fn wait_listening(addr: SocketAddr) {
    for _ in 0..500 {
        if TcpStream::connect(addr).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server at {addr} never came up");
}

/// Deterministic per-cycle RNG (xorshift), so a failing soak run reproduces
/// from its cycle count and seed.
struct Xor(u64);
impl Xor {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }
    fn range(&mut self, lo: u64, hi_incl: u64) -> u64 {
        lo + self.next() % (hi_incl - lo + 1).max(1)
    }
}

struct Server {
    shutdown: Arc<Mutex<bool>>,
    handle: Option<thread::JoinHandle<()>>,
    addr: SocketAddr,
}

fn serve(cfg: Config) -> Server {
    let srv = zydecodb::server::Server::new();
    let shutdown = srv.shutdown_flag();
    let addr = cfg.listen;
    let handle = thread::spawn(move || srv.run(cfg).unwrap());
    wait_listening(addr);
    Server {
        shutdown,
        handle: Some(handle),
        addr,
    }
}

impl Server {
    fn stop(&mut self) {
        *self.shutdown.lock().unwrap() = true;
        self.handle.take().unwrap().join().unwrap();
    }
}

#[test]
#[ignore = "hours-long soak; run explicitly with REPLICA_SOAK_MINUTES"]
fn replica_soak_failover_cycles() {
    let minutes: u64 = std::env::var("REPLICA_SOAK_MINUTES")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    // Exact cycle count overrides the clock (reproduction and CI).
    let max_cycles: Option<u64> = std::env::var("REPLICA_SOAK_CYCLES")
        .ok()
        .and_then(|s| s.parse().ok());
    let seed: u64 = std::env::var("REPLICA_SOAK_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0x5EED);
    let mut rng = Xor(seed.max(1));

    let tmp = TempDir::new().unwrap();
    let root = tmp.path().to_path_buf();
    if std::env::var("REPLICA_SOAK_KEEP").is_ok() {
        eprintln!("replica-soak keeping artifacts at {}", root.display());
        std::mem::forget(tmp);
    }
    let ship_dir = root.join("ship");
    let hmac_key = root.join("ship.hmac");
    write_secret_file(&hmac_key, b"replica-soak-hmac-key-material!!!!!!!");
    let hmac_bytes = std::fs::read(&hmac_key).unwrap();

    // Write bookkeeping. `hist` records EVERY attempted write per key
    // (including ones whose response was lost — the server may have applied
    // them); `max_acked` records the newest write per key the client got an
    // OK for. Failover invariant: the serving value for a key must be some
    // attempted write with n >= max_acked — anything older means an acked
    // write was lost, anything unknown means corruption.
    let hist: Arc<Mutex<std::collections::HashMap<Vec<u8>, Vec<(u64, Vec<u8>)>>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    let max_acked: Arc<Mutex<std::collections::HashMap<Vec<u8>, u64>>> =
        Arc::new(Mutex::new(std::collections::HashMap::new()));
    let write_counter = Arc::new(AtomicU64::new(0));
    let writer_stop = Arc::new(AtomicBool::new(false));

    let deadline = Instant::now() + Duration::from_secs(minutes * 60);
    let mut cycle: u64 = 0;

    // Cycle 0 primary starts from fresh dirs; later cycles reuse the
    // promoted replica's dirs.
    let mut primary_data = root.join("node0/data");
    let mut primary_wal = root.join("node0/wal");
    std::fs::create_dir_all(&primary_data).unwrap();
    std::fs::create_dir_all(&primary_wal).unwrap();

    while if let Some(m) = max_cycles {
        cycle < m
    } else {
        Instant::now() < deadline
    } {
        cycle += 1;
        let mut cfg = base_config(&root, &format!("serve{cycle}"), free_addr());
        // Point the serving config at the current primary dirs (the config
        // helper makes fresh ones; override).
        cfg.data_dir = primary_data.clone();
        cfg.wal_dir = primary_wal.clone();
        cfg.shipping = ShippingConfig {
            ship_dir: Some(ship_dir.clone()),
            mode: "copy".into(),
            heartbeat_ms: 100,
            hmac_key_file: Some(hmac_key.clone()),
        };
        let mut server = serve(cfg);
        let addr = server.addr;

        // Writer: continuous puts against the serving node until told to
        // stop. Every attempt joins the history; only ACKED writes raise the
        // must-survive watermark.
        let hist_w = Arc::clone(&hist);
        let acked_w = Arc::clone(&max_acked);
        let counter_w = Arc::clone(&write_counter);
        let stop_w = Arc::clone(&writer_stop);
        let writer = thread::spawn(move || {
            let mut rng = Xor(0xBEEF);
            while !stop_w.load(Ordering::Relaxed) {
                let n = counter_w.fetch_add(1, Ordering::Relaxed);
                let key = format!("key-{:08}", n % 50_000).into_bytes();
                let val = format!("val-{}-{:016x}", n, rng.next()).into_bytes();
                let result = request(addr, &put_frame(&key, &val));
                {
                    let mut h = hist_w.lock().unwrap();
                    h.entry(key.clone()).or_default().push((n, val));
                }
                match result {
                    Ok((STATUS_OK, _)) => {
                        let mut a = acked_w.lock().unwrap();
                        let e = a.entry(key).or_insert(0);
                        if n > *e {
                            *e = n;
                        }
                    }
                    Ok((_status, _)) => {} // rejected (e.g. mid-shutdown): not acked
                    Err(_) => {
                        thread::sleep(Duration::from_millis(20));
                    }
                }
            }
        });

        // Replica lag/catch-up: sync at randomized intervals while the
        // primary serves.
        let replica_root = root.join(format!("replica{cycle}"));
        let replica_data = replica_root.join("data");
        let replica_wal = replica_root.join("wal");
        std::fs::create_dir_all(&replica_data).unwrap();
        std::fs::create_dir_all(&replica_wal).unwrap();
        let serve_ms = rng.range(5_000, 20_000);
        let serve_until = Instant::now() + Duration::from_millis(serve_ms);
        while Instant::now() < serve_until && (max_cycles.is_some() || Instant::now() < deadline) {
            thread::sleep(Duration::from_millis(rng.range(200, 2_000)));
            let mut rep = Replica::new(ship_dir.clone(), replica_wal.clone())
                .with_hmac_key(Some(hmac_bytes.clone()));
            rep.sync().unwrap();
        }

        // Failover: stop the primary (seals + ships the active segment),
        // final sync, materialize, promote.
        writer_stop.store(true, Ordering::Relaxed);
        writer.join().unwrap();
        server.stop();

        let mut rep = Replica::new(ship_dir.clone(), replica_wal.clone())
            .with_hmac_key(Some(hmac_bytes.clone()));
        rep.sync().unwrap();
        {
            let mut eng = Engine::open(EngineConfig {
                data_dir: replica_data.clone(),
                wal_dir: replica_wal.clone(),
                ..Default::default()
            })
            .unwrap();
            eng.shutdown().unwrap();
        }
        let out = replica::promote(&ship_dir, &replica_wal, &replica_data).unwrap();
        assert!(out.new_epoch > out.previous_epoch, "cycle {cycle}: epoch must advance");

        // Serve the promoted node and verify equivalence: every write acked
        // in the LAST cycle, plus a random sample across all history.
        let mut cfg = base_config(&root, &format!("verify{cycle}"), free_addr());
        cfg.data_dir = replica_data.clone();
        cfg.wal_dir = replica_wal.clone();
        cfg.shipping = ShippingConfig {
            ship_dir: Some(ship_dir.clone()),
            mode: "copy".into(),
            heartbeat_ms: 0,
            hmac_key_file: Some(hmac_key.clone()),
        };
        let mut verify_server = serve(cfg);

        {
            let max_acked = max_acked.lock().unwrap();
            let hist = hist.lock().unwrap();
            // Sample across all acked history, plus the most recent keys
            // exhaustively (the failover window).
            let keys: Vec<&Vec<u8>> = max_acked.keys().collect();
            if !keys.is_empty() {
                let sample_n = 200usize.min(keys.len());
                let mut picked: Vec<&Vec<u8>> = (0..sample_n)
                    .map(|_| keys[rng.range(0, (keys.len() - 1) as u64) as usize])
                    .collect();
                picked.extend(keys.iter().rev().take(500).map(|k| *k));
                picked.sort();
                picked.dedup();
                for k in picked {
                    let floor = max_acked[k];
                    let (st, body) = request(verify_server.addr, &get_frame(k))
                        .expect("promoted node must answer");
                    assert_eq!(
                        st, STATUS_OK,
                        "cycle {cycle}: acked key {:?} lost in failover",
                        String::from_utf8_lossy(k)
                    );
                    // Parse "val-{n}-{hex}" back out of the serving value.
                    let body_s = String::from_utf8(body.clone())
                        .unwrap_or_else(|_| panic!("cycle {cycle}: corrupt value bytes"));
                    let served_n: u64 = body_s
                        .strip_prefix("val-")
                        .and_then(|r| r.split('-').next())
                        .and_then(|n| n.parse().ok())
                        .unwrap_or_else(|| {
                            panic!("cycle {cycle}: unknown value format: {body_s}")
                        });
                    assert!(
                        served_n >= floor,
                        "cycle {cycle}: key {:?} serves n={} but acked n={} — acked write lost",
                        String::from_utf8_lossy(k),
                        served_n,
                        floor
                    );
                    let known = hist[k].iter().any(|(n, v)| *n == served_n && *v == body_s.as_bytes());
                    assert!(
                        known,
                        "cycle {cycle}: key {:?} serves unknown value {:?} — corruption",
                        String::from_utf8_lossy(k),
                        body_s
                    );
                }
            }
        }

        eprintln!(
            "replica-soak cycle={cycle} keys_tracked={} writes_total={} ok",
            max_acked.lock().unwrap().len(),
            write_counter.load(Ordering::Relaxed)
        );

        // The promoted node becomes next cycle's primary.
        writer_stop.store(false, Ordering::Relaxed);
        primary_data = replica_data;
        primary_wal = replica_wal;
        // Stop the verify server; the next loop iteration re-serves the same
        // dirs as the new primary with shipping heartbeats on.
        verify_server.stop();
    }

    eprintln!(
        "replica-soak done: cycles={} keys_tracked={} writes_total={}",
        cycle,
        max_acked.lock().unwrap().len(),
        write_counter.load(Ordering::Relaxed)
    );
}
