//! Reference workloads for published performance numbers (1.0 Section 4).
//!
//! Spawns an ephemeral server (or connects to `--addr`), runs fixed seeded
//! scenarios, and emits a JSON array of per-workload summaries.
//!
//! Usage:
//!   ref-workloads [--ops N] [--out PATH] [--seed U64] [--watchers K]
//!   ref-workloads --addr 127.0.0.1:9470

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tempfile::TempDir;
use zydecodb::config::{Config, RequireAuth, SecurityConfig};
use zydecodb::server::Server;
use zydecodb_document::wire::{
    self, AggregatePayload, DocPutPayload, FindPayload, IndexDefPayload, UpdatePayload, WatchFrame,
    WatchPayload, WireProjection,
};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN,
};

struct Args {
    ops: u32,
    seed: u64,
    out: Option<PathBuf>,
    addr: Option<SocketAddr>,
    watchers: usize,
}

fn parse_args() -> Args {
    let mut ops = 2000u32;
    let mut seed = 42u64;
    let mut out = None;
    let mut addr = None;
    let mut watchers = 4usize;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--ops" => ops = it.next().expect("--ops").parse().unwrap(),
            "--seed" => seed = it.next().expect("--seed").parse().unwrap(),
            "--out" => out = Some(PathBuf::from(it.next().expect("--out"))),
            "--addr" => addr = Some(it.next().expect("--addr").parse().unwrap()),
            "--watchers" => watchers = it.next().expect("--watchers").parse().unwrap(),
            "-h" | "--help" => {
                eprintln!(
                    "ref-workloads [--ops N] [--seed U64] [--out PATH] [--addr HOST:PORT] [--watchers K]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown arg: {other}"),
        }
    }
    Args {
        ops,
        seed,
        out,
        addr,
        watchers,
    }
}

fn free_addr() -> SocketAddr {
    let l = TcpListener::bind("127.0.0.1:0").unwrap();
    let a = l.local_addr().unwrap();
    drop(l);
    a
}

fn connect(addr: SocketAddr) -> TcpStream {
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(addr) {
            s.set_read_timeout(Some(Duration::from_secs(30))).unwrap();
            s.set_write_timeout(Some(Duration::from_secs(30))).unwrap();
            return s;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("connect {addr} failed");
}

fn write_request(stream: &mut impl Write, req: &RequestEnvelope) {
    stream.write_all(&req.encode()).unwrap();
    stream.flush().unwrap();
}

fn read_response(stream: &mut impl Read) -> ResponseEnvelope {
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    stream.read_exact(&mut header).unwrap();
    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).unwrap();
    }
    ResponseEnvelope::new(status, payload)
}

fn roundtrip(s: &mut TcpStream, req: &RequestEnvelope) -> ResponseEnvelope {
    write_request(s, req);
    read_response(s)
}

fn percentile_us(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn rss_kb() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb);
        }
    }
    None
}

#[derive(serde::Serialize)]
struct WorkloadResult {
    workload: String,
    ops: u64,
    elapsed_ms: u64,
    ops_per_sec: f64,
    p50_us: u64,
    p99_us: u64,
    rss_kb_peak: u64,
    notes: String,
}

fn summarize(
    name: &str,
    latencies: &mut [u64],
    elapsed: Duration,
    rss_peak: u64,
    notes: &str,
) -> WorkloadResult {
    latencies.sort_unstable();
    let ops = latencies.len() as u64;
    let secs = elapsed.as_secs_f64().max(1e-9);
    WorkloadResult {
        workload: name.into(),
        ops,
        elapsed_ms: elapsed.as_millis() as u64,
        ops_per_sec: ops as f64 / secs,
        p50_us: percentile_us(latencies, 0.50),
        p99_us: percentile_us(latencies, 0.99),
        rss_kb_peak: rss_peak,
        notes: notes.into(),
    }
}

fn timed_op(latencies: &mut Vec<u64>, rss_peak: &mut u64, mut f: impl FnMut()) {
    let t0 = Instant::now();
    f();
    latencies.push(t0.elapsed().as_micros() as u64);
    if let Some(kb) = rss_kb() {
        *rss_peak = (*rss_peak).max(kb);
    }
}

fn spawn_server_with_cs() -> (SocketAddr, Arc<Mutex<bool>>, JoinHandle<()>) {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let data_dir = tmp.path().join("data");
    let wal_dir = tmp.path().join("wal");
    std::fs::create_dir_all(&data_dir).unwrap();
    std::fs::create_dir_all(&wal_dir).unwrap();
    let mut cfg = Config {
        listen: addr,
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
        security: SecurityConfig {
            require_auth: RequireAuth::False,
            keys_file: tmp.path().join("keys.toml"),
            // Harness preload + measure loops must not hit the default 1k rps cap.
            rate_limit_rps: 1_000_000,
            ..Default::default()
        },
        tls: Default::default(),
        listen_unix: None,
        runtime: Default::default(),
        fair: Default::default(),
        aggregation: Default::default(),
        change_streams: Default::default(),
    };
    cfg.change_streams.enabled = true;
    cfg.change_streams.heartbeat_ms = 500;
    cfg.change_streams.write_timeout_ms = 10_000;
    cfg.change_streams.max_subscriptions = 64;
    cfg.change_streams.max_subscriptions_per_tenant = 32;
    let server = Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || {
        let _keep = tmp;
        server.run(cfg).unwrap();
    });
    for _ in 0..100 {
        if TcpStream::connect(addr).is_ok() {
            break;
        }
        thread::sleep(Duration::from_millis(20));
    }
    (addr, shutdown, handle)
}

fn define_index(s: &mut TcpStream, collection: &str, name: &str, fields: &[&str]) {
    let p = IndexDefPayload {
        collection: collection.into(),
        index_name: name.into(),
        fields: fields.iter().map(|f| f.to_string()).collect(),
        unique: false,
        expire_after_seconds: 0,
        directions: vec![true; fields.len()],
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::IndexDef, p.encode()));
    assert_eq!(resp.status, Status::Ok, "IndexDef: {:?}", resp);
}

fn doc_put(s: &mut TcpStream, collection: &str, doc_id: &[u8], body: &str) {
    let p = DocPutPayload {
        collection: collection.into(),
        doc_id: doc_id.to_vec(),
        body: body.as_bytes().to_vec(),
        relaxed: false,
        expires_at: 0,
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::DocPut, p.encode()));
    assert_eq!(resp.status, Status::Ok, "DocPut: {:?}", resp);
}

fn kv_put(s: &mut TcpStream, key: &[u8], value: &[u8]) {
    let p = PutPayload {
        routing_key: [0; 16],
        txid: 0,
        expires_at: 0,
        key: key.to_vec(),
        value: value.to_vec(),
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::Put, p.encode()));
    assert_eq!(resp.status, Status::Ok, "Put: {:?}", resp);
}

fn kv_get(s: &mut TcpStream, key: &[u8]) {
    let p = KeyPayload {
        routing_key: [0; 16],
        snapshot_seq: 0,
        key: key.to_vec(),
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::Get, p.encode()));
    assert_eq!(resp.status, Status::Ok, "Get: {:?}", resp);
}

fn find(s: &mut TcpStream, collection: &str, filter: &str) {
    let p = FindPayload {
        collection: collection.into(),
        filter: filter.as_bytes().to_vec(),
        sort: Vec::new(),
        projection: WireProjection::None,
        skip: 0,
        limit: 50,
        cursor: Vec::new(),
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::Find, p.encode()));
    assert_eq!(resp.status, Status::Ok, "Find: {:?}", resp);
}

fn run_point_get(addr: SocketAddr, ops: u32, seed: u64) -> WorkloadResult {
    let mut s = connect(addr);
    let n_keys = 10_000u32;
    for i in 0..n_keys {
        let k = format!("k{:08}", i);
        let v = format!("v{seed}-{i}");
        kv_put(&mut s, k.as_bytes(), v.as_bytes());
    }
    let mut lat = Vec::with_capacity(ops as usize);
    let mut rss = rss_kb().unwrap_or(0);
    let t0 = Instant::now();
    for i in 0..ops {
        let k = format!("k{:08}", (seed.wrapping_mul(i as u64 + 1)) as u32 % n_keys);
        timed_op(&mut lat, &mut rss, || kv_get(&mut s, k.as_bytes()));
    }
    summarize(
        "point_get",
        &mut lat,
        t0.elapsed(),
        rss,
        "KV Get against 10k preloaded keys",
    )
}

fn run_find_indexed(addr: SocketAddr, ops: u32) -> WorkloadResult {
    let mut s = connect(addr);
    define_index(&mut s, "users", "by_age", &["age"]);
    for i in 0..5_000u32 {
        let body = format!(r#"{{"age":{},"name":"u{}"}}"#, 20 + (i % 60), i);
        doc_put(&mut s, "users", format!("u{i}").as_bytes(), &body);
    }
    let mut lat = Vec::with_capacity(ops as usize);
    let mut rss = rss_kb().unwrap_or(0);
    let t0 = Instant::now();
    for i in 0..ops {
        let age = 20 + (i % 60);
        let filter = format!(r#"{{"age":{age}}}"#);
        timed_op(&mut lat, &mut rss, || find(&mut s, "users", &filter));
    }
    summarize(
        "find_indexed",
        &mut lat,
        t0.elapsed(),
        rss,
        "Find on users.by_age secondary index",
    )
}

fn run_find_unindexed(addr: SocketAddr, ops: u32) -> WorkloadResult {
    let mut s = connect(addr);
    for i in 0..2_000u32 {
        let body = format!(r#"{{"color":"c{}","n":{}}}"#, i % 20, i);
        doc_put(&mut s, "items", format!("i{i}").as_bytes(), &body);
    }
    let mut lat = Vec::with_capacity(ops as usize);
    let mut rss = rss_kb().unwrap_or(0);
    let t0 = Instant::now();
    for i in 0..ops {
        let filter = format!(r#"{{"color":"c{}"}}"#, i % 20);
        timed_op(&mut lat, &mut rss, || find(&mut s, "items", &filter));
    }
    summarize(
        "find_unindexed",
        &mut lat,
        t0.elapsed(),
        rss,
        "Find without usable secondary index (collection scan)",
    )
}

fn run_upsert(addr: SocketAddr, ops: u32) -> WorkloadResult {
    let mut s = connect(addr);
    doc_put(&mut s, "accounts", b"seed", r#"{"email":"seed@x","n":0}"#);
    let mut lat = Vec::with_capacity(ops as usize);
    let mut rss = rss_kb().unwrap_or(0);
    let t0 = Instant::now();
    for i in 0..ops {
        let email = format!("u{}@ex.com", i % 500);
        let filter = format!(r#"{{"email":"{email}"}}"#);
        let upd = format!(r#"{{"$set":{{"email":"{email}","n":1}}}}"#);
        timed_op(&mut lat, &mut rss, || {
            let p = UpdatePayload {
                collection: "accounts".into(),
                filter: filter.as_bytes().to_vec(),
                update: upd.as_bytes().to_vec(),
                multi: false,
                relaxed: false,
                upsert: true,
            };
            let resp = roundtrip(&mut s, &RequestEnvelope::new(Command::Update, p.encode()));
            assert_eq!(resp.status, Status::Ok, "Update upsert: {:?}", resp);
        });
    }
    summarize(
        "upsert",
        &mut lat,
        t0.elapsed(),
        rss,
        "Update upsert=true on rotating email keys",
    )
}

fn run_tx_commit(addr: SocketAddr, ops: u32) -> WorkloadResult {
    let mut s = connect(addr);
    doc_put(&mut s, "txcol", b"seed", r#"{"n":0}"#);
    let mut lat = Vec::with_capacity(ops as usize);
    let mut rss = rss_kb().unwrap_or(0);
    let t0 = Instant::now();
    for i in 0..ops {
        timed_op(&mut lat, &mut rss, || {
            let begin = roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![]));
            assert_eq!(begin.status, Status::Ok);
            let body = format!(r#"{{"n":{i}}}"#);
            let p = DocPutPayload {
                collection: "txcol".into(),
                doc_id: format!("t{i}").into_bytes(),
                body: body.into_bytes(),
                relaxed: false,
                expires_at: 0,
            };
            let put = roundtrip(&mut s, &RequestEnvelope::new(Command::DocPut, p.encode()));
            assert_eq!(put.status, Status::Ok);
            let commit = roundtrip(&mut s, &RequestEnvelope::new(Command::Commit, vec![]));
            assert_eq!(commit.status, Status::Ok, "Commit: {:?}", commit);
        });
    }
    summarize(
        "tx_commit",
        &mut lat,
        t0.elapsed(),
        rss,
        "Begin → DocPut → Commit per op",
    )
}

fn run_aggregate(addr: SocketAddr, ops: u32) -> WorkloadResult {
    let mut s = connect(addr);
    for i in 0..1_000u32 {
        let body = format!(r#"{{"g":{},"v":1}}"#, i % 10);
        doc_put(&mut s, "agg", format!("d{i}").as_bytes(), &body);
    }
    let pipeline = r#"[{"$group":{"_id":"$g","n":{"$sum":"$v"}}}]"#;
    let mut lat = Vec::with_capacity(ops as usize);
    let mut rss = rss_kb().unwrap_or(0);
    let t0 = Instant::now();
    for _ in 0..ops {
        timed_op(&mut lat, &mut rss, || {
            let p = AggregatePayload {
                collection: "agg".into(),
                pipeline: pipeline.as_bytes().to_vec(),
            };
            let resp = roundtrip(
                &mut s,
                &RequestEnvelope::new(Command::Aggregate, p.encode()),
            );
            assert_eq!(resp.status, Status::Ok, "Aggregate: {:?}", resp);
        });
    }
    summarize(
        "aggregate",
        &mut lat,
        t0.elapsed(),
        rss,
        "$group sum over 1k docs / 10 groups",
    )
}

fn run_watch_fanout(addr: SocketAddr, ops: u32, watchers: usize) -> WorkloadResult {
    let mut writer = connect(addr);
    doc_put(&mut writer, "stream", b"seed", r#"{"x":0}"#);

    let mut watches = Vec::new();
    for _ in 0..watchers {
        let mut s = connect(addr);
        write_request(
            &mut s,
            &RequestEnvelope::new(
                Command::Watch,
                WatchPayload {
                    collection: "stream".into(),
                    resume_token: Vec::new(),
                }
                .encode(),
            ),
        );
        let resp = read_response(&mut s);
        assert_eq!(resp.status, Status::Ok, "Watch open: {:?}", resp);
        match wire::decode_watch_frame(&resp.payload).unwrap() {
            WatchFrame::Ack { .. } => {}
            other => panic!("expected ACK, got {other:?}"),
        }
        watches.push(s);
    }

    let mut lat = Vec::with_capacity(ops as usize);
    let mut rss = rss_kb().unwrap_or(0);
    let t0 = Instant::now();
    for i in 0..ops {
        timed_op(&mut lat, &mut rss, || {
            let body = format!(r#"{{"y":{i}}}"#);
            doc_put(&mut writer, "stream", format!("e{i}").as_bytes(), &body);
            for s in &mut watches {
                loop {
                    let resp = read_response(s);
                    assert_eq!(resp.status, Status::Ok);
                    match wire::decode_watch_frame(&resp.payload).unwrap() {
                        WatchFrame::Heartbeat { .. } => continue,
                        WatchFrame::Event { .. } => break,
                        WatchFrame::Ack { .. } => continue,
                    }
                }
            }
        });
    }
    summarize(
        "watch_fanout",
        &mut lat,
        t0.elapsed(),
        rss,
        &format!("{watchers} Watch subscribers; write + drain one event each"),
    )
}

fn main() {
    let args = parse_args();
    let (addr, shutdown, handle) = match args.addr {
        Some(addr) => (addr, None, None),
        None => {
            let (addr, shutdown, handle) = spawn_server_with_cs();
            (addr, Some(shutdown), Some(handle))
        }
    };

    let ops = args.ops;
    let heavy = (ops / 4).max(50);

    let mut results = Vec::new();
    eprintln!("running point_get...");
    results.push(run_point_get(addr, ops, args.seed));
    eprintln!("running find_indexed...");
    results.push(run_find_indexed(addr, heavy));
    eprintln!("running find_unindexed...");
    results.push(run_find_unindexed(addr, heavy));
    eprintln!("running upsert...");
    results.push(run_upsert(addr, ops));
    eprintln!("running tx_commit...");
    results.push(run_tx_commit(addr, heavy));
    eprintln!("running aggregate...");
    results.push(run_aggregate(addr, heavy));
    eprintln!("running watch_fanout...");
    results.push(run_watch_fanout(addr, heavy.min(200), args.watchers));

    let json = serde_json::to_string_pretty(&results).unwrap();
    if let Some(path) = &args.out {
        if let Some(parent) = path.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        std::fs::write(path, &json).unwrap();
        eprintln!("wrote {}", path.display());
    }
    println!("{json}");

    if let (Some(shutdown), Some(handle)) = (shutdown, handle) {
        *shutdown.lock().unwrap() = true;
        let _ = handle.join();
    }
}
