//! Short paced put/get microbench for nightly regression (~minutes).
//!
//! Emits JSON: `{ "p99_us", "p50_us", "ops_sec", "rss_bytes", "ops", "elapsed_ms" }`.

use std::path::PathBuf;
use std::time::{Duration, Instant};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn rss_bytes() -> u64 {
    let Ok(text) = std::fs::read_to_string("/proc/self/status") else {
        return 0;
    };
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            if let Some(kb) = rest.split_whitespace().next() {
                if let Ok(v) = kb.parse::<u64>() {
                    return v * 1024;
                }
            }
        }
    }
    0
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn main() {
    let mut data_dir = PathBuf::from("/tmp/zydeco-bench-regression");
    let mut ops = 50_000u32;
    let mut warmup = 5_000u32;
    let mut it = std::env::args().skip(1);
    while let Some(a) = it.next() {
        match a.as_str() {
            "--data-dir" => data_dir = PathBuf::from(it.next().unwrap()),
            "--ops" => ops = it.next().unwrap().parse().unwrap(),
            "--warmup" => warmup = it.next().unwrap().parse().unwrap(),
            "-h" | "--help" => {
                eprintln!("bench-regression [--data-dir PATH] [--ops N] [--warmup N]");
                std::process::exit(0);
            }
            other => panic!("unknown arg: {other}"),
        }
    }

    let _ = std::fs::remove_dir_all(&data_dir);
    let wal_dir = data_dir.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let mut e = Engine::open(EngineConfig {
        data_dir: data_dir.clone(),
        wal_dir,
        block_cache_bytes: 64 * 1024 * 1024,
        ..Default::default()
    })
    .unwrap()
    .with_group_commit(false);

    // Preload keyspace.
    for i in 0..10_000u32 {
        let k = uk(format!("k{i:05}").as_bytes());
        e.put(k, format!("v{i}").into_bytes(), 0).unwrap();
    }
    e.sync_wal().unwrap();

    for i in 0..warmup {
        let k = uk(format!("k{:05}", i % 10_000).as_bytes());
        let _ = e.get(&k).unwrap();
    }

    let mut lat = Vec::with_capacity(ops as usize);
    let t0 = Instant::now();
    let mut rss_peak = rss_bytes();
    for i in 0..ops {
        let k = uk(format!("k{:05}", i % 10_000).as_bytes());
        let start = Instant::now();
        if i % 4 == 0 {
            e.put(k.clone(), b"x".to_vec(), 0).unwrap();
        } else {
            let _ = e.get(&k).unwrap();
        }
        lat.push(start.elapsed().as_micros() as u64);
        rss_peak = rss_peak.max(rss_bytes());
        // Light pacing so we don't only measure empty-cache bursts.
        if i % 128 == 0 {
            thread_yield();
        }
    }
    let elapsed = t0.elapsed();
    e.shutdown().unwrap();

    lat.sort_unstable();
    let secs = elapsed.as_secs_f64().max(1e-9);
    let out = serde_json::json!({
        "ops": ops,
        "elapsed_ms": elapsed.as_millis() as u64,
        "ops_sec": (ops as f64) / secs,
        "p50_us": percentile(&lat, 0.50),
        "p99_us": percentile(&lat, 0.99),
        "rss_bytes": rss_peak,
        "mix": "75% get / 25% put, 10k key hot set",
    });
    println!("{}", serde_json::to_string_pretty(&out).unwrap());
}

fn thread_yield() {
    std::thread::sleep(Duration::from_micros(1));
}
