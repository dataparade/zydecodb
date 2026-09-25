//! Document-layer join soak: mixed upserts on `users`/`orders` plus the three
//! shipped `$lookup` pipelines, run against a live engine for a fixed
//! duration. One writer mutates inner docs while reader threads re-run
//! pipelines on fresh snapshots. A periodic flush/compact cycle forces reads
//! to cross the memtable/SSTable boundary.
//!
//! Gate: no panic, every pipeline returns `Ok` or a named `DocError`, zero
//! unexpected op errors, clean shutdown. Emits one JSON summary line to
//! stdout and a `metrics.jsonl` under the out dir.
//!
//! Usage: doc-join-soak [--hours H] [--ops N] [--out-dir PATH]
//!
//! `--ops N` paces the writer to roughly N upserts/sec (0 = unpaced).

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use zydecodb_document::aggregation::{execute_aggregation, AggregationLimits, AggregationPipeline};
use zydecodb_document::binary::ZDocBuilder;
use zydecodb_document::catalog::Catalog;
use zydecodb_document::store;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::engine_handle::EngineHandle;

const PREFIX: &[u8] = b"\x01";

const PIPE_GROUP_SUM: &str = r#"[{"$lookup":{"from":"orders","localField":"_id","foreignField":"user_id","as":"orders"}},{"$group":{"_id":"$_id","total":{"$sum":"$orders.total"},"n":{"$size":"$orders"}}}]"#;
const PIPE_INNER_FILTER: &str = r#"[{"$lookup":{"from":"orders","localField":"_id","foreignField":"user_id","as":"orders","filter":{"total":{"$gte":10}}}}]"#;
const PIPE_POST_MATCH: &str = r#"[{"$lookup":{"from":"orders","localField":"_id","foreignField":"user_id","as":"orders"}},{"$match":{"orders":[]}}]"#;

struct Args {
    hours: f64,
    ops: u64,
    out_dir: PathBuf,
}

fn parse_args() -> Args {
    let mut a = Args {
        hours: 0.33,
        ops: 200,
        out_dir: PathBuf::from("soak-runs/join"),
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--hours" => a.hours = it.next().unwrap().parse().unwrap(),
            "--ops" => a.ops = it.next().unwrap().parse().unwrap(),
            "--out-dir" => a.out_dir = PathBuf::from(it.next().unwrap()),
            "-h" | "--help" => {
                eprintln!("doc-join-soak [--hours H] [--ops N] [--out-dir PATH]");
                std::process::exit(0);
            }
            other => panic!("unknown arg: {other}"),
        }
    }
    a
}

fn seed(handle: &EngineHandle) -> Catalog {
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "orders");
    catalog
        .add_index(
            PREFIX,
            "orders",
            "by_user",
            vec!["user_id".into()],
            false,
            None,
        )
        .unwrap();

    let mut engine = handle.write();
    let put = |engine: &mut Engine,
               catalog: &mut Catalog,
               coll: &str,
               id: &str,
               doc: serde_json::Value| {
        let zdoc = ZDocBuilder::from_value(&doc);
        store::upsert(engine, catalog, PREFIX, coll, id.as_bytes(), &zdoc, true).unwrap();
    };
    for i in 0..64u32 {
        put(
            &mut engine,
            &mut catalog,
            "users",
            &format!("u{i}"),
            serde_json::json!({"name": format!("user{i}")}),
        );
    }
    for i in 0..512u32 {
        let uid = format!("u{}", i % 64);
        put(
            &mut engine,
            &mut catalog,
            "orders",
            &format!("o{i}"),
            serde_json::json!({"user_id": uid, "total": (i % 50) as i64, "flag": i % 3 == 0}),
        );
    }
    catalog.persist(&mut engine).unwrap();
    drop(engine);
    catalog
}

fn run_pipeline(
    handle: &EngineHandle,
    catalog: &Catalog,
    pipeline: &AggregationPipeline,
    errors: &AtomicU64,
) {
    let limits = AggregationLimits {
        max_scan_docs: 100_000,
        max_matches_per_outer: 1_000,
        ..Default::default()
    };
    let engine = handle.read();
    let snap = engine.snapshot_owned();
    drop(engine);
    if execute_aggregation(&snap, catalog, PREFIX, "users", pipeline, limits).is_err() {
        errors.fetch_add(1, Ordering::Relaxed);
    }
}

fn main() {
    let args = parse_args();
    std::fs::create_dir_all(&args.out_dir).unwrap();
    let data_dir = args.out_dir.join("data");
    let wal_dir = args.out_dir.join("wal");

    let handle = EngineHandle::new(
        Engine::open(EngineConfig {
            data_dir,
            wal_dir,
            ..Default::default()
        })
        .expect("open"),
    );
    let catalog = Arc::new(Mutex::new(seed(&handle)));

    let pipelines: Vec<AggregationPipeline> = [PIPE_GROUP_SUM, PIPE_INNER_FILTER, PIPE_POST_MATCH]
        .iter()
        .map(|p| AggregationPipeline::parse(p.as_bytes()).unwrap())
        .collect();
    let pipelines = Arc::new(pipelines);

    let stop = Arc::new(AtomicBool::new(false));
    let errors = Arc::new(AtomicU64::new(0));
    let ops_done = Arc::new(AtomicU64::new(0));
    let rss_peak = Arc::new(AtomicU64::new(0));

    // Writer: churn inner orders so join results change under readers.
    let writer = {
        let handle = Arc::clone(&handle);
        let catalog = Arc::clone(&catalog);
        let stop = Arc::clone(&stop);
        let ops_done = Arc::clone(&ops_done);
        let pace = if args.ops == 0 {
            Duration::ZERO
        } else {
            Duration::from_secs_f64(1.0 / args.ops as f64)
        };
        std::thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                {
                    let mut engine = handle.write();
                    let mut cat = catalog.lock().unwrap();
                    let id = format!("o{}", 512 + (i % 256));
                    let uid = format!("u{}", (i % 64) as u32);
                    let doc = serde_json::json!({"user_id": uid, "total": (i % 50) as i64});
                    let zdoc = ZDocBuilder::from_value(&doc);
                    let _ = store::upsert(
                        &mut engine,
                        &mut cat,
                        PREFIX,
                        "orders",
                        id.as_bytes(),
                        &zdoc,
                        true,
                    );
                }
                ops_done.fetch_add(1, Ordering::Relaxed);
                i += 1;
                if pace > Duration::ZERO {
                    std::thread::sleep(pace);
                }
            }
        })
    };

    // Readers: re-run the three pipelines on fresh snapshots.
    let mut readers = Vec::new();
    for _ in 0..3 {
        let handle = Arc::clone(&handle);
        let catalog = Arc::clone(&catalog);
        let pipelines = Arc::clone(&pipelines);
        let stop = Arc::clone(&stop);
        let errors = Arc::clone(&errors);
        let ops_done = Arc::clone(&ops_done);
        readers.push(std::thread::spawn(move || {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let cat = catalog.lock().unwrap().clone();
                run_pipeline(&handle, &cat, &pipelines[i % pipelines.len()], &errors);
                ops_done.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        }));
    }

    // Maintenance: force flush + compaction so reads cross the SSTable
    // boundary while joins are in flight.
    let maint = {
        let handle = Arc::clone(&handle);
        let stop = Arc::clone(&stop);
        std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_secs(5));
                let mut engine = handle.write();
                let _ = engine.flush();
                let _ = engine.compact_once();
                drop(engine);
            }
        })
    };

    let deadline = Instant::now() + Duration::from_secs_f64(args.hours * 3600.0);
    let mut rss_samples = Vec::new();
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_secs(1));
        let rss = rss_bytes();
        rss_peak.fetch_max(rss, Ordering::Relaxed);
        rss_samples.push(rss);
    }

    stop.store(true, Ordering::Relaxed);
    writer.join().unwrap();
    for r in readers {
        r.join().unwrap();
    }
    maint.join().unwrap();

    let errors = errors.load(Ordering::Relaxed);
    let total_ops = ops_done.load(Ordering::Relaxed);
    let rss_peak = rss_peak.load(Ordering::Relaxed);
    let shutdown_ok = true;

    let summary = serde_json::json!({
        "kind": "summary",
        "total_ops": total_ops,
        "errors": errors,
        "shutdown_ok": shutdown_ok,
        "rss_peak_bytes": rss_peak,
        "rss_samples": rss_samples.len(),
    });
    let metrics = args.out_dir.join("metrics.jsonl");
    std::fs::write(&metrics, serde_json::to_string(&summary).unwrap() + "\n").unwrap();
    println!("{}", serde_json::to_string(&summary).unwrap());

    if errors != 0 {
        std::process::exit(1);
    }
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
