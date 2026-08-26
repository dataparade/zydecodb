//! `$lookup` benchmark: N-fact x K-dimension join, doc-layer.
//!
//! Three scenarios:
//!   id-probe    — INLJ, foreignField "_id" (virtual index)
//!   k-probe     — INLJ, foreignField "k"   (secondary index)
//!   hash        — bounded hash join on "k" (no inner index)
//!
//! Emits one JSON object per scenario:
//! `{scenario, fact_docs, dim_docs, runs, elapsed_ms, docs_sec,
//!   probe_p50_us, probe_p99_us, rss_bytes, load_ms, backfill_ms}`

use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;
use zydecodb_document::aggregation::{execute_aggregation, AggregationLimits, AggregationPipeline};
use zydecodb_document::binary::ZDocBuilder;
use zydecodb_document::catalog::Catalog;
use zydecodb_document::encoding;
use zydecodb_document::planner::{self, AccessPath};
use zydecodb_document::{keys, store};
use zydecodb_engine::engine::{Engine, EngineConfig};

const PREFIX: &[u8] = b"\x01";

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

struct Args {
    data_dir: PathBuf,
    fact_docs: u32,
    dim_docs: u32,
    scenario: String,
    runs: u32,
}

fn parse_args() -> Args {
    let mut a = Args {
        data_dir: PathBuf::from("/tmp/zydeco-bench-join"),
        fact_docs: 1_000_000,
        dim_docs: 1_000,
        scenario: "both".into(),
        runs: 3,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--data-dir" => a.data_dir = PathBuf::from(it.next().unwrap()),
            "--fact-docs" => a.fact_docs = it.next().unwrap().parse().unwrap(),
            "--dim-docs" => a.dim_docs = it.next().unwrap().parse().unwrap(),
            "--scenario" => a.scenario = it.next().unwrap(),
            "--runs" => a.runs = it.next().unwrap().parse().unwrap(),
            "-h" | "--help" => {
                eprintln!(
                    "bench-join [--data-dir PATH] [--fact-docs N] [--dim-docs N] \
                     [--scenario id|index|hash|both] [--runs N]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown arg: {other}"),
        }
    }
    a
}

/// One inner equality probe, timed — the per-outer-document cost of INLJ.
fn time_probe(
    snap: &zydecodb_engine::SnapshotHandle,
    dims: &zydecodb_document::catalog::CollectionMeta,
    foreign_field: &str,
    value: &serde_json::Value,
) -> u64 {
    let start = Instant::now();
    let path = planner::plan_equality(PREFIX, dims, foreign_field, value).unwrap();
    match path {
        AccessPath::ById(id) => {
            let dk = keys::doc_key(PREFIX, dims.id, &id);
            let _ = snap.get(&dk).unwrap();
        }
        AccessPath::IndexScan { lo, hi, .. } => {
            let iter = snap.scan(lo, hi).unwrap();
            for item in iter {
                let (_ikey, doc_id) = item.unwrap();
                let mut dk = keys::doc_prefix(PREFIX, dims.id).to_vec();
                dk.extend_from_slice(&doc_id);
                let _ = snap.get(&dk).unwrap();
            }
        }
        AccessPath::CollectionScan => unreachable!("plan_equality never scans"),
    }
    start.elapsed().as_micros() as u64
}

fn time_hash_probe(map: &HashMap<Vec<u8>, ()>, value: &serde_json::Value) -> u64 {
    let start = Instant::now();
    let mut key = Vec::new();
    encoding::encode_value(value, &mut key);
    let _ = map.get(&key);
    start.elapsed().as_micros() as u64
}

#[derive(Clone, Copy)]
enum ProbeKind {
    Inlj { foreign_field: &'static str },
    Hash,
}

fn run_scenario(
    e: &Engine,
    cat: &Catalog,
    kind: ProbeKind,
    args: &Args,
    load_ms: u64,
    backfill_ms: u64,
) -> serde_json::Value {
    let foreign_field = match kind {
        ProbeKind::Inlj { foreign_field } => foreign_field,
        ProbeKind::Hash => "k",
    };
    let scenario = match kind {
        ProbeKind::Inlj { foreign_field } => format!("{foreign_field}-probe"),
        ProbeKind::Hash => "hash".into(),
    };
    let pipeline = AggregationPipeline::parse(
        format!(
            r#"[{{"$lookup":{{"from":"dims","localField":"k","foreignField":"{foreign_field}","as":"dim"}}}}]"#
        )
        .as_bytes(),
    )
    .unwrap();
    let limits = AggregationLimits {
        max_scan_docs: args.fact_docs as usize,
        max_matches_per_outer: 16,
        // The bare-[$lookup] pipeline retains every joined doc; the bench
        // measures exactly that workload, so the cap must not fire.
        max_memory_bytes: usize::MAX,
        max_result_bytes: usize::MAX,
        max_hash_bytes: usize::MAX,
        ..Default::default()
    };

    // Warmup: one full pass to populate block cache / bloom filters.
    let warm =
        execute_aggregation(&e.snapshot_owned(), cat, PREFIX, "facts", &pipeline, limits).unwrap();
    assert_eq!(warm.rows.len(), args.fact_docs as usize);

    let mut elapsed_ms = Vec::new();
    let mut rss_peak = rss_bytes();
    for _ in 0..args.runs {
        let t0 = Instant::now();
        let result =
            execute_aggregation(&e.snapshot_owned(), cat, PREFIX, "facts", &pipeline, limits)
                .unwrap();
        assert_eq!(result.rows.len(), args.fact_docs as usize);
        elapsed_ms.push(t0.elapsed().as_millis() as u64);
        rss_peak = rss_peak.max(rss_bytes());
    }
    elapsed_ms.sort_unstable();
    let median_ms = elapsed_ms[elapsed_ms.len() / 2];

    // Per-probe latency over a strided sample of join keys.
    let mut probes = Vec::new();
    let samples = 10_000u32;
    match kind {
        ProbeKind::Inlj { foreign_field } => {
            let snap = e.snapshot_owned();
            let dims = cat.collection(PREFIX, "dims").unwrap();
            for i in 0..samples {
                let key = format!(
                    "d{}",
                    (i as u64 * args.dim_docs as u64 / samples as u64) as u32
                );
                probes.push(time_probe(
                    &snap,
                    dims,
                    foreign_field,
                    &serde_json::Value::String(key),
                ));
            }
        }
        ProbeKind::Hash => {
            let mut map = HashMap::new();
            for i in 0..args.dim_docs {
                let v = serde_json::Value::String(format!("d{i}"));
                let mut key = Vec::new();
                encoding::encode_value(&v, &mut key);
                map.insert(key, ());
            }
            for i in 0..samples {
                let key = format!(
                    "d{}",
                    (i as u64 * args.dim_docs as u64 / samples as u64) as u32
                );
                probes.push(time_hash_probe(&map, &serde_json::Value::String(key)));
            }
        }
    }
    probes.sort_unstable();

    let secs = (median_ms as f64).max(1.0) / 1000.0;
    serde_json::json!({
        "scenario": scenario,
        "fact_docs": args.fact_docs,
        "dim_docs": args.dim_docs,
        "runs": args.runs,
        "elapsed_ms": median_ms,
        "docs_sec": (args.fact_docs as f64) / secs,
        "probe_p50_us": percentile(&probes, 0.50),
        "probe_p99_us": percentile(&probes, 0.99),
        "rss_bytes": rss_peak,
        "load_ms": load_ms,
        "backfill_ms": backfill_ms,
    })
}

fn main() {
    let args = parse_args();
    let _ = std::fs::remove_dir_all(&args.data_dir);
    let wal_dir = args.data_dir.join("wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let mut e = Engine::open(EngineConfig {
        data_dir: args.data_dir.clone(),
        wal_dir,
        block_cache_bytes: 64 * 1024 * 1024,
        ..Default::default()
    })
    .unwrap()
    .with_group_commit(false);

    let mut cat = Catalog::default();
    cat.ensure_collection(PREFIX, "facts");
    cat.ensure_collection(PREFIX, "dims");
    cat.persist(&mut e).unwrap();

    // Load phase: dimension docs first, then facts. `pad` keeps doc size
    // realistic (~150B) without dominating probe cost. EngineBusy means the
    // 256 MiB in-flight WAL cap fired — back off and let the background
    // memtable flush drain it, then retry (standard bulk-load posture).
    let put = |coll: &str, id: &str, doc: serde_json::Value, e: &mut Engine, cat: &mut Catalog| {
        let zdoc = ZDocBuilder::from_value(&doc);
        let mut backoff_us = 100u64;
        loop {
            match store::upsert(e, cat, PREFIX, coll, id.as_bytes(), &zdoc, true) {
                Ok(_) => break,
                Err(zydecodb_document::error::DocError::Engine(
                    zydecodb_engine::errors::EngineError::EngineBusy(_),
                )) => {
                    // The in-flight WAL counter resets when a completed flush
                    // is APPLIED, and apply processing runs on the calling
                    // thread — sleeping without calling into the engine
                    // livelocks. Drain first, then back off.
                    e.drain_background_work().unwrap();
                    std::thread::sleep(std::time::Duration::from_micros(backoff_us));
                    backoff_us = (backoff_us * 2).min(50_000);
                }
                Err(e) => panic!("load failed: {e}"),
            }
        }
    };
    let t0 = Instant::now();
    for i in 0..args.dim_docs {
        let doc = serde_json::json!({"k": format!("d{i}"), "attrs": format!("dim-{i}")});
        put("dims", &format!("d{i}"), doc, &mut e, &mut cat);
    }
    let pad = "x".repeat(100);
    for i in 0..args.fact_docs {
        let doc = serde_json::json!({
            "k": format!("d{}", i % args.dim_docs),
            "amount": i,
            "pad": pad,
        });
        put("facts", &format!("f{i:07}"), doc, &mut e, &mut cat);
        if i % 50_000 == 49_999 {
            e.sync_wal().unwrap();
        }
    }
    e.sync_wal().unwrap();
    let load_ms = t0.elapsed().as_millis() as u64;

    let want_hash = matches!(args.scenario.as_str(), "hash" | "both");
    let want_inlj = matches!(args.scenario.as_str(), "id" | "index" | "both");
    if !want_hash && !want_inlj {
        panic!("unknown scenario: {}", args.scenario);
    }

    let mut results = Vec::new();
    if want_hash {
        results.push(run_scenario(&e, &cat, ProbeKind::Hash, &args, load_ms, 0));
    }

    // Backfill: secondary index on dims.k for the INLJ index-probe scenario.
    let backfill_ms = if want_inlj {
        let t0 = Instant::now();
        store::define_index(
            &mut e,
            &mut cat,
            PREFIX,
            "dims",
            "by_k",
            vec!["k".into()],
            false,
            None,
        )
        .unwrap();
        t0.elapsed().as_millis() as u64
    } else {
        0
    };

    if want_inlj {
        let fields: Vec<&str> = match args.scenario.as_str() {
            "id" => vec!["_id"],
            "index" => vec!["k"],
            "both" => vec!["_id", "k"],
            _ => unreachable!(),
        };
        for foreign_field in fields {
            results.push(run_scenario(
                &e,
                &cat,
                ProbeKind::Inlj { foreign_field },
                &args,
                load_ms,
                backfill_ms,
            ));
        }
    }
    println!("{}", serde_json::to_string_pretty(&results).unwrap());

    e.shutdown().unwrap();
}
