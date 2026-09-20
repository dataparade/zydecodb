#![no_main]
//! Parse arbitrary bytes as an aggregation pipeline and, when the parse
//! succeeds, EXECUTE it against a tiny seeded engine — once with an index on
//! `orders.user_id` (INLJ) and once without (hash join). Must never panic;
//! every failure must surface as a `DocError`.

use libfuzzer_sys::fuzz_target;
use std::sync::OnceLock;
use zydecodb_document::aggregation::{execute_aggregation, AggregationLimits, AggregationPipeline};
use zydecodb_document::binary::ZDocBuilder;
use zydecodb_document::catalog::Catalog;
use zydecodb_document::store;
use zydecodb_engine::engine::{Engine, EngineConfig};

const PREFIX: &[u8] = b"\x01";

struct Ctx {
    indexed: (Engine, Catalog),
    unindexed: (Engine, Catalog),
}

fn seed_engine(indexed: bool) -> (Engine, Catalog) {
    let tmp = tempfile::TempDir::new().unwrap();
    let mut engine = Engine::open(EngineConfig {
        data_dir: tmp.path().join("data"),
        wal_dir: tmp.path().join("wal"),
        ..Default::default()
    })
    .unwrap();
    // Keep the tempdir alive for the process lifetime (fine for a fuzzer).
    let _ = Box::leak(Box::new(tmp));

    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "orders");
    if indexed {
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
    }
    catalog.persist(&mut engine).unwrap();

    let put = |engine: &mut Engine,
               catalog: &mut Catalog,
               coll: &str,
               id: &str,
               doc: serde_json::Value| {
        let zdoc = ZDocBuilder::from_value(&doc);
        store::upsert(engine, catalog, PREFIX, coll, id.as_bytes(), &zdoc, true).unwrap();
    };
    for (id, name) in [("u1", "alice"), ("u2", "bob"), ("u3", "carol")] {
        put(
            &mut engine,
            &mut catalog,
            "users",
            id,
            serde_json::json!({"name": name}),
        );
    }
    let orders = [
        (
            "o1",
            serde_json::json!({"user_id": "u1", "total": 10, "flag": true}),
        ),
        ("o2", serde_json::json!({"user_id": "u1", "total": 20.5})),
        ("o3", serde_json::json!({"user_id": "u2", "total": "oops"})),
        ("o4", serde_json::json!({"user_id": "u99", "total": 1})),
        ("o5", serde_json::json!({"user_id": ["u1"], "total": 2})),
        ("o6", serde_json::json!({"total": 3})),
    ];
    for (id, doc) in orders {
        put(&mut engine, &mut catalog, "orders", id, doc);
    }
    (engine, catalog)
}

fn get_ctx() -> &'static Ctx {
    static CTX: OnceLock<Ctx> = OnceLock::new();
    CTX.get_or_init(|| Ctx {
        indexed: seed_engine(true),
        unindexed: seed_engine(false),
    })
}

fuzz_target!(|data: &[u8]| {
    let Ok(pipeline) = AggregationPipeline::parse(data) else {
        return;
    };
    let limits = AggregationLimits {
        max_scan_docs: 64,
        max_groups: 64,
        max_memory_bytes: 64 * 1024,
        max_result_bytes: 64 * 1024,
        max_matches_per_outer: 16,
        max_hash_bytes: 64 * 1024,
    };
    let ctx = get_ctx();
    for (engine, catalog) in [&ctx.indexed, &ctx.unindexed] {
        // Every outcome must be a named DocError, never a panic.
        let _ = execute_aggregation(
            &engine.snapshot_owned(),
            catalog,
            PREFIX,
            "users",
            &pipeline,
            limits,
        );
    }
});
