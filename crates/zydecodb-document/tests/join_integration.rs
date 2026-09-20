//! Integration tests for `$lookup` (indexed nested-loop join) against a real
//! engine: 1:N joins, left-outer semantics, `_id` foreign keys, `$lookup` →
//! `$group`, and the adversarial cases (missing inner index, unknown/cross-
//! tenant `from`, per-outer match bound, TTL'd inner data).

use serde_json::{json, Value};
use tempfile::TempDir;
use zydecodb_document::aggregation::{execute_aggregation, AggregationLimits, AggregationPipeline};
use zydecodb_document::binary::ZDocBuilder;
use zydecodb_document::catalog::Catalog;
use zydecodb_document::join::{execute_lookup, JoinStrategy};
use zydecodb_document::store;
use zydecodb_engine::engine::{Engine, EngineConfig};

const PREFIX: &[u8] = b"\x01";
const OTHER_PREFIX: &[u8] = b"\x01other-tenant-000";

fn open(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
}

fn pipeline(value: Value) -> AggregationPipeline {
    AggregationPipeline::parse(&serde_json::to_vec(&value).unwrap()).unwrap()
}

fn put(e: &mut Engine, cat: &mut Catalog, coll: &str, id: &str, doc: Value) {
    let zdoc = ZDocBuilder::from_value(&doc);
    store::upsert(e, cat, PREFIX, coll, id.as_bytes(), &zdoc, true).unwrap();
}

/// users (outer) + orders (inner, indexed on `user_id`).
fn seed() -> (TempDir, Engine, Catalog) {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
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
    catalog.persist(&mut engine).unwrap();

    put(
        &mut engine,
        &mut catalog,
        "users",
        "u1",
        json!({"name": "alice"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u2",
        json!({"name": "bob"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u3",
        json!({"name": "carol"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o1",
        json!({"user_id": "u1", "total": 10}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o2",
        json!({"user_id": "u1", "total": 20}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o3",
        json!({"user_id": "u2", "total": 5}),
    );
    (dir, engine, catalog)
}

fn run(
    engine: &Engine,
    catalog: &Catalog,
    coll: &str,
    pipeline: &AggregationPipeline,
) -> Vec<Value> {
    execute_aggregation(
        &engine.snapshot_owned(),
        catalog,
        PREFIX,
        coll,
        pipeline,
        AggregationLimits::default(),
    )
    .unwrap()
    .rows
}

fn run_join(
    engine: &Engine,
    catalog: &Catalog,
    coll: &str,
    pipeline: &AggregationPipeline,
    limits: AggregationLimits,
) -> zydecodb_document::join::JoinResult {
    let spec = pipeline.lookup.as_ref().unwrap();
    execute_lookup(
        &engine.snapshot_owned(),
        PREFIX,
        catalog.collection(PREFIX, coll).unwrap(),
        catalog.collection(PREFIX, &spec.from).unwrap(),
        &pipeline.filter,
        spec,
        &pipeline.post_filter,
        limits,
    )
    .unwrap()
}

/// users + orders with no inner index — hash-join eligible.
fn seed_unindexed() -> (TempDir, Engine, Catalog) {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "orders");
    catalog.persist(&mut engine).unwrap();

    put(
        &mut engine,
        &mut catalog,
        "users",
        "u1",
        json!({"name": "alice"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u2",
        json!({"name": "bob"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u3",
        json!({"name": "carol"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o1",
        json!({"user_id": "u1", "total": 10}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o2",
        json!({"user_id": "u1", "total": 20}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o3",
        json!({"user_id": "u2", "total": 5}),
    );
    (dir, engine, catalog)
}

#[test]
fn one_to_many_join_splices_as_array() {
    let (_dir, engine, catalog) = seed();
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
    );
    assert_eq!(rows.len(), 3);
    let by_id = |id: &str| rows.iter().find(|r| r["_id"] == json!(id)).unwrap();
    assert_eq!(by_id("u1")["orders"].as_array().unwrap().len(), 2);
    assert_eq!(by_id("u2")["orders"].as_array().unwrap().len(), 1);
    // Left outer: no orders for carol, but the document survives.
    assert_eq!(by_id("u3")["orders"], json!([]));
    // Inner documents carry their full bodies.
    assert_eq!(by_id("u2")["orders"][0]["total"], json!(5));
}

#[test]
fn missing_or_non_scalar_local_field_is_empty_array() {
    let (_dir, mut engine, mut catalog) = seed();
    // u4 has no joining field at all; u5 has an array (non-scalar).
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u4",
        json!({"name": "dave"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u5",
        json!({"name": "erin", "tags": ["a", "b"]}),
    );
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "tags", "foreignField": "user_id", "as": "hits"
        }}])),
    );
    // No user has a scalar `tags`; every row gets an empty array.
    assert_eq!(rows.len(), 5);
    for row in rows {
        assert_eq!(row["hits"], json!([]));
    }
}

#[test]
fn join_on_id_foreign_field() {
    let (_dir, engine, catalog) = seed();
    // orders.user_id -> users._id, walking the other direction.
    let rows = run(
        &engine,
        &catalog,
        "orders",
        &pipeline(json!([{"$lookup": {
            "from": "users", "localField": "user_id", "foreignField": "_id", "as": "user"
        }}])),
    );
    assert_eq!(rows.len(), 3);
    let by_id = |id: &str| rows.iter().find(|r| r["_id"] == json!(id)).unwrap();
    assert_eq!(by_id("o1")["user"][0]["name"], json!("alice"));
    assert_eq!(by_id("o3")["user"][0]["name"], json!("bob"));
}

#[test]
fn lookup_then_group_counts_joined_docs() {
    let (_dir, engine, catalog) = seed();
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$group": {"_id": null, "users": {"$count": {}}}}
        ])),
    );
    // The $group sees join-output documents (one per outer user).
    assert_eq!(rows, vec![json!({"_id": null, "users": 3})]);
}

#[test]
fn match_then_lookup_filters_outer_side() {
    let (_dir, engine, catalog) = seed();
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$match": {"_id": "u1"}},
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }}
        ])),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["orders"].as_array().unwrap().len(), 2);
}

#[test]
fn missing_inner_index_over_scan_gate_is_a_named_error() {
    let (_dir, engine, catalog) = seed_unindexed();
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "hits"
        }}])),
        AggregationLimits {
            max_scan_docs: 0,
            ..Default::default()
        },
    )
    .unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("no index on 'user_id'"), "got: {msg}");
    assert!(msg.contains("create an index"), "got: {msg}");
    assert!(msg.contains("doc_count"), "got: {msg}");
}

#[test]
fn unknown_from_collection_is_a_named_error() {
    let (_dir, engine, catalog) = seed();
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "nope", "localField": "_id", "foreignField": "user_id", "as": "hits"
        }}])),
        AggregationLimits::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("nope"));
}

#[test]
fn from_never_crosses_tenants() {
    let (_dir, mut engine, mut catalog) = seed();
    // A collection named "secrets" exists ONLY under another tenant prefix.
    catalog.ensure_collection(OTHER_PREFIX, "secrets");
    catalog.persist(&mut engine).unwrap();
    let zdoc = ZDocBuilder::from_value(&json!({"k": "x"}));
    store::upsert(
        &mut engine,
        &mut catalog,
        OTHER_PREFIX,
        "secrets",
        b"s1",
        &zdoc,
        true,
    )
    .unwrap();

    // From the default tenant, `from: "secrets"` must not resolve.
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "secrets", "localField": "_id", "foreignField": "k", "as": "hits"
        }}])),
        AggregationLimits::default(),
    )
    .unwrap_err();
    assert!(err.to_string().contains("secrets"));
}

#[test]
fn per_outer_match_bound_is_enforced() {
    let (_dir, mut engine, mut catalog) = seed();
    for i in 0..5 {
        put(
            &mut engine,
            &mut catalog,
            "orders",
            &format!("x{i}"),
            json!({"user_id": "u1"}),
        );
    }
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([
            {"$match": {"_id": "u1"}},
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }}
        ])),
        AggregationLimits {
            max_matches_per_outer: 3,
            ..Default::default()
        },
    );
    // u1 now has 7 matching orders; the bound rejects rather than truncating.
    let err = match err {
        Ok(_) => panic!("expected bound error"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("more than 3 matches"),
        "got: {err}"
    );
}

#[test]
fn counters_stay_exact_against_ttl_joined_data() {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "sessions");
    catalog
        .add_index(
            PREFIX,
            "sessions",
            "by_user",
            vec!["user_id".into()],
            false,
            None,
        )
        .unwrap();
    catalog.persist(&mut engine).unwrap();

    put(&mut engine, &mut catalog, "users", "u1", json!({}));
    // One live session, one already-expired session for the same user.
    let live = ZDocBuilder::from_value(&json!({"user_id": "u1"}));
    store::upsert(
        &mut engine,
        &mut catalog,
        PREFIX,
        "sessions",
        b"s1",
        &live,
        true,
    )
    .unwrap();
    let dead = ZDocBuilder::from_value(&json!({"user_id": "u1"}));
    store::upsert_with_expiry(
        &mut engine,
        &mut catalog,
        PREFIX,
        "sessions",
        b"s2",
        &dead,
        true,
        1,
    )
    .unwrap();
    assert_eq!(catalog.collection(PREFIX, "sessions").unwrap().doc_count, 2);

    // The expired doc is hidden from the join immediately (lazy read)...
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "sessions", "localField": "_id", "foreignField": "user_id", "as": "sessions"
        }}])),
    );
    assert_eq!(rows[0]["sessions"].as_array().unwrap().len(), 1);

    // ...and once swept, the counters drop to match.
    let swept = store::sweep_expired_with_counts(&mut engine, &mut catalog).unwrap();
    assert_eq!(swept, 2); // doc key + index key
    let sessions = catalog.collection(PREFIX, "sessions").unwrap();
    assert_eq!(sessions.doc_count, 1);
    assert_eq!(sessions.indexes[0].entry_count, 1);
}

#[test]
fn hash_join_one_to_many() {
    let (_dir, engine, catalog) = seed_unindexed();
    let pipe = pipeline(json!([{"$lookup": {
        "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
    }}]));
    let result = run_join(
        &engine,
        &catalog,
        "users",
        &pipe,
        AggregationLimits::default(),
    );
    assert_eq!(result.strategy, JoinStrategy::Hash);
    assert_eq!(result.docs.len(), 3);
    let by_id = |id: &str| result.docs.iter().find(|r| r["_id"] == json!(id)).unwrap();
    assert_eq!(by_id("u1")["orders"].as_array().unwrap().len(), 2);
    assert_eq!(by_id("u2")["orders"].as_array().unwrap().len(), 1);
    assert_eq!(by_id("u3")["orders"], json!([]));
    assert_eq!(by_id("u2")["orders"][0]["total"], json!(5));
}

#[test]
fn hash_join_missing_local_field_is_empty_array() {
    let (_dir, mut engine, mut catalog) = seed_unindexed();
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u4",
        json!({"name": "dave"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u5",
        json!({"name": "erin", "tags": ["a", "b"]}),
    );
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "tags", "foreignField": "user_id", "as": "hits"
        }}])),
    );
    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(row["hits"], json!([]));
    }
}

#[test]
fn hash_join_skips_non_scalar_inner_values() {
    let (_dir, mut engine, mut catalog) = seed_unindexed();
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o4",
        json!({"user_id": ["u1"], "total": 99}),
    );
    let result = run_join(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
        AggregationLimits::default(),
    );
    assert_eq!(result.strategy, JoinStrategy::Hash);
    let u1 = result
        .docs
        .iter()
        .find(|r| r["_id"] == json!("u1"))
        .unwrap();
    assert_eq!(u1["orders"].as_array().unwrap().len(), 2);
}

#[test]
fn indexed_lookup_selects_inlj() {
    let (_dir, engine, catalog) = seed();
    let result = run_join(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
        AggregationLimits::default(),
    );
    assert_eq!(result.strategy, JoinStrategy::Inlj);
}

#[test]
fn hash_join_max_hash_bytes_is_enforced() {
    let (_dir, engine, catalog) = seed_unindexed();
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
        AggregationLimits {
            max_hash_bytes: 16,
            ..Default::default()
        },
    );
    let err = match err {
        Ok(_) => panic!("expected hash-bytes bound error"),
        Err(e) => e,
    };
    assert!(err.to_string().contains("hash join exceeds"), "got: {err}");
}

#[test]
fn hash_join_per_outer_match_bound_is_enforced() {
    let (_dir, mut engine, mut catalog) = seed_unindexed();
    for i in 0..5 {
        put(
            &mut engine,
            &mut catalog,
            "orders",
            &format!("x{i}"),
            json!({"user_id": "u1"}),
        );
    }
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([
            {"$match": {"_id": "u1"}},
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }}
        ])),
        AggregationLimits {
            max_matches_per_outer: 3,
            ..Default::default()
        },
    );
    let err = match err {
        Ok(_) => panic!("expected bound error"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("more than 3 matches"),
        "got: {err}"
    );
}

#[test]
fn hash_join_hides_ttl_inner_docs() {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "sessions");
    catalog.persist(&mut engine).unwrap();

    put(&mut engine, &mut catalog, "users", "u1", json!({}));
    let live = ZDocBuilder::from_value(&json!({"user_id": "u1"}));
    store::upsert(
        &mut engine,
        &mut catalog,
        PREFIX,
        "sessions",
        b"s1",
        &live,
        true,
    )
    .unwrap();
    let dead = ZDocBuilder::from_value(&json!({"user_id": "u1"}));
    store::upsert_with_expiry(
        &mut engine,
        &mut catalog,
        PREFIX,
        "sessions",
        b"s2",
        &dead,
        true,
        1,
    )
    .unwrap();

    let result = run_join(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "sessions", "localField": "_id", "foreignField": "user_id", "as": "sessions"
        }}])),
        AggregationLimits::default(),
    );
    assert_eq!(result.strategy, JoinStrategy::Hash);
    assert_eq!(result.docs[0]["sessions"].as_array().unwrap().len(), 1);
}

#[test]
fn inlj_and_hash_return_identical_docs() {
    let (_dir, mut engine, mut catalog) = seed_unindexed();
    let pipe = pipeline(json!([{"$lookup": {
        "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
    }}]));
    let hashed = run_join(
        &engine,
        &catalog,
        "users",
        &pipe,
        AggregationLimits::default(),
    );
    assert_eq!(hashed.strategy, JoinStrategy::Hash);

    store::define_index(
        &mut engine,
        &mut catalog,
        PREFIX,
        "orders",
        "by_user",
        vec!["user_id".into()],
        false,
        None,
    )
    .unwrap();

    let indexed = run_join(
        &engine,
        &catalog,
        "users",
        &pipe,
        AggregationLimits::default(),
    );
    assert_eq!(indexed.strategy, JoinStrategy::Inlj);
    assert_eq!(hashed.docs, indexed.docs);
}

#[test]
fn match_lookup_group_filters_then_counts() {
    let (_dir, engine, catalog) = seed();
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$match": {"_id": "u1"}},
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$group": {"_id": null, "n": {"$count": {}}}}
        ])),
    );
    assert_eq!(rows, vec![json!({"_id": null, "n": 1})]);
}

#[test]
fn lookup_max_memory_bytes_is_enforced() {
    let (_dir, engine, catalog) = seed();
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
        AggregationLimits {
            max_memory_bytes: 1,
            ..Default::default()
        },
    );
    let err = match err {
        Ok(_) => panic!("expected memory bound error"),
        Err(e) => e,
    };
    assert!(
        err.to_string().contains("join state exceeds 1 bytes"),
        "got: {err}"
    );
}

#[test]
fn lookup_outer_scan_bound_is_enforced() {
    let (_dir, engine, catalog) = seed();
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
        AggregationLimits {
            max_scan_docs: 2,
            ..Default::default()
        },
    );
    let err = match err {
        Ok(_) => panic!("expected outer scan bound error"),
        Err(e) => e,
    };
    assert!(
        err.to_string()
            .contains("query scan exceeds 2 candidate documents"),
        "got: {err}"
    );
}

#[test]
fn dotted_join_fields_use_inlj() {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "orders");
    catalog
        .add_index(
            PREFIX,
            "orders",
            "by_customer",
            vec!["customer.id".into()],
            false,
            None,
        )
        .unwrap();
    catalog.persist(&mut engine).unwrap();
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u1",
        json!({"profile": {"uid": "u1"}}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u2",
        json!({"profile": {"uid": "u2"}}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o1",
        json!({"customer": {"id": "u1"}, "total": 10}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o2",
        json!({"customer": {"id": "u1"}, "total": 20}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o3",
        json!({"customer": {"id": "u2"}, "total": 5}),
    );

    let pipe = pipeline(json!([{"$lookup": {
        "from": "orders", "localField": "profile.uid", "foreignField": "customer.id", "as": "orders"
    }}]));
    let result = run_join(
        &engine,
        &catalog,
        "users",
        &pipe,
        AggregationLimits::default(),
    );
    assert_eq!(result.strategy, JoinStrategy::Inlj);
    assert_eq!(result.docs.len(), 2);
    let by_id = |id: &str| result.docs.iter().find(|r| r["_id"] == json!(id)).unwrap();
    assert_eq!(by_id("u1")["orders"].as_array().unwrap().len(), 2);
    assert_eq!(by_id("u2")["orders"].as_array().unwrap().len(), 1);
}

#[test]
fn compound_leading_field_selects_inlj() {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "orders");
    catalog
        .add_index(
            PREFIX,
            "orders",
            "by_user_total",
            vec!["user_id".into(), "total".into()],
            false,
            None,
        )
        .unwrap();
    catalog.persist(&mut engine).unwrap();
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u1",
        json!({"name": "alice"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u2",
        json!({"name": "bob"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "users",
        "u3",
        json!({"name": "carol"}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o1",
        json!({"user_id": "u1", "total": 10}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o2",
        json!({"user_id": "u1", "total": 20}),
    );
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o3",
        json!({"user_id": "u2", "total": 5}),
    );

    let result = run_join(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
        AggregationLimits::default(),
    );
    assert_eq!(result.strategy, JoinStrategy::Inlj);
    let by_id = |id: &str| result.docs.iter().find(|r| r["_id"] == json!(id)).unwrap();
    assert_eq!(by_id("u1")["orders"].as_array().unwrap().len(), 2);
    assert_eq!(by_id("u2")["orders"].as_array().unwrap().len(), 1);
    assert_eq!(by_id("u3")["orders"], json!([]));
}

#[test]
fn compound_non_leading_field_falls_to_hash() {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "orders");
    catalog
        .add_index(
            PREFIX,
            "orders",
            "by_total_user",
            vec!["total".into(), "user_id".into()],
            false,
            None,
        )
        .unwrap();
    catalog.persist(&mut engine).unwrap();
    put(&mut engine, &mut catalog, "users", "u1", json!({}));
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o1",
        json!({"user_id": "u1", "total": 10}),
    );

    let result = run_join(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
        AggregationLimits::default(),
    );
    assert_eq!(result.strategy, JoinStrategy::Hash);
    assert_eq!(result.docs[0]["orders"].as_array().unwrap().len(), 1);
}

#[test]
fn lookup_group_sums_joined_totals() {
    for (strategy, seeded) in [("inlj", seed()), ("hash", seed_unindexed())] {
        let (_dir, engine, catalog) = seeded;
        let rows = run(
            &engine,
            &catalog,
            "users",
            &pipeline(json!([
                {"$lookup": {
                    "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
                }},
                {"$group": {
                    "_id": "$_id",
                    "spend": {"$sum": "$orders.total"},
                    "n": {"$size": "$orders"}
                }}
            ])),
        );
        assert_eq!(
            rows,
            vec![
                json!({"_id": "u1", "spend": 30, "n": 2}),
                json!({"_id": "u2", "spend": 5, "n": 1}),
                json!({"_id": "u3", "spend": 0, "n": 0}),
            ],
            "{strategy}"
        );
    }
}

#[test]
fn lookup_filter_limits_inner_side() {
    for (strategy, seeded) in [("inlj", seed()), ("hash", seed_unindexed())] {
        let (_dir, engine, catalog) = seeded;
        let rows = run(
            &engine,
            &catalog,
            "users",
            &pipeline(json!([
                {"$lookup": {
                    "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
                    "filter": {"total": {"$gte": 10}}
                }}
            ])),
        );
        let orders_of = |id: &str| {
            rows.iter()
                .find(|r| r["_id"] == json!(id))
                .unwrap()["orders"]
                .as_array()
                .unwrap()
                .clone()
        };
        assert_eq!(orders_of("u1").len(), 2, "{strategy}");
        assert_eq!(orders_of("u2"), Vec::<Value>::new(), "{strategy}");
        assert_eq!(orders_of("u3"), Vec::<Value>::new(), "{strategy}");
    }
}

#[test]
fn lookup_filter_on_inner_id_and_empty_filter() {
    let (_dir, engine, catalog) = seed();

    // `_id` predicates on the inner side match the document id.
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
                "filter": {"_id": "o1"}
            }}
        ])),
    );
    let u1 = rows.iter().find(|r| r["_id"] == json!("u1")).unwrap();
    assert_eq!(u1["orders"].as_array().unwrap().len(), 1);
    assert_eq!(u1["orders"][0]["_id"], json!("o1"));

    // An empty filter object is identical to no filter at all.
    let unfiltered = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }}
        ])),
    );
    let empty_filter = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
                "filter": {}
            }}
        ])),
    );
    assert_eq!(unfiltered, empty_filter);
}

#[test]
fn lookup_filter_supports_regex_and_in() {
    let (_dir, engine, catalog) = seed();

    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
                "filter": {"_id": {"$regex": "^o[12]$"}}
            }}
        ])),
    );
    let u1 = rows.iter().find(|r| r["_id"] == json!("u1")).unwrap();
    assert_eq!(u1["orders"].as_array().unwrap().len(), 2);
    let u2 = rows.iter().find(|r| r["_id"] == json!("u2")).unwrap();
    assert_eq!(u2["orders"], json!([]));

    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
                "filter": {"total": {"$in": [5, 20]}}
            }}
        ])),
    );
    let u1 = rows.iter().find(|r| r["_id"] == json!("u1")).unwrap();
    assert_eq!(u1["orders"].as_array().unwrap().len(), 1);
    assert_eq!(u1["orders"][0]["total"], json!(20));
    let u2 = rows.iter().find(|r| r["_id"] == json!("u2")).unwrap();
    assert_eq!(u2["orders"].as_array().unwrap().len(), 1);
    assert_eq!(u2["orders"][0]["total"], json!(5));
}

#[test]
fn post_match_anti_join() {
    let (_dir, engine, catalog) = seed();
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$match": {"orders": []}}
        ])),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_id"], json!("u3"));
    assert_eq!(rows[0]["orders"], json!([]));
}

#[test]
fn post_match_elem_match_on_children() {
    let (_dir, engine, catalog) = seed();
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$match": {"orders": {"$elemMatch": {"total": {"$gt": 15}}}}}
        ])),
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0]["_id"], json!("u1"));
    assert_eq!(rows[0]["orders"].as_array().unwrap().len(), 2);
}

#[test]
fn post_match_then_group_counts_survivors() {
    let (_dir, engine, catalog) = seed();
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$match": {"orders": {"$ne": []}}},
            {"$group": {"_id": null, "n": {"$count": {}}, "spend": {"$sum": "$orders.total"}}}
        ])),
    );
    assert_eq!(rows, vec![json!({"_id": null, "n": 2, "spend": 35})]);
}

#[test]
fn match_lookup_match_group_end_to_end() {
    let (_dir, engine, catalog) = seed();
    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([
            {"$match": {"name": {"$ne": "bob"}}},
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$match": {"orders": []}},
            {"$group": {"_id": null, "n": {"$count": {}}}}
        ])),
    );
    assert_eq!(rows, vec![json!({"_id": null, "n": 1})]);
}

#[test]
fn inlj_filter_rescues_hot_key_under_match_bound() {
    let (_dir, mut engine, mut catalog) = seed();
    for i in 0..1500 {
        put(
            &mut engine,
            &mut catalog,
            "orders",
            &format!("x{i:04}"),
            json!({"user_id": "u1", "batch": 1, "n": i}),
        );
    }
    let limits = AggregationLimits {
        max_matches_per_outer: 1000,
        ..Default::default()
    };

    // 1,500 index hits for one outer document, but the filter keeps 10:
    // rejected documents are skipped before the bound is charged.
    let rows = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([
            {"$match": {"_id": "u1"}},
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
                "filter": {"batch": 1, "n": {"$lt": 10}}
            }}
        ])),
        limits,
    )
    .unwrap()
    .rows;
    assert_eq!(rows[0]["orders"].as_array().unwrap().len(), 10);

    // One survivor over the bound is still a named error, not a truncation.
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([
            {"$match": {"_id": "u1"}},
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
                "filter": {"batch": 1, "n": {"$lt": 1001}}
            }}
        ])),
        limits,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("more than 1000 matches"),
        "got: {err}"
    );
}

#[test]
fn hash_filter_keeps_build_under_hash_bytes_cap() {
    let (_dir, engine, catalog) = seed_unindexed();
    let limits = AggregationLimits {
        max_hash_bytes: 150,
        ..Default::default()
    };

    // Unfiltered, all three orders exceed the cap.
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
        }}])),
        limits,
    )
    .unwrap_err();
    assert!(err.to_string().contains("hash join exceeds"), "got: {err}");

    // The filter keeps one document out of the map entirely.
    let rows = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
            "filter": {"_id": "o1"}
        }}])),
        limits,
    )
    .unwrap()
    .rows;
    let u1 = rows.iter().find(|r| r["_id"] == json!("u1")).unwrap();
    assert_eq!(u1["orders"].as_array().unwrap().len(), 1);
    let u2 = rows.iter().find(|r| r["_id"] == json!("u2")).unwrap();
    assert_eq!(u2["orders"], json!([]));
}

#[test]
fn hash_scan_gate_not_bypassed_by_selective_filter() {
    let (_dir, engine, catalog) = seed_unindexed();
    // The filter would keep nothing, but the inner doc_count (3) is over the
    // scan gate (2): strategy selection fails before any scan.
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
            "filter": {"_id": "nope"}
        }}])),
        AggregationLimits {
            max_scan_docs: 2,
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(err.to_string().contains("no index on 'user_id'"), "got: {err}");
}

#[test]
fn post_filter_rejected_docs_do_not_consume_memory_bound() {
    let (_dir, engine, catalog) = seed();
    let limits = AggregationLimits {
        max_memory_bytes: 1,
        ..Default::default()
    };

    // Every joined document is rejected: nothing is charged, zero rows.
    let rows = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$match": {"name": "nobody"}}
        ])),
        limits,
    )
    .unwrap()
    .rows;
    assert_eq!(rows, Vec::<Value>::new());

    // One survivor over the byte budget is a named error.
    let err = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$match": {"name": "alice"}}
        ])),
        limits,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("join state exceeds 1 bytes"),
        "got: {err}"
    );
}

#[test]
fn max_result_bytes_enforced_on_post_filtered_output() {
    let (_dir, engine, catalog) = seed();
    let result = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "users",
        &pipeline(json!([
            {"$lookup": {
                "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
            }},
            {"$match": {"orders": {"$ne": []}}}
        ])),
        AggregationLimits::default(),
    )
    .unwrap();
    assert_eq!(result.rows.len(), 2);
    let err =
        zydecodb_document::wire::encode_aggregate_response(&result.rows, 1).unwrap_err();
    assert!(err.to_string().contains("result"), "got: {err}");
}

#[test]
fn held_snapshot_with_filters_misses_post_snap_writes() {
    let (_dir, mut engine, mut catalog) = seed();
    let pipe = pipeline(json!([
        {"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
            "filter": {"total": {"$gte": 10}}
        }},
        {"$match": {"orders": {"$ne": []}}}
    ]));
    let snap = engine.snapshot_owned();

    // After the snapshot: insert a doc that WOULD pass the filter, and
    // delete one that DID.
    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o4",
        json!({"user_id": "u1", "total": 99}),
    );
    store::delete(&mut engine, &mut catalog, PREFIX, "orders", b"o1").unwrap();

    let order_ids = |rows: &[Value], id: &str| {
        rows.iter()
            .find(|r| r["_id"] == json!(id))
            .map(|r| {
                r["orders"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|o| o["_id"].as_str().unwrap().to_string())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    };

    let held = execute_aggregation(
        &snap,
        &catalog,
        PREFIX,
        "users",
        &pipe,
        AggregationLimits::default(),
    )
    .unwrap()
    .rows;
    assert_eq!(held.len(), 1);
    assert_eq!(order_ids(&held, "u1"), vec!["o1", "o2"]);

    let fresh = run(&engine, &catalog, "users", &pipe);
    assert_eq!(fresh.len(), 1);
    assert_eq!(order_ids(&fresh, "u1"), vec!["o2", "o4"]);
}

#[test]
fn filtered_join_stable_across_flush_and_compaction() {
    let (_dir, mut engine, mut catalog) = seed();
    let pipe = pipeline(json!([
        {"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
            "filter": {"total": {"$gte": 10}}
        }},
        {"$match": {"orders": {"$ne": []}}}
    ]));
    let expected = run(&engine, &catalog, "users", &pipe);

    // Pin a snapshot, then move every byte out from under it.
    let snap = engine.snapshot_owned();
    engine.flush().unwrap();
    engine.compact_all().unwrap();

    let held = execute_aggregation(
        &snap,
        &catalog,
        PREFIX,
        "users",
        &pipe,
        AggregationLimits::default(),
    )
    .unwrap()
    .rows;
    assert_eq!(held, expected, "held snapshot across flush+compact");

    let fresh = run(&engine, &catalog, "users", &pipe);
    assert_eq!(fresh, expected, "fresh snapshot after flush+compact");
}

#[test]
fn lookup_filter_still_hides_ttl_inner_docs() {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "users");
    catalog.ensure_collection(PREFIX, "sessions");
    catalog.persist(&mut engine).unwrap();

    put(&mut engine, &mut catalog, "users", "u1", json!({}));
    let live = ZDocBuilder::from_value(&json!({"user_id": "u1", "kind": "live"}));
    store::upsert(
        &mut engine,
        &mut catalog,
        PREFIX,
        "sessions",
        b"s1",
        &live,
        true,
    )
    .unwrap();
    // Expired, but would pass the filter if TTL did not hide it first.
    let dead = ZDocBuilder::from_value(&json!({"user_id": "u1", "kind": "live"}));
    store::upsert_with_expiry(
        &mut engine,
        &mut catalog,
        PREFIX,
        "sessions",
        b"s2",
        &dead,
        true,
        1,
    )
    .unwrap();

    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "sessions", "localField": "_id", "foreignField": "user_id", "as": "sessions",
            "filter": {"kind": "live"}
        }}])),
    );
    assert_eq!(rows[0]["sessions"].as_array().unwrap().len(), 1);
    assert_eq!(rows[0]["sessions"][0]["_id"], json!("s1"));
}

#[test]
fn lookup_filter_cannot_widen_tenant_scope() {
    let (_dir, mut engine, mut catalog) = seed();
    // Same collection name under another tenant, with a doc that would pass
    // the filter and match the join key.
    catalog.ensure_collection(OTHER_PREFIX, "orders");
    catalog.persist(&mut engine).unwrap();
    let zdoc = ZDocBuilder::from_value(&json!({"user_id": "u1", "total": 1000}));
    store::upsert(
        &mut engine,
        &mut catalog,
        OTHER_PREFIX,
        "orders",
        b"other1",
        &zdoc,
        true,
    )
    .unwrap();

    let rows = run(
        &engine,
        &catalog,
        "users",
        &pipeline(json!([{"$lookup": {
            "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders",
            "filter": {"total": {"$gte": 0}}
        }}])),
    );
    let u1 = rows.iter().find(|r| r["_id"] == json!("u1")).unwrap();
    let ids: Vec<&str> = u1["orders"]
        .as_array()
        .unwrap()
        .iter()
        .map(|o| o["_id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, vec!["o1", "o2"]);
}

#[test]
fn held_snapshot_misses_post_snap_inner_write() {
    let (_dir, mut engine, mut catalog) = seed();
    let pipe = pipeline(json!([{"$lookup": {
        "from": "orders", "localField": "_id", "foreignField": "user_id", "as": "orders"
    }}]));
    let spec = pipe.lookup.as_ref().unwrap();
    let snap = engine.snapshot_owned();

    put(
        &mut engine,
        &mut catalog,
        "orders",
        "o4",
        json!({"user_id": "u1", "total": 99}),
    );

    let held = execute_lookup(
        &snap,
        PREFIX,
        catalog.collection(PREFIX, "users").unwrap(),
        catalog.collection(PREFIX, "orders").unwrap(),
        &pipe.filter,
        spec,
        &pipe.post_filter,
        AggregationLimits::default(),
    )
    .unwrap();
    let u1 = held.docs.iter().find(|r| r["_id"] == json!("u1")).unwrap();
    assert_eq!(u1["orders"].as_array().unwrap().len(), 2);

    let fresh = run_join(
        &engine,
        &catalog,
        "users",
        &pipe,
        AggregationLimits::default(),
    );
    let u1 = fresh.docs.iter().find(|r| r["_id"] == json!("u1")).unwrap();
    assert_eq!(u1["orders"].as_array().unwrap().len(), 3);
}
