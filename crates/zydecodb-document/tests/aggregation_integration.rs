use serde_json::{json, Value};
use tempfile::TempDir;
use zydecodb_document::aggregation::{execute_aggregation, AggregationLimits, AggregationPipeline};
use zydecodb_document::binary::ZDocBuilder;
use zydecodb_document::catalog::Catalog;
use zydecodb_document::store;
use zydecodb_engine::engine::{Engine, EngineConfig};

const PREFIX: &[u8] = b"\x01";

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

fn seed() -> (TempDir, Engine, Catalog) {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "sales");
    catalog
        .add_index(
            PREFIX,
            "sales",
            "by_active",
            vec!["active".into()],
            false,
            None,
        )
        .unwrap();
    catalog.persist(&mut engine).unwrap();

    let documents = [
        json!({"active": true, "include": true, "team": "b", "amount": 2}),
        json!({"active": true, "include": true, "team": "a", "amount": 3}),
        json!({"active": true, "include": true, "team": "b", "amount": 1.5}),
        json!({"active": true, "include": true, "amount": "not numeric"}),
        json!({"active": true, "include": false, "team": "a", "amount": 100}),
    ];
    for (index, document) in documents.into_iter().enumerate() {
        if index % 2 == 0 {
            let zdoc = ZDocBuilder::from_value(&document);
            store::upsert(
                &mut engine,
                &catalog,
                PREFIX,
                "sales",
                index.to_string().as_bytes(),
                &zdoc,
                true,
            )
            .unwrap();
        } else {
            let json = serde_json::to_vec(&document).unwrap();
            store::upsert(
                &mut engine,
                &catalog,
                PREFIX,
                "sales",
                index.to_string().as_bytes(),
                &json,
                false,
            )
            .unwrap();
        }
    }
    (dir, engine, catalog)
}

#[test]
fn streams_grouped_sum_and_count_in_scalar_order() {
    let (_dir, engine, catalog) = seed();
    let pipeline = pipeline(json!([
        {"$match": {"active": true, "include": true}},
        {"$group": {
            "_id": "$team",
            "total": {"$sum": "$amount"},
            "count": {"$count": {}}
        }}
    ]));
    let result = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "sales",
        &pipeline,
        AggregationLimits::default(),
    )
    .unwrap();

    assert_eq!(
        result.rows,
        vec![
            json!({"_id": null, "count": 1, "total": 0}),
            json!({"_id": "a", "count": 1, "total": 3}),
            json!({"_id": "b", "count": 2, "total": 3.5}),
        ]
    );
    assert_eq!(result.scanned_docs, 5);
    assert_eq!(result.matched_docs, 4);
    assert_eq!(
        result.result_bytes,
        serde_json::to_vec(&result.rows).unwrap().len()
    );
}

#[test]
fn scan_limit_counts_index_candidates_before_residual_filtering() {
    let (_dir, engine, catalog) = seed();
    let pipeline = pipeline(json!([
        {"$match": {"active": true, "include": false}},
        {"$group": {"_id": null, "count": {"$count": {}}}}
    ]));
    let limits = AggregationLimits {
        max_scan_docs: 4,
        ..Default::default()
    };
    let error = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "sales",
        &pipeline,
        limits,
    )
    .unwrap_err();
    assert!(error.to_string().contains("exceeds 4 candidate"));
}

#[test]
fn rejects_non_scalar_group_keys_and_numeric_overflow() {
    let dir = TempDir::new().unwrap();
    let mut engine = open(&dir);
    let mut catalog = Catalog::default();
    catalog.ensure_collection(PREFIX, "values");

    for (id, document) in [
        ("object", json!({"key": {"nested": true}, "amount": 0})),
        ("max", json!({"key": "number", "amount": i64::MAX})),
        ("one", json!({"key": "number", "amount": 1})),
    ] {
        let zdoc = ZDocBuilder::from_value(&document);
        store::upsert(
            &mut engine,
            &catalog,
            PREFIX,
            "values",
            id.as_bytes(),
            &zdoc,
            true,
        )
        .unwrap();
    }

    let object_key = pipeline(json!([
        {"$match": {"_id": "object"}},
        {"$group": {"_id": "$key", "count": {"$count": {}}}}
    ]));
    assert!(execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "values",
        &object_key,
        AggregationLimits::default(),
    )
    .unwrap_err()
    .to_string()
    .contains("scalar or null"));

    let overflow = pipeline(json!([
        {"$match": {"key": "number"}},
        {"$group": {"_id": null, "total": {"$sum": "$amount"}}}
    ]));
    assert!(execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "values",
        &overflow,
        AggregationLimits::default(),
    )
    .unwrap_err()
    .to_string()
    .contains("integer overflow"));
}

#[test]
fn enforces_group_and_memory_limits() {
    let (_dir, engine, catalog) = seed();
    let pipeline = pipeline(json!([
        {"$group": {"_id": "$team", "count": {"$count": {}}}}
    ]));

    let group_error = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "sales",
        &pipeline,
        AggregationLimits {
            max_groups: 1,
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(group_error.to_string().contains("group count exceeds 1"));

    let memory_error = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "sales",
        &pipeline,
        AggregationLimits {
            max_memory_bytes: 1,
            ..Default::default()
        },
    )
    .unwrap_err();
    assert!(memory_error
        .to_string()
        .contains("group state exceeds 1 bytes"));
}

#[test]
fn enforces_result_byte_limit_on_encode_path() {
    let (_dir, engine, catalog) = seed();
    let pipeline = pipeline(json!([
        {"$group": {"_id": "$team", "count": {"$count": {}}}}
    ]));
    let result = execute_aggregation(
        &engine.snapshot_owned(),
        &catalog,
        PREFIX,
        "sales",
        &pipeline,
        AggregationLimits::default(),
    )
    .unwrap();
    assert!(result.result_bytes > 1);
    let err = zydecodb_document::wire::encode_aggregate_response(&result.rows, 1).unwrap_err();
    assert!(err.to_string().contains("result") || err.to_string().contains("byte"));
}
