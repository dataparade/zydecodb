//! Counter-exactness tests: doc_count / entry_count must track every
//! mutation path — upsert/replace/delete, filtered deletes, TTL sweep,
//! index backfill, and crash recovery — with zero drift.

use tempfile::TempDir;
use zydecodb_document::catalog::Catalog;
use zydecodb_document::{store, update};
use zydecodb_engine::engine::{Engine, EngineConfig};

/// Legacy single-tenant storage prefix (KS_USER only).
const PREFIX: &[u8] = b"\x01";

fn open(dir: &TempDir) -> Engine {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
    .unwrap()
}

fn counts(cat: &Catalog, name: &str) -> (u64, Vec<u64>) {
    let c = cat.collection(PREFIX, name).unwrap();
    (
        c.doc_count,
        c.indexes.iter().map(|i| i.entry_count).collect(),
    )
}

/// Seq of the newest version of a system key (`None` when absent). A rewrite
/// with identical bytes still gets a new seq, so this detects any write.
fn sys_seq(e: &Engine, key: &[u8]) -> Option<u64> {
    e.snapshot_get_with_seq(u64::MAX, key)
        .unwrap()
        .map(|(_, seq)| seq)
}

#[test]
fn upsert_replace_delete_round_trip() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    // Insert two.
    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":30}"#,
        false,
    )
    .unwrap();
    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        b"u2",
        br#"{"age":25}"#,
        false,
    )
    .unwrap();
    assert_eq!(counts(&cat, "users"), (2, vec![2]));

    // Replace one: no count movement.
    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":31}"#,
        false,
    )
    .unwrap();
    assert_eq!(counts(&cat, "users"), (2, vec![2]));

    // Delete one, then re-delete (no-op), then delete the last.
    assert!(store::delete(&mut e, &mut cat, PREFIX, "users", b"u1").unwrap());
    assert_eq!(counts(&cat, "users"), (1, vec![1]));
    assert!(!store::delete(&mut e, &mut cat, PREFIX, "users", b"u1").unwrap());
    assert_eq!(counts(&cat, "users"), (1, vec![1]));
    assert!(store::delete(&mut e, &mut cat, PREFIX, "users", b"u2").unwrap());
    assert_eq!(counts(&cat, "users"), (0, vec![0]));
}

#[test]
fn filtered_delete_ids_skips_stale_candidates() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.ensure_collection(PREFIX, "users");
    cat.persist(&mut e).unwrap();

    for (id, n) in [(b"u1" as &[u8], 1), (b"u2", 5), (b"u3", 9)] {
        let body = format!("{{\"n\":{n}}}");
        store::upsert(
            &mut e,
            &mut cat,
            PREFIX,
            "users",
            id,
            body.as_bytes(),
            false,
        )
        .unwrap();
    }
    assert_eq!(counts(&cat, "users"), (3, vec![]));

    // Filter matches only n >= 5; u1 is a stale candidate and must contribute
    // zero delta.
    let filter =
        zydecodb_document::filter::Filter::parse(&serde_json::json!({"n": {"$gte": 5}})).unwrap();
    let ids = vec![
        b"u1".to_vec(),
        b"u2".to_vec(),
        b"u3".to_vec(),
        b"missing".to_vec(),
    ];
    let deleted =
        store::delete_ids(&mut e, &mut cat, PREFIX, "users", &ids, Some(&filter)).unwrap();
    assert_eq!(deleted, 2);
    assert_eq!(counts(&cat, "users"), (1, vec![]));
}

#[test]
fn ttl_sweep_decrements_exactly_once() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    // Plain secondary index (no TTL derivation), so explicit `expires_at`
    // stands; the index still tracks one entry per doc.
    cat.add_index(PREFIX, "sessions", "by_t", vec!["t".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    // Two docs with explicit expiry in the past, one that never expires.
    store::upsert_with_expiry(
        &mut e,
        &mut cat,
        PREFIX,
        "sessions",
        b"s1",
        br#"{"t":1}"#,
        false,
        1,
    )
    .unwrap();
    store::upsert_with_expiry(
        &mut e,
        &mut cat,
        PREFIX,
        "sessions",
        b"s2",
        br#"{"t":2}"#,
        false,
        1,
    )
    .unwrap();
    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "sessions",
        b"s3",
        br#"{"t":3}"#,
        false,
    )
    .unwrap();
    assert_eq!(counts(&cat, "sessions"), (3, vec![3]));

    let swept = store::sweep_expired_with_counts(&mut e, &mut cat).unwrap();
    // 2 doc keys + 2 index keys.
    assert_eq!(swept, 4);
    assert_eq!(counts(&cat, "sessions"), (1, vec![1]));

    // A second sweep is a no-op: tombstoned keys are not re-counted.
    let swept = store::sweep_expired_with_counts(&mut e, &mut cat).unwrap();
    assert_eq!(swept, 0);
    assert_eq!(counts(&cat, "sessions"), (1, vec![1]));
}

#[test]
fn reupsert_over_expired_unswept_doc_is_a_replace() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.ensure_collection(PREFIX, "sessions");
    cat.persist(&mut e).unwrap();

    store::upsert_with_expiry(
        &mut e,
        &mut cat,
        PREFIX,
        "sessions",
        b"s1",
        br#"{"v":1}"#,
        false,
        1,
    )
    .unwrap();
    assert_eq!(counts(&cat, "sessions"), (1, vec![]));

    // The doc is expired but not yet swept: it still occupies its counter
    // slot, so re-upserting must be a replace (delta 0), not an insert.
    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "sessions",
        b"s1",
        br#"{"v":2}"#,
        false,
    )
    .unwrap();
    assert_eq!(counts(&cat, "sessions"), (1, vec![]));

    // The doc is live again; the sweep must not decrement it.
    let swept = store::sweep_expired_with_counts(&mut e, &mut cat).unwrap();
    assert_eq!(swept, 0);
    assert_eq!(counts(&cat, "sessions"), (1, vec![]));
}

#[test]
fn index_backfill_sets_entry_count() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.ensure_collection(PREFIX, "users");
    cat.persist(&mut e).unwrap();

    for i in 0..7 {
        let id = format!("u{i}");
        let body = format!("{{\"age\":{i}}}");
        store::upsert(
            &mut e,
            &mut cat,
            PREFIX,
            "users",
            id.as_bytes(),
            body.as_bytes(),
            false,
        )
        .unwrap();
    }
    assert_eq!(counts(&cat, "users"), (7, vec![]));

    store::define_index(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        "by_age",
        vec!["age".into()],
        false,
        None,
    )
    .unwrap();
    assert_eq!(counts(&cat, "users"), (7, vec![7]));

    // Post-backfill writes keep both counters in lockstep.
    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        b"u7",
        br#"{"age":7}"#,
        false,
    )
    .unwrap();
    assert_eq!(counts(&cat, "users"), (8, vec![8]));
    assert!(store::delete(&mut e, &mut cat, PREFIX, "users", b"u7").unwrap());
    assert_eq!(counts(&cat, "users"), (7, vec![7]));
}

#[test]
fn updates_do_not_move_counts() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_n", vec!["n".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    for id in [b"u1" as &[u8], b"u2"] {
        store::upsert(&mut e, &mut cat, PREFIX, "users", id, br#"{"n":1}"#, false).unwrap();
    }
    let upd = update::UpdateDoc::parse(&serde_json::json!({"$inc": {"n": 1}})).unwrap();
    let ids = vec![b"u1".to_vec(), b"u2".to_vec()];
    let modified =
        update::apply_to_ids(&mut e, &mut cat, PREFIX, "users", &ids, &upd, None).unwrap();
    assert_eq!(modified, 2);
    assert_eq!(counts(&cat, "users"), (2, vec![2]));
}

#[test]
fn crash_replays_data_and_counts_together() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = open(&dir);
        let mut cat = Catalog::default();
        cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
            .unwrap();
        cat.persist(&mut e).unwrap();
        for i in 0..5 {
            let id = format!("u{i}");
            let body = format!("{{\"age\":{i}}}");
            store::upsert(
                &mut e,
                &mut cat,
                PREFIX,
                "users",
                id.as_bytes(),
                body.as_bytes(),
                false,
            )
            .unwrap();
        }
        assert!(store::delete(&mut e, &mut cat, PREFIX, "users", b"u0").unwrap());
        // Drop WITHOUT clean shutdown: data and counters survive only via WAL
        // replay. The catalog blob rides in the same WAL record as each write,
        // so replay must reconstruct both in lockstep.
    }
    let e = open(&dir);
    let cat = Catalog::load(&e).unwrap();
    assert_eq!(counts(&cat, "users"), (4, vec![4]));
    // And the data itself agrees.
    let snap = e.snapshot_owned();
    for i in 1..5 {
        let id = format!("u{i}");
        assert!(
            zydecodb_document::query::get_by_id(&snap, &cat, PREFIX, "users", id.as_bytes())
                .unwrap()
                .is_some()
        );
    }
    assert!(
        zydecodb_document::query::get_by_id(&snap, &cat, PREFIX, "users", b"u0")
            .unwrap()
            .is_none()
    );
}

#[test]
fn crash_after_sweep_does_not_double_decrement() {
    let dir = TempDir::new().unwrap();
    {
        let mut e = open(&dir);
        let mut cat = Catalog::default();
        cat.ensure_collection(PREFIX, "sessions");
        cat.persist(&mut e).unwrap();
        store::upsert_with_expiry(
            &mut e,
            &mut cat,
            PREFIX,
            "sessions",
            b"s1",
            br#"{"v":1}"#,
            false,
            1,
        )
        .unwrap();
        store::upsert(
            &mut e,
            &mut cat,
            PREFIX,
            "sessions",
            b"s2",
            br#"{"v":2}"#,
            false,
        )
        .unwrap();
        let swept = store::sweep_expired_with_counts(&mut e, &mut cat).unwrap();
        assert_eq!(swept, 1);
        assert_eq!(counts(&cat, "sessions"), (1, vec![]));
        // Drop without shutdown: the durable sweep tombstone + catalog blob
        // replay together, so the post-crash sweep has nothing to re-count.
    }
    let mut e = open(&dir);
    let mut cat = Catalog::load(&e).unwrap();
    assert_eq!(counts(&cat, "sessions"), (1, vec![]));
    let swept = store::sweep_expired_with_counts(&mut e, &mut cat).unwrap();
    assert_eq!(swept, 0);
    assert_eq!(counts(&cat, "sessions"), (1, vec![]));
}

/// The schema blob is DDL-only: document writes move the per-collection
/// counter records and never rewrite the blob.
#[test]
fn document_writes_do_not_rewrite_the_catalog_blob() {
    use zydecodb_document::catalog::{counter_sys_key, CollectionCounters, CATALOG_SYS_KEY};

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();
    let coll_id = cat.collection(PREFIX, "users").unwrap().id;
    let blob_before = e.sys_get(CATALOG_SYS_KEY).unwrap().unwrap();
    let blob_seq_before = sys_seq(&e, CATALOG_SYS_KEY).unwrap();
    // The blob carries no counts at all.
    let text = String::from_utf8(blob_before.clone()).unwrap();
    assert!(!text.contains("doc_count"), "{text}");
    assert!(!text.contains("entry_count"), "{text}");

    for i in 0..50u32 {
        let id = format!("u{i}");
        let body = format!("{{\"age\":{i}}}");
        store::upsert(
            &mut e,
            &mut cat,
            PREFIX,
            "users",
            id.as_bytes(),
            body.as_bytes(),
            false,
        )
        .unwrap();
    }
    for i in 0..10u32 {
        let id = format!("u{i}");
        assert!(store::delete(&mut e, &mut cat, PREFIX, "users", id.as_bytes()).unwrap());
    }
    assert_eq!(counts(&cat, "users"), (40, vec![40]));

    // Same bytes, same version: the blob was not written again.
    let blob_after = e.sys_get(CATALOG_SYS_KEY).unwrap().unwrap();
    assert_eq!(blob_before, blob_after);
    assert_eq!(sys_seq(&e, CATALOG_SYS_KEY), Some(blob_seq_before));

    // The counter record carries the live counts.
    let rec = e.sys_get(&counter_sys_key(coll_id)).unwrap().unwrap();
    let dec = CollectionCounters::decode(&rec).unwrap();
    assert_eq!(dec.doc_count, 40);
    assert_eq!(dec.indexes, vec![(0, 40)]);

    // And a reopen reads them back from the record, not the blob.
    drop(e);
    let e2 = open(&dir);
    let cat2 = Catalog::load(&e2).unwrap();
    assert_eq!(counts(&cat2, "users"), (40, vec![40]));
}

/// A catalog blob written by a version that stored counts inline still loads
/// with those counts, keeps them across an unrelated DDL (which rewrites the
/// blob without counts and seeds the counter records) and across a reopen.
#[test]
fn legacy_blob_with_inline_counts_migrates_without_losing_them() {
    use zydecodb_document::catalog::{counter_sys_key, CATALOG_SYS_KEY};

    let dir = TempDir::new().unwrap();
    let legacy = serde_json::json!({
        "next_collection_id": 2,
        "next_index_id": 2,
        "collections": [
            {
                "id": 0,
                "prefix": [1],
                "name": "users",
                "doc_count": 1234,
                "indexes": [
                    {"id": 0, "name": "by_age", "fields": ["age"], "unique": false, "entry_count": 1234},
                    {"id": 1, "name": "by_email", "fields": ["email"], "unique": true, "entry_count": 1200}
                ]
            },
            {
                "id": 1,
                "prefix": [1],
                "name": "orders",
                "doc_count": 77,
                "indexes": []
            }
        ]
    });
    {
        let mut e = open(&dir);
        e.sys_put(
            CATALOG_SYS_KEY.to_vec(),
            serde_json::to_vec(&legacy).unwrap(),
        )
        .unwrap();
        e.shutdown().unwrap();
    }

    let mut e = open(&dir);
    let mut cat = Catalog::load(&e).unwrap();
    assert_eq!(counts(&cat, "users"), (1234, vec![1234, 1200]));
    assert_eq!(counts(&cat, "orders"), (77, vec![]));
    // No counter records yet: the counts came from the blob.
    assert!(e.sys_get(&counter_sys_key(0)).unwrap().is_none());
    assert!(e.sys_get(&counter_sys_key(1)).unwrap().is_none());

    // A write to one collection moves only that collection's record.
    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "orders",
        b"o1",
        br#"{"total":5}"#,
        false,
    )
    .unwrap();
    assert_eq!(counts(&cat, "orders"), (78, vec![]));
    assert!(e.sys_get(&counter_sys_key(1)).unwrap().is_some());
    assert!(e.sys_get(&counter_sys_key(0)).unwrap().is_none());

    // An unrelated DDL rewrites the blob (dropping inline counts) and seeds
    // every collection's counter record, so nothing is lost.
    store::define_index(
        &mut e,
        &mut cat,
        PREFIX,
        "orders",
        "by_total",
        vec!["total".into()],
        false,
        None,
    )
    .unwrap();
    assert_eq!(counts(&cat, "users"), (1234, vec![1234, 1200]));
    assert_eq!(counts(&cat, "orders"), (78, vec![1]));
    let text = String::from_utf8(e.sys_get(CATALOG_SYS_KEY).unwrap().unwrap()).unwrap();
    assert!(!text.contains("doc_count"), "{text}");
    assert!(e.sys_get(&counter_sys_key(0)).unwrap().is_some());

    // Crash (no clean shutdown) and reopen: counts come back from the records.
    drop(e);
    let e = open(&dir);
    let cat = Catalog::load(&e).unwrap();
    assert_eq!(counts(&cat, "users"), (1234, vec![1234, 1200]));
    assert_eq!(counts(&cat, "orders"), (78, vec![1]));
}

/// A multi-collection transaction-style commit writes one counter record per
/// collection with a non-zero delta, and the batch limit accounts for them.
#[test]
fn commit_with_deltas_touches_one_record_per_collection() {
    use zydecodb_document::catalog::{counter_sys_key, CollectionCounters};
    use zydecodb_engine::engine::BatchOp;

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    let a = cat.ensure_collection(PREFIX, "a");
    let b = cat.ensure_collection(PREFIX, "b");
    let c = cat.ensure_collection(PREFIX, "c");
    cat.persist(&mut e).unwrap();

    let mut ops = Vec::new();
    for coll in [a, b] {
        ops.push(BatchOp::Put {
            key: zydecodb_document::keys::doc_key(PREFIX, coll, b"x"),
            value: b"\x00{}".to_vec(),
            expires_at: 0,
        });
    }
    let seq_c_before = sys_seq(&e, &counter_sys_key(c));
    store::commit_with_deltas(&mut e, &mut cat, ops, &[(a, 1), (b, 1), (c, 0)]).unwrap();
    assert_eq!(counts(&cat, "a"), (1, vec![]));
    assert_eq!(counts(&cat, "b"), (1, vec![]));
    assert_eq!(counts(&cat, "c"), (0, vec![]));
    let rec =
        CollectionCounters::decode(&e.sys_get(&counter_sys_key(a)).unwrap().unwrap()).unwrap();
    assert_eq!(rec.doc_count, 1);
    // Zero delta: c's record was not rewritten.
    assert_eq!(sys_seq(&e, &counter_sys_key(c)), seq_c_before);

    // MAX_BATCH_KEYS ops plus two counter records overflow the batch.
    let too_many: Vec<BatchOp> = (0..zydecodb_engine::keys::MAX_BATCH_KEYS - 1)
        .map(|i| BatchOp::Put {
            key: zydecodb_document::keys::doc_key(PREFIX, a, format!("k{i}").as_bytes()),
            value: b"\x00{}".to_vec(),
            expires_at: 0,
        })
        .collect();
    let err = store::commit_with_deltas(&mut e, &mut cat, too_many, &[(a, 1), (b, 1)]).unwrap_err();
    assert!(matches!(
        err,
        zydecodb_document::error::DocError::BatchTooLarge(_)
    ));
    // Nothing moved on the failed commit.
    assert_eq!(counts(&cat, "a"), (1, vec![]));
}
