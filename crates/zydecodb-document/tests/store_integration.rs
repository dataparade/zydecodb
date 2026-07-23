//! Integration tests for the document layer against a real engine.

use tempfile::TempDir;
use zydecodb_document::catalog::Catalog;
use zydecodb_document::error::DocError;
use zydecodb_document::{keys, query, store};
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

fn body_name(snap: &zydecodb_engine::SnapshotHandle, cat: &Catalog, id: &[u8]) -> String {
    let body = query::get_by_id(snap, cat, PREFIX, "users", id)
        .unwrap()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    v["name"].as_str().unwrap().to_string()
}

fn doc_ids(page: &query::QueryPage) -> Vec<Vec<u8>> {
    page.rows.iter().map(|r| r.doc_id.clone()).collect()
}

#[test]
fn upsert_get_and_index_orders_by_field() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":30,"name":"alice"}"#,
        false,
    )
    .unwrap();
    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u2",
        br#"{"age":25,"name":"bob"}"#,
        false,
    )
    .unwrap();

    let snap = e.snapshot_owned();
    assert_eq!(body_name(&snap, &cat, b"u1"), "alice");

    // Ascending by age: u2 (25) before u1 (30).
    let spec =
        query::build_index_scan_spec(&cat, PREFIX, "users", "by_age", None, None, None, 10, true)
            .unwrap();
    let page = query::execute_index_scan(&snap, &spec).unwrap();
    assert_eq!(doc_ids(&page), vec![b"u2".to_vec(), b"u1".to_vec()]);
    assert!(page.next_cursor.is_none());
}

#[test]
fn bulk_delete_and_update_apply_to_all_candidates() {
    use zydecodb_document::update::{self, UpdateDoc};

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    let ids: Vec<Vec<u8>> = (0..5u8).map(|i| vec![b'u', b'0' + i]).collect();
    for id in &ids {
        store::upsert(&mut e, &cat, PREFIX, "users", id, br#"{"age":30}"#, false).unwrap();
    }

    // Atomic bulk update: every matching doc moves to the new age bucket.
    let upd = UpdateDoc::parse_bytes(br#"{"$set":{"age":31}}"#).unwrap();
    let modified = update::apply_to_ids(&mut e, &cat, PREFIX, "users", &ids, &upd, None).unwrap();
    assert_eq!(modified, 5);

    let snap = e.snapshot_owned();
    let spec = query::build_index_scan_spec(
        &cat,
        PREFIX,
        "users",
        "by_age",
        Some(b"[31]"),
        Some(b"[32]"),
        None,
        100,
        true,
    )
    .unwrap();
    assert_eq!(
        query::execute_index_scan(&snap, &spec).unwrap().rows.len(),
        5
    );
    drop(snap);

    // Atomic bulk delete: bodies and index entries all gone.
    let deleted = store::delete_ids(&mut e, &cat, PREFIX, "users", &ids, None).unwrap();
    assert_eq!(deleted, 5);
    let snap = e.snapshot_owned();
    let spec =
        query::build_index_scan_spec(&cat, PREFIX, "users", "by_age", None, None, None, 100, true)
            .unwrap();
    assert!(query::execute_index_scan(&snap, &spec)
        .unwrap()
        .rows
        .is_empty());
}

/// Regression: filtered updates/deletes must re-verify the filter per document
/// at write time. Candidate ids are selected from a lock-free snapshot, so a
/// document can stop matching between selection and write — those stale
/// candidates must be skipped and not counted, making a value-pinning filter a
/// true per-document compare-and-swap (the DBaaS control plane relies on this
/// for entitlement caps and revision allocation).
#[test]
fn filtered_write_recheck_skips_stale_candidates() {
    use zydecodb_document::filter::Filter;
    use zydecodb_document::update::{self, UpdateDoc};

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"count":4}"#,
        false,
    )
    .unwrap();

    // Phase 1 (as docdispatch does it): select candidates matching count == 4.
    let filter = Filter::parse_bytes(br#"{"count":4}"#).unwrap();
    let snap = e.snapshot_owned();
    let ids = query::find_ids(&snap, &cat, PREFIX, "users", &filter, 100).unwrap();
    assert_eq!(ids.len(), 1);
    drop(snap);

    // A concurrent writer bumps the count BEFORE our write runs.
    let bump = UpdateDoc::parse_bytes(br#"{"$inc":{"count":1}}"#).unwrap();
    assert!(update::apply_to_id(&mut e, &cat, PREFIX, "users", b"u1", &bump).unwrap());

    // Phase 2 with the stale candidate list: the re-check must skip it.
    let inc = UpdateDoc::parse_bytes(br#"{"$inc":{"count":1}}"#).unwrap();
    let modified =
        update::apply_to_ids(&mut e, &cat, PREFIX, "users", &ids, &inc, Some(&filter)).unwrap();
    assert_eq!(modified, 0, "stale candidate must not be updated");

    // The document kept the concurrent writer's value (5), not 6.
    let snap = e.snapshot_owned();
    let body = query::get_by_id(&snap, &cat, PREFIX, "users", b"u1")
        .unwrap()
        .unwrap();
    let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(v["count"], serde_json::json!(5));
    drop(snap);

    // Same contract for filtered deletes: stale candidates survive.
    let deleted = store::delete_ids(&mut e, &cat, PREFIX, "users", &ids, Some(&filter)).unwrap();
    assert_eq!(deleted, 0, "stale candidate must not be deleted");
    let snap = e.snapshot_owned();
    assert!(query::get_by_id(&snap, &cat, PREFIX, "users", b"u1")
        .unwrap()
        .is_some());

    // And with a still-matching filter, the write proceeds normally.
    drop(snap);
    let filter5 = Filter::parse_bytes(br#"{"count":5}"#).unwrap();
    let modified =
        update::apply_to_ids(&mut e, &cat, PREFIX, "users", &ids, &inc, Some(&filter5)).unwrap();
    assert_eq!(modified, 1);
}

#[test]
fn unique_index_rejects_duplicate_value() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(
        PREFIX,
        "users",
        "by_email",
        vec!["email".into()],
        true,
        None,
    )
    .unwrap();
    cat.persist(&mut e).unwrap();

    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"email":"a@x.com"}"#,
        false,
    )
    .unwrap();

    // A different document with the same unique value is rejected.
    let err = store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u2",
        br#"{"email":"a@x.com"}"#,
        false,
    )
    .unwrap_err();
    assert!(matches!(err, DocError::DuplicateKey(_)));

    // Re-upserting the SAME document with its value is allowed (idempotent).
    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"email":"a@x.com"}"#,
        false,
    )
    .unwrap();

    // A distinct value for a new document is allowed.
    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u2",
        br#"{"email":"b@x.com"}"#,
        false,
    )
    .unwrap();

    // Updating u1 to collide with u2's value is rejected.
    let err = store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"email":"b@x.com"}"#,
        false,
    )
    .unwrap_err();
    assert!(matches!(err, DocError::DuplicateKey(_)));
}

#[test]
fn updating_indexed_field_moves_the_entry() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":30}"#,
        false,
    )
    .unwrap();
    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":40}"#,
        false,
    )
    .unwrap();

    let snap = e.snapshot_owned();
    // Old bucket [30,31) is empty; new bucket [40,41) has u1.
    let old = query::build_index_scan_spec(
        &cat,
        PREFIX,
        "users",
        "by_age",
        Some(b"[30]"),
        Some(b"[31]"),
        None,
        10,
        false,
    )
    .unwrap();
    assert!(query::execute_index_scan(&snap, &old)
        .unwrap()
        .rows
        .is_empty());

    let new = query::build_index_scan_spec(
        &cat,
        PREFIX,
        "users",
        "by_age",
        Some(b"[40]"),
        Some(b"[41]"),
        None,
        10,
        false,
    )
    .unwrap();
    assert_eq!(
        doc_ids(&query::execute_index_scan(&snap, &new).unwrap()),
        vec![b"u1".to_vec()]
    );
}

#[test]
fn delete_removes_doc_and_index_entries() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":30}"#,
        false,
    )
    .unwrap();
    assert!(store::delete(&mut e, &cat, PREFIX, "users", b"u1").unwrap());
    assert!(!store::delete(&mut e, &cat, PREFIX, "users", b"u1").unwrap());

    let snap = e.snapshot_owned();
    assert!(query::get_by_id(&snap, &cat, PREFIX, "users", b"u1")
        .unwrap()
        .is_none());
    let spec =
        query::build_index_scan_spec(&cat, PREFIX, "users", "by_age", None, None, None, 10, false)
            .unwrap();
    assert!(query::execute_index_scan(&snap, &spec)
        .unwrap()
        .rows
        .is_empty());
}

#[test]
fn pagination_walks_all_rows_in_order() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "users", "by_age", vec!["age".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();

    for (id, age) in [("u1", 30), ("u2", 25), ("u3", 40), ("u4", 35)] {
        let body = format!(r#"{{"age":{age}}}"#);
        store::upsert(
            &mut e,
            &cat,
            PREFIX,
            "users",
            id.as_bytes(),
            body.as_bytes(),
            false,
        )
        .unwrap();
    }

    // Page through with limit 2; expect ascending age order across pages.
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor: Option<Vec<u8>> = None;
    loop {
        let snap = e.snapshot_owned();
        let spec = query::build_index_scan_spec(
            &cat,
            PREFIX,
            "users",
            "by_age",
            None,
            None,
            cursor.as_deref(),
            2,
            false,
        )
        .unwrap();
        let page = query::execute_index_scan(&snap, &spec).unwrap();
        seen.extend(doc_ids(&page));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    // Ages 25,30,35,40 -> u2,u1,u4,u3
    assert_eq!(
        seen,
        vec![
            b"u2".to_vec(),
            b"u1".to_vec(),
            b"u4".to_vec(),
            b"u3".to_vec()
        ]
    );
}

#[test]
fn define_index_backfills_existing_documents() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    // Collection exists, but no index yet.
    cat.ensure_collection(PREFIX, "users");
    cat.persist(&mut e).unwrap();

    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":30}"#,
        false,
    )
    .unwrap();
    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u2",
        br#"{"age":25}"#,
        false,
    )
    .unwrap();

    // Define the index after the data exists -> must backfill.
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

    let snap = e.snapshot_owned();
    let spec =
        query::build_index_scan_spec(&cat, PREFIX, "users", "by_age", None, None, None, 10, false)
            .unwrap();
    assert_eq!(
        doc_ids(&query::execute_index_scan(&snap, &spec).unwrap()),
        vec![b"u2".to_vec(), b"u1".to_vec()]
    );

    // And the committed catalog survives a reopen.
    drop(snap);
    drop(e);
    let e2 = open(&dir);
    let cat2 = Catalog::load(&e2).unwrap();
    assert!(cat2
        .collection(PREFIX, "users")
        .unwrap()
        .indexes
        .iter()
        .any(|i| i.name == "by_age"));
}

#[test]
fn orphan_index_keys_without_catalog_entry_are_invisible() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    let coll_id = cat.ensure_collection(PREFIX, "users");
    cat.persist(&mut e).unwrap();
    store::upsert(
        &mut e,
        &cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":30}"#,
        false,
    )
    .unwrap();

    // Simulate a backfill that wrote index entries but crashed BEFORE the
    // catalog commit: write an orphan index entry for an index id the
    // committed catalog never references.
    let orphan = keys::index_key(PREFIX, coll_id, 999, b"\x02orphan", b"u1");
    e.put(orphan, b"u1".to_vec(), 0).unwrap();
    drop(e);

    // Reopen: the catalog has no such index, so it is unusable (invisible),
    // while the document itself is intact.
    let e2 = open(&dir);
    let cat2 = Catalog::load(&e2).unwrap();
    let snap = e2.snapshot_owned();
    assert!(query::get_by_id(&snap, &cat2, PREFIX, "users", b"u1")
        .unwrap()
        .is_some());
    let err = query::build_index_scan_spec(
        &cat2, PREFIX, "users", "by_age", None, None, None, 10, false,
    )
    .unwrap_err();
    assert!(matches!(err, DocError::IndexNotFound(_)));
}

#[test]
fn oversized_document_batch_is_rejected() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    // More indexes than one atomic batch allows (doc op + one put per index).
    for i in 0..=zydecodb_engine::keys::MAX_BATCH_KEYS {
        cat.add_index(
            PREFIX,
            "users",
            &format!("idx{i}"),
            vec![format!("f{i}")],
            false,
            None,
        )
        .unwrap();
    }
    cat.persist(&mut e).unwrap();

    let err = store::upsert(&mut e, &cat, PREFIX, "users", b"u1", b"{}", false).unwrap_err();
    assert!(matches!(err, DocError::BatchTooLarge(_)));
    // Nothing persisted.
    let snap = e.snapshot_owned();
    assert!(query::get_by_id(&snap, &cat, PREFIX, "users", b"u1")
        .unwrap()
        .is_none());
}
