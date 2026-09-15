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
        &mut cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":30,"name":"alice"}"#,
        false,
    )
    .unwrap();
    store::upsert(
        &mut e,
        &mut cat,
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
        store::upsert(
            &mut e,
            &mut cat,
            PREFIX,
            "users",
            id,
            br#"{"age":30}"#,
            false,
        )
        .unwrap();
    }

    // Atomic bulk update: every matching doc moves to the new age bucket.
    let upd = UpdateDoc::parse_bytes(br#"{"$set":{"age":31}}"#).unwrap();
    let modified =
        update::apply_to_ids(&mut e, &mut cat, PREFIX, "users", &ids, &upd, None).unwrap();
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
    let deleted = store::delete_ids(&mut e, &mut cat, PREFIX, "users", &ids, None).unwrap();
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
        &mut cat,
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
    assert!(update::apply_to_id(&mut e, &mut cat, PREFIX, "users", b"u1", &bump).unwrap());

    // Phase 2 with the stale candidate list: the re-check must skip it.
    let inc = UpdateDoc::parse_bytes(br#"{"$inc":{"count":1}}"#).unwrap();
    let modified =
        update::apply_to_ids(&mut e, &mut cat, PREFIX, "users", &ids, &inc, Some(&filter)).unwrap();
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
    let deleted =
        store::delete_ids(&mut e, &mut cat, PREFIX, "users", &ids, Some(&filter)).unwrap();
    assert_eq!(deleted, 0, "stale candidate must not be deleted");
    let snap = e.snapshot_owned();
    assert!(query::get_by_id(&snap, &cat, PREFIX, "users", b"u1")
        .unwrap()
        .is_some());

    // And with a still-matching filter, the write proceeds normally.
    drop(snap);
    let filter5 = Filter::parse_bytes(br#"{"count":5}"#).unwrap();
    let modified = update::apply_to_ids(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        &ids,
        &inc,
        Some(&filter5),
    )
    .unwrap();
    assert_eq!(modified, 1);
}

#[test]
fn filtered_positional_set_maintains_indexes() {
    use zydecodb_document::filter::Filter;
    use zydecodb_document::query::FindSpec;
    use zydecodb_document::update::{self, UpdateDoc};

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.ensure_collection(PREFIX, "orders");
    cat.add_index(
        PREFIX,
        "orders",
        "by_status",
        vec!["status".into()],
        false,
        None,
    )
    .unwrap();
    cat.persist(&mut e).unwrap();

    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "orders",
        b"o1",
        br#"{"status":"open","items":[{"skuId":"A","qty":1},{"skuId":"B","qty":2}]}"#,
        false,
    )
    .unwrap();

    let upd = UpdateDoc::parse_bytes(br#"{"$set":{"items.$[skuId=B].qty":9,"status":"packed"}}"#)
        .unwrap();
    assert!(update::apply_to_id(&mut e, &mut cat, PREFIX, "orders", b"o1", &upd).unwrap());

    let snap = e.snapshot_owned();
    let page = query::execute_find(
        &snap,
        &cat,
        PREFIX,
        "orders",
        &FindSpec {
            filter: Filter::parse_bytes(br#"{"status":"packed"}"#).unwrap(),
            sort: vec![],
            projection: None,
            skip: 0,
            limit: 10,
            cursor: None,
        },
        query::MAX_SORT_BUFFER,
    )
    .unwrap();
    assert_eq!(page.rows.len(), 1);
    let body: serde_json::Value =
        serde_json::from_slice(page.rows[0].body.as_ref().unwrap()).unwrap();
    assert_eq!(body["items"][1]["qty"], 9);
    assert_eq!(body["status"], "packed");

    // Old status index entry is gone.
    let old = query::execute_find(
        &snap,
        &cat,
        PREFIX,
        "orders",
        &FindSpec {
            filter: Filter::parse_bytes(br#"{"status":"open"}"#).unwrap(),
            sort: vec![],
            projection: None,
            skip: 0,
            limit: 10,
            cursor: None,
        },
        query::MAX_SORT_BUFFER,
    )
    .unwrap();
    assert!(old.rows.is_empty());
}

#[test]
fn if_match_succeeds_when_revision_current() {
    use zydecodb_document::update::{self, UpdateDoc};

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.ensure_collection(PREFIX, "users");
    cat.persist(&mut e).unwrap();
    let seq = store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"n":1}"#,
        false,
    )
    .unwrap();
    assert!(seq > 0);
    let rev = store::doc_revision(&e, &cat, PREFIX, "users", b"u1")
        .unwrap()
        .unwrap();
    assert_eq!(rev, seq);

    store::check_if_match(&e, &cat, PREFIX, "users", b"u1", rev).unwrap();
    let new_seq = store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"n":2}"#,
        false,
    )
    .unwrap();
    assert!(new_seq > rev);

    assert!(matches!(
        store::check_if_match(&e, &cat, PREFIX, "users", b"u1", rev),
        Err(zydecodb_document::error::DocError::StaleRevision)
    ));

    let upd = UpdateDoc::parse_bytes(br#"{"$inc":{"n":1}}"#).unwrap();
    let cur = store::doc_revision(&e, &cat, PREFIX, "users", b"u1")
        .unwrap()
        .unwrap();
    let after =
        update::apply_to_id_if_match(&mut e, &mut cat, PREFIX, "users", b"u1", &upd, cur).unwrap();
    assert!(after > cur);
    assert!(matches!(
        update::apply_to_id_if_match(&mut e, &mut cat, PREFIX, "users", b"u1", &upd, cur),
        Err(zydecodb_document::error::DocError::StaleRevision)
    ));
    assert!(matches!(
        store::check_if_match(&e, &cat, PREFIX, "users", b"missing", 1),
        Err(zydecodb_document::error::DocError::StaleRevision)
    ));
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
        &mut cat,
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
        &mut cat,
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
        &mut cat,
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
        &mut cat,
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
        &mut cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"email":"b@x.com"}"#,
        false,
    )
    .unwrap_err();
    assert!(matches!(err, DocError::DuplicateKey(_)));
}

/// `update_many` that would set two documents to the same unique value must
/// fail before anything is written: neither body changes and the unique index
/// keeps exactly one entry per document. The committed-state check alone
/// cannot see this (neither doc owns the value yet); the batch-level claim
/// tracking must.
#[test]
fn update_many_rejects_intra_batch_unique_collision() {
    use serde_json::json;
    use zydecodb_document::update::{self, UpdateDoc};

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
    for (id, email) in [(b"u1", "a@x.com"), (b"u2", "b@x.com"), (b"u3", "c@x.com")] {
        let body = serde_json::to_vec(&json!({"email": email, "grp": 1})).unwrap();
        store::upsert(&mut e, &mut cat, PREFIX, "users", id, &body, false).unwrap();
    }
    let coll_id = cat.collection(PREFIX, "users").unwrap().id;
    let idx_id = cat.collection(PREFIX, "users").unwrap().indexes[0].id;
    let index_entries = |e: &Engine| -> Vec<Vec<u8>> {
        let lo = keys::index_prefix(PREFIX, coll_id, idx_id);
        let hi = keys::prefix_upper_bound(&lo);
        let snap = e.snapshot_owned();
        snap.scan(lo, hi).unwrap().map(|r| r.unwrap().1).collect()
    };
    assert_eq!(index_entries(&e).len(), 3);

    let ids: Vec<Vec<u8>> = vec![b"u1".to_vec(), b"u2".to_vec(), b"u3".to_vec()];
    let collide = UpdateDoc::parse(&json!({"$set": {"email": "same@x.com"}})).unwrap();
    let err =
        update::apply_to_ids(&mut e, &mut cat, PREFIX, "users", &ids, &collide, None).unwrap_err();
    assert!(matches!(err, DocError::DuplicateKey(_)), "got {err:?}");

    // Nothing changed: bodies, index cardinality, and counters.
    let snap = e.snapshot_owned();
    for (id, email) in [(b"u1", "a@x.com"), (b"u2", "b@x.com"), (b"u3", "c@x.com")] {
        let body = query::get_by_id(&snap, &cat, PREFIX, "users", id)
            .unwrap()
            .unwrap();
        let v: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(v["email"], json!(email), "{}", String::from_utf8_lossy(id));
    }
    drop(snap);
    let mut entries = index_entries(&e);
    entries.sort();
    assert_eq!(
        entries,
        vec![b"u1".to_vec(), b"u2".to_vec(), b"u3".to_vec()]
    );
    assert_eq!(cat.collection(PREFIX, "users").unwrap().doc_count, 3);

    // A non-colliding multi-doc update on the same unique field still commits
    // atomically (each doc gets a distinct value).
    let inc = UpdateDoc::parse(&json!({"$set": {"grp": 2}})).unwrap();
    let n = update::apply_to_ids(&mut e, &mut cat, PREFIX, "users", &ids, &inc, None).unwrap();
    assert_eq!(n, 3);
    assert_eq!(index_entries(&e).len(), 3);

    // Filtered update_many where only one candidate still matches: no claim
    // conflict, since the other candidates are skipped.
    let filter = zydecodb_document::filter::Filter::parse(&json!({"email": "a@x.com"})).unwrap();
    let n = update::apply_to_ids(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        &ids,
        &collide,
        Some(&filter),
    )
    .unwrap();
    assert_eq!(n, 1);
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
        &mut cat,
        PREFIX,
        "users",
        b"u1",
        br#"{"age":30}"#,
        false,
    )
    .unwrap();
    assert!(store::delete(&mut e, &mut cat, PREFIX, "users", b"u1").unwrap());
    assert!(!store::delete(&mut e, &mut cat, PREFIX, "users", b"u1").unwrap());

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
            &mut cat,
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

/// Defining a unique index over documents that already share a value must
/// fail with DuplicateKey, leave the catalog (in memory and on disk)
/// unchanged, and leave no index keys behind. Once the duplicate is fixed the
/// same DDL succeeds and later duplicate writes are rejected.
#[test]
fn define_unique_index_over_existing_duplicates_is_rejected_cleanly() {
    use serde_json::json;

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    let coll_id = cat.ensure_collection(PREFIX, "users");
    cat.persist(&mut e).unwrap();

    // Enough documents to span several backfill chunks, with one duplicate
    // pair buried in the middle (ZDoc and raw JSON bodies mixed).
    let n = zydecodb_engine::keys::MAX_BATCH_KEYS * 2 + 7;
    for i in 0..n {
        let email = if i == n / 2 {
            "dup@x.com".to_string()
        } else {
            format!("u{i}@x.com")
        };
        let body = json!({"email": email, "i": i});
        if i % 2 == 0 {
            let zdoc = zydecodb_document::binary::ZDocBuilder::from_value(&body);
            store::upsert(
                &mut e,
                &mut cat,
                PREFIX,
                "users",
                format!("u{i}").as_bytes(),
                &zdoc,
                true,
            )
            .unwrap();
        } else {
            store::upsert(
                &mut e,
                &mut cat,
                PREFIX,
                "users",
                format!("u{i}").as_bytes(),
                &serde_json::to_vec(&body).unwrap(),
                false,
            )
            .unwrap();
        }
    }
    // The second half of the duplicate pair, at the very end of the range.
    store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        b"zz-dup",
        br#"{"email":"dup@x.com","i":-1}"#,
        false,
    )
    .unwrap();
    let before_blob = e
        .sys_get(zydecodb_document::catalog::CATALOG_SYS_KEY)
        .unwrap()
        .unwrap();
    let before_cat = cat.clone();
    let next_index_id = 0u32; // no index defined yet: the first id handed out

    let err = store::define_index(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        "by_email",
        vec!["email".into()],
        true,
        None,
    )
    .unwrap_err();
    assert!(matches!(err, DocError::DuplicateKey(_)), "got {err:?}");

    // Catalog untouched in memory and on disk.
    assert_eq!(cat, before_cat);
    assert!(cat.collection(PREFIX, "users").unwrap().indexes.is_empty());
    let after_blob = e
        .sys_get(zydecodb_document::catalog::CATALOG_SYS_KEY)
        .unwrap()
        .unwrap();
    assert_eq!(before_blob, after_blob);

    // No orphan index keys under the id the failed DDL would have used.
    let lo = keys::index_prefix(PREFIX, coll_id, next_index_id);
    let hi = keys::prefix_upper_bound(&lo);
    let snap = e.snapshot_owned();
    assert_eq!(snap.scan(lo, hi).unwrap().count(), 0, "orphan index keys");
    drop(snap);

    // Fix the duplicate; the DDL now succeeds and enforces going forward.
    assert!(store::delete(&mut e, &mut cat, PREFIX, "users", b"zz-dup").unwrap());
    store::define_index(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        "by_email",
        vec!["email".into()],
        true,
        None,
    )
    .unwrap();
    let idx = &cat.collection(PREFIX, "users").unwrap().indexes[0];
    assert!(idx.unique);
    assert_eq!(idx.entry_count, n as u64);
    let err = store::upsert(
        &mut e,
        &mut cat,
        PREFIX,
        "users",
        b"late",
        br#"{"email":"dup@x.com"}"#,
        false,
    )
    .unwrap_err();
    assert!(matches!(err, DocError::DuplicateKey(_)));
}

/// A unique TTL index over duplicates must not stamp any document's expiry.
/// A stamp rewrites the doc key, so an unchanged seq proves nothing was
/// written back to the documents by the rejected DDL.
#[test]
fn rejected_unique_ttl_index_does_not_stamp_expiry() {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    let coll_id = cat.ensure_collection(PREFIX, "sess");
    cat.persist(&mut e).unwrap();
    // A timestamp far enough in the future that the index entries are live
    // when the duplicate scan runs.
    let at = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 3_600_000;
    let body = format!(r#"{{"at":{at}}}"#);
    for id in [b"s1", b"s2"] {
        store::upsert(&mut e, &mut cat, PREFIX, "sess", id, body.as_bytes(), false).unwrap();
    }
    let seq_of = |e: &Engine, id: &[u8]| {
        let k = keys::doc_key(PREFIX, coll_id, id);
        e.get_with_seq(&k).unwrap().expect("doc present").1
    };
    let before = [seq_of(&e, b"s1"), seq_of(&e, b"s2")];

    let err = store::define_index(
        &mut e,
        &mut cat,
        PREFIX,
        "sess",
        "ttl_unique",
        vec!["at".into()],
        true,
        Some(60),
    )
    .unwrap_err();
    assert!(matches!(err, DocError::DuplicateKey(_)), "got {err:?}");

    assert_eq!([seq_of(&e, b"s1"), seq_of(&e, b"s2")], before);
    let snap = e.snapshot_owned();
    for id in [b"s1", b"s2"] {
        assert!(query::get_by_id(&snap, &cat, PREFIX, "sess", id)
            .unwrap()
            .is_some());
    }
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
        &mut cat,
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

    let err = store::upsert(&mut e, &mut cat, PREFIX, "users", b"u1", b"{}", false).unwrap_err();
    assert!(matches!(err, DocError::BatchTooLarge(_)));
    // Nothing persisted.
    let snap = e.snapshot_owned();
    assert!(query::get_by_id(&snap, &cat, PREFIX, "users", b"u1")
        .unwrap()
        .is_none());
}

#[test]
fn expires_at_change_rewrites_index_keys_for_compaction_reclaim() {
    use zydecodb_document::filter::Filter;
    use zydecodb_document::query::FindSpec;
    use zydecodb_engine::compaction::CompactionConfig;

    let dir = TempDir::new().unwrap();
    let mut e = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        compaction: {
            let mut c = CompactionConfig::default();
            c.l0_trigger = 2;
            c.target_file_bytes = 512;
            c
        },
        ..Default::default()
    })
    .unwrap();
    let mut cat = Catalog::default();
    cat.ensure_collection(PREFIX, "sess");
    cat.add_index(
        PREFIX,
        "sess",
        "by_status",
        vec!["status".into()],
        false,
        None,
    )
    .unwrap();
    cat.persist(&mut e).unwrap();

    let far = 4_000_000_000_000u64; // far future millis
    store::upsert_with_expiry(
        &mut e,
        &mut cat,
        PREFIX,
        "sess",
        b"s1",
        br#"{"status":"open"}"#,
        false,
        far,
    )
    .unwrap();
    e.force_flush().unwrap();

    // Same indexed fields, shorter expiry — intersection index keys must be rewritten.
    store::upsert_with_expiry(
        &mut e,
        &mut cat,
        PREFIX,
        "sess",
        b"s1",
        br#"{"status":"open"}"#,
        false,
        1,
    )
    .unwrap();
    e.force_flush().unwrap();
    e.drain_compaction().unwrap();

    let snap = e.snapshot_owned();
    assert!(query::get_by_id(&snap, &cat, PREFIX, "sess", b"s1")
        .unwrap()
        .is_none());
    let page = query::execute_find(
        &snap,
        &cat,
        PREFIX,
        "sess",
        &FindSpec {
            filter: Filter::parse_bytes(br#"{"status":"open"}"#).unwrap(),
            sort: vec![],
            projection: None,
            skip: 0,
            limit: 10,
            cursor: None,
        },
        query::MAX_SORT_BUFFER,
    )
    .unwrap();
    assert!(page.rows.is_empty(), "expired body+index must both reclaim");
}

/// A garbage `VK_ZDOC` body planted directly through the engine (what raw-KV
/// aliasing, disk corruption, or a legacy writer could leave behind) must
/// surface as `DocError::Corrupt` from every read/write path. Before the
/// reader was bounds-checked, `[0x03]` (an i64 tag with no payload) hit an
/// `unwrap()` and, with `panic = "abort"`, took the whole server down.
#[test]
fn corrupt_zdoc_body_is_reported_not_a_panic() {
    use serde_json::json;
    use zydecodb_document::filter::Filter;
    use zydecodb_document::query::FindSpec;
    use zydecodb_document::update::{self, UpdateDoc};

    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.add_index(PREFIX, "c", "by_n", vec!["n".into()], false, None)
        .unwrap();
    cat.persist(&mut e).unwrap();
    store::upsert(&mut e, &mut cat, PREFIX, "c", b"good", br#"{"n":1}"#, false).unwrap();
    let coll_id = cat.collection(PREFIX, "c").unwrap().id;

    let garbage: [&[u8]; 6] = [
        &[store::VK_ZDOC, 0x03],
        &[store::VK_ZDOC, 0x05, 0xff, 0xff, 0xff, 0xff, b'a'],
        &[store::VK_ZDOC, 0x07, 9, 0, 0, 0, 0xff, 0xff, 0xff, 0xff],
        &[
            store::VK_ZDOC,
            0x07,
            17,
            0,
            0,
            0,
            1,
            0,
            0,
            0,
            200,
            0,
            0,
            0,
            210,
            0,
            0,
            0,
        ],
        &[store::VK_ZDOC, 0x06, 13, 0, 0, 0, 1, 0, 0, 0, 99, 0, 0, 0],
        &[], // empty body
    ];
    let all = FindSpec {
        filter: Filter::parse(&json!({})).unwrap(),
        sort: vec![],
        projection: None,
        skip: 0,
        limit: 10,
        cursor: None,
    };
    let by_n = FindSpec {
        filter: Filter::parse(&json!({"n": 1})).unwrap(),
        ..all.clone()
    };
    for (i, bad) in garbage.iter().enumerate() {
        let id = format!("bad{i}").into_bytes();
        e.put(keys::doc_key(PREFIX, coll_id, &id), bad.to_vec(), 0)
            .unwrap();
        let snap = e.snapshot_owned();

        let err = query::get_by_id(&snap, &cat, PREFIX, "c", &id).unwrap_err();
        assert!(
            matches!(err, DocError::Corrupt(_)),
            "get_by_id #{i}: {err:?}"
        );

        let err = query::execute_find(&snap, &cat, PREFIX, "c", &all, query::MAX_SORT_BUFFER)
            .unwrap_err();
        assert!(matches!(err, DocError::Corrupt(_)), "find #{i}: {err:?}");

        // Filtered paths evaluate the corrupt view: no match, no panic. The
        // good document is still returned.
        let page =
            query::execute_find(&snap, &cat, PREFIX, "c", &by_n, query::MAX_SORT_BUFFER).unwrap();
        assert_eq!(page.rows.len(), 1, "filtered find #{i}");
        assert_eq!(page.rows[0].doc_id, b"good");
        assert_eq!(
            query::count(&snap, &cat, PREFIX, "c", &by_n.filter).unwrap(),
            1
        );
        let n_distinct = query::distinct(&snap, &cat, PREFIX, "c", "n", &by_n.filter).unwrap();
        assert_eq!(n_distinct, vec![json!(1)]);
        drop(snap);

        // Write paths that must decode the old body report Corrupt too.
        let upd = UpdateDoc::parse(&json!({"$set": {"n": 2}})).unwrap();
        let err = update::apply_to_id(&mut e, &mut cat, PREFIX, "c", &id, &upd).unwrap_err();
        assert!(matches!(err, DocError::Corrupt(_)), "update #{i}: {err:?}");
        let err = store::current_json_body(&e, &cat, PREFIX, "c", &id).unwrap_err();
        assert!(
            matches!(err, DocError::Corrupt(_)),
            "current_json_body #{i}"
        );

        // Delete of a corrupt ZDoc body: the view yields no index keys, the
        // body row itself is removed, and the collection is readable again.
        assert!(store::delete(&mut e, &mut cat, PREFIX, "c", &id).unwrap());
        let snap = e.snapshot_owned();
        let page =
            query::execute_find(&snap, &cat, PREFIX, "c", &all, query::MAX_SORT_BUFFER).unwrap();
        assert_eq!(page.rows.len(), 1, "after delete #{i}");
    }
}
