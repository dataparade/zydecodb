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
