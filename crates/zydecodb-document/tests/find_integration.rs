//! Integration tests for the filter-driven query layer (find/update/count/
//! distinct) against a real engine.

use serde_json::{json, Value};
use tempfile::TempDir;
use zydecodb_document::catalog::Catalog;
use zydecodb_document::filter::Filter;
use zydecodb_document::query::{self, FindSpec, Projection};
use zydecodb_document::store;
use zydecodb_document::update::{self, UpdateDoc};
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

/// Seed a `people` collection (indexed on `age`) with a handful of docs.
fn seed(indexed: bool) -> (TempDir, Engine, Catalog) {
    let dir = TempDir::new().unwrap();
    let mut e = open(&dir);
    let mut cat = Catalog::default();
    cat.ensure_collection(PREFIX, "people");
    if indexed {
        cat.add_index(PREFIX, "people", "by_age", vec!["age".into()], false, None)
            .unwrap();
    }
    cat.persist(&mut e).unwrap();

    let docs = [
        (
            b"a".as_slice(),
            json!({"name": "Ada", "age": 30, "city": "London"}),
        ),
        (
            b"b".as_slice(),
            json!({"name": "Bo", "age": 25, "city": "NOLA"}),
        ),
        (
            b"c".as_slice(),
            json!({"name": "Cy", "age": 40, "city": "NOLA"}),
        ),
        (
            b"d".as_slice(),
            json!({"name": "Di", "age": 35, "city": "Paris"}),
        ),
    ];
    for (id, doc) in docs {
        let body = serde_json::to_vec(&doc).unwrap();
        store::upsert(&mut e, &cat, PREFIX, "people", id, &body, false).unwrap();
    }
    (dir, e, cat)
}

fn spec(filter: Value) -> FindSpec {
    FindSpec {
        filter: Filter::parse(&filter).unwrap(),
        sort: vec![],
        projection: None,
        skip: 0,
        limit: 100,
        cursor: None,
    }
}

fn names(page: &query::QueryPage) -> Vec<String> {
    page.rows
        .iter()
        .map(|r| {
            let v: Value = serde_json::from_slice(r.body.as_ref().unwrap()).unwrap();
            v["name"].as_str().unwrap().to_string()
        })
        .collect()
}

fn run(e: &Engine, cat: &Catalog, s: &FindSpec) -> query::QueryPage {
    let snap = e.snapshot_owned();
    query::execute_find(&snap, cat, PREFIX, "people", s, query::MAX_SORT_BUFFER).unwrap()
}

#[test]
fn find_unindexed_field_uses_collection_scan() {
    let (_d, e, cat) = seed(false);
    let mut s = spec(json!({"city": "NOLA"}));
    s.sort = vec![("age".into(), true)];
    let page = run(&e, &cat, &s);
    assert_eq!(names(&page), vec!["Bo", "Cy"]);
}

#[test]
fn find_indexed_range_returns_sorted() {
    let (_d, e, cat) = seed(true);
    let mut s = spec(json!({"age": {"$gte": 30}}));
    s.sort = vec![("age".into(), true)];
    let page = run(&e, &cat, &s);
    assert_eq!(names(&page), vec!["Ada", "Di", "Cy"]);
}

#[test]
fn find_by_id_fast_path() {
    let (_d, e, cat) = seed(true);
    let page = run(&e, &cat, &spec(json!({"_id": "c"})));
    assert_eq!(names(&page), vec!["Cy"]);
}

#[test]
fn sort_skip_limit_projection() {
    let (_d, e, cat) = seed(false);
    let s = FindSpec {
        filter: Filter::MatchAll,
        sort: vec![("age".into(), false)], // descending
        projection: Some(Projection::Include(vec!["name".into()])),
        skip: 1,
        limit: 2,
        cursor: None,
    };
    let page = run(&e, &cat, &s);
    // ages desc: Cy(40), Di(35), Ada(30), Bo(25); skip 1, take 2 -> Di, Ada
    assert_eq!(names(&page), vec!["Di", "Ada"]);
    // Projection kept only name (+ _id, absent from body here).
    let body: Value = serde_json::from_slice(page.rows[0].body.as_ref().unwrap()).unwrap();
    assert!(body.get("age").is_none());
    assert_eq!(body["name"], json!("Di"));
}

#[test]
fn pagination_key_mode_over_index() {
    let (_d, e, cat) = seed(true);
    // No sort + index path => KEY cursor mode.
    let mut all = Vec::new();
    let mut cursor = None;
    loop {
        let s = FindSpec {
            filter: Filter::parse(&json!({"age": {"$gte": 0}})).unwrap(),
            sort: vec![],
            projection: None,
            skip: 0,
            limit: 2,
            cursor: cursor.clone(),
        };
        let page = run(&e, &cat, &s);
        all.extend(names(&page));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    // index order is by age asc.
    assert_eq!(all, vec!["Bo", "Ada", "Di", "Cy"]);
}

#[test]
fn pagination_offset_mode_with_sort() {
    let (_d, e, cat) = seed(false);
    let mut all = Vec::new();
    let mut cursor = None;
    loop {
        let s = FindSpec {
            filter: Filter::MatchAll,
            sort: vec![("name".into(), true)],
            projection: None,
            skip: 0,
            limit: 1,
            cursor: cursor.clone(),
        };
        let page = run(&e, &cat, &s);
        all.extend(names(&page));
        match page.next_cursor {
            Some(c) => cursor = Some(c),
            None => break,
        }
    }
    assert_eq!(all, vec!["Ada", "Bo", "Cy", "Di"]);
}

/// Mimic the server's paginated-read snapshot selection: a cursor re-pins the
/// same sequence ceiling via `snapshot_at`; the first page captures the latest.
fn run_rr(e: &Engine, cat: &Catalog, s: &FindSpec) -> query::QueryPage {
    let snap = match query::cursor_snapshot_seq(s.cursor.as_deref()) {
        Some(seq) => e.snapshot_at(seq),
        None => e.snapshot_owned(),
    };
    query::execute_find(&snap, cat, PREFIX, "people", s, query::MAX_SORT_BUFFER).unwrap()
}

#[test]
fn pagination_is_repeatable_read_across_inserts() {
    let (_d, mut e, cat) = seed(true);
    let page_spec = |cursor: Option<Vec<u8>>| FindSpec {
        filter: Filter::parse(&json!({"age": {"$gte": 0}})).unwrap(),
        sort: vec![],
        projection: None,
        skip: 0,
        limit: 2,
        cursor,
    };

    // Page 1 (key-cursor mode over the by_age index): Bo(25), Ada(30).
    let page1 = run_rr(&e, &cat, &page_spec(None));
    assert_eq!(names(&page1), vec!["Bo", "Ada"]);
    let cursor = page1.next_cursor.clone().expect("more pages remain");

    // Insert rows whose ages (33, 36) land *inside* the not-yet-read range. A
    // read-committed cursor would observe them; a repeatable-read one must not.
    for (id, doc) in [
        (b"e".as_slice(), json!({"name": "Ed", "age": 36})),
        (b"f".as_slice(), json!({"name": "Fi", "age": 33})),
    ] {
        let body = serde_json::to_vec(&doc).unwrap();
        store::upsert(&mut e, &cat, PREFIX, "people", id, &body, false).unwrap();
    }

    // Drain remaining pages from the pinned snapshot.
    let mut all = names(&page1);
    let mut cursor = Some(cursor);
    while let Some(c) = cursor.take() {
        let page = run_rr(&e, &cat, &page_spec(Some(c)));
        all.extend(names(&page));
        cursor = page.next_cursor;
    }

    // Exactly the four originals, in index order; Ed/Fi never appear.
    assert_eq!(all, vec!["Bo", "Ada", "Di", "Cy"]);
}

#[test]
fn update_one_moves_index_entry() {
    let (_d, mut e, cat) = seed(true);
    let upd = UpdateDoc::parse(&json!({"$inc": {"age": 5}})).unwrap();
    let id = {
        let snap = e.snapshot_owned();
        query::find_first_id(
            &snap,
            &cat,
            PREFIX,
            "people",
            &Filter::parse(&json!({"name":"Bo"})).unwrap(),
        )
        .unwrap()
        .unwrap()
    };
    assert!(update::apply_to_id(&mut e, &cat, PREFIX, "people", &id, &upd).unwrap());

    // Bo was 25, now 30; querying age 30 by index returns both Ada and Bo.
    let mut s = spec(json!({"age": 30}));
    s.sort = vec![("name".into(), true)];
    let page = run(&e, &cat, &s);
    assert_eq!(names(&page), vec!["Ada", "Bo"]);
}

#[test]
fn update_many_and_count_and_distinct() {
    let (_d, mut e, cat) = seed(true);
    let filter = Filter::parse(&json!({"city": "NOLA"})).unwrap();
    let upd = UpdateDoc::parse(&json!({"$set": {"city": "New Orleans"}})).unwrap();

    let ids = {
        let snap = e.snapshot_owned();
        query::find_ids(&snap, &cat, PREFIX, "people", &filter, 1000).unwrap()
    };
    assert_eq!(ids.len(), 2);
    for id in &ids {
        update::apply_to_id(&mut e, &cat, PREFIX, "people", id, &upd).unwrap();
    }

    let snap = e.snapshot_owned();
    let n = query::count(
        &snap,
        &cat,
        PREFIX,
        "people",
        &Filter::parse(&json!({"city":"New Orleans"})).unwrap(),
    )
    .unwrap();
    assert_eq!(n, 2);

    let mut cities =
        query::distinct(&snap, &cat, PREFIX, "people", "city", &Filter::MatchAll).unwrap();
    cities.sort_by_key(|v| v.as_str().unwrap().to_string());
    let cities: Vec<String> = cities
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    assert_eq!(cities, vec!["London", "New Orleans", "Paris"]);
}

#[test]
fn delete_many_removes_matches() {
    let (_d, mut e, cat) = seed(true);
    let filter = Filter::parse(&json!({"age": {"$lt": 35}})).unwrap();
    let ids = {
        let snap = e.snapshot_owned();
        query::find_ids(&snap, &cat, PREFIX, "people", &filter, 1000).unwrap()
    };
    for id in &ids {
        store::delete(&mut e, &cat, PREFIX, "people", id).unwrap();
    }
    let snap = e.snapshot_owned();
    let remaining = query::count(&snap, &cat, PREFIX, "people", &Filter::MatchAll).unwrap();
    assert_eq!(remaining, 2); // Cy(40), Di(35)
}
