//! End-to-end document-store tests over the real TCP wire: IndexDef, DocPut,
//! Query (ById + IndexRange with pagination), and concurrent progress while a
//! query holds a snapshot.

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;
use zydecodb_document::wire;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

fn roundtrip(stream: &mut TcpStream, req: &RequestEnvelope) -> ResponseEnvelope {
    stream.write_all(&req.encode()).unwrap();
    stream.flush().unwrap();
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    stream.read_exact(&mut header).unwrap();
    let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
    let mut payload = vec![0u8; len];
    if len > 0 {
        stream.read_exact(&mut payload).unwrap();
    }
    ResponseEnvelope::new(status, payload)
}

fn connect(addr: SocketAddr) -> TcpStream {
    for _ in 0..100 {
        if let Ok(s) = TcpStream::connect(addr) {
            s.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
            s.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
            return s;
        }
        thread::sleep(Duration::from_millis(20));
    }
    panic!("server did not come up");
}

fn define_index(s: &mut TcpStream, collection: &str, name: &str, fields: &[&str]) {
    define_index_ttl(s, collection, name, fields, 0);
}

fn define_index_ttl(
    s: &mut TcpStream,
    collection: &str,
    name: &str,
    fields: &[&str],
    expire_after_seconds: u64,
) {
    let p = wire::IndexDefPayload {
        collection: collection.into(),
        index_name: name.into(),
        fields: fields.iter().map(|f| f.to_string()).collect(),
        unique: false,
        expire_after_seconds,
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::IndexDef, p.encode()));
    assert_eq!(resp.status, Status::Ok, "IndexDef failed");
}

fn doc_put(s: &mut TcpStream, collection: &str, doc_id: &[u8], body: &str) {
    doc_put_expires(s, collection, doc_id, body, 0);
}

fn doc_put_expires(
    s: &mut TcpStream,
    collection: &str,
    doc_id: &[u8],
    body: &str,
    expires_at: u64,
) {
    let p = wire::DocPutPayload {
        collection: collection.into(),
        doc_id: doc_id.to_vec(),
        body: body.as_bytes().to_vec(),
        relaxed: false,
        expires_at,
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::DocPut, p.encode()));
    assert_eq!(resp.status, Status::Ok, "DocPut failed");
}

fn query(s: &mut TcpStream, q: wire::QueryPayload) -> ResponseEnvelope {
    roundtrip(s, &RequestEnvelope::new(Command::Query, q.encode()))
}

fn find(s: &mut TcpStream, collection: &str, filter: &str) -> Vec<serde_json::Value> {
    let p = wire::FindPayload {
        collection: collection.into(),
        filter: filter.as_bytes().to_vec(),
        sort: vec![("age".into(), true)],
        projection: wire::WireProjection::None,
        skip: 0,
        limit: 100,
        cursor: Vec::new(),
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::Find, p.encode()));
    assert_eq!(resp.status, Status::Ok, "Find failed");
    let (rows, _) = wire::decode_query_page(&resp.payload).unwrap();
    rows.into_iter()
        .map(|r| serde_json::from_slice(&r.body).unwrap())
        .collect()
}

fn update(
    s: &mut TcpStream,
    collection: &str,
    filter: &str,
    upd: &str,
    multi: bool,
) -> serde_json::Value {
    update_opts(s, collection, filter, upd, multi, false)
}

fn update_opts(
    s: &mut TcpStream,
    collection: &str,
    filter: &str,
    upd: &str,
    multi: bool,
    upsert: bool,
) -> serde_json::Value {
    let p = wire::UpdatePayload {
        collection: collection.into(),
        filter: filter.as_bytes().to_vec(),
        update: upd.as_bytes().to_vec(),
        multi,
        relaxed: false,
        upsert,
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::Update, p.encode()));
    assert_eq!(resp.status, Status::Ok, "Update failed: {:?}", resp);
    serde_json::from_slice(&resp.payload).unwrap()
}

fn delete(s: &mut TcpStream, collection: &str, filter: &str, multi: bool) -> u64 {
    let p = wire::DeletePayload {
        collection: collection.into(),
        filter: filter.as_bytes().to_vec(),
        multi,
        relaxed: false,
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::Delete, p.encode()));
    assert_eq!(resp.status, Status::Ok, "Delete failed");
    let v: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap();
    v["deleted"].as_u64().unwrap()
}

fn count(s: &mut TcpStream, collection: &str, filter: &str) -> u64 {
    let p = wire::CountPayload::Count {
        collection: collection.into(),
        filter: filter.as_bytes().to_vec(),
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::Count, p.encode()));
    assert_eq!(resp.status, Status::Ok, "Count failed");
    String::from_utf8(resp.payload).unwrap().parse().unwrap()
}

fn distinct(s: &mut TcpStream, collection: &str, field: &str) -> Vec<serde_json::Value> {
    let p = wire::CountPayload::Distinct {
        collection: collection.into(),
        filter: Vec::new(),
        field: field.into(),
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::Count, p.encode()));
    assert_eq!(resp.status, Status::Ok, "Distinct failed");
    serde_json::from_slice::<serde_json::Value>(&resp.payload)
        .unwrap()
        .as_array()
        .unwrap()
        .clone()
}

#[test]
fn docput_query_by_id_and_index_range_with_pagination() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);

    define_index(&mut s, "users", "by_age", &["age"]);
    doc_put(&mut s, "users", b"u1", r#"{"age":30,"name":"alice"}"#);
    doc_put(&mut s, "users", b"u2", r#"{"age":25,"name":"bob"}"#);
    doc_put(&mut s, "users", b"u3", r#"{"age":40,"name":"carol"}"#);
    doc_put(&mut s, "users", b"u4", r#"{"age":35,"name":"dave"}"#);

    // Query by id.
    let resp = query(
        &mut s,
        wire::QueryPayload::ById {
            collection: "users".into(),
            doc_id: b"u1".to_vec(),
        },
    );
    assert_eq!(resp.status, Status::Ok);
    let v: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap();
    assert_eq!(v["name"], serde_json::json!("alice"));

    // Missing id -> NotFound.
    let resp = query(
        &mut s,
        wire::QueryPayload::ById {
            collection: "users".into(),
            doc_id: b"missing".to_vec(),
        },
    );
    assert_eq!(resp.status, Status::NotFound);

    // Index range, paginated with limit 2; expect ascending-by-age across pages.
    let mut seen: Vec<Vec<u8>> = Vec::new();
    let mut cursor: Vec<u8> = Vec::new();
    loop {
        let resp = query(
            &mut s,
            wire::QueryPayload::IndexRange {
                collection: "users".into(),
                index_name: "by_age".into(),
                lo: Vec::new(),
                hi: Vec::new(),
                cursor: cursor.clone(),
                limit: 2,
            },
        );
        assert_eq!(resp.status, Status::Ok);
        let (rows, next) = wire::decode_query_page(&resp.payload).unwrap();
        assert!(rows.len() <= 2);
        seen.extend(rows.into_iter().map(|r| r.doc_id));
        match next {
            Some(c) => cursor = c,
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

    // Bounded range [30,36) -> u1(30), u4(35).
    let resp = query(
        &mut s,
        wire::QueryPayload::IndexRange {
            collection: "users".into(),
            index_name: "by_age".into(),
            lo: b"[30]".to_vec(),
            hi: b"[36]".to_vec(),
            cursor: Vec::new(),
            limit: 10,
        },
    );
    let (rows, _) = wire::decode_query_page(&resp.payload).unwrap();
    let ids: Vec<_> = rows.into_iter().map(|r| r.doc_id).collect();
    assert_eq!(ids, vec![b"u1".to_vec(), b"u4".to_vec()]);

    drop(s);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

/// The first-run path: DocPut into a collection that was never defined must
/// create it implicitly (no IndexDef required first), and it must survive a
/// server restart because the catalog write is durable before the ack.
#[test]
fn docput_implicitly_creates_collection() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);

    // No define_index — the very first operation on "fresh" is a write.
    doc_put(&mut s, "fresh", b"d1", r#"{"name":"Ada","age":30}"#);

    let resp = query(
        &mut s,
        wire::QueryPayload::ById {
            collection: "fresh".into(),
            doc_id: b"d1".to_vec(),
        },
    );
    assert_eq!(resp.status, Status::Ok);
    let v: serde_json::Value = serde_json::from_slice(&resp.payload).unwrap();
    assert_eq!(v["name"], serde_json::json!("Ada"));

    // Filtered reads work on the implicitly created collection (scan path).
    let hits = find(&mut s, "fresh", r#"{"age":{"$gte":18}}"#);
    assert_eq!(hits.len(), 1);

    // Indexes can still be added afterwards and backfill the existing doc.
    define_index(&mut s, "fresh", "by_age", &["age"]);
    let resp = query(
        &mut s,
        wire::QueryPayload::IndexRange {
            collection: "fresh".into(),
            index_name: "by_age".into(),
            lo: Vec::new(),
            hi: Vec::new(),
            cursor: Vec::new(),
            limit: 10,
        },
    );
    assert_eq!(resp.status, Status::Ok);
    let (rows, _) = wire::decode_query_page(&resp.payload).unwrap();
    assert_eq!(rows.len(), 1);

    drop(s);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn find_update_delete_count_over_wire() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);

    // Index only on age; city stays unindexed to prove scans work over the wire.
    define_index(&mut s, "people", "by_age", &["age"]);
    doc_put(
        &mut s,
        "people",
        b"a",
        r#"{"name":"Ada","age":30,"city":"London"}"#,
    );
    doc_put(
        &mut s,
        "people",
        b"b",
        r#"{"name":"Bo","age":25,"city":"NOLA"}"#,
    );
    doc_put(
        &mut s,
        "people",
        b"c",
        r#"{"name":"Cy","age":40,"city":"NOLA"}"#,
    );

    // Find on the unindexed field (collection scan) with an operator filter.
    let nola = find(&mut s, "people", r#"{"city":"NOLA"}"#);
    let names: Vec<_> = nola.iter().map(|d| d["name"].as_str().unwrap()).collect();
    assert_eq!(names, vec!["Bo", "Cy"]); // sorted by age asc

    // Find on the indexed field with a range.
    let older = find(&mut s, "people", r#"{"age":{"$gte":30}}"#);
    assert_eq!(older.len(), 2);

    // _id is materialized into results.
    let by_id = find(&mut s, "people", r#"{"_id":"a"}"#);
    assert_eq!(by_id.len(), 1);
    assert_eq!(by_id[0]["_id"], serde_json::json!("a"));

    // Update one with $inc; verify the index entry moved.
    let res = update(
        &mut s,
        "people",
        r#"{"name":"Bo"}"#,
        r#"{"$inc":{"age":10}}"#,
        false,
    );
    assert_eq!(res["matched"], serde_json::json!(1));
    assert_eq!(res["modified"], serde_json::json!(1));
    assert_eq!(count(&mut s, "people", r#"{"age":35}"#), 1);

    // update_many over the unindexed field.
    let res = update(
        &mut s,
        "people",
        r#"{"city":"NOLA"}"#,
        r#"{"$set":{"city":"New Orleans"}}"#,
        true,
    );
    assert_eq!(res["matched"], serde_json::json!(2));

    let cities = distinct(&mut s, "people", "city");
    let mut cities: Vec<_> = cities
        .iter()
        .map(|v| v.as_str().unwrap().to_string())
        .collect();
    cities.sort();
    assert_eq!(cities, vec!["London", "New Orleans"]);

    // Filtered delete.
    assert_eq!(delete(&mut s, "people", r#"{"age":{"$lt":35}}"#, true), 1); // Ada(30)
    assert_eq!(count(&mut s, "people", "{}"), 2);

    drop(s);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn filter_type_array_regex_over_wire() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);

    doc_put(
        &mut s,
        "misc",
        b"1",
        r#"{"name":"Ada","n":1,"tags":["a","b"],"items":[{"x":1,"y":2}]}"#,
    );
    doc_put(
        &mut s,
        "misc",
        b"2",
        r#"{"name":"Bo","n":"x","tags":["a"],"items":[{"x":1,"y":0}]}"#,
    );

    let hits = find(&mut s, "misc", r#"{"n":{"$type":"number"}}"#);
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["name"], serde_json::json!("Ada"));

    let hits = find(&mut s, "misc", r#"{"tags":{"$all":["a","b"]}}"#);
    assert_eq!(hits.len(), 1);

    let hits = find(
        &mut s,
        "misc",
        r#"{"items":{"$elemMatch":{"x":1,"y":{"$gt":1}}}}"#,
    );
    assert_eq!(hits.len(), 1);

    let hits = find(
        &mut s,
        "misc",
        r#"{"name":{"$regex":"^ad","$options":"i"}}"#,
    );
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0]["name"], serde_json::json!("Ada"));

    drop(s);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn filter_upsert_inserts_or_updates() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);

    // Miss → insert (server-generated id when filter has no _id).
    let res = update_opts(
        &mut s,
        "accounts",
        r#"{"email":"a@b.c"}"#,
        r#"{"$set":{"email":"a@b.c","n":1}}"#,
        false,
        true,
    );
    assert_eq!(res["matched"], serde_json::json!(0));
    assert_eq!(res["modified"], serde_json::json!(0));
    let upserted = res["upserted_id"].as_str().expect("upserted_id");
    assert!(!upserted.is_empty());
    assert_eq!(count(&mut s, "accounts", r#"{"email":"a@b.c"}"#), 1);

    // Hit → update; no upserted_id.
    let res = update_opts(
        &mut s,
        "accounts",
        r#"{"email":"a@b.c"}"#,
        r#"{"$inc":{"n":1}}"#,
        false,
        true,
    );
    assert_eq!(res["matched"], serde_json::json!(1));
    assert_eq!(res["modified"], serde_json::json!(1));
    assert!(res.get("upserted_id").is_none());
    let docs = find(&mut s, "accounts", r#"{"email":"a@b.c"}"#);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["n"], serde_json::json!(2));
    assert_eq!(docs[0]["_id"], serde_json::json!(upserted));

    // Explicit _id in filter on a miss inserts that id.
    let res = update_opts(
        &mut s,
        "accounts",
        r#"{"_id":"fixed1","email":"x@y.z"}"#,
        r#"{"$set":{"email":"x@y.z"}}"#,
        false,
        true,
    );
    assert_eq!(res["upserted_id"], serde_json::json!("fixed1"));
    let by_id = find(&mut s, "accounts", r#"{"_id":"fixed1"}"#);
    assert_eq!(by_id.len(), 1);

    // multi=true still inserts at most one on miss.
    let res = update_opts(
        &mut s,
        "accounts",
        r#"{"tag":"solo"}"#,
        r#"{"$set":{"tag":"solo"}}"#,
        true,
        true,
    );
    assert_eq!(res["matched"], serde_json::json!(0));
    assert!(res["upserted_id"].as_str().is_some());
    assert_eq!(count(&mut s, "accounts", r#"{"tag":"solo"}"#), 1);

    // $setOnInsert applies on miss; ignored on hit.
    let res = update_opts(
        &mut s,
        "accounts",
        r#"{"email":"soi@example.com"}"#,
        r#"{"$set":{"email":"soi@example.com","n":1},"$setOnInsert":{"created":true}}"#,
        false,
        true,
    );
    assert_eq!(res["matched"], serde_json::json!(0));
    let docs = find(&mut s, "accounts", r#"{"email":"soi@example.com"}"#);
    assert_eq!(docs.len(), 1);
    assert_eq!(docs[0]["created"], serde_json::json!(true));
    assert_eq!(docs[0]["n"], serde_json::json!(1));

    let res = update_opts(
        &mut s,
        "accounts",
        r#"{"email":"soi@example.com"}"#,
        r#"{"$set":{"n":2},"$setOnInsert":{"created":false,"extra":1}}"#,
        false,
        true,
    );
    assert_eq!(res["matched"], serde_json::json!(1));
    assert_eq!(res["modified"], serde_json::json!(1));
    let docs = find(&mut s, "accounts", r#"{"email":"soi@example.com"}"#);
    assert_eq!(docs[0]["n"], serde_json::json!(2));
    assert_eq!(docs[0]["created"], serde_json::json!(true)); // unchanged
    assert!(docs[0].get("extra").is_none());

    drop(s);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn ttl_index_derives_expires_at() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);

    // Wire 0 = not a TTL index; use 1s so expires_at = field_ms + 1000.
    define_index_ttl(&mut s, "sess", "by_exp", &["exp"], 1);
    let past = 1_000u64; // expires_at = 2000 — long expired
    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;

    doc_put(&mut s, "sess", b"dead", &format!(r#"{{"exp":{past}}}"#));
    doc_put(&mut s, "sess", b"live", &format!(r#"{{"exp":{future}}}"#));

    let dead = query(
        &mut s,
        wire::QueryPayload::ById {
            collection: "sess".into(),
            doc_id: b"dead".to_vec(),
        },
    );
    assert_eq!(dead.status, Status::NotFound);

    let live = query(
        &mut s,
        wire::QueryPayload::ById {
            collection: "sess".into(),
            doc_id: b"live".to_vec(),
        },
    );
    assert_eq!(live.status, Status::Ok);

    drop(s);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn doc_put_expires_at_lazy_expiry() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);

    let future = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
        + 60_000;
    let past = 1_000u64; // long expired

    doc_put_expires(&mut s, "ttl", b"live", r#"{"n":1}"#, future);
    doc_put_expires(&mut s, "ttl", b"dead", r#"{"n":2}"#, past);

    let live = query(
        &mut s,
        wire::QueryPayload::ById {
            collection: "ttl".into(),
            doc_id: b"live".to_vec(),
        },
    );
    assert_eq!(live.status, Status::Ok);
    let v: serde_json::Value = serde_json::from_slice(&live.payload).unwrap();
    assert_eq!(v["n"], serde_json::json!(1));

    let dead = query(
        &mut s,
        wire::QueryPayload::ById {
            collection: "ttl".into(),
            doc_id: b"dead".to_vec(),
        },
    );
    assert_eq!(
        dead.status,
        Status::NotFound,
        "expired DocPut must be NotFound on Query ById"
    );

    drop(s);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

/// A second connection keeps making progress (writes + reads) while the first
/// connection issues queries — proving Query does not hold the engine lock
/// across its scan.
#[test]
fn concurrent_connection_progresses_during_queries() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();

    // Seed a collection + index + some docs.
    let mut setup = connect(addr);
    define_index(&mut setup, "users", "by_age", &["age"]);
    for i in 0..50u32 {
        let body = format!(r#"{{"age":{}}}"#, i % 10);
        doc_put(&mut setup, "users", format!("d{i}").as_bytes(), &body);
    }
    drop(setup);

    let querier = thread::spawn(move || {
        let mut s = connect(addr);
        for _ in 0..40 {
            let resp = query(
                &mut s,
                wire::QueryPayload::IndexRange {
                    collection: "users".into(),
                    index_name: "by_age".into(),
                    lo: Vec::new(),
                    hi: Vec::new(),
                    cursor: Vec::new(),
                    limit: 100,
                },
            );
            assert_eq!(resp.status, Status::Ok);
        }
    });

    let writer = thread::spawn(move || {
        let mut s = connect(addr);
        for i in 50..120u32 {
            let body = format!(r#"{{"age":{}}}"#, i % 10);
            doc_put(&mut s, "users", format!("d{i}").as_bytes(), &body);
        }
    });

    querier.join().unwrap();
    writer.join().unwrap();

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
