//! Conformance-vector generator (the wire protocol's single source of truth).
//!
//! Emits `clients/conformance/vectors.json` from the *real* server encoders in
//! [`zydecodb_document::wire`] and [`zydecodb_engine::frame`]. Every client
//! (Python, Go, TypeScript, ...) runs its codec against these vectors so the N
//! implementations can never silently drift from the server's bytes.
//!
//! Run from anywhere in the workspace:
//! ```bash
//! cargo run -p zydecodb-document --bin gen_conformance
//! ```
//!
//! JSON-body fields (document/filter/update/bounds) are carried as opaque
//! pre-serialized byte strings (`*_json`), because the conformance contract is
//! about *framing bytes*, not about any one language's JSON serializer. A
//! client's codec must accept those bytes verbatim.

use std::fmt::Write as _;
use std::path::PathBuf;

use serde_json::{json, Value};
use zydecodb_document::query::{QueryPage, QueryRow};
use zydecodb_document::wire::{
    self, CountPayload, DeletePayload, DocDelPayload, DocPutPayload, FindPayload, IndexDefPayload,
    QueryPayload, UpdatePayload, WireProjection,
};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, KeyPayload, PutPayload, RequestEnvelope, PROTO_VERSION};

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        let _ = write!(s, "{b:02x}");
    }
    s
}

/// Build one request vector: the payload bytes plus the full framed envelope.
fn req(name: &str, kind: &str, input: Value, command: Command, payload: Vec<u8>) -> Value {
    let envelope = RequestEnvelope::new(command, payload.clone()).encode();
    json!({
        "name": name,
        "kind": kind,
        "command": command.as_u8(),
        "input": input,
        "payload_hex": hex(&payload),
        "envelope_hex": hex(&envelope),
    })
}

fn payload_vectors() -> Vec<Value> {
    let mut v = Vec::new();

    // ---- Put ----
    let p = PutPayload {
        routing_key: [0; 16],
        txid: 0,
        expires_at: 0,
        key: b"k1".to_vec(),
        value: b"v1".to_vec(),
    };
    v.push(req(
        "put_basic",
        "Put",
        json!({"key_hex": hex(b"k1"), "value_hex": hex(b"v1"), "expires_at": 0}),
        Command::Put,
        p.encode(),
    ));

    let p = PutPayload {
        routing_key: [0; 16],
        txid: 0,
        expires_at: 1700000000000,
        key: b"k2".to_vec(),
        value: b"v2".to_vec(),
    };
    v.push(req(
        "put_ttl",
        "Put",
        json!({"key_hex": hex(b"k2"), "value_hex": hex(b"v2"), "expires_at": 1700000000000u64}),
        Command::Put,
        p.encode(),
    ));

    // ---- Get ----
    let p = KeyPayload {
        routing_key: [0; 16],
        snapshot_seq: 0,
        key: b"k1".to_vec(),
    };
    v.push(req(
        "get_basic",
        "Get",
        json!({"key_hex": hex(b"k1")}),
        Command::Get,
        p.encode(),
    ));

    // ---- Del ----
    let p = KeyPayload {
        routing_key: [0; 16],
        snapshot_seq: 0,
        key: b"k1".to_vec(),
    };
    v.push(req(
        "del_basic",
        "Del",
        json!({"key_hex": hex(b"k1")}),
        Command::Del,
        p.encode(),
    ));

    // ---- DocPut ----
    let p = DocPutPayload {
        collection: "users".into(),
        doc_id: b"u1".to_vec(),
        body: br#"{"age":30}"#.to_vec(),
        relaxed: false,
        expires_at: 0,
    };
    v.push(req(
        "doc_put_basic",
        "DocPut",
        json!({"collection":"users","doc_id":"u1","body_json":"{\"age\":30}","relaxed":false}),
        Command::DocPut,
        p.encode(),
    ));
    let p = DocPutPayload { relaxed: true, ..p };
    v.push(req(
        "doc_put_relaxed",
        "DocPut",
        json!({"collection":"users","doc_id":"u1","body_json":"{\"age\":30}","relaxed":true}),
        Command::DocPut,
        p.encode(),
    ));

    // ---- DocDel ----
    let p = DocDelPayload {
        collection: "users".into(),
        doc_id: b"u1".to_vec(),
    };
    v.push(req(
        "doc_del_basic",
        "DocDel",
        json!({"collection":"users","doc_id":"u1"}),
        Command::DocDel,
        p.encode(),
    ));

    // ---- IndexDef ----
    let p = IndexDefPayload {
        collection: "users".into(),
        index_name: "by_age".into(),
        fields: vec!["age".into()],
        unique: false,
    };
    v.push(req(
        "index_def_single",
        "IndexDef",
        json!({"collection":"users","index_name":"by_age","fields":["age"],"unique":false}),
        Command::IndexDef,
        p.encode(),
    ));
    let p = IndexDefPayload {
        collection: "users".into(),
        index_name: "by_email".into(),
        fields: vec!["email".into(), "name".into()],
        unique: true,
    };
    v.push(req(
        "index_def_unique_multi",
        "IndexDef",
        json!({"collection":"users","index_name":"by_email","fields":["email","name"],"unique":true}),
        Command::IndexDef,
        p.encode(),
    ));

    // ---- Query (ById / IndexRange) ----
    let p = QueryPayload::ById {
        collection: "users".into(),
        doc_id: b"u1".to_vec(),
    };
    v.push(req(
        "query_by_id",
        "QueryById",
        json!({"collection":"users","doc_id":"u1"}),
        Command::Query,
        p.encode(),
    ));
    let p = QueryPayload::IndexRange {
        collection: "users".into(),
        index_name: "by_age".into(),
        lo: b"[18]".to_vec(),
        hi: b"[65]".to_vec(),
        cursor: vec![],
        limit: 50,
    };
    v.push(req(
        "query_index_range_bounded",
        "QueryIndexRange",
        json!({"collection":"users","index_name":"by_age","lo_json":"[18]","hi_json":"[65]","cursor_hex":"","limit":50}),
        Command::Query,
        p.encode(),
    ));
    let p = QueryPayload::IndexRange {
        collection: "users".into(),
        index_name: "by_age".into(),
        lo: vec![],
        hi: vec![],
        cursor: vec![0xab, 0xcd],
        limit: 100,
    };
    v.push(req(
        "query_index_range_unbounded_with_cursor",
        "QueryIndexRange",
        json!({"collection":"users","index_name":"by_age","lo_json":"","hi_json":"","cursor_hex":"abcd","limit":100}),
        Command::Query,
        p.encode(),
    ));

    // ---- Find ----
    let p = FindPayload {
        collection: "users".into(),
        filter: br#"{"age":{"$gte":18}}"#.to_vec(),
        sort: vec![("age".into(), true), ("name".into(), false)],
        projection: WireProjection::Include(vec!["name".into(), "age".into()]),
        skip: 5,
        limit: 50,
        cursor: vec![1, 2, 3],
    };
    v.push(req(
        "find_full",
        "Find",
        json!({
            "collection":"users",
            "filter_json":"{\"age\":{\"$gte\":18}}",
            "sort":[["age",true],["name",false]],
            "projection":{"mode":"include","fields":["name","age"]},
            "skip":5,"limit":50,"cursor_hex":"010203"
        }),
        Command::Find,
        p.encode(),
    ));
    let p = FindPayload {
        collection: "c".into(),
        filter: vec![],
        sort: vec![],
        projection: WireProjection::None,
        skip: 0,
        limit: 1,
        cursor: vec![],
    };
    v.push(req(
        "find_minimal",
        "Find",
        json!({
            "collection":"c","filter_json":"","sort":[],
            "projection":{"mode":"none","fields":[]},
            "skip":0,"limit":1,"cursor_hex":""
        }),
        Command::Find,
        p.encode(),
    ));
    let p = FindPayload {
        collection: "users".into(),
        filter: vec![],
        sort: vec![],
        projection: WireProjection::Exclude(vec!["secret".into()]),
        skip: 0,
        limit: 100,
        cursor: vec![],
    };
    v.push(req(
        "find_exclude_projection",
        "Find",
        json!({
            "collection":"users","filter_json":"","sort":[],
            "projection":{"mode":"exclude","fields":["secret"]},
            "skip":0,"limit":100,"cursor_hex":""
        }),
        Command::Find,
        p.encode(),
    ));

    // ---- Update ----
    let p = UpdatePayload {
        collection: "users".into(),
        filter: br#"{"_id":"u1"}"#.to_vec(),
        update: br#"{"$set":{"name":"x"}}"#.to_vec(),
        multi: true,
        relaxed: true,
        upsert: false,
    };
    v.push(req(
        "update_multi_relaxed",
        "Update",
        json!({
            "collection":"users","filter_json":"{\"_id\":\"u1\"}",
            "update_json":"{\"$set\":{\"name\":\"x\"}}","multi":true,"relaxed":true,"upsert":false
        }),
        Command::Update,
        p.encode(),
    ));
    let p = UpdatePayload {
        collection: "users".into(),
        filter: br#"{"age":{"$lt":0}}"#.to_vec(),
        update: br#"{"$inc":{"n":1}}"#.to_vec(),
        multi: false,
        relaxed: false,
        upsert: false,
    };
    v.push(req(
        "update_one_durable",
        "Update",
        json!({
            "collection":"users","filter_json":"{\"age\":{\"$lt\":0}}",
            "update_json":"{\"$inc\":{\"n\":1}}","multi":false,"relaxed":false,"upsert":false
        }),
        Command::Update,
        p.encode(),
    ));
    let p = UpdatePayload {
        collection: "users".into(),
        filter: br#"{"email":"a@b.c"}"#.to_vec(),
        update: br#"{"$set":{"email":"a@b.c","n":1}}"#.to_vec(),
        multi: false,
        relaxed: false,
        upsert: true,
    };
    v.push(req(
        "update_upsert",
        "Update",
        json!({
            "collection":"users","filter_json":"{\"email\":\"a@b.c\"}",
            "update_json":"{\"$set\":{\"email\":\"a@b.c\",\"n\":1}}",
            "multi":false,"relaxed":false,"upsert":true
        }),
        Command::Update,
        p.encode(),
    ));

    // ---- Delete ----
    let p = DeletePayload {
        collection: "users".into(),
        filter: br#"{"stale":true}"#.to_vec(),
        multi: true,
        relaxed: false,
    };
    v.push(req(
        "delete_multi_durable",
        "Delete",
        json!({"collection":"users","filter_json":"{\"stale\":true}","multi":true,"relaxed":false}),
        Command::Delete,
        p.encode(),
    ));
    let p = DeletePayload {
        collection: "users".into(),
        filter: br#"{"_id":"u1"}"#.to_vec(),
        multi: false,
        relaxed: true,
    };
    v.push(req(
        "delete_one_relaxed",
        "Delete",
        json!({"collection":"users","filter_json":"{\"_id\":\"u1\"}","multi":false,"relaxed":true}),
        Command::Delete,
        p.encode(),
    ));

    // ---- Count / Distinct ----
    let p = CountPayload::Count {
        collection: "users".into(),
        filter: br#"{"active":true}"#.to_vec(),
    };
    v.push(req(
        "count_with_filter",
        "Count",
        json!({"collection":"users","filter_json":"{\"active\":true}"}),
        Command::Count,
        p.encode(),
    ));
    let p = CountPayload::Distinct {
        collection: "users".into(),
        filter: vec![],
        field: "city".into(),
    };
    v.push(req(
        "distinct_no_filter",
        "Distinct",
        json!({"collection":"users","filter_json":"","field":"city"}),
        Command::Count,
        p.encode(),
    ));

    // ---- SessionInit / Ping (raw payloads on the envelope) ----
    v.push(req(
        "session_init",
        "SessionInit",
        json!({"api_key":"zdk_example"}),
        Command::SessionInit,
        b"zdk_example".to_vec(),
    ));
    v.push(req("ping", "Ping", json!({}), Command::Ping, Vec::new()));

    v
}

/// Decode vectors: server-produced response pages a client must parse.
fn response_vectors() -> Vec<Value> {
    let mut v = Vec::new();

    let page = QueryPage {
        rows: vec![
            QueryRow {
                doc_id: b"u1".to_vec(),
                body: Some(br#"{"_id":"u1"}"#.to_vec()),
            },
            QueryRow {
                doc_id: b"u2".to_vec(),
                body: Some(br#"{"_id":"u2"}"#.to_vec()),
            },
        ],
        next_cursor: Some(b"next-page".to_vec()),
    };
    v.push(json!({
        "name": "query_page_two_rows",
        "kind": "QueryPage",
        "bytes_hex": hex(&wire::encode_query_page(&page)),
        "decoded": {
            "rows": [
                {"doc_id":"u1","body_json":"{\"_id\":\"u1\"}"},
                {"doc_id":"u2","body_json":"{\"_id\":\"u2\"}"}
            ],
            "next_cursor_hex": hex(b"next-page")
        }
    }));

    let page = QueryPage {
        rows: vec![],
        next_cursor: None,
    };
    v.push(json!({
        "name": "query_page_empty",
        "kind": "QueryPage",
        "bytes_hex": hex(&wire::encode_query_page(&page)),
        "decoded": {"rows": [], "next_cursor_hex": null}
    }));

    let page = QueryPage {
        rows: vec![QueryRow {
            doc_id: b"u3".to_vec(),
            body: None,
        }],
        next_cursor: None,
    };
    v.push(json!({
        "name": "query_page_row_without_body",
        "kind": "QueryPage",
        "bytes_hex": hex(&wire::encode_query_page(&page)),
        "decoded": {"rows": [{"doc_id":"u3","body_json":""}], "next_cursor_hex": null}
    }));

    v
}

fn commands_map() -> Value {
    // Implemented 0.9 opcodes (frozen). Reserved Begin/Commit/Rollback/SchemaDef
    // are intentionally omitted from this map.
    json!({
        "Put": Command::Put.as_u8(),
        "Get": Command::Get.as_u8(),
        "Del": Command::Del.as_u8(),
        "Query": Command::Query.as_u8(),
        "DocPut": Command::DocPut.as_u8(),
        "DocDel": Command::DocDel.as_u8(),
        "Find": Command::Find.as_u8(),
        "Update": Command::Update.as_u8(),
        "Delete": Command::Delete.as_u8(),
        "Count": Command::Count.as_u8(),
        "IndexDef": Command::IndexDef.as_u8(),
        "SessionInit": Command::SessionInit.as_u8(),
        "SetContext": Command::SetContext.as_u8(),
        "AdminDropTenant": Command::AdminDropTenant.as_u8(),
        "Ping": Command::Ping.as_u8(),
        "Stats": Command::Stats.as_u8(),
    })
}

fn statuses_map() -> Value {
    // Append-only wire statuses (see zydecodb_engine::errors).
    json!({
        "Ok": Status::Ok as u8,
        "NotFound": Status::NotFound as u8,
        "Error": Status::Error as u8,
        "Conflict": Status::Conflict as u8,
        "IoError": Status::IoError as u8,
        "InvalidKey": Status::InvalidKey as u8,
        "InvalidValue": Status::InvalidValue as u8,
        "EngineBusy": Status::EngineBusy as u8,
        "ProtocolError": Status::ProtocolError as u8,
        "PolicyRejected": Status::PolicyRejected as u8,
        "UnsupportedFormat": Status::UnsupportedFormat as u8,
        "Unauthorized": Status::Unauthorized as u8,
        "Forbidden": Status::Forbidden as u8,
    })
}

fn main() {
    let doc = json!({
        "note": "GENERATED by `cargo run -p zydecodb-document --bin gen_conformance`. Do not edit by hand. The authority is the Rust encoders in zydecodb-document/src/wire.rs and zydecodb-engine/src/frame.rs.",
        "proto_version": PROTO_VERSION,
        "envelope_header_len": zydecodb_engine::frame::ENVELOPE_HEADER_LEN,
        "commands": commands_map(),
        "statuses": statuses_map(),
        "requests": payload_vectors(),
        "responses": response_vectors(),
    });

    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let out = manifest_dir
        .join("..")
        .join("..")
        .join("clients")
        .join("conformance")
        .join("vectors.json");
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).expect("create conformance dir");
    }
    let mut text = serde_json::to_string_pretty(&doc).expect("serialize vectors");
    text.push('\n');
    std::fs::write(&out, text).expect("write vectors.json");
    eprintln!("wrote {}", out.display());
}
