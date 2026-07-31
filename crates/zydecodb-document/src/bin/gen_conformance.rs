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
    self, AggregatePayload, CountPayload, DeletePayload, DocDelPayload, DocPutIfMatchPayload,
    DocPutPayload, DocUpdateIfMatchPayload, FindPayload, IndexDefPayload, QueryPayload,
    UpdatePayload, WatchPayload, WireProjection, WATCH_OP_DELETE, WATCH_OP_UPSERT,
};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope, PROTO_VERSION,
};

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
    let p = DocPutPayload {
        collection: "users".into(),
        doc_id: b"u2".to_vec(),
        body: br#"{"age":40}"#.to_vec(),
        relaxed: false,
        expires_at: 1700000000000,
    };
    v.push(req(
        "doc_put_expires",
        "DocPut",
        json!({"collection":"users","doc_id":"u2","body_json":"{\"age\":40}","relaxed":false,"expires_at":1700000000000u64}),
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
        expire_after_seconds: 0,
        directions: vec![true],
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
        expire_after_seconds: 0,
        directions: vec![true, true],
    };
    v.push(req(
        "index_def_unique_multi",
        "IndexDef",
        json!({"collection":"users","index_name":"by_email","fields":["email","name"],"unique":true}),
        Command::IndexDef,
        p.encode(),
    ));
    let p = IndexDefPayload {
        collection: "sess".into(),
        index_name: "by_exp".into(),
        fields: vec!["exp".into()],
        unique: false,
        expire_after_seconds: 3600,
        directions: vec![true],
    };
    v.push(req(
        "index_def_ttl",
        "IndexDef",
        json!({"collection":"sess","index_name":"by_exp","fields":["exp"],"unique":false,"expire_after_seconds":3600}),
        Command::IndexDef,
        p.encode(),
    ));
    let p = IndexDefPayload {
        collection: "events".into(),
        index_name: "by_owner_ts".into(),
        fields: vec!["ownerId".into(), "updatedAt".into()],
        unique: false,
        expire_after_seconds: 0,
        directions: vec![true, false],
    };
    v.push(req(
        "index_def_directional",
        "IndexDef",
        json!({"collection":"events","index_name":"by_owner_ts","fields":["ownerId","updatedAt"],"unique":false,"directions":[true,false]}),
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
        include_bodies: true,
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
        include_bodies: true,
    };
    v.push(req(
        "query_index_range_unbounded_with_cursor",
        "QueryIndexRange",
        json!({"collection":"users","index_name":"by_age","lo_json":"","hi_json":"","cursor_hex":"abcd","limit":100,"include_bodies":true}),
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
        include_bodies: false,
    };
    v.push(req(
        "query_index_range_ids_only",
        "QueryIndexRange",
        json!({"collection":"users","index_name":"by_age","lo_json":"[18]","hi_json":"[65]","cursor_hex":"","limit":50,"include_bodies":false}),
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

    // ---- DocGetRev ----
    let p = QueryPayload::ById {
        collection: "users".into(),
        doc_id: b"u1".to_vec(),
    };
    v.push(req(
        "doc_get_rev",
        "DocGetRev",
        json!({"collection":"users","doc_id":"u1"}),
        Command::DocGetRev,
        p.encode(),
    ));

    // ---- FindRev (same Find payload, different opcode) ----
    let p = FindPayload {
        collection: "users".into(),
        filter: br#"{"age":{"$gte":18}}"#.to_vec(),
        sort: vec![("age".into(), true)],
        projection: WireProjection::None,
        skip: 0,
        limit: 50,
        cursor: vec![],
    };
    v.push(req(
        "find_rev_basic",
        "FindRev",
        json!({
            "collection":"users","filter_json":"{\"age\":{\"$gte\":18}}",
            "sort":[["age",true]],
            "projection":{"mode":"none","fields":[]},
            "skip":0,"limit":50,"cursor_hex":""
        }),
        Command::FindRev,
        p.encode(),
    ));

    // ---- DocPutIfMatch ----
    let p = DocPutIfMatchPayload {
        collection: "users".into(),
        doc_id: b"u1".to_vec(),
        body: br#"{"age":31}"#.to_vec(),
        relaxed: false,
        if_match: 7,
        expires_at: 0,
    };
    v.push(req(
        "doc_put_if_match",
        "DocPutIfMatch",
        json!({
            "collection":"users","doc_id":"u1","body_json":"{\"age\":31}",
            "relaxed":false,"if_match":7
        }),
        Command::DocPutIfMatch,
        p.encode(),
    ));

    // ---- DocUpdateIfMatch ----
    let p = DocUpdateIfMatchPayload {
        collection: "users".into(),
        doc_id: b"u1".to_vec(),
        update: br#"{"$inc":{"age":1}}"#.to_vec(),
        relaxed: false,
        if_match: 7,
    };
    v.push(req(
        "doc_update_if_match",
        "DocUpdateIfMatch",
        json!({
            "collection":"users","doc_id":"u1",
            "update_json":"{\"$inc\":{\"age\":1}}","relaxed":false,"if_match":7
        }),
        Command::DocUpdateIfMatch,
        p.encode(),
    ));

    // ---- Watch ----
    let p = WatchPayload {
        collection: "users".into(),
        resume_token: vec![],
    };
    v.push(req(
        "watch_empty_resume",
        "Watch",
        json!({"collection":"users","resume_token_hex":""}),
        Command::Watch,
        p.encode(),
    ));
    let p = WatchPayload {
        collection: "events".into(),
        resume_token: vec![0xab, 0xcd, 0xef],
    };
    v.push(req(
        "watch_with_resume",
        "Watch",
        json!({"collection":"events","resume_token_hex":"abcdef"}),
        Command::Watch,
        p.encode(),
    ));

    // ---- Aggregate ----
    let pipeline = br#"[{"$match":{"active":true}},{"$group":{"_id":"$team","n":{"$count":{}}}}]"#;
    let p = AggregatePayload {
        collection: "sales".into(),
        pipeline: pipeline.to_vec(),
    };
    v.push(req(
        "aggregate_match_group",
        "Aggregate",
        json!({
            "collection":"sales",
            "pipeline_json":"[{\"$match\":{\"active\":true}},{\"$group\":{\"_id\":\"$team\",\"n\":{\"$count\":{}}}}]"
        }),
        Command::Aggregate,
        p.encode(),
    ));

    // ---- Begin / Commit / Rollback (empty payloads) ----
    v.push(req(
        "begin_empty",
        "Begin",
        json!({}),
        Command::Begin,
        vec![],
    ));
    v.push(req(
        "commit_empty",
        "Commit",
        json!({}),
        Command::Commit,
        vec![],
    ));
    v.push(req(
        "rollback_empty",
        "Rollback",
        json!({}),
        Command::Rollback,
        vec![],
    ));

    // ---- SessionInit / Ping / Stats / SetContext / AdminDropTenant / SchemaDef ----
    v.push(req(
        "session_init",
        "SessionInit",
        json!({"api_key":"zdk_example"}),
        Command::SessionInit,
        b"zdk_example".to_vec(),
    ));
    v.push(req("ping", "Ping", json!({}), Command::Ping, Vec::new()));
    v.push(req(
        "stats_empty",
        "Stats",
        json!({}),
        Command::Stats,
        Vec::new(),
    ));
    let tenant = [0x11u8; 16];
    v.push(req(
        "set_context",
        "SetContext",
        json!({"tenant_hex": hex(&tenant)}),
        Command::SetContext,
        tenant.to_vec(),
    ));
    let mut drop_payload = tenant.to_vec();
    drop_payload.push(1); // compact = true
    v.push(req(
        "admin_drop_tenant",
        "AdminDropTenant",
        json!({"tenant_hex": hex(&tenant), "compact": true}),
        Command::AdminDropTenant,
        drop_payload,
    ));
    v.push(req(
        "schema_def_reserved",
        "SchemaDef",
        json!({}),
        Command::SchemaDef,
        Vec::new(),
    ));

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
                revision: None,
            },
            QueryRow {
                doc_id: b"u2".to_vec(),
                body: Some(br#"{"_id":"u2"}"#.to_vec()),
                revision: None,
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
            revision: None,
        }],
        next_cursor: None,
    };
    v.push(json!({
        "name": "query_page_row_without_body",
        "kind": "QueryPage",
        "bytes_hex": hex(&wire::encode_query_page(&page)),
        "decoded": {"rows": [{"doc_id":"u3","body_json":""}], "next_cursor_hex": null}
    }));

    let page = QueryPage {
        rows: vec![QueryRow {
            doc_id: b"u1".to_vec(),
            body: Some(br#"{"_id":"u1"}"#.to_vec()),
            revision: Some(42),
        }],
        next_cursor: None,
    };
    v.push(json!({
        "name": "query_page_with_revision",
        "kind": "QueryPageRev",
        "bytes_hex": hex(&wire::encode_query_page_with_revision(&page)),
        "decoded": {
            "rows": [{"doc_id":"u1","body_json":"{\"_id\":\"u1\"}","revision":42}],
            "next_cursor_hex": null
        }
    }));

    v.push(json!({
        "name": "doc_get_rev_response",
        "kind": "DocGetRevResponse",
        "bytes_hex": hex(&wire::encode_doc_get_rev_response(br#"{"age":30}"#, 7)),
        "decoded": {"body_json":"{\"age\":30}","revision":7}
    }));

    v.push(json!({
        "name": "begin_response",
        "kind": "BeginResponse",
        "bytes_hex": hex(&wire::encode_begin_response(1, 42)),
        "decoded": {"tx_id": 1, "snapshot_seq": 42}
    }));

    v.push(json!({
        "name": "commit_response",
        "kind": "CommitResponse",
        "bytes_hex": hex(&wire::encode_commit_response(99)),
        "decoded": {"seq": 99}
    }));

    v.push(json!({
        "name": "stage_ack",
        "kind": "StageAck",
        "bytes_hex": hex(&wire::encode_stage_ack(3, 12)),
        "decoded": {"logical_ops": 3, "estimated_keys": 12}
    }));

    let agg_rows = vec![
        serde_json::json!({"_id":"a","n":1}),
        serde_json::json!({"_id":"b","n":2}),
    ];
    v.push(json!({
        "name": "aggregate_two_rows",
        "kind": "AggregateResponse",
        "bytes_hex": hex(&wire::encode_aggregate_response(&agg_rows, 4 * 1024 * 1024).unwrap()),
        "decoded": {
            "rows_json": [
                "{\"_id\":\"a\",\"n\":1}",
                "{\"_id\":\"b\",\"n\":2}"
            ]
        }
    }));

    let resume = b"\x01\x02\x03\x04".as_slice();
    v.push(json!({
        "name": "watch_frame_ack",
        "kind": "WatchFrameAck",
        "bytes_hex": hex(&wire::encode_watch_ack(resume)),
        "decoded": {"resume_token_hex": hex(resume)}
    }));
    v.push(json!({
        "name": "watch_frame_event_upsert",
        "kind": "WatchFrameEvent",
        "bytes_hex": hex(&wire::encode_watch_event(
            resume,
            WATCH_OP_UPSERT,
            b"u1",
            br#"{"_id":"u1","age":30}"#,
        )),
        "decoded": {
            "resume_token_hex": hex(resume),
            "op": "upsert",
            "doc_id": "u1",
            "body_json": "{\"_id\":\"u1\",\"age\":30}"
        }
    }));
    v.push(json!({
        "name": "watch_frame_event_delete",
        "kind": "WatchFrameEvent",
        "bytes_hex": hex(&wire::encode_watch_event(
            resume,
            WATCH_OP_DELETE,
            b"u2",
            &[],
        )),
        "decoded": {
            "resume_token_hex": hex(resume),
            "op": "delete",
            "doc_id": "u2",
            "body_json": ""
        }
    }));
    v.push(json!({
        "name": "watch_frame_heartbeat",
        "kind": "WatchFrameHeartbeat",
        "bytes_hex": hex(&wire::encode_watch_heartbeat(resume)),
        "decoded": {"resume_token_hex": hex(resume)}
    }));

    // Full response envelopes for every status byte (wire-freeze gate).
    for (name, status, detail) in [
        ("status_not_found", Status::NotFound, ""),
        ("status_error", Status::Error, "internal failure"),
        ("status_conflict", Status::Conflict, "revision mismatch"),
        ("status_io_error", Status::IoError, "disk write failed"),
        ("status_invalid_key", Status::InvalidKey, "key too long"),
        ("status_invalid_value", Status::InvalidValue, "bad document"),
        (
            "status_engine_busy",
            Status::EngineBusy,
            "rate limit exceeded",
        ),
        (
            "status_protocol_error",
            Status::ProtocolError,
            "unknown command 0x99",
        ),
        (
            "status_policy_rejected",
            Status::PolicyRejected,
            "quota exceeded",
        ),
        (
            "status_unsupported_format",
            Status::UnsupportedFormat,
            "unknown sstable version",
        ),
        (
            "status_unauthorized",
            Status::Unauthorized,
            "invalid api key",
        ),
        ("status_forbidden", Status::Forbidden, "read-only key"),
    ] {
        let env = if detail.is_empty() {
            ResponseEnvelope::new(status, Vec::new())
        } else {
            ResponseEnvelope::error(status, detail)
        };
        v.push(json!({
            "name": name,
            "kind": "StatusResponse",
            "bytes_hex": hex(&env.encode()),
            "decoded": {
                "status": status.as_u8(),
                "status_name": format!("{status:?}").replace("Status::", ""),
                "detail": detail,
            }
        }));
    }

    v
}

fn commands_map() -> Value {
    // Wire v1 opcodes (frozen for 1.x). SchemaDef is reserved but listed.
    json!({
        "Put": Command::Put.as_u8(),
        "Get": Command::Get.as_u8(),
        "Del": Command::Del.as_u8(),
        "Begin": Command::Begin.as_u8(),
        "Commit": Command::Commit.as_u8(),
        "Rollback": Command::Rollback.as_u8(),
        "Query": Command::Query.as_u8(),
        "DocPut": Command::DocPut.as_u8(),
        "DocDel": Command::DocDel.as_u8(),
        "Find": Command::Find.as_u8(),
        "Update": Command::Update.as_u8(),
        "Delete": Command::Delete.as_u8(),
        "Count": Command::Count.as_u8(),
        "DocGetRev": Command::DocGetRev.as_u8(),
        "FindRev": Command::FindRev.as_u8(),
        "DocPutIfMatch": Command::DocPutIfMatch.as_u8(),
        "DocUpdateIfMatch": Command::DocUpdateIfMatch.as_u8(),
        "Aggregate": Command::Aggregate.as_u8(),
        "Watch": Command::Watch.as_u8(),
        "IndexDef": Command::IndexDef.as_u8(),
        "SchemaDef": Command::SchemaDef.as_u8(),
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
