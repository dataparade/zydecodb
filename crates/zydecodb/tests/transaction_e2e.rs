//! End-to-end bounded transaction tests over the real TCP wire.

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::thread;
use std::time::Duration;
use zydecodb_document::wire;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{
    Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN,
};

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

fn doc_put(s: &mut TcpStream, collection: &str, doc_id: &[u8], body: &str) {
    let p = wire::DocPutPayload {
        collection: collection.into(),
        doc_id: doc_id.to_vec(),
        body: body.as_bytes().to_vec(),
        relaxed: false,
        expires_at: 0,
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::DocPut, p.encode()));
    assert_eq!(resp.status, Status::Ok, "DocPut failed: {:?}", resp.payload);
}

fn define_unique_index(s: &mut TcpStream, collection: &str, name: &str, fields: &[&str]) {
    let p = wire::IndexDefPayload {
        collection: collection.into(),
        index_name: name.into(),
        fields: fields.iter().map(|f| f.to_string()).collect(),
        unique: true,
        expire_after_seconds: 0,
        directions: vec![true; fields.len()],
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::IndexDef, p.encode()));
    assert_eq!(resp.status, Status::Ok, "IndexDef failed");
}

#[test]
fn mixed_doc_kv_commit_and_read_your_writes() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);

    // Ensure collection exists before Begin (no implicit create in tx).
    doc_put(&mut s, "users", b"seed", r#"{"n":0}"#);

    let begin = roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![]));
    assert_eq!(begin.status, Status::Ok);
    let (txid, snap) = wire::decode_begin_response(&begin.payload).unwrap();
    assert!(txid >= 1);
    assert!(snap > 0);

    // Stage KV + docs.
    let put = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::Put,
            PutPayload {
                routing_key: [0; 16],
                txid: 0,
                expires_at: 0,
                key: b"session".to_vec(),
                value: b"active".to_vec(),
            }
            .encode(),
        ),
    );
    assert_eq!(put.status, Status::Ok);
    let (_ops, _keys) = wire::decode_stage_ack(&put.payload).unwrap();

    let dp = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::DocPut,
            wire::DocPutPayload {
                collection: "users".into(),
                doc_id: b"u1".to_vec(),
                body: br#"{"n":1}"#.to_vec(),
                relaxed: false,
                expires_at: 0,
            }
            .encode(),
        ),
    );
    assert_eq!(dp.status, Status::Ok);

    // Read-your-writes.
    let get = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::Get,
            KeyPayload {
                routing_key: [0; 16],
                snapshot_seq: 0,
                key: b"session".to_vec(),
            }
            .encode(),
        ),
    );
    assert_eq!(get.status, Status::Ok);
    assert_eq!(get.payload, b"active");

    let grev = roundtrip(
        &mut s,
        &RequestEnvelope::new(Command::DocGetRev, wire::encode_doc_get_rev("users", b"u1")),
    );
    assert_eq!(grev.status, Status::Ok);
    let (body, rev) = wire::decode_doc_get_rev_response(&grev.payload).unwrap();
    assert!(body.contains(&b'1'));
    assert_eq!(rev, 0); // uncommitted staged revision

    // Other connection cannot see staged data.
    let mut s2 = connect(addr);
    let external = roundtrip(
        &mut s2,
        &RequestEnvelope::new(
            Command::Get,
            KeyPayload {
                routing_key: [0; 16],
                snapshot_seq: 0,
                key: b"session".to_vec(),
            }
            .encode(),
        ),
    );
    assert_eq!(external.status, Status::NotFound);

    let commit = roundtrip(&mut s, &RequestEnvelope::new(Command::Commit, vec![]));
    assert_eq!(commit.status, Status::Ok, "{:?}", commit.payload);
    let seq = wire::decode_commit_response(&commit.payload).unwrap();
    assert!(seq > 0);

    let visible = roundtrip(
        &mut s2,
        &RequestEnvelope::new(
            Command::Get,
            KeyPayload {
                routing_key: [0; 16],
                snapshot_seq: 0,
                key: b"session".to_vec(),
            }
            .encode(),
        ),
    );
    assert_eq!(visible.status, Status::Ok);
    assert_eq!(visible.payload, b"active");

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn rollback_and_disconnect_leave_nothing() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);
    doc_put(&mut s, "users", b"seed", r#"{"n":0}"#);

    let begin = roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![]));
    assert_eq!(begin.status, Status::Ok);
    let _ = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::Put,
            PutPayload {
                routing_key: [0; 16],
                txid: 0,
                expires_at: 0,
                key: b"tmp".to_vec(),
                value: b"x".to_vec(),
            }
            .encode(),
        ),
    );
    let rb = roundtrip(&mut s, &RequestEnvelope::new(Command::Rollback, vec![]));
    assert_eq!(rb.status, Status::Ok);

    let get = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::Get,
            KeyPayload {
                routing_key: [0; 16],
                snapshot_seq: 0,
                key: b"tmp".to_vec(),
            }
            .encode(),
        ),
    );
    assert_eq!(get.status, Status::NotFound);

    // Disconnect mid-tx.
    {
        let mut s3 = connect(addr);
        assert_eq!(
            roundtrip(&mut s3, &RequestEnvelope::new(Command::Begin, vec![])).status,
            Status::Ok
        );
        let _ = roundtrip(
            &mut s3,
            &RequestEnvelope::new(
                Command::Put,
                PutPayload {
                    routing_key: [0; 16],
                    txid: 0,
                    expires_at: 0,
                    key: b"gone".to_vec(),
                    value: b"y".to_vec(),
                }
                .encode(),
            ),
        );
        // drop without commit
    }
    thread::sleep(Duration::from_millis(50));
    let gone = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::Get,
            KeyPayload {
                routing_key: [0; 16],
                snapshot_seq: 0,
                key: b"gone".to_vec(),
            }
            .encode(),
        ),
    );
    assert_eq!(gone.status, Status::NotFound);

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn concurrent_writer_causes_commit_conflict() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);
    doc_put(&mut s, "users", b"u1", r#"{"n":1}"#);

    let begin = roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![]));
    assert_eq!(begin.status, Status::Ok);
    let grev = roundtrip(
        &mut s,
        &RequestEnvelope::new(Command::DocGetRev, wire::encode_doc_get_rev("users", b"u1")),
    );
    let (_body, rev) = wire::decode_doc_get_rev_response(&grev.payload).unwrap();

    // Concurrent writer updates the same doc.
    let mut s2 = connect(addr);
    doc_put(&mut s2, "users", b"u1", r#"{"n":2}"#);

    let staged = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::DocPutIfMatch,
            wire::DocPutIfMatchPayload {
                collection: "users".into(),
                doc_id: b"u1".to_vec(),
                body: br#"{"n":3}"#.to_vec(),
                relaxed: false,
                if_match: rev,
                expires_at: 0,
            }
            .encode(),
        ),
    );
    // Staging validates against begin snapshot; may succeed at stage time.
    assert_eq!(staged.status, Status::Ok, "{:?}", staged.payload);

    let commit = roundtrip(&mut s, &RequestEnvelope::new(Command::Commit, vec![]));
    assert_eq!(commit.status, Status::Conflict, "{:?}", commit.payload);

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn unique_index_transfer_inside_transaction() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);
    doc_put(&mut s, "users", b"a", r#"{"email":"x@ex.com"}"#);
    define_unique_index(&mut s, "users", "by_email", &["email"]);
    doc_put(&mut s, "users", b"b", r#"{"email":"y@ex.com"}"#);

    let begin = roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![]));
    assert_eq!(begin.status, Status::Ok);

    // Transfer email from a -> b atomically.
    let _ = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::DocPut,
            wire::DocPutPayload {
                collection: "users".into(),
                doc_id: b"a".to_vec(),
                body: br#"{"email":"z@ex.com"}"#.to_vec(),
                relaxed: false,
                expires_at: 0,
            }
            .encode(),
        ),
    );
    let _ = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::DocPut,
            wire::DocPutPayload {
                collection: "users".into(),
                doc_id: b"b".to_vec(),
                body: br#"{"email":"x@ex.com"}"#.to_vec(),
                relaxed: false,
                expires_at: 0,
            }
            .encode(),
        ),
    );
    let commit = roundtrip(&mut s, &RequestEnvelope::new(Command::Commit, vec![]));
    assert_eq!(commit.status, Status::Ok, "{:?}", commit.payload);

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn unique_collision_inside_transaction_rejected() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);
    doc_put(&mut s, "users", b"a", r#"{"email":"a@ex.com"}"#);
    define_unique_index(&mut s, "users", "by_email", &["email"]);
    doc_put(&mut s, "users", b"b", r#"{"email":"b@ex.com"}"#);

    assert_eq!(
        roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![])).status,
        Status::Ok
    );
    let _ = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::DocPut,
            wire::DocPutPayload {
                collection: "users".into(),
                doc_id: b"a".to_vec(),
                body: br#"{"email":"same@ex.com"}"#.to_vec(),
                relaxed: false,
                expires_at: 0,
            }
            .encode(),
        ),
    );
    let _ = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::DocPut,
            wire::DocPutPayload {
                collection: "users".into(),
                doc_id: b"b".to_vec(),
                body: br#"{"email":"same@ex.com"}"#.to_vec(),
                relaxed: false,
                expires_at: 0,
            }
            .encode(),
        ),
    );
    let commit = roundtrip(&mut s, &RequestEnvelope::new(Command::Commit, vec![]));
    assert_eq!(commit.status, Status::Conflict, "{:?}", commit.payload);

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn nested_begin_and_forbidden_ops_rejected() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);
    doc_put(&mut s, "users", b"seed", r#"{}"#);

    assert_eq!(
        roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![])).status,
        Status::Ok
    );
    let nested = roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![]));
    assert_eq!(nested.status, Status::ProtocolError);

    // Filter queries are rejected and abort the open transaction.
    let find = roundtrip(
        &mut s,
        &RequestEnvelope::new(
            Command::Find,
            wire::FindPayload {
                collection: "users".into(),
                filter: br#"{}"#.to_vec(),
                sort: vec![],
                projection: wire::WireProjection::None,
                skip: 0,
                limit: 10,
                cursor: vec![],
            }
            .encode(),
        ),
    );
    assert_eq!(find.status, Status::ProtocolError);
    // After abort, Commit with no active tx is a protocol error.
    let commit = roundtrip(&mut s, &RequestEnvelope::new(Command::Commit, vec![]));
    assert_eq!(commit.status, Status::ProtocolError);

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn empty_commit_succeeds() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);
    assert_eq!(
        roundtrip(&mut s, &RequestEnvelope::new(Command::Begin, vec![])).status,
        Status::Ok
    );
    let commit = roundtrip(&mut s, &RequestEnvelope::new(Command::Commit, vec![]));
    assert_eq!(commit.status, Status::Ok, "{:?}", commit.payload);
    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
