#[path = "common/mod.rs"]
mod common;
use common::*;

use std::io::Read;
use std::thread;
use std::time::{Duration, Instant};
use tempfile::TempDir;

use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

use zydecodb_document::wire::{
    DocPutPayload, FindPayload, IndexDefPayload, UpdatePayload, WireProjection,
};

#[test]
fn test_filter_complexity_exhaustion() {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let config = base_config(&tmp, addr);

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);

    // Create collection via index definition
    let idx = IndexDefPayload {
        collection: "test".to_string(),
        index_name: "idx_a".to_string(),
        fields: vec!["a".to_string()],
        unique: false,
        expire_after_seconds: 0,
        directions: vec![true],
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::IndexDef, idx.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    // Insert a document
    let put = DocPutPayload {
        collection: "test".to_string(),
        doc_id: b"doc1".to_vec(),
        body: b"{\"a\": 1}".to_vec(),
        relaxed: false,
        expires_at: 0,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::DocPut, put.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    // Build a deeply nested $or / $and filter
    // e.g. {"$or": [{"$and": [{"a": 1}, {"$or": [...]}]}]}
    let depth = 1000;
    let mut filter_json = String::from("{\"a\": 1}");
    for i in 0..depth {
        if i % 2 == 0 {
            filter_json = format!("{{\"$or\": [{}, {{\"a\": 2}}]}}", filter_json);
        } else {
            filter_json = format!("{{\"$and\": [{}, {{\"a\": 1}}]}}", filter_json);
        }
    }

    let find = FindPayload {
        collection: "test".to_string(),
        filter: filter_json.into_bytes(),
        sort: vec![],
        projection: WireProjection::None,
        skip: 0,
        limit: 10,
        cursor: vec![],
    };

    let req = RequestEnvelope::new(Command::Find, find.encode());

    // We send the request and expect either a quick error (if complexity is limited)
    // or a quick success (if it handles it efficiently). It should NOT hang.
    write_request(&mut stream, &req);

    let start = Instant::now();
    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    let res = stream.read_exact(&mut header);
    let elapsed = start.elapsed();

    // If it took too long, it's a vulnerability
    assert!(
        elapsed < Duration::from_secs(2),
        "VULNERABILITY SURFACED: Deeply nested filter pinned the thread for {:?}",
        elapsed
    );

    if res.is_ok() {
        let (status, _) = ResponseEnvelope::parse_header(&header).unwrap();
        // It might be ProtocolError (if we reject deep nesting) or Ok (if we evaluate it fast)
        assert!(
            status == Status::ProtocolError
                || status == Status::Ok
                || status == Status::Error
                || status == Status::InvalidValue,
            "Unexpected status: {:?}",
            status
        );
    } else {
        // If it crashed (stack overflow), that's also a vulnerability
        panic!("VULNERABILITY SURFACED: Server crashed on deeply nested filter (likely Stack Overflow)");
    }

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}

#[test]
fn test_deep_graft_update_rejected_without_crash() {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let config = base_config(&tmp, addr);

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || server.run(config).unwrap());

    let mut stream = wait_connect(addr);

    let idx = IndexDefPayload {
        collection: "test".to_string(),
        index_name: "idx_a".to_string(),
        fields: vec!["a".to_string()],
        unique: false,
        expire_after_seconds: 0,
        directions: vec![true],
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::IndexDef, idx.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    let put = DocPutPayload {
        collection: "test".to_string(),
        doc_id: b"doc1".to_vec(),
        body: b"{\"a\": 1}".to_vec(),
        relaxed: false,
        expires_at: 0,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::DocPut, put.encode()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    // A dotted path is a flat JSON string — serde's parse depth cap does not
    // bound it. 5000 segments would graft 5000 nesting levels in ONE update
    // (and blow the stack on the next read) if the path cap didn't reject it.
    let path = std::iter::repeat("s")
        .take(5000)
        .enumerate()
        .map(|(i, s)| format!("{s}{i}"))
        .collect::<Vec<_>>()
        .join(".");
    let update_json = format!("{{\"$set\": {{\"{path}\": 1}}}}");
    let upd = UpdatePayload {
        collection: "test".to_string(),
        filter: br#"{"a": 1}"#.to_vec(),
        update: update_json.into_bytes(),
        multi: false,
        relaxed: false,
        upsert: false,
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Update, upd.encode()),
    );
    let resp = read_response(&mut stream);
    assert_ne!(
        resp.status,
        Status::Ok,
        "a 5000-segment graft path must be rejected, not applied"
    );

    // The server survived: the document is still intact and readable.
    let find = FindPayload {
        collection: "test".to_string(),
        filter: br#"{"a": 1}"#.to_vec(),
        sort: vec![],
        projection: WireProjection::None,
        skip: 0,
        limit: 10,
        cursor: vec![],
    };
    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Find, find.encode()),
    );
    assert_eq!(
        read_response(&mut stream).status,
        Status::Ok,
        "server must still answer after rejecting the deep graft"
    );

    *shutdown.lock().unwrap() = true;
    handle.join().unwrap();
}
