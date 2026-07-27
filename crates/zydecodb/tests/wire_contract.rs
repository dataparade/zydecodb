//! Wire-contract tests for the 1.x freeze: unknown opcodes return
//! ProtocolError without closing the connection; reserved SchemaDef does the
//! same; a subsequent Ping still works.

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::io::Write;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, PROTO_VERSION};

#[test]
fn unknown_opcode_returns_protocol_error_and_keeps_connection() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut stream = wait_connect(addr);

    // Unrecognized opcode 0x99 with empty payload.
    stream
        .write_all(&[PROTO_VERSION, 0x99, 0, 0, 0, 0])
        .unwrap();
    stream.flush().unwrap();
    let resp = read_response(&mut stream);
    assert_eq!(resp.status, Status::ProtocolError);
    assert!(
        String::from_utf8_lossy(&resp.payload).contains("unknown command"),
        "payload={:?}",
        String::from_utf8_lossy(&resp.payload)
    );

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Ping, Vec::new()),
    );
    let ping = read_response(&mut stream);
    assert_eq!(ping.status, Status::Ok);

    shutdown_join(&shutdown, handle);
}

#[test]
fn schema_def_reserved_returns_protocol_error() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut stream = wait_connect(addr);

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::SchemaDef, Vec::new()),
    );
    let resp = read_response(&mut stream);
    assert_eq!(resp.status, Status::ProtocolError);

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Ping, Vec::new()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    shutdown_join(&shutdown, handle);
}

#[test]
fn unused_doc_put_flag_bit_returns_protocol_error() {
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut stream = wait_connect(addr);

    let mut payload = zydecodb_document::wire::DocPutPayload {
        collection: "c".into(),
        doc_id: b"d1".to_vec(),
        body: br#"{}"#.to_vec(),
        relaxed: false,
        expires_at: 0,
    }
    .encode();
    *payload.last_mut().unwrap() |= 0x80;

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::DocPut, payload),
    );
    let resp = read_response(&mut stream);
    assert_eq!(resp.status, Status::ProtocolError);

    write_request(
        &mut stream,
        &RequestEnvelope::new(Command::Ping, Vec::new()),
    );
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    shutdown_join(&shutdown, handle);
}
