//! Change-stream (Watch) end-to-end tests over the real TCP wire.

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::io::Read;
use std::net::{SocketAddr, TcpStream};
use std::time::Duration;
use tempfile::TempDir;
use zydecodb_document::wire::{
    self, WatchFrame, WatchPayload, WATCH_OP_DELETE, WATCH_OP_UPSERT,
};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope, ENVELOPE_HEADER_LEN};

fn connect(addr: SocketAddr) -> TcpStream {
    wait_connect(addr)
}

fn roundtrip(stream: &mut TcpStream, req: &RequestEnvelope) -> ResponseEnvelope {
    write_request(stream, req);
    read_response(stream)
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
    assert_eq!(resp.status, Status::Ok, "DocPut failed: {:?}", resp);
}

fn doc_del(s: &mut TcpStream, collection: &str, doc_id: &[u8]) {
    let p = wire::DocDelPayload {
        collection: collection.into(),
        doc_id: doc_id.to_vec(),
    };
    let resp = roundtrip(s, &RequestEnvelope::new(Command::DocDel, p.encode()));
    assert_eq!(resp.status, Status::Ok, "DocDel failed: {:?}", resp);
}

fn spawn_cs_server() -> (SocketAddr, ArcShutdown, JoinHandle, TempDir) {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let mut cfg = base_config(&tmp, addr);
    cfg.change_streams.enabled = true;
    cfg.change_streams.heartbeat_ms = 200;
    cfg.change_streams.write_timeout_ms = 2000;
    cfg.change_streams.max_subscriptions = 8;
    cfg.change_streams.max_subscriptions_per_tenant = 4;
    cfg.fsync_interval_ms = 10;
    let (shutdown, handle) = spawn_server(cfg);
    wait_tcp_up(addr);
    // Keep tmp alive for the server thread.
    (addr, shutdown, handle, tmp)
}

type ArcShutdown = std::sync::Arc<std::sync::Mutex<bool>>;
type JoinHandle = std::thread::JoinHandle<()>;

fn open_watch(addr: SocketAddr, collection: &str, resume: &[u8]) -> (TcpStream, Vec<u8>) {
    let mut s = connect(addr);
    let p = WatchPayload {
        collection: collection.into(),
        resume_token: resume.to_vec(),
    };
    write_request(&mut s, &RequestEnvelope::new(Command::Watch, p.encode()));
    let resp = read_response(&mut s);
    assert_eq!(resp.status, Status::Ok, "Watch open failed: {:?}", resp);
    match wire::decode_watch_frame(&resp.payload).unwrap() {
        WatchFrame::Ack { resume_token } => (s, resume_token),
        other => panic!("expected ACK, got {other:?}"),
    }
}

fn next_event(s: &mut TcpStream) -> (Vec<u8>, u8, Vec<u8>, Vec<u8>) {
    for _ in 0..50 {
        let resp = read_response(s);
        assert_eq!(resp.status, Status::Ok, "Watch frame status: {:?}", resp);
        match wire::decode_watch_frame(&resp.payload).unwrap() {
            WatchFrame::Heartbeat { .. } => continue,
            WatchFrame::Event {
                resume_token,
                op,
                doc_id,
                body,
            } => return (resume_token, op, doc_id, body),
            WatchFrame::Ack { .. } => panic!("unexpected second ACK"),
        }
    }
    panic!("timed out waiting for watch event");
}

#[test]
fn watch_upsert_delete_order_and_resume() {
    let (addr, shutdown, handle, _tmp) = spawn_cs_server();
    let mut writer = connect(addr);
    doc_put(&mut writer, "items", b"a", r#"{"n":1}"#);

    let (mut watch, _ack) = open_watch(addr, "items", &[]);
    doc_put(&mut writer, "items", b"b", r#"{"n":2}"#);
    doc_del(&mut writer, "items", b"a");

    let (tok1, op1, id1, body1) = next_event(&mut watch);
    assert_eq!(op1, WATCH_OP_UPSERT);
    assert_eq!(id1, b"b");
    assert!(String::from_utf8_lossy(&body1).contains("\"n\":2"));

    let (tok2, op2, id2, body2) = next_event(&mut watch);
    assert_eq!(op2, WATCH_OP_DELETE);
    assert_eq!(id2, b"a");
    assert!(body2.is_empty());
    assert_ne!(tok1, tok2);

    // Resume exclusive after tok1: must see the delete, not the upsert again.
    drop(watch);
    let (mut watch2, _) = open_watch(addr, "items", &tok1);
    let (_t, op, id, _) = next_event(&mut watch2);
    assert_eq!(op, WATCH_OP_DELETE);
    assert_eq!(id, b"a");

    drop(writer);
    drop(watch2);
    shutdown_join(&shutdown, handle);
}

#[test]
fn watch_disabled_rejected() {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let cfg = base_config(&tmp, addr); // change_streams.enabled = false
    let (shutdown, handle) = spawn_server(cfg);
    wait_tcp_up(addr);

    let mut s = connect(addr);
    doc_put(&mut s, "c", b"1", r#"{}"#);
    let p = WatchPayload {
        collection: "c".into(),
        resume_token: vec![],
    };
    let resp = roundtrip(&mut s, &RequestEnvelope::new(Command::Watch, p.encode()));
    assert_eq!(resp.status, Status::Forbidden);

    drop(s);
    shutdown_join(&shutdown, handle);
}

#[test]
fn watch_subscription_cap() {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    let mut cfg = base_config(&tmp, addr);
    cfg.change_streams.enabled = true;
    cfg.change_streams.max_subscriptions = 1;
    cfg.change_streams.max_subscriptions_per_tenant = 1;
    let (shutdown, handle) = spawn_server(cfg);
    wait_tcp_up(addr);

    let mut writer = connect(addr);
    doc_put(&mut writer, "c", b"1", r#"{}"#);

    let (w1, _) = open_watch(addr, "c", &[]);
    let mut s2 = connect(addr);
    let p = WatchPayload {
        collection: "c".into(),
        resume_token: vec![],
    };
    let resp = roundtrip(&mut s2, &RequestEnvelope::new(Command::Watch, p.encode()));
    assert_eq!(resp.status, Status::EngineBusy);

    drop(writer);
    drop(s2);
    drop(w1);
    shutdown_join(&shutdown, handle);
}

#[test]
fn watch_heartbeat_arrives_when_idle() {
    let (addr, shutdown, handle, _tmp) = spawn_cs_server();
    let mut writer = connect(addr);
    doc_put(&mut writer, "hb", b"1", r#"{}"#);
    let (mut watch, _) = open_watch(addr, "hb", &[]);

    let mut saw_heartbeat = false;
    for _ in 0..30 {
        // Short read timeout so we can poll.
        watch
            .set_read_timeout(Some(Duration::from_millis(300)))
            .unwrap();
        let mut header = [0u8; ENVELOPE_HEADER_LEN];
        match watch.read_exact(&mut header) {
            Ok(()) => {
                let (status, len) = ResponseEnvelope::parse_header(&header).unwrap();
                let mut payload = vec![0u8; len];
                if len > 0 {
                    watch.read_exact(&mut payload).unwrap();
                }
                assert_eq!(status, Status::Ok);
                if matches!(
                    wire::decode_watch_frame(&payload).unwrap(),
                    WatchFrame::Heartbeat { .. }
                ) {
                    saw_heartbeat = true;
                    break;
                }
            }
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                continue;
            }
            Err(e) => panic!("read failed: {e}"),
        }
    }
    assert!(saw_heartbeat, "expected heartbeat while idle");

    drop(writer);
    drop(watch);
    shutdown_join(&shutdown, handle);
}

#[test]
fn watch_many_events_stay_ordered() {
    let (addr, shutdown, handle, _tmp) = spawn_cs_server();
    let mut writer = connect(addr);
    doc_put(&mut writer, "burst", b"seed", r#"{"i":-1}"#);
    let (mut watch, _) = open_watch(addr, "burst", &[]);

    const N: usize = 50;
    for i in 0..N {
        doc_put(
            &mut writer,
            "burst",
            format!("{i:04}").as_bytes(),
            &format!(r#"{{"i":{i}}}"#),
        );
    }

    let mut seen = Vec::with_capacity(N);
    for _ in 0..N {
        let (_tok, op, id, body) = next_event(&mut watch);
        assert_eq!(op, WATCH_OP_UPSERT);
        seen.push((id, body));
    }
    for i in 0..N {
        assert_eq!(seen[i].0, format!("{i:04}").as_bytes());
        assert!(String::from_utf8_lossy(&seen[i].1).contains(&format!("\"i\":{i}")));
    }

    drop(writer);
    drop(watch);
    shutdown_join(&shutdown, handle);
}

#[test]
fn aggregate_rejects_unsupported_and_respects_read_acl_shape() {
    // Aggregation rejection over the wire (parser).
    let (addr, shutdown, handle) = spawn_ephemeral_server();
    let mut s = connect(addr);
    doc_put(&mut s, "sales", b"1", r#"{"team":"a","amount":1}"#);

    let bad = r#"[{"$lookup":{"from":"x"}},{"$group":{"_id":null,"n":{"$count":{}}}}]"#;
    let p = wire::AggregatePayload {
        collection: "sales".into(),
        pipeline: bad.as_bytes().to_vec(),
    };
    let resp = roundtrip(&mut s, &RequestEnvelope::new(Command::Aggregate, p.encode()));
    assert_ne!(resp.status, Status::Ok);

    let good = r#"[{"$group":{"_id":"$team","n":{"$count":{}}}}]"#;
    let p = wire::AggregatePayload {
        collection: "sales".into(),
        pipeline: good.as_bytes().to_vec(),
    };
    let resp = roundtrip(&mut s, &RequestEnvelope::new(Command::Aggregate, p.encode()));
    assert_eq!(resp.status, Status::Ok);

    drop(s);
    shutdown_join(&shutdown, handle);
}
