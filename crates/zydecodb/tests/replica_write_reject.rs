//! Replica write-rejection audit: every mutating / session-mutating opcode
//! returns Forbidden on a read replica (1.0 Section 5).

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::path::PathBuf;
use std::thread;
use tempfile::TempDir;
use zydecodb::config::{ReplicaConfig, RequireAuth, SecurityConfig};
use zydecodb_document::wire::{
    AggregatePayload, DeletePayload, DocDelPayload, DocPutIfMatchPayload, DocPutPayload,
    DocUpdateIfMatchPayload, FindPayload, IndexDefPayload, UpdatePayload, WatchPayload,
    WireProjection,
};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, PutPayload, RequestEnvelope};

fn replica_only_server() -> (
    std::net::SocketAddr,
    std::sync::Arc<std::sync::Mutex<bool>>,
    std::thread::JoinHandle<()>,
) {
    let tmp = TempDir::new().unwrap();
    let addr = free_addr();
    // Empty ship dir is enough to mark the process as a replica (read_only).
    let from = tmp.path().join("from");
    std::fs::create_dir_all(&from).unwrap();
    let hmac = tmp.path().join("ship.hmac");
    write_secret_file(&hmac, b"replica-write-reject-hmac-key-material!");

    let mut cfg = base_config(&tmp, addr);
    cfg.security = SecurityConfig {
        require_auth: RequireAuth::False,
        keys_file: PathBuf::from("/nonexistent"),
        rate_limit_rps: 1_000_000,
        ..Default::default()
    };
    cfg.change_streams.enabled = false;
    cfg.replica = ReplicaConfig {
        from: Some(from),
        poll_ms: 200,
        hmac_key_file: Some(hmac),
    };
    assert!(
        !cfg.change_streams.enabled,
        "change streams must stay off on replicas"
    );

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = thread::spawn(move || {
        let _keep = tmp;
        if let Err(e) = server.run(cfg) {
            panic!("replica server failed: {e}");
        }
    });
    wait_tcp_up(addr);
    (addr, shutdown, handle)
}

fn put_payload() -> Vec<u8> {
    PutPayload {
        routing_key: [0; 16],
        txid: 0,
        expires_at: 0,
        key: b"k".to_vec(),
        value: b"v".to_vec(),
    }
    .encode()
}

fn forbidden_on_replica() -> Vec<(Command, Vec<u8>)> {
    vec![
        (Command::Put, put_payload()),
        (Command::Del, put_payload()),
        (
            Command::DocPut,
            DocPutPayload {
                collection: "c".into(),
                doc_id: b"d".to_vec(),
                body: br#"{}"#.to_vec(),
                relaxed: false,
                expires_at: 0,
            }
            .encode(),
        ),
        (
            Command::DocDel,
            DocDelPayload {
                collection: "c".into(),
                doc_id: b"d".to_vec(),
            }
            .encode(),
        ),
        (
            Command::Update,
            UpdatePayload {
                collection: "c".into(),
                filter: br#"{}"#.to_vec(),
                update: br#"{"$set":{"n":1}}"#.to_vec(),
                multi: false,
                relaxed: false,
                upsert: false,
            }
            .encode(),
        ),
        (
            Command::Delete,
            DeletePayload {
                collection: "c".into(),
                filter: br#"{}"#.to_vec(),
                multi: false,
                relaxed: false,
            }
            .encode(),
        ),
        (
            Command::DocPutIfMatch,
            DocPutIfMatchPayload {
                collection: "c".into(),
                doc_id: b"d".to_vec(),
                body: br#"{}"#.to_vec(),
                relaxed: false,
                if_match: 1,
                expires_at: 0,
            }
            .encode(),
        ),
        (
            Command::DocUpdateIfMatch,
            DocUpdateIfMatchPayload {
                collection: "c".into(),
                doc_id: b"d".to_vec(),
                update: br#"{"$set":{"n":1}}"#.to_vec(),
                relaxed: false,
                if_match: 1,
            }
            .encode(),
        ),
        (
            Command::IndexDef,
            IndexDefPayload {
                collection: "c".into(),
                index_name: "i".into(),
                fields: vec!["n".into()],
                unique: false,
                expire_after_seconds: 0,
                directions: vec![true],
            }
            .encode(),
        ),
        (Command::AdminDropTenant, {
            let mut p = vec![0u8; 16];
            p.push(0);
            p
        }),
        (Command::Begin, vec![]),
        (Command::Commit, vec![]),
        (Command::Rollback, vec![]),
        (Command::SetContext, vec![0u8; 16]),
        (
            Command::Watch,
            WatchPayload {
                collection: "c".into(),
                resume_token: vec![],
            }
            .encode(),
        ),
    ]
}

#[test]
fn replica_rejects_every_mutating_opcode() {
    let (addr, shutdown, handle) = replica_only_server();
    let mut stream = wait_connect(addr);

    for (cmd, payload) in forbidden_on_replica() {
        write_request(&mut stream, &RequestEnvelope::new(cmd, payload));
        let resp = read_response(&mut stream);
        assert_eq!(
            resp.status,
            Status::Forbidden,
            "replica must Forbidden {cmd:?}"
        );
        let msg = String::from_utf8_lossy(&resp.payload);
        assert!(
            msg.contains("read-only") || msg.contains("primary-only"),
            "replica {cmd:?} unexpected message: {msg}"
        );
        if cmd == Command::Watch {
            // Watch ends the connection after the error frame.
            stream = wait_connect(addr);
        }
    }

    // Reads still allowed.
    write_request(
        &mut stream,
        &RequestEnvelope::new(
            Command::Find,
            FindPayload {
                collection: "c".into(),
                filter: br#"{}"#.to_vec(),
                sort: vec![],
                projection: WireProjection::None,
                skip: 0,
                limit: 1,
                cursor: vec![],
            }
            .encode(),
        ),
    );
    let find = read_response(&mut stream);
    assert_ne!(find.status, Status::Forbidden, "Find must not be Forbidden");

    write_request(
        &mut stream,
        &RequestEnvelope::new(
            Command::Aggregate,
            AggregatePayload {
                collection: "c".into(),
                pipeline: br#"[{"$group":{"_id":null,"n":{"$count":{}}}}]"#.to_vec(),
            }
            .encode(),
        ),
    );
    let agg = read_response(&mut stream);
    assert_ne!(
        agg.status,
        Status::Forbidden,
        "Aggregate must not be Forbidden"
    );

    write_request(&mut stream, &RequestEnvelope::new(Command::Ping, vec![]));
    assert_eq!(read_response(&mut stream).status, Status::Ok);

    shutdown_join(&shutdown, handle);
}

#[test]
fn replica_forbidden_list_is_complete() {
    // Keep in sync with server::is_write_command + Watch primary-only path.
    let cmds: Vec<Command> = forbidden_on_replica().into_iter().map(|(c, _)| c).collect();
    for expected in [
        Command::Put,
        Command::Del,
        Command::DocPut,
        Command::DocDel,
        Command::Update,
        Command::Delete,
        Command::DocPutIfMatch,
        Command::DocUpdateIfMatch,
        Command::IndexDef,
        Command::AdminDropTenant,
        Command::Begin,
        Command::Commit,
        Command::Rollback,
        Command::SetContext,
        Command::Watch,
    ] {
        assert!(cmds.contains(&expected), "missing {expected:?}");
    }
    assert_eq!(cmds.len(), 15);
}
