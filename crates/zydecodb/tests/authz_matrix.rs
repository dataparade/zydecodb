//! Exhaustive opcode × role × tenant/ACL authorization matrix (1.0 Section 5).
//!
//! Expected outcomes are documented in `docs/INTERNAL.md` (authz matrix).
//! Asserts status only — not full response bodies.

#[path = "common/mod.rs"]
mod common;
use common::*;

use std::net::TcpStream;
use tempfile::TempDir;
use zydecodb::security::keys::{KeyRole, KeyStore};
use zydecodb_document::wire::{
    AggregatePayload, CountPayload, DeletePayload, DocDelPayload, DocPutIfMatchPayload,
    DocPutPayload, DocUpdateIfMatchPayload, FindPayload, IndexDefPayload, QueryPayload,
    UpdatePayload, WatchPayload, WireProjection,
};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, KeyPayload, PutPayload, RequestEnvelope};

struct Keys {
    ro: String,
    rw: String,
    admin: String,
    rw_b: String,
    acl: String,
}

fn setup() -> (
    std::net::SocketAddr,
    Keys,
    std::sync::Arc<std::sync::Mutex<bool>>,
    std::thread::JoinHandle<()>,
) {
    let tmp = TempDir::new().unwrap();
    let keys_file = tmp.path().join("keys.toml");
    let keys = Keys {
        ro: KeyStore::create_key(
            &keys_file,
            "ro",
            KeyRole::ReadOnly,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![],
        )
        .unwrap(),
        rw: KeyStore::create_key(
            &keys_file,
            "rw",
            KeyRole::ReadWrite,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![],
        )
        .unwrap(),
        admin: KeyStore::create_key(
            &keys_file,
            "admin",
            KeyRole::Admin,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec![],
        )
        .unwrap(),
        rw_b: KeyStore::create_key(
            &keys_file,
            "rw_b",
            KeyRole::ReadWrite,
            "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            vec![],
        )
        .unwrap(),
        acl: KeyStore::create_key(
            &keys_file,
            "acl",
            KeyRole::ReadWrite,
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            vec!["events:".to_string()],
        )
        .unwrap(),
    };

    let addr = free_addr();
    let mut cfg = auth_config(&tmp, addr, keys_file);
    cfg.security.legacy_single_tenant = false;
    cfg.security.rate_limit_rps = 1_000_000;
    cfg.change_streams.enabled = true;
    cfg.change_streams.heartbeat_ms = 500;
    cfg.change_streams.max_subscriptions = 64;

    let server = zydecodb::server::Server::new();
    let shutdown = server.shutdown_flag();
    let handle = std::thread::spawn(move || {
        let _keep = tmp;
        server.run(cfg).unwrap();
    });
    wait_tcp_up(addr);

    // Seed tenant A data for isolation / conditional-write cells.
    let mut s = wait_connect(addr);
    session_init_ok(&mut s, &keys.rw);
    roundtrip(
        &mut s,
        Command::Put,
        PutPayload {
            routing_key: [0; 16],
            txid: 0,
            expires_at: 0,
            key: b"seed".to_vec(),
            value: b"v".to_vec(),
        }
        .encode(),
    );
    roundtrip(
        &mut s,
        Command::DocPut,
        DocPutPayload {
            collection: "c".into(),
            doc_id: b"d1".to_vec(),
            body: br#"{"n":1}"#.to_vec(),
            relaxed: false,
            expires_at: 0,
        }
        .encode(),
    );
    drop(s);

    (addr, keys, shutdown, handle)
}

fn roundtrip(stream: &mut TcpStream, cmd: Command, payload: Vec<u8>) -> Status {
    write_request(stream, &RequestEnvelope::new(cmd, payload));
    read_response(stream).status
}

fn expect(stream: &mut TcpStream, cmd: Command, payload: Vec<u8>, want: Status, cell: &str) {
    let got = roundtrip(stream, cmd, payload);
    assert_eq!(got, want, "cell={cell} cmd={cmd:?}");
}

fn authed(addr: std::net::SocketAddr, secret: &str) -> TcpStream {
    let mut s = wait_connect(addr);
    session_init_ok(&mut s, secret);
    s
}

fn kv_put(key: &[u8]) -> Vec<u8> {
    PutPayload {
        routing_key: [0; 16],
        txid: 0,
        expires_at: 0,
        key: key.to_vec(),
        value: b"x".to_vec(),
    }
    .encode()
}

fn kv_get(key: &[u8]) -> Vec<u8> {
    KeyPayload {
        routing_key: [0; 16],
        snapshot_seq: 0,
        key: key.to_vec(),
    }
    .encode()
}

fn doc_put(coll: &str) -> Vec<u8> {
    DocPutPayload {
        collection: coll.into(),
        doc_id: b"x".to_vec(),
        body: br#"{}"#.to_vec(),
        relaxed: false,
        expires_at: 0,
    }
    .encode()
}

fn find(coll: &str) -> Vec<u8> {
    FindPayload {
        collection: coll.into(),
        filter: br#"{}"#.to_vec(),
        sort: vec![],
        projection: WireProjection::None,
        skip: 0,
        limit: 10,
        cursor: vec![],
    }
    .encode()
}

fn aggregate(coll: &str) -> Vec<u8> {
    AggregatePayload {
        collection: coll.into(),
        pipeline: br#"[{"$group":{"_id":null,"n":{"$count":{}}}}]"#.to_vec(),
    }
    .encode()
}

fn watch(coll: &str) -> Vec<u8> {
    WatchPayload {
        collection: coll.into(),
        resume_token: vec![],
    }
    .encode()
}

/// Minimal valid payload per opcode (authz gate fires before deep semantics).
fn payload(cmd: Command) -> Vec<u8> {
    match cmd {
        Command::Put | Command::Del => kv_put(b"k"),
        Command::Get => kv_get(b"seed"),
        Command::Begin | Command::Commit | Command::Rollback | Command::Ping | Command::Stats => {
            vec![]
        }
        Command::Query | Command::DocGetRev => QueryPayload::ById {
            collection: "c".into(),
            doc_id: b"d1".to_vec(),
        }
        .encode(),
        Command::DocPut => doc_put("c"),
        Command::DocDel => DocDelPayload {
            collection: "c".into(),
            doc_id: b"d1".to_vec(),
        }
        .encode(),
        Command::Find | Command::FindRev => find("c"),
        Command::Update => UpdatePayload {
            collection: "c".into(),
            filter: br#"{"n":1}"#.to_vec(),
            update: br#"{"$set":{"n":2}}"#.to_vec(),
            multi: false,
            relaxed: false,
            upsert: false,
        }
        .encode(),
        Command::Delete => DeletePayload {
            collection: "c".into(),
            filter: br#"{"n":1}"#.to_vec(),
            multi: false,
            relaxed: false,
        }
        .encode(),
        Command::Count => CountPayload::Count {
            collection: "c".into(),
            filter: br#"{}"#.to_vec(),
        }
        .encode(),
        Command::DocPutIfMatch => DocPutIfMatchPayload {
            collection: "c".into(),
            doc_id: b"d1".to_vec(),
            body: br#"{"n":9}"#.to_vec(),
            relaxed: false,
            if_match: 1,
            expires_at: 0,
        }
        .encode(),
        Command::DocUpdateIfMatch => DocUpdateIfMatchPayload {
            collection: "c".into(),
            doc_id: b"d1".to_vec(),
            update: br#"{"$set":{"n":3}}"#.to_vec(),
            relaxed: false,
            if_match: 1,
        }
        .encode(),
        Command::Aggregate => aggregate("c"),
        Command::Watch => watch("c"),
        Command::IndexDef => IndexDefPayload {
            collection: "c".into(),
            index_name: "by_n".into(),
            fields: vec!["n".into()],
            unique: false,
            expire_after_seconds: 0,
            directions: vec![true],
        }
        .encode(),
        Command::SchemaDef => vec![],
        Command::SessionInit => b"unused".to_vec(),
        Command::SetContext => vec![0u8; 16],
        Command::AdminDropTenant => {
            let mut p = vec![0xbb; 16];
            p.push(0);
            p
        }
    }
}

fn is_write(cmd: Command) -> bool {
    matches!(
        cmd,
        Command::Put
            | Command::Del
            | Command::DocPut
            | Command::DocDel
            | Command::Update
            | Command::Delete
            | Command::DocPutIfMatch
            | Command::DocUpdateIfMatch
            | Command::IndexDef
            | Command::Begin
            | Command::Commit
    )
}

fn is_admin_only(cmd: Command) -> bool {
    matches!(cmd, Command::SetContext | Command::AdminDropTenant)
}

#[test]
fn authz_matrix_anonymous_and_roles() {
    let (addr, keys, shutdown, handle) = setup();

    // --- Anonymous: every opcode except Ping/SessionInit needs auth ---
    {
        let mut s = wait_connect(addr);
        for cmd in all_commands() {
            if cmd == Command::SessionInit {
                // Wrong key → Unauthorized (valid init tested elsewhere).
                expect(
                    &mut s,
                    Command::SessionInit,
                    b"zdk_not_a_real_key".to_vec(),
                    Status::Unauthorized,
                    "anon/SessionInit-bad",
                );
                continue;
            }
            if cmd == Command::Ping {
                expect(&mut s, cmd, vec![], Status::Ok, "anon/Ping");
                continue;
            }
            if cmd == Command::SchemaDef {
                // Reserved: ProtocolError (fail-closed) even before auth depth.
                expect(&mut s, cmd, vec![], Status::ProtocolError, "anon/SchemaDef");
                continue;
            }
            let want = Status::Unauthorized;
            if cmd == Command::Watch {
                expect(&mut s, cmd, payload(cmd), want, "anon/Watch");
                // Watch path may close; reopen.
                s = wait_connect(addr);
            } else {
                expect(&mut s, cmd, payload(cmd), want, &format!("anon/{cmd:?}"));
            }
        }
    }

    // --- ReadOnly: reads Ok; writes Forbidden; admin Forbidden ---
    {
        let mut s = authed(addr, &keys.ro);
        for cmd in all_commands() {
            match cmd {
                Command::SessionInit => {
                    expect(
                        &mut s,
                        cmd,
                        keys.ro.as_bytes().to_vec(),
                        Status::ProtocolError,
                        "ro/SessionInit-again",
                    );
                }
                Command::SchemaDef => {
                    expect(&mut s, cmd, vec![], Status::ProtocolError, "ro/SchemaDef");
                }
                Command::Ping | Command::Stats => {
                    expect(&mut s, cmd, vec![], Status::Ok, &format!("ro/{cmd:?}"));
                }
                Command::Rollback => {
                    // No open tx: authenticated no-op.
                    expect(&mut s, cmd, vec![], Status::Ok, "ro/Rollback");
                }
                Command::Commit => {
                    // No open tx → aborted (ProtocolError), not a role bypass.
                    expect(&mut s, cmd, vec![], Status::ProtocolError, "ro/Commit");
                }
                c if is_admin_only(c) || is_write(c) => {
                    expect(
                        &mut s,
                        c,
                        payload(c),
                        Status::Forbidden,
                        &format!("ro/{c:?}"),
                    );
                }
                Command::Watch => {
                    expect(&mut s, cmd, payload(cmd), Status::Ok, "ro/Watch");
                    s = authed(addr, &keys.ro);
                }
                c => {
                    // Reads: Ok or NotFound still means authz passed.
                    let got = roundtrip(&mut s, c, payload(c));
                    assert!(
                        matches!(got, Status::Ok | Status::NotFound),
                        "ro/{c:?} got {got:?}"
                    );
                }
            }
        }
    }

    // --- ReadWrite own tenant: writes/reads Ok; admin Forbidden ---
    {
        let mut s = authed(addr, &keys.rw);
        for cmd in all_commands() {
            match cmd {
                Command::SessionInit => {
                    expect(
                        &mut s,
                        cmd,
                        keys.rw.as_bytes().to_vec(),
                        Status::ProtocolError,
                        "rw/SessionInit-again",
                    );
                }
                Command::SchemaDef => {
                    expect(&mut s, cmd, vec![], Status::ProtocolError, "rw/SchemaDef");
                }
                c if is_admin_only(c) => {
                    expect(
                        &mut s,
                        c,
                        payload(c),
                        Status::Forbidden,
                        &format!("rw/{c:?}"),
                    );
                }
                Command::Begin => {
                    expect(&mut s, cmd, vec![], Status::Ok, "rw/Begin");
                    expect(&mut s, Command::Rollback, vec![], Status::Ok, "rw/Rollback");
                }
                Command::Commit => {
                    expect(&mut s, Command::Begin, vec![], Status::Ok, "rw/Begin2");
                    expect(&mut s, cmd, vec![], Status::Ok, "rw/Commit");
                }
                Command::Rollback => {}
                Command::Watch => {
                    expect(&mut s, cmd, payload(cmd), Status::Ok, "rw/Watch");
                    s = authed(addr, &keys.rw);
                }
                Command::DocPutIfMatch | Command::DocUpdateIfMatch => {
                    // Authz must not Forbidden; PreconditionFailed/Ok/NotFound ok.
                    let got = roundtrip(&mut s, cmd, payload(cmd));
                    assert_ne!(got, Status::Unauthorized, "rw/{cmd:?}");
                    assert_ne!(got, Status::Forbidden, "rw/{cmd:?}");
                }
                c => {
                    let got = roundtrip(&mut s, c, payload(c));
                    assert_ne!(got, Status::Unauthorized, "rw/{c:?}");
                    assert_ne!(got, Status::Forbidden, "rw/{c:?}");
                }
            }
        }
    }

    // --- Admin: SetContext + AdminDropTenant Ok ---
    {
        let mut s = authed(addr, &keys.admin);
        expect(
            &mut s,
            Command::Aggregate,
            aggregate("c"),
            Status::Ok,
            "admin/Aggregate",
        );
        expect(
            &mut s,
            Command::SetContext,
            vec![0xbb; 16],
            Status::Ok,
            "admin/SetContext",
        );
        // Drop empty tenant B (compact=0) — authz + admin path.
        let mut drop_payload = vec![0xbb; 16];
        drop_payload.push(0);
        expect(
            &mut s,
            Command::AdminDropTenant,
            drop_payload,
            Status::Ok,
            "admin/AdminDropTenant",
        );
    }

    // --- Cross-tenant: B cannot see A's seeded key (NotFound, not leak) ---
    {
        let mut s = authed(addr, &keys.rw_b);
        expect(
            &mut s,
            Command::Get,
            kv_get(b"seed"),
            Status::NotFound,
            "cross/Get",
        );
        let got = roundtrip(&mut s, Command::Query, payload(Command::Query));
        assert!(
            matches!(got, Status::Ok | Status::NotFound),
            "cross/Query {got:?}"
        );
        // Aggregate over empty tenant collection is still authorized.
        let got = roundtrip(&mut s, Command::Aggregate, aggregate("c"));
        assert_ne!(got, Status::Forbidden, "cross/Aggregate");
        assert_ne!(got, Status::Unauthorized, "cross/Aggregate");
    }

    // --- Prefix ACL deny ---
    {
        let mut s = authed(addr, &keys.acl);
        expect(
            &mut s,
            Command::Get,
            kv_get(b"users:1"),
            Status::Forbidden,
            "acl/Get-users",
        );
        expect(
            &mut s,
            Command::Put,
            kv_put(b"events:ok"),
            Status::Ok,
            "acl/Put-events",
        );
        expect(
            &mut s,
            Command::Find,
            find("users"),
            Status::Forbidden,
            "acl/Find-users",
        );
        expect(
            &mut s,
            Command::Aggregate,
            aggregate("users"),
            Status::Forbidden,
            "acl/Aggregate-users",
        );
        expect(
            &mut s,
            Command::Watch,
            watch("users"),
            Status::Forbidden,
            "acl/Watch-users",
        );
        s = authed(addr, &keys.acl);
        // ACL allows collection `events`; NotFound means authz passed (no docs yet).
        let got = roundtrip(&mut s, Command::Watch, watch("events"));
        assert!(
            matches!(got, Status::Ok | Status::NotFound),
            "acl/Watch-events got {got:?}"
        );
        s = authed(addr, &keys.acl);
        expect(
            &mut s,
            Command::DocPutIfMatch,
            DocPutIfMatchPayload {
                collection: "users".into(),
                doc_id: b"x".to_vec(),
                body: br#"{}"#.to_vec(),
                relaxed: false,
                if_match: 1,
                expires_at: 0,
            }
            .encode(),
            Status::Forbidden,
            "acl/DocPutIfMatch-users",
        );
    }

    shutdown_join(&shutdown, handle);
}

fn all_commands() -> Vec<Command> {
    vec![
        Command::Put,
        Command::Get,
        Command::Del,
        Command::Begin,
        Command::Commit,
        Command::Rollback,
        Command::Query,
        Command::DocPut,
        Command::DocDel,
        Command::Find,
        Command::Update,
        Command::Delete,
        Command::Count,
        Command::DocGetRev,
        Command::FindRev,
        Command::DocPutIfMatch,
        Command::DocUpdateIfMatch,
        Command::Aggregate,
        Command::Watch,
        Command::IndexDef,
        Command::SchemaDef,
        Command::SessionInit,
        Command::SetContext,
        Command::AdminDropTenant,
        Command::Ping,
        Command::Stats,
    ]
}

#[test]
fn authz_matrix_covers_every_command_variant() {
    // Compile-time-ish exhaustiveness: if Command gains a variant, update all_commands.
    assert_eq!(all_commands().len(), 26);
    for b in 0u8..=255 {
        if let Some(cmd) = Command::from_u8(b) {
            assert!(
                all_commands().contains(&cmd),
                "Command::{cmd:?} (0x{b:02x}) missing from authz matrix"
            );
        }
    }
}
