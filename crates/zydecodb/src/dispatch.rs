use crate::security::keys::KeyRole;
use crate::security::{SecurityRuntime, SessionState};
use std::io::{Read, Write};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use zydecodb_engine::engine::Engine;
use zydecodb_engine::errors::{EngineError, Status};
use zydecodb_engine::frame::{Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope};
use zydecodb_engine::keys::KS_USER;

pub struct DispatchOutcome {
    pub response: ResponseEnvelope,
    pub session: SessionState,
    /// For durable writes (PUT/DEL), the assigned sequence number the caller
    /// must make durable before acknowledging. `None` for reads and control
    /// commands. Raw-KV writes are always durable-by-default (no relaxed flag).
    pub commit_seq: Option<u64>,
}

/// Route one raw-KV command. Takes the shared engine (not a held lock) so the
/// engine mutex can be scoped per command: control commands take no lock, `Get`
/// and `Stats` take a brief lock to capture a snapshot/counters then read off it,
/// and only `Put`/`Del` hold the lock across the write. This mirrors the document
/// path and keeps the engine mutex free during the (lock-free) read of a `Get`.
pub fn handle_request(
    engine: &Arc<Mutex<Engine>>,
    req: RequestEnvelope,
    session: SessionState,
    security: &SecurityRuntime,
) -> DispatchOutcome {
    let start = Instant::now();
    let outcome = handle_request_inner(engine, req, session, security);
    let client_key_len = outcome.client_key_len;
    crate::security::audit::log_request(
        &security.audit,
        &outcome.session,
        outcome.command,
        client_key_len,
        outcome.response.status,
        start.elapsed(),
    );
    DispatchOutcome {
        response: outcome.response,
        session: outcome.session,
        commit_seq: outcome.commit_seq,
    }
}

struct InnerOutcome {
    response: ResponseEnvelope,
    session: SessionState,
    command: Command,
    client_key_len: Option<usize>,
    /// Sequence number of a durable write to await before acking (PUT/DEL).
    commit_seq: Option<u64>,
}

fn handle_request_inner(
    engine: &Arc<Mutex<Engine>>,
    req: RequestEnvelope,
    session: SessionState,
    security: &SecurityRuntime,
) -> InnerOutcome {
    if !req.command.is_v1() {
        return InnerOutcome {
            response: ResponseEnvelope::error(Status::ProtocolError, "unimplemented"),
            session,
            command: req.command,
            client_key_len: None,
            commit_seq: None,
        };
    }

    match req.command {
        Command::SessionInit => handle_session_init(req, session, security),
        Command::SetContext => handle_set_context(req, session),
        Command::Ping => {
            if security.require_auth
                && !session.authenticated
                && !security.allow_unauthenticated_ping
            {
                return unauthorized(session, Command::Ping, "authentication required");
            }
            InnerOutcome {
                response: ResponseEnvelope::ok(vec![]),
                session,
                command: Command::Ping,
                client_key_len: None,
                commit_seq: None,
            }
        }
        Command::Put | Command::Get | Command::Del | Command::Stats => {
            if security.require_auth && !session.authenticated {
                return unauthorized(session, req.command, "authentication required");
            }
            match req.command {
                Command::Put => handle_put(engine, req, session, security),
                Command::Get => handle_get(engine, req, session, security),
                Command::Del => handle_del(engine, req, session, security),
                Command::Stats => {
                    // Brief lock: stats are in-memory counters, no disk I/O.
                    let json = engine.lock().unwrap().stats().to_json();
                    InnerOutcome {
                        response: ResponseEnvelope::ok(json),
                        session,
                        command: Command::Stats,
                        client_key_len: None,
                        commit_seq: None,
                    }
                }
                _ => unreachable!(),
            }
        }
        _ => InnerOutcome {
            response: ResponseEnvelope::error(Status::ProtocolError, "unimplemented"),
            session,
            command: req.command,
            client_key_len: None,
            commit_seq: None,
        },
    }
}

fn handle_session_init(
    req: RequestEnvelope,
    session: SessionState,
    security: &SecurityRuntime,
) -> InnerOutcome {
    if session.authenticated {
        return InnerOutcome {
            response: ResponseEnvelope::error(Status::ProtocolError, "already authenticated"),
            session,
            command: Command::SessionInit,
            client_key_len: None,
            commit_seq: None,
        };
    }
    if req.payload.is_empty() || req.payload.len() > 512 {
        return unauthorized(session, Command::SessionInit, "invalid api key");
    }
    match security.keys.load().verify(&req.payload) {
        Some(record) => {
            let new_session = SessionState::from_key_record(&record);
            InnerOutcome {
                response: ResponseEnvelope::ok(vec![]),
                session: new_session,
                command: Command::SessionInit,
                client_key_len: None,
                commit_seq: None,
            }
        }
        None => unauthorized(session, Command::SessionInit, "invalid api key"),
    }
}

fn handle_set_context(req: RequestEnvelope, session: SessionState) -> InnerOutcome {
    if !session.authenticated || !session.is_admin() {
        return forbidden(session, Command::SetContext, "admin role required");
    }
    if req.payload.len() != 16 {
        return InnerOutcome {
            response: ResponseEnvelope::error(Status::ProtocolError, "tenant must be 16 bytes"),
            session,
            command: Command::SetContext,
            client_key_len: None,
            commit_seq: None,
        };
    }
    let mut tenant = [0u8; 16];
    tenant.copy_from_slice(&req.payload);
    let mut new_session = session;
    new_session.tenant = tenant;
    InnerOutcome {
        response: ResponseEnvelope::ok(vec![]),
        session: new_session,
        command: Command::SetContext,
        client_key_len: None,
        commit_seq: None,
    }
}

fn handle_put(
    engine: &Arc<Mutex<Engine>>,
    req: RequestEnvelope,
    session: SessionState,
    security: &SecurityRuntime,
) -> InnerOutcome {
    if session.role == Some(KeyRole::ReadOnly) {
        return forbidden(session, Command::Put, "read-only key");
    }
    match PutPayload::decode(&req.payload) {
        Ok(p) => {
            // ACL check needs only the session; do it before touching the lock.
            if let Some(resp) = check_prefix_acl(&session, &p.key) {
                return InnerOutcome {
                    response: resp,
                    session,
                    command: Command::Put,
                    client_key_len: Some(p.key.len()),
                    commit_seq: None,
                };
            }
            let key = storage_key(&session, &p.key, security.legacy_single_tenant);
            let result = engine.lock().unwrap().put(key, p.value, p.expires_at);
            match result {
                Ok(seq) => InnerOutcome {
                    response: ResponseEnvelope::ok(seq.to_be_bytes().to_vec()),
                    session,
                    command: Command::Put,
                    client_key_len: Some(p.key.len()),
                    commit_seq: Some(seq),
                },
                Err(e) => InnerOutcome {
                    response: ResponseEnvelope::error(e.status(), &e.to_string()),
                    session,
                    command: Command::Put,
                    client_key_len: Some(p.key.len()),
                    commit_seq: None,
                },
            }
        }
        Err(e) => InnerOutcome {
            response: ResponseEnvelope::error(e.status(), &e.to_string()),
            session,
            command: Command::Put,
            client_key_len: None,
            commit_seq: None,
        },
    }
}

fn handle_get(
    engine: &Arc<Mutex<Engine>>,
    req: RequestEnvelope,
    session: SessionState,
    security: &SecurityRuntime,
) -> InnerOutcome {
    match KeyPayload::decode(&req.payload) {
        Ok(p) => {
            if let Some(resp) = check_prefix_acl(&session, &p.key) {
                return InnerOutcome {
                    response: resp,
                    session,
                    command: Command::Get,
                    client_key_len: Some(p.key.len()),
                    commit_seq: None,
                };
            }
            let key = storage_key(&session, &p.key, security.legacy_single_tenant);
            // Mirror the document read path: capture an owned snapshot under a
            // brief lock, release the engine mutex, then read off the snapshot
            // lock-free so a slow SSTable block read never serializes writers.
            let snapshot = engine.lock().unwrap().snapshot_owned();
            match snapshot.get(&key) {
                Ok(Some(value)) => InnerOutcome {
                    response: ResponseEnvelope::ok(value),
                    session,
                    command: Command::Get,
                    client_key_len: Some(p.key.len()),
                    commit_seq: None,
                },
                Ok(None) => InnerOutcome {
                    response: ResponseEnvelope::not_found(),
                    session,
                    command: Command::Get,
                    client_key_len: Some(p.key.len()),
                    commit_seq: None,
                },
                Err(e) => InnerOutcome {
                    response: ResponseEnvelope::error(e.status(), &e.to_string()),
                    session,
                    command: Command::Get,
                    client_key_len: Some(p.key.len()),
                    commit_seq: None,
                },
            }
        }
        Err(e) => InnerOutcome {
            response: ResponseEnvelope::error(e.status(), &e.to_string()),
            session,
            command: Command::Get,
            client_key_len: None,
            commit_seq: None,
        },
    }
}

fn handle_del(
    engine: &Arc<Mutex<Engine>>,
    req: RequestEnvelope,
    session: SessionState,
    security: &SecurityRuntime,
) -> InnerOutcome {
    if session.role == Some(KeyRole::ReadOnly) {
        return forbidden(session, Command::Del, "read-only key");
    }
    match KeyPayload::decode(&req.payload) {
        Ok(p) => {
            if let Some(resp) = check_prefix_acl(&session, &p.key) {
                return InnerOutcome {
                    response: resp,
                    session,
                    command: Command::Del,
                    client_key_len: Some(p.key.len()),
                    commit_seq: None,
                };
            }
            let key = storage_key(&session, &p.key, security.legacy_single_tenant);
            let result = engine.lock().unwrap().del(key);
            match result {
                Ok((deleted, seq)) => {
                    let mut payload = vec![if deleted { 1 } else { 0 }];
                    payload.extend_from_slice(&seq.to_be_bytes());
                    InnerOutcome {
                        response: ResponseEnvelope::ok(payload),
                        session,
                        command: Command::Del,
                        client_key_len: Some(p.key.len()),
                        commit_seq: Some(seq),
                    }
                }
                Err(e) => InnerOutcome {
                    response: ResponseEnvelope::error(e.status(), &e.to_string()),
                    session,
                    command: Command::Del,
                    client_key_len: Some(p.key.len()),
                    commit_seq: None,
                },
            }
        }
        Err(e) => InnerOutcome {
            response: ResponseEnvelope::error(e.status(), &e.to_string()),
            session,
            command: Command::Del,
            client_key_len: None,
            commit_seq: None,
        },
    }
}

fn storage_key(session: &SessionState, client_key: &[u8], legacy_single_tenant: bool) -> Vec<u8> {
    let use_legacy = legacy_single_tenant && session.tenant == [0u8; 16];
    if use_legacy {
        let mut key = Vec::with_capacity(1 + client_key.len());
        key.push(KS_USER);
        key.extend_from_slice(client_key);
        key
    } else {
        let mut key = Vec::with_capacity(1 + 16 + client_key.len());
        key.push(KS_USER);
        key.extend_from_slice(&session.tenant);
        key.extend_from_slice(client_key);
        key
    }
}

fn check_prefix_acl(session: &SessionState, client_key: &[u8]) -> Option<ResponseEnvelope> {
    if session.allowed_prefixes.is_empty() {
        return None;
    }
    let allowed = session
        .allowed_prefixes
        .iter()
        .any(|p| client_key.starts_with(p.as_bytes()));
    if allowed {
        None
    } else {
        Some(ResponseEnvelope::error(
            Status::Forbidden,
            "key prefix not allowed",
        ))
    }
}

fn unauthorized(session: SessionState, command: Command, msg: &str) -> InnerOutcome {
    InnerOutcome {
        response: ResponseEnvelope::error(Status::Unauthorized, msg),
        session,
        command,
        client_key_len: None,
        commit_seq: None,
    }
}

fn forbidden(session: SessionState, command: Command, msg: &str) -> InnerOutcome {
    InnerOutcome {
        response: ResponseEnvelope::error(Status::Forbidden, msg),
        session,
        command,
        client_key_len: None,
        commit_seq: None,
    }
}

pub fn read_request<R: Read>(reader: &mut R) -> Result<RequestEnvelope, EngineError> {
    use zydecodb_engine::frame::{ENVELOPE_HEADER_LEN, PROTO_VERSION};

    let mut header = [0u8; ENVELOPE_HEADER_LEN];
    reader.read_exact(&mut header)?;
    if header[0] != PROTO_VERSION {
        return Err(EngineError::Protocol(format!(
            "unsupported protocol version 0x{:02x}",
            header[0]
        )));
    }
    let (command, len) = RequestEnvelope::parse_header(&header)?;
    if len > zydecodb_engine::keys::MAX_VALUE_BYTES + 4096 {
        return Err(EngineError::Protocol("payload too large".into()));
    }
    let mut payload = vec![0u8; len];
    if len > 0 {
        reader.read_exact(&mut payload)?;
    }
    Ok(RequestEnvelope { command, payload })
}

pub fn write_response<W: Write>(
    writer: &mut W,
    resp: &ResponseEnvelope,
) -> Result<(), EngineError> {
    let bytes = resp.encode();
    writer.write_all(&bytes).map_err(EngineError::from)
}
