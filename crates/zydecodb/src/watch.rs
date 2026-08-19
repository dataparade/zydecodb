//! Collection change-stream subscriptions (Watch opcode).
//!
//! Primary-only, dedicated-connection streaming over retained WAL archives.
//! Delivery is at-least-once for fsynced document-body events only.

use crate::commit::CommitCoordinator;
use crate::security::runtime::SecurityRuntime;
use crate::security::SessionState;
use crate::shared::{SharedCatalog, SharedEngine};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use zydecodb_document::binary::ValueView;
use zydecodb_document::store::VK_ZDOC;
use zydecodb_document::wire::{self, WatchPayload, WATCH_OP_DELETE, WATCH_OP_UPSERT};
use zydecodb_engine::change_log::{self, LogicalChangeKind, ResumeToken};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{RequestEnvelope, ResponseEnvelope};
use zydecodb_engine::keys::KS_USER;

/// Global + per-tenant subscription capacity tracker.
#[derive(Default)]
pub struct WatchRegistry {
    global: AtomicUsize,
    per_tenant: Mutex<HashMap<[u8; 16], usize>>,
    /// Per-subscription resume cursor (id → after_seq) for consumer-lag gauge.
    subscriber_seqs: Mutex<HashMap<usize, u64>>,
    next_sub_id: AtomicUsize,
}

impl WatchRegistry {
    pub fn try_acquire(&self, tenant: &[u8; 16], max_global: usize, max_tenant: usize) -> bool {
        let cur = self.global.load(Ordering::SeqCst);
        if cur >= max_global {
            return false;
        }
        let mut map = self.per_tenant.lock().unwrap();
        let tenant_count = map.get(tenant).copied().unwrap_or(0);
        if tenant_count >= max_tenant {
            return false;
        }
        if self.global.fetch_add(1, Ordering::SeqCst) >= max_global {
            self.global.fetch_sub(1, Ordering::SeqCst);
            return false;
        }
        *map.entry(*tenant).or_insert(0) += 1;
        true
    }

    pub fn release(&self, tenant: &[u8; 16]) {
        self.global.fetch_sub(1, Ordering::SeqCst);
        let mut map = self.per_tenant.lock().unwrap();
        if let Some(c) = map.get_mut(tenant) {
            *c = c.saturating_sub(1);
            if *c == 0 {
                map.remove(tenant);
            }
        }
    }

    /// Register a subscriber cursor; returns an id for later updates/release.
    pub fn register_cursor(&self, after_seq: u64) -> usize {
        let id = self.next_sub_id.fetch_add(1, Ordering::Relaxed);
        self.subscriber_seqs.lock().unwrap().insert(id, after_seq);
        id
    }

    pub fn update_cursor(&self, id: usize, after_seq: u64) {
        if let Some(slot) = self.subscriber_seqs.lock().unwrap().get_mut(&id) {
            *slot = after_seq;
        }
    }

    pub fn unregister_cursor(&self, id: usize) {
        self.subscriber_seqs.lock().unwrap().remove(&id);
    }

    /// Slowest subscriber resume seq, if any subscribers are registered.
    pub fn min_resume_seq(&self) -> Option<u64> {
        self.subscriber_seqs.lock().unwrap().values().copied().min()
    }
}

fn update_consumer_lag(engine: &SharedEngine, registry: &WatchRegistry) {
    let guard = engine.read();
    let Some(m) = guard.metrics() else {
        return;
    };
    let latest = guard
        .change_log_manifest()
        .and_then(|man| man.latest_seq())
        .unwrap_or(0);
    let lag = match registry.min_resume_seq() {
        Some(min) => latest.saturating_sub(min),
        None => 0,
    };
    m.change_stream_consumer_lag_seqs.set(lag as i64);
}

fn tenant_prefix(session: &SessionState, legacy_single_tenant: bool) -> Vec<u8> {
    let use_legacy = legacy_single_tenant && session.tenant == [0u8; 16];
    if use_legacy {
        vec![KS_USER]
    } else {
        let mut p = Vec::with_capacity(1 + 16);
        p.push(KS_USER);
        p.extend_from_slice(&session.tenant);
        p
    }
}

/// Open a Watch subscription and stream events until disconnect/error.
pub fn run_watch_stream<S: Read + Write>(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    security: &SecurityRuntime,
    registry: &WatchRegistry,
    session: &SessionState,
    req: &RequestEnvelope,
    stream: &mut S,
    shutdown: &Arc<Mutex<bool>>,
) -> Result<(), Box<dyn std::error::Error>> {
    if security.require_auth && !session.authenticated {
        write_err(stream, Status::Unauthorized, "authentication required")?;
        return Ok(());
    }
    if security.read_only {
        write_err(stream, Status::Forbidden, "change streams are primary-only")?;
        return Ok(());
    }
    if !security.change_streams.enabled {
        write_err(stream, Status::Forbidden, "change streams disabled")?;
        return Ok(());
    }

    let payload = match WatchPayload::decode(&req.payload) {
        Ok(p) => p,
        Err(e) => {
            write_err(stream, Status::ProtocolError, &e.to_string())?;
            return Ok(());
        }
    };
    if let Some(resp) = crate::security::check_collection_prefix_acl(session, &payload.collection) {
        write_response(stream, &resp)?;
        return Ok(());
    }

    let tenant = session.tenant;
    let cfg_cs = &security.change_streams;
    if !registry.try_acquire(
        &tenant,
        cfg_cs.max_subscriptions,
        cfg_cs.max_subscriptions_per_tenant,
    ) {
        write_err(
            stream,
            Status::EngineBusy,
            "change stream subscription limit",
        )?;
        return Ok(());
    }
    {
        let guard = engine.read();
        if let Some(m) = guard.metrics() {
            m.change_stream_subscriptions.inc();
        }
    }
    let mut watch_guard = WatchGuard {
        registry,
        tenant,
        engine,
        cursor_id: None,
        reason: "peer_close",
    };

    let prefix = tenant_prefix(session, security.legacy_single_tenant);
    let (collection_id, database_id) = {
        let guard = engine.read();
        if guard.change_log_config().is_none() {
            watch_guard.reason = "disabled";
            write_err(
                stream,
                Status::Forbidden,
                "change stream archive not configured",
            )?;
            return Ok(());
        }
        let cat = catalog.read().unwrap();
        let Some(coll) = cat.collection(&prefix, &payload.collection) else {
            watch_guard.reason = "not_found";
            write_err(
                stream,
                Status::NotFound,
                &format!("collection not found: {}", payload.collection),
            )?;
            return Ok(());
        };
        (coll.id, guard.database_id_for_change_log())
    };

    let (mut after_seq, mut after_ord) = if payload.resume_token.is_empty() {
        (commit.durable_seq(), u32::MAX)
    } else {
        match ResumeToken::decode(&payload.resume_token) {
            Ok(token) => {
                if token.database_id != database_id {
                    watch_guard.reason = "bad_token";
                    write_err(
                        stream,
                        Status::ProtocolError,
                        "resume token database mismatch",
                    )?;
                    return Ok(());
                }
                if token.tenant_prefix != prefix {
                    watch_guard.reason = "forbidden";
                    write_err(stream, Status::Forbidden, "resume token tenant mismatch")?;
                    return Ok(());
                }
                if token.collection_id != collection_id {
                    watch_guard.reason = "forbidden";
                    write_err(
                        stream,
                        Status::Forbidden,
                        "resume token collection mismatch",
                    )?;
                    return Ok(());
                }
                let earliest = {
                    let guard = engine.read();
                    guard
                        .change_log_manifest()
                        .and_then(|m| m.earliest_seq())
                        .unwrap_or(0)
                };
                if token.seq < earliest {
                    watch_guard.reason = "retention_gap";
                    write_err(
                        stream,
                        Status::Conflict,
                        "resume token older than retained history",
                    )?;
                    return Ok(());
                }
                (token.seq, token.op_ordinal)
            }
            Err(e) => {
                watch_guard.reason = "bad_token";
                write_err(stream, Status::ProtocolError, &e.to_string())?;
                return Ok(());
            }
        }
    };

    let start_token = ResumeToken {
        database_id,
        tenant_prefix: prefix.clone(),
        collection_id,
        seq: after_seq,
        op_ordinal: after_ord,
    };
    write_ok(stream, &wire::encode_watch_ack(&start_token.encode()))?;

    let heartbeat = Duration::from_millis(cfg_cs.heartbeat_ms.max(1));
    let write_timeout = Duration::from_millis(cfg_cs.write_timeout_ms.max(1));
    let mut last_frame = Instant::now();
    let mut cursor_token = start_token;
    let cursor_id = registry.register_cursor(after_seq);
    watch_guard.cursor_id = Some(cursor_id);
    update_consumer_lag(engine, registry);

    loop {
        if *shutdown.lock().unwrap() {
            watch_guard.reason = "shutdown";
            return Ok(());
        }
        if session.authenticated {
            let store = security.keys.load();
            if !store.is_session_valid(session) {
                watch_guard.reason = "revoked";
                write_err(stream, Status::Unauthorized, "session revoked")?;
                return Ok(());
            }
        }

        {
            let cat = catalog.read().unwrap();
            if cat.collection(&prefix, &payload.collection).is_none() {
                watch_guard.reason = "collection_removed";
                write_err(stream, Status::NotFound, "collection removed")?;
                return Ok(());
            }
        }

        let changes = {
            let guard = engine.read();
            let Some(cfg) = guard.change_log_config() else {
                watch_guard.reason = "disabled";
                write_err(stream, Status::Forbidden, "change stream archive disabled")?;
                return Ok(());
            };
            let Some(manifest) = guard.change_log_manifest() else {
                watch_guard.reason = "disabled";
                write_err(stream, Status::Forbidden, "change stream archive disabled")?;
                return Ok(());
            };
            if let Some(earliest) = manifest.earliest_seq() {
                if after_seq < earliest {
                    watch_guard.reason = "retention_gap";
                    write_err(
                        stream,
                        Status::Conflict,
                        "fell behind change stream retention",
                    )?;
                    return Ok(());
                }
            }
            let active = guard.active_wal_path();
            change_log::iter_logical_changes_after(
                cfg,
                manifest,
                Some(&active),
                &prefix,
                collection_id,
                after_seq,
                after_ord,
            )?
        };

        let durable = commit.durable_seq();
        let mut emitted = 0usize;
        for change in changes {
            if change.seq > durable {
                break;
            }
            let body = match change.kind {
                LogicalChangeKind::Upsert => match stored_to_json(&change.stored_value) {
                    Ok(b) => b,
                    Err(e) => {
                        write_err(stream, Status::Error, &e)?;
                        return Ok(());
                    }
                },
                LogicalChangeKind::Delete => Vec::new(),
            };
            let op = match change.kind {
                LogicalChangeKind::Upsert => WATCH_OP_UPSERT,
                LogicalChangeKind::Delete => WATCH_OP_DELETE,
            };
            let token = ResumeToken {
                database_id,
                tenant_prefix: prefix.clone(),
                collection_id,
                seq: change.seq,
                op_ordinal: change.op_ordinal,
            };
            let frame = wire::encode_watch_event(&token.encode(), op, &change.doc_id, &body);
            if !write_ok_deadline(stream, &frame, write_timeout)? {
                watch_guard.reason = "slow_consumer";
                write_err(stream, Status::EngineBusy, "slow change stream consumer")?;
                return Ok(());
            }
            if let Some(m) = engine.read().metrics() {
                m.change_stream_events_total.inc();
            }
            after_seq = change.seq;
            after_ord = change.op_ordinal;
            cursor_token = token;
            last_frame = Instant::now();
            emitted += 1;
        }
        if emitted > 0 {
            registry.update_cursor(cursor_id, after_seq);
        }
        update_consumer_lag(engine, registry);

        if emitted == 0 {
            if last_frame.elapsed() >= heartbeat {
                let frame = wire::encode_watch_heartbeat(&cursor_token.encode());
                if !write_ok_deadline(stream, &frame, write_timeout)? {
                    watch_guard.reason = "slow_consumer";
                    write_err(stream, Status::EngineBusy, "slow change stream consumer")?;
                    return Ok(());
                }
                if let Some(m) = engine.read().metrics() {
                    m.change_stream_heartbeats_total.inc();
                }
                last_frame = Instant::now();
            }
            let _ =
                commit.wait_durable_advance(after_seq, heartbeat.min(Duration::from_millis(500)));
        }
    }
}

struct WatchGuard<'a> {
    registry: &'a WatchRegistry,
    tenant: [u8; 16],
    engine: &'a SharedEngine,
    cursor_id: Option<usize>,
    reason: &'static str,
}

impl Drop for WatchGuard<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.cursor_id.take() {
            self.registry.unregister_cursor(id);
        }
        self.registry.release(&self.tenant);
        update_consumer_lag(self.engine, self.registry);
        let guard = self.engine.read();
        if let Some(m) = guard.metrics() {
            m.change_stream_subscriptions.dec();
            m.change_stream_disconnects_total
                .with_label_values(&[self.reason])
                .inc();
        }
    }
}

fn stored_to_json(stored: &[u8]) -> Result<Vec<u8>, String> {
    let Some((&kind, payload)) = stored.split_first() else {
        return Err("empty stored document".into());
    };
    if kind == VK_ZDOC {
        let value = ValueView::new(payload)
            .to_value()
            .map_err(|e| e.to_string())?;
        return serde_json::to_vec(&value).map_err(|e| e.to_string());
    }
    Ok(payload.to_vec())
}

fn write_ok<S: Write>(stream: &mut S, payload: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
    write_response(stream, &ResponseEnvelope::ok(payload.to_vec()))
}

fn write_ok_deadline<S: Write>(
    stream: &mut S,
    payload: &[u8],
    _timeout: Duration,
) -> Result<bool, Box<dyn std::error::Error>> {
    match write_response(stream, &ResponseEnvelope::ok(payload.to_vec())) {
        Ok(()) => Ok(true),
        Err(e) => {
            let msg = e.to_string();
            if msg.contains("timed out") || msg.contains("WouldBlock") {
                Ok(false)
            } else {
                Err(e)
            }
        }
    }
}

fn write_err<S: Write>(
    stream: &mut S,
    status: Status,
    msg: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    write_response(stream, &ResponseEnvelope::error(status, msg))
}

fn write_response<S: Write>(
    stream: &mut S,
    resp: &ResponseEnvelope,
) -> Result<(), Box<dyn std::error::Error>> {
    stream.write_all(&resp.encode())?;
    stream.flush()?;
    Ok(())
}
