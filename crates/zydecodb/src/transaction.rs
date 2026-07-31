//! Bounded per-connection atomic transactions.
//!
//! Isolation: begin-snapshot + staged overlay (direct-key read-your-writes).
//! Commit: validate revisions/uniques under the write lock, then one
//! [`Engine::write_batch`]. Not general-purpose MVCC.

use crate::commit::CommitCoordinator;
use crate::security::keys::KeyRole;
use crate::security::{SecurityRuntime, SessionState};
use crate::shared::{SharedCatalog, SharedEngine};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::{Duration, Instant};
use zydecodb_document::catalog::Catalog;
use zydecodb_document::error::{DocError, DocResult};
use zydecodb_document::update::UpdateDoc;
use zydecodb_document::{query, store, wire};
use zydecodb_engine::engine::{BatchOp, Engine};
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope};
use zydecodb_engine::keys::{KS_USER, MAX_BATCH_KEYS};
use zydecodb_engine::SnapshotHandle;

/// Maximum logical staged write operations per transaction.
pub const MAX_STAGED_OPS: usize = 256;
/// Maximum staged value body bytes (document bodies + KV values).
pub const MAX_STAGED_BYTES: usize = 32 * 1024 * 1024;
/// Hard lifetime for an open transaction.
pub const MAX_TX_DURATION: Duration = Duration::from_secs(30);

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
struct DocKey {
    collection: String,
    doc_id: Vec<u8>,
}

#[derive(Clone)]
enum StagedDocState {
    Upsert {
        /// Final ZDoc body bytes (without value_kind prefix).
        zdoc: Vec<u8>,
        json: Value,
        expires_at: u64,
    },
    Deleted,
}

#[derive(Clone)]
struct StagedDocument {
    /// Revision/existence observed at first touch from the begin snapshot.
    base_revision: Option<u64>,
    /// Explicit ifMatch from the first conditional write that touched this doc.
    expected_if_match: Option<u64>,
    state: StagedDocState,
}

#[derive(Clone)]
enum StagedKv {
    Put { value: Vec<u8>, expires_at: u64 },
    Del,
}

struct KvBase {
    /// Existence+revision at first touch.
    base_revision: Option<u64>,
}

/// Per-connection transaction state. Lives only in the connection thread.
pub struct TransactionState {
    pub id: u64,
    deadline: Instant,
    snapshot: SnapshotHandle,
    session_fingerprint: SessionFingerprint,
    tenant_prefix: Vec<u8>,
    documents: BTreeMap<DocKey, StagedDocument>,
    kv: BTreeMap<Vec<u8>, StagedKv>,
    kv_bases: BTreeMap<Vec<u8>, KvBase>,
    /// Logical write operations accepted (coalesced keys still count once each write).
    logical_ops: u32,
    staged_bytes: usize,
}

#[derive(Clone, PartialEq, Eq)]
struct SessionFingerprint {
    authenticated: bool,
    key_id: Option<String>,
    secret_hash: Option<String>,
    role: Option<KeyRole>,
    tenant: [u8; 16],
    allowed_prefixes: Vec<String>,
}

impl SessionFingerprint {
    fn from_session(s: &SessionState) -> Self {
        Self {
            authenticated: s.authenticated,
            key_id: s.key_id.clone(),
            secret_hash: s.secret_hash.clone(),
            role: s.role,
            tenant: s.tenant,
            allowed_prefixes: s.allowed_prefixes.clone(),
        }
    }

    fn matches(&self, s: &SessionState) -> bool {
        self == &Self::from_session(s)
    }
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

fn storage_key(session: &SessionState, client_key: &[u8], legacy_single_tenant: bool) -> Vec<u8> {
    let mut key = tenant_prefix(session, legacy_single_tenant);
    key.extend_from_slice(client_key);
    key
}

fn err_response(e: &DocError) -> ResponseEnvelope {
    ResponseEnvelope::error(e.status(), &e.to_string())
}

fn aborted(msg: &str) -> ResponseEnvelope {
    ResponseEnvelope::error(Status::ProtocolError, msg)
}

fn stage_ack(tx: &TransactionState) -> ResponseEnvelope {
    ResponseEnvelope::ok(wire::encode_stage_ack(
        tx.logical_ops,
        tx.estimated_keys() as u32,
    ))
}

fn with_tx_metrics(engine: &SharedEngine, f: impl FnOnce(&zydecodb_engine::metrics::Metrics)) {
    if let Some(m) = engine.read().metrics() {
        f(m);
    }
}

impl TransactionState {
    pub fn begin(
        engine: &SharedEngine,
        session: &SessionState,
        security: &SecurityRuntime,
        next_id: &mut u64,
    ) -> Result<Self, ResponseEnvelope> {
        if security.require_auth && !session.authenticated {
            return Err(ResponseEnvelope::error(
                Status::Unauthorized,
                "authentication required",
            ));
        }
        if session.role == Some(KeyRole::ReadOnly) {
            return Err(ResponseEnvelope::error(Status::Forbidden, "read-only key"));
        }
        if security.read_only {
            return Err(ResponseEnvelope::error(
                Status::Forbidden,
                "read replica is read-only",
            ));
        }
        let id = *next_id;
        *next_id = next_id.wrapping_add(1).max(1);
        let snapshot = engine.read().snapshot_owned();
        let now = Instant::now();
        Ok(Self {
            id,
            deadline: now + MAX_TX_DURATION,
            snapshot,
            session_fingerprint: SessionFingerprint::from_session(session),
            tenant_prefix: tenant_prefix(session, security.legacy_single_tenant),
            documents: BTreeMap::new(),
            kv: BTreeMap::new(),
            kv_bases: BTreeMap::new(),
            logical_ops: 0,
            staged_bytes: 0,
        })
    }

    pub fn expired(&self) -> bool {
        Instant::now() >= self.deadline
    }

    fn estimated_keys(&self) -> usize {
        // Conservative: each doc write ≈ 1 body + many index keys; KV = 1.
        let n = self
            .kv
            .len()
            .saturating_add(self.documents.len().saturating_mul(1 + 128));
        n.min(MAX_BATCH_KEYS.saturating_add(1))
    }

    fn bump_logical(&mut self, add_bytes: usize) -> Result<(), ResponseEnvelope> {
        if self.logical_ops as usize >= MAX_STAGED_OPS {
            return Err(ResponseEnvelope::error(
                Status::InvalidValue,
                "transaction exceeded max staged operations",
            ));
        }
        if self.staged_bytes.saturating_add(add_bytes) > MAX_STAGED_BYTES {
            return Err(ResponseEnvelope::error(
                Status::InvalidValue,
                "transaction exceeded max staged bytes",
            ));
        }
        self.logical_ops += 1;
        self.staged_bytes = self.staged_bytes.saturating_add(add_bytes);
        Ok(())
    }

    fn ensure_session(
        &self,
        session: &SessionState,
        security: &SecurityRuntime,
    ) -> Result<(), ResponseEnvelope> {
        if !self.session_fingerprint.matches(session) {
            return Err(aborted("transaction aborted: session changed"));
        }
        if security.require_auth && !session.authenticated {
            return Err(ResponseEnvelope::error(
                Status::Unauthorized,
                "authentication required",
            ));
        }
        if session.role == Some(KeyRole::ReadOnly) {
            return Err(ResponseEnvelope::error(Status::Forbidden, "read-only key"));
        }
        if security.read_only {
            return Err(ResponseEnvelope::error(
                Status::Forbidden,
                "read replica is read-only",
            ));
        }
        Ok(())
    }
}

/// Handle Begin / Commit / Rollback. On Commit success, `tx` is cleared and
/// durability is awaited via `commit`.
#[allow(clippy::too_many_arguments)]
pub fn handle_control(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    req: &RequestEnvelope,
    session: &SessionState,
    security: &SecurityRuntime,
    tx: &mut Option<TransactionState>,
    next_tx_id: &mut u64,
) -> ResponseEnvelope {
    // Auth gate before any control-path side effects (including no-op Rollback).
    if security.require_auth && !session.authenticated {
        return ResponseEnvelope::error(Status::Unauthorized, "authentication required");
    }
    match req.command {
        Command::Begin => {
            if tx.is_some() {
                return aborted("transaction already open");
            }
            if !req.payload.is_empty() {
                return ResponseEnvelope::error(
                    Status::ProtocolError,
                    "Begin payload must be empty",
                );
            }
            match TransactionState::begin(engine, session, security, next_tx_id) {
                Ok(state) => {
                    let resp = ResponseEnvelope::ok(wire::encode_begin_response(
                        state.id,
                        state.snapshot.seq_upper(),
                    ));
                    *tx = Some(state);
                    with_tx_metrics(engine, |m| m.tx_begin_total.inc());
                    resp
                }
                Err(e) => e,
            }
        }
        Command::Rollback => {
            if !req.payload.is_empty() {
                return ResponseEnvelope::error(
                    Status::ProtocolError,
                    "Rollback payload must be empty",
                );
            }
            if tx.take().is_some() {
                with_tx_metrics(engine, |m| m.tx_abort_total.inc());
            }
            ResponseEnvelope::ok(vec![])
        }
        Command::Commit => {
            if !req.payload.is_empty() {
                return ResponseEnvelope::error(
                    Status::ProtocolError,
                    "Commit payload must be empty",
                );
            }
            let Some(state) = tx.take() else {
                return aborted("no active transaction");
            };
            if state.expired() {
                with_tx_metrics(engine, |m| m.tx_timeout_total.inc());
                return aborted("transaction aborted: timed out");
            }
            if let Err(e) = state.ensure_session(session, security) {
                with_tx_metrics(engine, |m| m.tx_abort_total.inc());
                return e;
            }
            match commit_transaction(engine, catalog, &state) {
                Ok(seq) => {
                    commit.commit(seq, false);
                    with_tx_metrics(engine, |m| m.tx_commit_total.inc());
                    ResponseEnvelope::ok(wire::encode_commit_response(seq))
                }
                Err(e) => {
                    // Transaction is already cleared (taken); surface error.
                    with_tx_metrics(engine, |m| m.tx_abort_total.inc());
                    err_response(&e)
                }
            }
        }
        _ => ResponseEnvelope::error(Status::ProtocolError, "unimplemented"),
    }
}

/// Stage or read inside an open transaction. Returns `None` when the command
/// should fall through to the normal auto-commit path (Ping/Stats).
pub fn handle_in_transaction(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    req: &RequestEnvelope,
    session: &SessionState,
    security: &SecurityRuntime,
    tx: &mut TransactionState,
) -> Option<ResponseEnvelope> {
    if tx.expired() {
        with_tx_metrics(engine, |m| m.tx_timeout_total.inc());
        return Some(aborted("transaction aborted: timed out"));
    }
    if let Err(e) = tx.ensure_session(session, security) {
        return Some(e);
    }
    if !req.command.is_transaction_allowed() {
        return Some(aborted(
            "transaction aborted: command not allowed inside a transaction",
        ));
    }
    match req.command {
        Command::Ping | Command::Stats => None,
        Command::Get => Some(tx_get(tx, session, security, &req.payload)),
        Command::Put => Some(tx_put(tx, session, security, &req.payload)),
        Command::Del => Some(tx_del(tx, session, security, &req.payload)),
        Command::DocGetRev => Some(tx_doc_get_rev(tx, catalog, &req.payload)),
        Command::DocPut => Some(tx_doc_put(tx, catalog, session, &req.payload, None)),
        Command::DocPutIfMatch => Some(tx_doc_put_if_match(tx, catalog, session, &req.payload)),
        Command::DocDel => Some(tx_doc_del(tx, catalog, session, &req.payload)),
        Command::DocUpdateIfMatch => {
            Some(tx_doc_update_if_match(tx, catalog, session, &req.payload))
        }
        _ => Some(aborted(
            "transaction aborted: command not allowed inside a transaction",
        )),
    }
}

fn tx_get(
    tx: &TransactionState,
    session: &SessionState,
    security: &SecurityRuntime,
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match KeyPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return ResponseEnvelope::error(e.status(), &e.to_string()),
    };
    if let Some(resp) = crate::security::check_key_prefix_acl(session, &p.key) {
        return resp;
    }
    let key = storage_key(session, &p.key, security.legacy_single_tenant);
    if let Some(staged) = tx.kv.get(&key) {
        return match staged {
            StagedKv::Put { value, .. } => ResponseEnvelope::ok(value.clone()),
            StagedKv::Del => ResponseEnvelope::not_found(),
        };
    }
    match tx.snapshot.get(&key) {
        Ok(Some(v)) => ResponseEnvelope::ok(v),
        Ok(None) => ResponseEnvelope::not_found(),
        Err(e) => ResponseEnvelope::error(e.status(), &e.to_string()),
    }
}

fn tx_put(
    tx: &mut TransactionState,
    session: &SessionState,
    security: &SecurityRuntime,
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match PutPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return ResponseEnvelope::error(e.status(), &e.to_string()),
    };
    if let Some(resp) = crate::security::check_key_prefix_acl(session, &p.key) {
        return resp;
    }
    let key = storage_key(session, &p.key, security.legacy_single_tenant);
    if let Err(e) = ensure_kv_base(tx, &key) {
        return err_response(&e);
    }
    if let Err(e) = tx.bump_logical(p.value.len()) {
        return e;
    }
    tx.kv.insert(
        key,
        StagedKv::Put {
            value: p.value,
            expires_at: p.expires_at,
        },
    );
    stage_ack(tx)
}

fn tx_del(
    tx: &mut TransactionState,
    session: &SessionState,
    security: &SecurityRuntime,
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match KeyPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return ResponseEnvelope::error(e.status(), &e.to_string()),
    };
    if let Some(resp) = crate::security::check_key_prefix_acl(session, &p.key) {
        return resp;
    }
    let key = storage_key(session, &p.key, security.legacy_single_tenant);
    if let Err(e) = ensure_kv_base(tx, &key) {
        return err_response(&e);
    }
    if let Err(e) = tx.bump_logical(0) {
        return e;
    }
    tx.kv.insert(key, StagedKv::Del);
    stage_ack(tx)
}

fn ensure_kv_base(tx: &mut TransactionState, key: &[u8]) -> DocResult<()> {
    if tx.kv_bases.contains_key(key) {
        return Ok(());
    }
    let base = tx.snapshot.get_with_seq(key)?.map(|(_, rev)| rev);
    tx.kv_bases.insert(
        key.to_vec(),
        KvBase {
            base_revision: base,
        },
    );
    Ok(())
}

fn tx_doc_get_rev(
    tx: &TransactionState,
    catalog: &SharedCatalog,
    payload: &[u8],
) -> ResponseEnvelope {
    let (collection, doc_id) = match wire::decode_doc_get_rev(payload) {
        Ok(v) => v,
        Err(e) => return err_response(&e),
    };
    let dk = DocKey {
        collection: collection.clone(),
        doc_id: doc_id.clone(),
    };
    if let Some(staged) = tx.documents.get(&dk) {
        return match &staged.state {
            StagedDocState::Deleted => ResponseEnvelope::not_found(),
            StagedDocState::Upsert { json, .. } => {
                let body = serde_json::to_vec(json).unwrap_or_default();
                // Uncommitted staged body: revision 0 (not a durable revision).
                ResponseEnvelope::ok(wire::encode_doc_get_rev_response(&body, 0))
            }
        };
    }
    let cat = catalog.read().unwrap();
    match query::get_by_id_with_revision(
        &tx.snapshot,
        &cat,
        &tx.tenant_prefix,
        &collection,
        &doc_id,
    ) {
        Ok(Some((body, rev))) => {
            ResponseEnvelope::ok(wire::encode_doc_get_rev_response(&body, rev))
        }
        Ok(None) => ResponseEnvelope::not_found(),
        Err(e) => err_response(&e),
    }
}

fn tx_doc_put(
    tx: &mut TransactionState,
    catalog: &SharedCatalog,
    session: &SessionState,
    payload: &[u8],
    if_match: Option<u64>,
) -> ResponseEnvelope {
    let (collection, doc_id, body, expires_at, relaxed) = if if_match.is_some() {
        let p = match wire::DocPutIfMatchPayload::decode(payload) {
            Ok(p) => p,
            Err(e) => return err_response(&e),
        };
        (p.collection, p.doc_id, p.body, p.expires_at, p.relaxed)
    } else {
        let p = match wire::DocPutPayload::decode(payload) {
            Ok(p) => p,
            Err(e) => return err_response(&e),
        };
        (p.collection, p.doc_id, p.body, p.expires_at, p.relaxed)
    };
    if relaxed {
        return aborted("relaxed durability not allowed inside a transaction");
    }

    if let Some(resp) = crate::security::check_collection_prefix_acl(session, &collection) {
        return resp;
    }

    let json_val: Value = match serde_json::from_slice(&body) {
        Ok(v) => v,
        Err(e) => return err_response(&DocError::InvalidJson(e.to_string())),
    };
    let zdoc = zydecodb_document::binary::ZDocBuilder::from_value(&json_val);

    {
        let cat = catalog.read().unwrap();
        if cat.collection(&tx.tenant_prefix, &collection).is_none() {
            return err_response(&DocError::CollectionNotFound(collection));
        }
    }

    let dk = DocKey {
        collection: collection.clone(),
        doc_id: doc_id.clone(),
    };
    if let Err(e) = ensure_doc_base(tx, catalog, &dk, if_match) {
        return err_response(&e);
    }
    if let Err(e) = tx.bump_logical(zdoc.len()) {
        return e;
    }
    let entry = tx.documents.get_mut(&dk).expect("base ensured");
    if let Some(expected) = if_match {
        if entry.expected_if_match.is_none() {
            entry.expected_if_match = Some(expected);
        }
    }
    entry.state = StagedDocState::Upsert {
        zdoc,
        json: json_val,
        expires_at,
    };
    stage_ack(tx)
}

fn tx_doc_put_if_match(
    tx: &mut TransactionState,
    catalog: &SharedCatalog,
    session: &SessionState,
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match wire::DocPutIfMatchPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return err_response(&e),
    };
    tx_doc_put(tx, catalog, session, payload, Some(p.if_match))
}

fn tx_doc_del(
    tx: &mut TransactionState,
    catalog: &SharedCatalog,
    session: &SessionState,
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match wire::DocDelPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return err_response(&e),
    };
    if let Some(resp) = crate::security::check_collection_prefix_acl(session, &p.collection) {
        return resp;
    }
    {
        let cat = catalog.read().unwrap();
        if cat.collection(&tx.tenant_prefix, &p.collection).is_none() {
            return err_response(&DocError::CollectionNotFound(p.collection));
        }
    }
    let dk = DocKey {
        collection: p.collection,
        doc_id: p.doc_id,
    };
    if let Err(e) = ensure_doc_base(tx, catalog, &dk, None) {
        return err_response(&e);
    }
    if let Err(e) = tx.bump_logical(0) {
        return e;
    }
    tx.documents.get_mut(&dk).expect("base").state = StagedDocState::Deleted;
    stage_ack(tx)
}

fn tx_doc_update_if_match(
    tx: &mut TransactionState,
    catalog: &SharedCatalog,
    session: &SessionState,
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match wire::DocUpdateIfMatchPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return err_response(&e),
    };
    if p.relaxed {
        return aborted("relaxed durability not allowed inside a transaction");
    }
    if let Some(resp) = crate::security::check_collection_prefix_acl(session, &p.collection) {
        return resp;
    }
    let upd = match UpdateDoc::parse_bytes(&p.update) {
        Ok(u) => u,
        Err(e) => return err_response(&e),
    };
    {
        let cat = catalog.read().unwrap();
        if cat.collection(&tx.tenant_prefix, &p.collection).is_none() {
            return err_response(&DocError::CollectionNotFound(p.collection));
        }
    }
    let dk = DocKey {
        collection: p.collection.clone(),
        doc_id: p.doc_id.clone(),
    };
    if let Err(e) = ensure_doc_base(tx, catalog, &dk, Some(p.if_match)) {
        return err_response(&e);
    }

    let mut body = match tx.documents.get(&dk).map(|s| &s.state) {
        Some(StagedDocState::Upsert { json, .. }) => json.clone(),
        Some(StagedDocState::Deleted) | None => {
            return err_response(&DocError::StaleRevision);
        }
    };

    if let Err(e) = upd.apply(&mut body) {
        return err_response(&e);
    }
    let zdoc = zydecodb_document::binary::ZDocBuilder::from_value(&body);
    if let Err(e) = tx.bump_logical(zdoc.len()) {
        return e;
    }
    let entry = tx.documents.get_mut(&dk).unwrap();
    if entry.expected_if_match.is_none() {
        entry.expected_if_match = Some(p.if_match);
    }
    entry.state = StagedDocState::Upsert {
        zdoc,
        json: body,
        expires_at: 0,
    };
    stage_ack(tx)
}

/// Ensure a document entry exists with base_revision from the begin snapshot.
/// Conditional writes validate ifMatch against the begin-snapshot revision.
fn ensure_doc_base(
    tx: &mut TransactionState,
    catalog: &SharedCatalog,
    dk: &DocKey,
    if_match: Option<u64>,
) -> DocResult<()> {
    if let Some(existing) = tx.documents.get(dk) {
        if let Some(expected) = if_match {
            match existing.base_revision {
                Some(rev) if rev == expected => {}
                _ => return Err(DocError::StaleRevision),
            }
            // Also require staged state hasn't deleted the doc before a later ifMatch write
            // unless we're replacing after delete (allowed for Put).
        }
        return Ok(());
    }

    let cat = catalog.read().unwrap();
    let base = query::get_by_id_with_revision(
        &tx.snapshot,
        &cat,
        &tx.tenant_prefix,
        &dk.collection,
        &dk.doc_id,
    )?;
    let base_revision = base.as_ref().map(|(_, r)| *r);
    if let Some(expected) = if_match {
        match base_revision {
            Some(rev) if rev == expected => {}
            _ => return Err(DocError::StaleRevision),
        }
    }

    // Seed with snapshot body so subsequent updates can apply without another read.
    let state = match base {
        Some((json_bytes, _)) => {
            let json: Value = serde_json::from_slice(&json_bytes)
                .map_err(|e| DocError::InvalidJson(e.to_string()))?;
            let zdoc = zydecodb_document::binary::ZDocBuilder::from_value(&json);
            StagedDocState::Upsert {
                zdoc,
                json,
                expires_at: 0,
            }
        }
        None => {
            // Missing doc: only unconditional put may proceed; ifMatch already failed.
            // Use Deleted as "absent" sentinel until a put arrives — but then Put
            // overwrites. For first Put on missing doc, we insert Upsert directly
            // in caller after ensure. Use a tombstone-absent marker via Deleted
            // only when base is None and we're about to put — actually put will
            // overwrite state. Seed as Deleted meaning "was absent".
            StagedDocState::Deleted
        }
    };

    tx.documents.insert(
        dk.clone(),
        StagedDocument {
            base_revision,
            expected_if_match: if_match,
            state,
        },
    );
    Ok(())
}

fn commit_transaction(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    tx: &TransactionState,
) -> DocResult<u64> {
    let (result, slowdown) = {
        let cat = catalog.read().unwrap();
        let mut guard = engine.write();
        let r = commit_under_lock(&mut guard, &cat, tx);
        let s = guard.take_write_slowdown();
        (r, s)
    };
    Engine::apply_write_slowdown(slowdown);
    result
}

fn commit_under_lock(
    engine: &mut Engine,
    catalog: &Catalog,
    tx: &TransactionState,
) -> DocResult<u64> {
    // 1. Validate document revisions against current committed state.
    for (dk, staged) in &tx.documents {
        let current = store::doc_revision(
            engine,
            catalog,
            &tx.tenant_prefix,
            &dk.collection,
            &dk.doc_id,
        )?;
        if current != staged.base_revision {
            return Err(DocError::StaleRevision);
        }
        if let Some(expected) = staged.expected_if_match {
            match current {
                Some(rev) if rev == expected => {}
                _ => return Err(DocError::StaleRevision),
            }
        }
        // Skip no-op seed entries that were never written (Deleted with no base
        // and never upgraded, or Upsert identical seed without a logical write).
        // We track logical writes globally; every entry in `documents` was touched
        // by a write (ensure_doc_base is only called from write paths).
        let _ = staged;
    }

    // 2. Validate KV bases.
    for (key, base) in &tx.kv_bases {
        let current = engine.get_with_seq(key)?.map(|(_, r)| r);
        if current != base.base_revision {
            return Err(DocError::StaleRevision);
        }
    }

    // 3. Transaction-wide unique-index validation.
    validate_unique_indexes(engine, catalog, tx)?;

    // 4. Build physical batch ops.
    let mut key_map: BTreeMap<Vec<u8>, BatchOp> = BTreeMap::new();

    for (dk, staged) in &tx.documents {
        match &staged.state {
            StagedDocState::Deleted => {
                // Only emit delete if the document existed at begin (or was created
                // then deleted in-tx — if base was None and deleted, no-op).
                if staged.base_revision.is_none() {
                    // Was absent and still deleted → nothing to write.
                    continue;
                }
                let ops = store::delete_ops(
                    engine,
                    catalog,
                    &tx.tenant_prefix,
                    &dk.collection,
                    &dk.doc_id,
                    None,
                )?;
                for op in ops {
                    insert_op(&mut key_map, op)?;
                }
            }
            StagedDocState::Upsert {
                zdoc,
                json: _,
                expires_at,
            } => {
                // If we only seeded from snapshot and never changed... still a
                // write was recorded. Re-upserting identical content is harmless
                // but bumps revision — acceptable for v1 (every stage write is intent).
                // Skip pure seed that is Deleted→absent handled above.
                // If base was None and state is Upsert, this is an insert.
                // If base was Some and state is Upsert seeded then overwritten, write.
                let old_doc = store::current_json_body(
                    engine,
                    catalog,
                    &tx.tenant_prefix,
                    &dk.collection,
                    &dk.doc_id,
                )?;
                let ops = store::upsert_ops_without_unique(
                    engine,
                    catalog,
                    &tx.tenant_prefix,
                    &dk.collection,
                    &dk.doc_id,
                    zdoc,
                    true,
                    *expires_at,
                    old_doc.as_ref(),
                )?;
                for op in ops {
                    insert_op(&mut key_map, op)?;
                }
            }
        }
    }

    for (key, staged) in &tx.kv {
        let op = match staged {
            StagedKv::Put { value, expires_at } => BatchOp::Put {
                key: key.clone(),
                value: value.clone(),
                expires_at: *expires_at,
            },
            StagedKv::Del => BatchOp::Del { key: key.clone() },
        };
        insert_op(&mut key_map, op)?;
    }

    if key_map.is_empty() {
        // Empty commit: still succeeds with current seq (no WAL write).
        return Ok(engine.snapshot_owned().seq_upper());
    }
    if key_map.len() > MAX_BATCH_KEYS {
        return Err(DocError::BatchTooLarge(key_map.len()));
    }
    let ops: Vec<BatchOp> = key_map.into_values().collect();
    Ok(engine.write_batch(ops)?)
}

fn insert_op(map: &mut BTreeMap<Vec<u8>, BatchOp>, op: BatchOp) -> DocResult<()> {
    let key = match &op {
        BatchOp::Put { key, .. } | BatchOp::Del { key } => key.clone(),
    };
    if map.contains_key(&key) {
        return Err(DocError::Protocol(format!(
            "duplicate storage key in transaction batch ({} bytes)",
            key.len()
        )));
    }
    map.insert(key, op);
    Ok(())
}

fn validate_unique_indexes(
    engine: &mut Engine,
    catalog: &Catalog,
    tx: &TransactionState,
) -> DocResult<()> {
    // claimed: (collection, index_id, enc) -> doc_id of final staged owner
    let mut claimed: HashMap<(String, u32, Vec<u8>), Vec<u8>> = HashMap::new();
    // final_claims[doc] = set of (collection, index_id, enc) still held after staging
    let mut final_claims: BTreeMap<DocKey, HashSet<(String, u32, Vec<u8>)>> = BTreeMap::new();

    for (dk, staged) in &tx.documents {
        let mut doc_final: HashSet<(String, u32, Vec<u8>)> = HashSet::new();
        if let StagedDocState::Upsert { json, .. } = &staged.state {
            for (name, _coll_id, idx_id, enc) in
                store::unique_encodings_for_doc(catalog, &tx.tenant_prefix, &dk.collection, json)?
            {
                let key = (dk.collection.clone(), idx_id, enc.clone());
                if let Some(other) = claimed.get(&key) {
                    if other.as_slice() != dk.doc_id.as_slice() {
                        return Err(DocError::DuplicateKey(format!(
                            "unique index '{name}' conflict inside transaction"
                        )));
                    }
                } else {
                    claimed.insert(key.clone(), dk.doc_id.clone());
                }
                doc_final.insert(key);
            }
        }
        final_claims.insert(dk.clone(), doc_final);
    }

    // Check committed owners for each claim.
    for ((collection, idx_id, enc), claimer) in &claimed {
        let coll = catalog
            .collection(&tx.tenant_prefix, collection)
            .ok_or_else(|| DocError::CollectionNotFound(collection.clone()))?;
        let owner = store::unique_owner(engine, &tx.tenant_prefix, coll.id, *idx_id, enc)?;
        if let Some(existing) = owner {
            if existing.as_slice() == claimer.as_slice() {
                continue;
            }
            let owner_key = DocKey {
                collection: collection.clone(),
                doc_id: existing.clone(),
            };
            // Owner is touched and no longer claims this encoding in final state.
            if let Some(owner_final) = final_claims.get(&owner_key) {
                let still_held = owner_final.contains(&(collection.clone(), *idx_id, enc.clone()));
                if !still_held {
                    continue;
                }
            } else {
                // Owner not touched → conflict.
                return Err(DocError::DuplicateKey(format!(
                    "unique index conflict on collection '{collection}'"
                )));
            }
            // Owner touched but still claims it (and claimer differs) — impossible
            // because claimed map would have collided earlier; treat as conflict.
            return Err(DocError::DuplicateKey(format!(
                "unique index conflict on collection '{collection}'"
            )));
        }
    }

    Ok(())
}
