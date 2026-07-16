//! Dispatch for the document commands (DocPut / DocDel / IndexDef / Query).
//!
//! Kept separate from [`crate::dispatch`] so the raw-KV `handle_request`
//! signature (and its tests) stay untouched. Writes and DDL run under the engine
//! lock for their whole operation; `Query` is two-phase: a snapshot is captured
//! under the lock, then iterated with the lock released so a long scan never
//! blocks concurrent writers.

use crate::commit::CommitCoordinator;
use crate::security::keys::KeyRole;
use crate::security::{SecurityRuntime, SessionState};
use std::sync::{Arc, Mutex, RwLock};
use zydecodb_document::catalog::Catalog;
use zydecodb_document::error::{DocError, DocResult};
use zydecodb_document::filter::Filter;
use zydecodb_document::query::{FindSpec, Projection, MAX_SORT_BUFFER};
use zydecodb_document::update::UpdateDoc;
use zydecodb_document::{query, store, update, wire};
use zydecodb_engine::engine::Engine;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope};
use zydecodb_engine::keys::KS_USER;

pub type SharedEngine = Arc<Mutex<Engine>>;
pub type SharedCatalog = Arc<RwLock<Catalog>>;

/// Whether a command is handled by the document layer.
pub fn is_document_command(cmd: Command) -> bool {
    matches!(
        cmd,
        Command::DocPut
            | Command::DocDel
            | Command::IndexDef
            | Command::Query
            | Command::Find
            | Command::Update
            | Command::Delete
            | Command::Count
    )
}

/// Storage prefix (`KS_USER` + optional tenant) mirroring `dispatch::storage_key`,
/// but without a trailing client key — the document layer appends its own
/// record structure.
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

fn err_response(e: &DocError) -> ResponseEnvelope {
    ResponseEnvelope::error(e.status(), &e.to_string())
}

/// Capture a read snapshot for a paginated read. When the request carries a
/// cursor, re-pin the same sequence ceiling the first page used (repeatable-read
/// pagination) via `snapshot_at`; otherwise capture the latest committed state.
fn read_snapshot(engine: &SharedEngine, cursor: Option<&[u8]>) -> zydecodb_engine::SnapshotHandle {
    let guard = engine.lock().unwrap();
    match query::cursor_snapshot_seq(cursor) {
        Some(seq) => guard.snapshot_at(seq),
        None => guard.snapshot_owned(),
    }
}

/// Route one document command, applying the same auth/role checks as the raw-KV
/// path. The session is never mutated by document commands.
pub fn handle_document(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    req: &RequestEnvelope,
    session: &SessionState,
    security: &SecurityRuntime,
) -> ResponseEnvelope {
    if security.require_auth && !session.authenticated {
        return ResponseEnvelope::error(Status::Unauthorized, "authentication required");
    }
    let is_write = matches!(
        req.command,
        Command::DocPut | Command::DocDel | Command::IndexDef | Command::Update | Command::Delete
    );
    if is_write && session.role == Some(KeyRole::ReadOnly) {
        return ResponseEnvelope::error(Status::Forbidden, "read-only key");
    }

    let prefix = tenant_prefix(session, security.legacy_single_tenant);

    match req.command {
        Command::DocPut => doc_put(engine, catalog, commit, &prefix, &req.payload),
        Command::DocDel => doc_del(engine, catalog, commit, &prefix, &req.payload),
        Command::IndexDef => index_def(engine, catalog, commit, &prefix, &req.payload),
        Command::Query => query_cmd(engine, catalog, &prefix, &req.payload),
        Command::Find => result(find_cmd(engine, catalog, &prefix, &req.payload)),
        Command::Update => result(update_cmd(engine, catalog, commit, &prefix, &req.payload)),
        Command::Delete => result(delete_cmd(engine, catalog, commit, &prefix, &req.payload)),
        Command::Count => result(count_cmd(engine, catalog, &prefix, &req.payload)),
        _ => ResponseEnvelope::error(Status::ProtocolError, "unimplemented"),
    }
}

/// Collapse a `DocResult<ResponseEnvelope>` into a response, mapping errors.
fn result(r: DocResult<ResponseEnvelope>) -> ResponseEnvelope {
    r.unwrap_or_else(|e| err_response(&e))
}

fn doc_put(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    prefix: &[u8],
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match wire::DocPutPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return err_response(&e),
    };
    
    // Parse incoming JSON and build ZDoc
    let json_val: serde_json::Value = match serde_json::from_slice(&p.body) {
        Ok(v) => v,
        Err(e) => return err_response(&DocError::InvalidJson(e.to_string())),
    };
    let zdoc_bytes = zydecodb_document::binary::ZDocBuilder::from_value(&json_val);

    // Lock order: catalog (read) then engine, consistent across all writers.
    let outcome = {
        let cat = catalog.read().unwrap();
        let mut guard = engine.lock().unwrap();
        store::upsert(&mut guard, &cat, prefix, &p.collection, &p.doc_id, &zdoc_bytes, true)
    };
    match outcome {
        Ok(seq) => {
            commit.commit(seq, p.relaxed);
            ResponseEnvelope::ok(seq.to_be_bytes().to_vec())
        }
        Err(e) => err_response(&e),
    }
}

fn doc_del(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    prefix: &[u8],
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match wire::DocDelPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return err_response(&e),
    };
    let outcome = {
        let cat = catalog.read().unwrap();
        let mut guard = engine.lock().unwrap();
        let r = store::delete(&mut guard, &cat, prefix, &p.collection, &p.doc_id);
        let seq = guard.last_buffered_seq();
        (r, seq)
    };
    match outcome.0 {
        Ok(deleted) => {
            // A delete-by-id is always durable-by-default (no relaxed flag on
            // DocDel); the seq it touched must reach disk before we ack.
            commit.commit(outcome.1, false);
            ResponseEnvelope::ok(vec![if deleted { 1 } else { 0 }])
        }
        Err(e) => err_response(&e),
    }
}

fn index_def(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    prefix: &[u8],
    payload: &[u8],
) -> ResponseEnvelope {
    let p = match wire::IndexDefPayload::decode(payload) {
        Ok(p) => p,
        Err(e) => return err_response(&e),
    };
    // Hold the catalog write lock for the whole DDL (serializing concurrent
    // DDL), and the engine lock for the backfill + catalog commit. Same
    // catalog-before-engine order as the write path, so no deadlock.
    let outcome = {
        let mut cat = catalog.write().unwrap();
        let mut guard = engine.lock().unwrap();
        let r = store::define_index(
            &mut guard,
            &mut cat,
            prefix,
            &p.collection,
            &p.index_name,
            p.fields,
            p.unique,
        );
        let seq = guard.last_buffered_seq();
        (r, seq)
    };
    match outcome.0 {
        Ok(()) => {
            // DDL is always made durable before acknowledging.
            commit.commit(outcome.1, false);
            ResponseEnvelope::ok(vec![])
        }
        Err(e) => err_response(&e),
    }
}

fn query_cmd(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    prefix: &[u8],
    payload: &[u8],
) -> ResponseEnvelope {
    let q = match wire::QueryPayload::decode(payload) {
        Ok(q) => q,
        Err(e) => return err_response(&e),
    };
    match q {
        wire::QueryPayload::ById { collection, doc_id } => {
            // Phase 1: capture a snapshot under the engine lock, then release.
            let snap = {
                let guard = engine.lock().unwrap();
                guard.snapshot_owned()
            };
            let cat = catalog.read().unwrap();
            match query::get_by_id(&snap, &cat, prefix, &collection, &doc_id) {
                Ok(Some(body)) => ResponseEnvelope::ok(body),
                Ok(None) => ResponseEnvelope::not_found(),
                Err(e) => err_response(&e),
            }
        }
        wire::QueryPayload::IndexRange {
            collection,
            index_name,
            lo,
            hi,
            cursor,
            limit,
        } => {
            // Phase 1: resolve the scan spec (catalog only) and capture a
            // snapshot (engine lock held only for snapshot_owned).
            let spec = {
                let cat = catalog.read().unwrap();
                query::build_index_scan_spec(
                    &cat,
                    prefix,
                    &collection,
                    &index_name,
                    opt(&lo),
                    opt(&hi),
                    opt(&cursor),
                    limit as usize,
                    true,
                )
            };
            let spec = match spec {
                Ok(s) => s,
                Err(e) => return err_response(&e),
            };
            let snap = read_snapshot(engine, opt(&cursor));
            // Phase 2: scan with the engine lock released.
            match query::execute_index_scan(&snap, &spec) {
                Ok(page) => ResponseEnvelope::ok(wire::encode_query_page(&page)),
                Err(e) => err_response(&e),
            }
        }
    }
}

/// Filter-based find: plan + residual filter + sort/projection/paging. Like
/// `Query`, it is two-phase — snapshot captured under the lock, scanned with the
/// lock released.
fn find_cmd(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    prefix: &[u8],
    payload: &[u8],
) -> DocResult<ResponseEnvelope> {
    let p = wire::FindPayload::decode(payload)?;
    let projection = match p.projection {
        wire::WireProjection::None => None,
        wire::WireProjection::Include(f) => Some(Projection::Include(f)),
        wire::WireProjection::Exclude(f) => Some(Projection::Exclude(f)),
    };
    let spec = FindSpec {
        filter: Filter::parse_bytes(&p.filter)?,
        sort: p.sort,
        projection,
        skip: p.skip as usize,
        limit: (p.limit as usize).max(1),
        cursor: opt(&p.cursor).map(|c| c.to_vec()),
    };
    let snap = read_snapshot(engine, spec.cursor.as_deref());
    let cat = catalog.read().unwrap();
    let page = query::execute_find(&snap, &cat, prefix, &p.collection, &spec)?;
    Ok(ResponseEnvelope::ok(wire::encode_query_page(&page)))
}

/// Filter-based update. Phase 1 selects candidate ids from a lock-free
/// snapshot; phase 2 applies one atomic batch per document (not globally
/// atomic, matching Mongo).
fn update_cmd(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    prefix: &[u8],
    payload: &[u8],
) -> DocResult<ResponseEnvelope> {
    let p = wire::UpdatePayload::decode(payload)?;
    let filter = Filter::parse_bytes(&p.filter)?;
    let upd = UpdateDoc::parse_bytes(&p.update)?;

    let ids = select_candidates(engine, catalog, prefix, &p.collection, &filter, p.multi)?;
    let matched = ids.len() as u64;
    let (modified, seq) = {
        let cat = catalog.read().unwrap();
        let mut guard = engine.lock().unwrap();
        let modified = update::apply_to_ids(&mut guard, &cat, prefix, &p.collection, &ids, &upd)?;
        let seq = guard.last_buffered_seq();
        (modified, seq)
    };
    // One durability wait covers the whole (possibly atomic) write set above.
    commit.commit(seq, p.relaxed);
    Ok(ResponseEnvelope::ok(
        format!("{{\"matched\":{matched},\"modified\":{modified}}}").into_bytes(),
    ))
}

/// Filter-based delete, same candidate-then-write shape as `update_cmd`.
fn delete_cmd(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    commit: &CommitCoordinator,
    prefix: &[u8],
    payload: &[u8],
) -> DocResult<ResponseEnvelope> {
    let p = wire::DeletePayload::decode(payload)?;
    let filter = Filter::parse_bytes(&p.filter)?;

    let ids = select_candidates(engine, catalog, prefix, &p.collection, &filter, p.multi)?;
    let (deleted, seq) = {
        let cat = catalog.read().unwrap();
        let mut guard = engine.lock().unwrap();
        let deleted = store::delete_ids(&mut guard, &cat, prefix, &p.collection, &ids)?;
        let seq = guard.last_buffered_seq();
        (deleted, seq)
    };
    commit.commit(seq, p.relaxed);
    Ok(ResponseEnvelope::ok(
        format!("{{\"deleted\":{deleted}}}").into_bytes(),
    ))
}

/// Select the document ids a filtered write applies to, from a lock-free
/// snapshot. `multi=false` selects at most the first match.
fn select_candidates(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    prefix: &[u8],
    collection: &str,
    filter: &Filter,
    multi: bool,
) -> DocResult<Vec<Vec<u8>>> {
    let snap = {
        let guard = engine.lock().unwrap();
        guard.snapshot_owned()
    };
    let cat = catalog.read().unwrap();
    if multi {
        query::find_ids(&snap, &cat, prefix, collection, filter, MAX_SORT_BUFFER)
    } else {
        Ok(
            query::find_first_id(&snap, &cat, prefix, collection, filter)?
                .into_iter()
                .collect(),
        )
    }
}

/// Filter-based count and distinct (read-only, two-phase).
fn count_cmd(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    prefix: &[u8],
    payload: &[u8],
) -> DocResult<ResponseEnvelope> {
    let p = wire::CountPayload::decode(payload)?;
    let snap = {
        let guard = engine.lock().unwrap();
        guard.snapshot_owned()
    };
    let cat = catalog.read().unwrap();
    match p {
        wire::CountPayload::Count { collection, filter } => {
            let filter = Filter::parse_bytes(&filter)?;
            let n = query::count(&snap, &cat, prefix, &collection, &filter)?;
            Ok(ResponseEnvelope::ok(n.to_string().into_bytes()))
        }
        wire::CountPayload::Distinct {
            collection,
            filter,
            field,
        } => {
            let filter = Filter::parse_bytes(&filter)?;
            let values = query::distinct(&snap, &cat, prefix, &collection, &field, &filter)?;
            let body = serde_json::to_vec(&serde_json::Value::Array(values))
                .map_err(|e| DocError::InvalidJson(e.to_string()))?;
            Ok(ResponseEnvelope::ok(body))
        }
    }
}

/// Treat an empty wire field as "absent".
fn opt(b: &[u8]) -> Option<&[u8]> {
    if b.is_empty() {
        None
    } else {
        Some(b)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::DurabilityMode;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::thread;
    use std::time::{Duration, Instant};
    use zydecodb_engine::engine::EngineConfig;

    fn rand_suffix() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    /// A document-layer fixture with a seeded collection and a `Sync`-mode commit
    /// coordinator whose fsync thread is deliberately NOT spawned. With no
    /// coordinator running, a durable (`relaxed = false`) commit blocks forever in
    /// `await_durable`, while a `relaxed` commit must return immediately — which is
    /// exactly the contrast these tests assert.
    struct Fixture {
        engine: SharedEngine,
        catalog: SharedCatalog,
        commit: Arc<CommitCoordinator>,
        prefix: Vec<u8>,
    }

    fn fixture(seed_ids: &[&str]) -> Fixture {
        let dir = std::env::temp_dir().join(format!("zydeco-docrelax-{}", rand_suffix()));
        let engine = Arc::new(Mutex::new(
            Engine::open(EngineConfig {
                data_dir: dir.join("data"),
                wal_dir: dir.join("wal"),
                ..Default::default()
            })
            .unwrap(),
        ));
        let catalog = Arc::new(RwLock::new(Catalog::default()));
        let prefix = vec![KS_USER];
        catalog.write().unwrap().ensure_collection(&prefix, "c");
        // Seed documents directly through the store (buffered WAL append; no
        // commit wait needed — the data is visible from the memtable at once).
        {
            let cat = catalog.read().unwrap();
            let mut e = engine.lock().unwrap();
            for id in seed_ids {
                let body = format!("{{\"_id\":\"{id}\",\"n\":1}}");
                store::upsert(&mut e, &cat, &prefix, "c", id.as_bytes(), body.as_bytes()).unwrap();
            }
        }
        let commit = CommitCoordinator::new(Arc::clone(&engine), DurabilityMode::Sync);
        Fixture {
            engine,
            catalog,
            commit,
            prefix,
        }
    }

    fn update_payload(id: &str, relaxed: bool) -> Vec<u8> {
        wire::UpdatePayload {
            collection: "c".into(),
            filter: format!("{{\"_id\":\"{id}\"}}").into_bytes(),
            update: b"{\"$inc\":{\"n\":1}}".to_vec(),
            multi: false,
            relaxed,
        }
        .encode()
    }

    #[test]
    fn relaxed_update_acks_without_durability_wait() {
        let fx = fixture(&["d1", "d2"]);

        // A relaxed update returns promptly even though no fsync thread is running.
        let start = Instant::now();
        let resp = update_cmd(
            &fx.engine,
            &fx.catalog,
            &fx.commit,
            &fx.prefix,
            &update_payload("d1", true),
        )
        .unwrap();
        assert_eq!(resp.status, Status::Ok);
        assert!(
            start.elapsed() < Duration::from_millis(50),
            "relaxed update must not block on the fsync"
        );

        // A durable update on the same fixture must block: with no coordinator
        // thread, its `seq` is never fsynced, so it stays parked until stop().
        let done = Arc::new(AtomicBool::new(false));
        let (engine, catalog, commit, prefix, done2) = (
            Arc::clone(&fx.engine),
            Arc::clone(&fx.catalog),
            Arc::clone(&fx.commit),
            fx.prefix.clone(),
            Arc::clone(&done),
        );
        let h = thread::spawn(move || {
            let r = update_cmd(
                &engine,
                &catalog,
                &commit,
                &prefix,
                &update_payload("d2", false),
            );
            done2.store(true, Ordering::SeqCst);
            r.map(|resp| resp.status)
        });
        thread::sleep(Duration::from_millis(150));
        assert!(
            !done.load(Ordering::SeqCst),
            "durable update must block while no fsync thread is running"
        );

        // Releasing the coordinator unblocks the parked durable write.
        fx.commit.stop();
        assert_eq!(h.join().unwrap().unwrap(), Status::Ok);
        assert!(done.load(Ordering::SeqCst));
    }

    #[test]
    fn relaxed_delete_acks_without_durability_wait() {
        let fx = fixture(&["d1"]);
        let payload = wire::DeletePayload {
            collection: "c".into(),
            filter: b"{\"_id\":\"d1\"}".to_vec(),
            multi: false,
            relaxed: true,
        }
        .encode();

        let start = Instant::now();
        let resp = delete_cmd(&fx.engine, &fx.catalog, &fx.commit, &fx.prefix, &payload).unwrap();
        assert_eq!(resp.status, Status::Ok);
        assert!(
            start.elapsed() < Duration::from_millis(50),
            "relaxed delete must not block on the fsync"
        );
        fx.commit.stop();
    }
}
