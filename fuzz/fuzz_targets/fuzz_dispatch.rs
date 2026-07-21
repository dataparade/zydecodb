#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::{Arc, Mutex, OnceLock};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::frame::RequestEnvelope;
use zydecodb::security::{SecurityRuntime, SessionState};

struct FuzzContext {
    engine: Arc<Mutex<Engine>>,
    security: SecurityRuntime,
}

fn get_context() -> &'static FuzzContext {
    static CONTEXT: OnceLock<FuzzContext> = OnceLock::new();
    CONTEXT.get_or_init(|| {
        let tmp = tempfile::TempDir::new().unwrap();
        let engine = Engine::open(EngineConfig {
            data_dir: tmp.path().join("data"),
            wal_dir: tmp.path().join("wal"),
            ..Default::default()
        }).unwrap();
        
        // Keep tempdir alive by leaking it (fine for a fuzzer process)
        let _ = Box::leak(Box::new(tmp));
        
        let security = SecurityRuntime::default();
        
        FuzzContext {
            engine: Arc::new(Mutex::new(engine)),
            security,
        }
    })
}

fuzz_target!(|data: &[u8]| {
    if let Ok(req) = RequestEnvelope::decode(data) {
        let ctx = get_context();
        let session = SessionState::anonymous();
        
        // Fuzz the raw-KV dispatch
        let _ = zydecodb::dispatch::handle_request(&ctx.engine, req.clone(), session.clone(), &ctx.security);
        
        // Fuzz the document dispatch
        let catalog = std::sync::Arc::new(std::sync::RwLock::new(zydecodb_document::catalog::Catalog::load(&ctx.engine.lock().unwrap()).unwrap()));
        let commit = zydecodb::commit::CommitCoordinator::new(ctx.engine.clone(), zydecodb::commit::DurabilityMode::Sync);
        let _ = zydecodb::docdispatch::handle_document(&ctx.engine, &catalog, &commit, &req, &session, &ctx.security);
    }
});
