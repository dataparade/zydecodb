#![no_main]

use libfuzzer_sys::fuzz_target;
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use zydecodb::security::{SecurityRuntime, SessionState};
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::engine_handle::EngineHandle;
use zydecodb_engine::frame::{Command, RequestEnvelope};

struct FuzzContext {
    engine: Arc<EngineHandle>,
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
        })
        .unwrap();

        // Keep tempdir alive by leaking it (fine for a fuzzer process)
        let _ = Box::leak(Box::new(tmp));

        let security = SecurityRuntime::default();

        FuzzContext {
            engine: EngineHandle::new(engine),
            security,
        }
    })
}

fn skip_opcode(cmd: Command) -> bool {
    matches!(
        cmd,
        Command::Watch | Command::Find | Command::FindRev | Command::Count | Command::Aggregate
    )
}

fuzz_target!(|data: &[u8]| {
    let Ok(req) = RequestEnvelope::decode(data) else {
        return;
    };
    if req.payload.len() > 4096 || skip_opcode(req.command) {
        return;
    }
    let ctx = get_context();
    let session = SessionState::anonymous();

    // Fuzz the raw-KV dispatch
    let _ = zydecodb::dispatch::handle_request(
        &ctx.engine,
        req.clone(),
        session.clone(),
        &ctx.security,
    );

    // Fuzz the document dispatch. Periodic (not Sync): this harness never
    // spawns the commit thread, so Sync `commit()` would wait forever on the
    // first durable write. Scan/Watch opcodes are covered by dedicated
    // targets and would walk the accumulating engine.
    let catalog = {
        let guard = ctx.engine.read();
        std::sync::Arc::new(std::sync::RwLock::new(
            zydecodb_document::catalog::Catalog::load(&guard).unwrap(),
        ))
    };
    let commit = zydecodb::commit::CommitCoordinator::new(
        &ctx.engine,
        zydecodb::commit::DurabilityMode::Periodic {
            interval: Duration::from_secs(3600),
        },
    );
    let _ = zydecodb::docdispatch::handle_document(
        &ctx.engine,
        &catalog,
        &commit,
        &req,
        &session,
        &ctx.security,
    );
});
