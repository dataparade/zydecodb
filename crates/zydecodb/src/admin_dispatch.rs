//! Live admin commands that need both engine and catalog.

use crate::admin::drop_tenant_on_engine;
use crate::security::{SecurityRuntime, SessionState};
use crate::shared::{SharedCatalog, SharedEngine};
use zydecodb_engine::engine::Engine;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::{Command, RequestEnvelope, ResponseEnvelope};

/// `AdminDropTenant` payload: 16-byte tenant id + 1-byte compact flag (0/1).
pub fn handle_admin_drop_tenant(
    engine: &SharedEngine,
    catalog: &SharedCatalog,
    req: &RequestEnvelope,
    session: &SessionState,
    security: &SecurityRuntime,
) -> ResponseEnvelope {
    if security.require_auth && !session.authenticated {
        return ResponseEnvelope::error(Status::Unauthorized, "authentication required");
    }
    if !session.is_admin() {
        return ResponseEnvelope::error(Status::Forbidden, "admin role required");
    }
    if req.payload.len() != 17 {
        return ResponseEnvelope::error(
            Status::ProtocolError,
            "AdminDropTenant payload must be 16-byte tenant + compact flag",
        );
    }
    let mut tenant = [0u8; 16];
    tenant.copy_from_slice(&req.payload[..16]);
    let compact = req.payload[16] != 0;

    let (result, slowdown) = {
        let mut cat = catalog.write().unwrap();
        let mut guard = engine.write();
        let r = drop_tenant_on_engine(&mut guard, &mut cat, &tenant, compact);
        let s = guard.take_write_slowdown();
        (r, s)
    };
    Engine::apply_write_slowdown(slowdown);

    match result {
        Ok(out) => {
            let body = format!(
                "{{\"deleted\":{},\"collections\":{}}}",
                out.deleted_keys, out.removed_collections
            );
            ResponseEnvelope::ok(body.into_bytes())
        }
        Err(e) => ResponseEnvelope::error(Status::Error, &e),
    }
}

pub fn is_admin_command(cmd: Command) -> bool {
    matches!(cmd, Command::AdminDropTenant)
}
