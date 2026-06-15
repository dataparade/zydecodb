use super::session::SessionState;
use crate::config::AuditConfig;
use std::time::Duration;
use tracing::info;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::Command;

pub fn log_request(
    cfg: &AuditConfig,
    session: &SessionState,
    command: Command,
    client_key_len: Option<usize>,
    status: Status,
    duration: Duration,
) {
    if !cfg.enabled {
        return;
    }

    let tenant_hex = hex::encode(session.tenant);
    let key_id = session.key_id.as_deref().unwrap_or("-");
    let cmd = format!("{command:?}");
    let client_key_len = client_key_len.unwrap_or(0);

    if cfg.log_client_key {
        info!(
            tenant = %tenant_hex,
            key_id = %key_id,
            cmd = %cmd,
            client_key_len = client_key_len,
            status = ?status,
            duration_us = duration.as_micros(),
            "audit"
        );
    } else {
        info!(
            tenant = %tenant_hex,
            key_id = %key_id,
            cmd = %cmd,
            client_key_len = client_key_len,
            status = ?status,
            duration_us = duration.as_micros(),
            "audit"
        );
    }
}

mod hex {
    pub fn encode(bytes: [u8; 16]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
