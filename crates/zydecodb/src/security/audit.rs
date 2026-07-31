use super::session::SessionState;
use crate::config::AuditConfig;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tracing::info;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::Command;

/// Process-wide request id allocator for audit lines.
static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

/// Allocate the next audit request id (monotonically increasing, wraps).
pub fn next_request_id() -> u64 {
    NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
}

/// A privacy-bounded capture of the client's KV key for audit lines: full
/// length plus at most the first 8 bytes. Even with `log_client_key = true`
/// the audit log never carries the whole key.
#[derive(Debug, Clone, Copy)]
pub struct AuditKey {
    len: usize,
    prefix: [u8; 8],
    prefix_len: usize,
}

impl AuditKey {
    pub fn capture(key: &[u8]) -> AuditKey {
        let prefix_len = key.len().min(8);
        let mut prefix = [0u8; 8];
        prefix[..prefix_len].copy_from_slice(&key[..prefix_len]);
        AuditKey {
            len: key.len(),
            prefix,
            prefix_len,
        }
    }

    fn prefix_hex(&self) -> String {
        self.prefix[..self.prefix_len]
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }
}

pub fn log_request(
    cfg: &AuditConfig,
    session: &SessionState,
    command: Command,
    client_key: Option<AuditKey>,
    status: Status,
    duration: Duration,
    request_id: u64,
) {
    if !cfg.enabled {
        return;
    }

    let tenant_hex = hex::encode(session.tenant);
    let key_id = session.key_id.as_deref().unwrap_or("-");
    let cmd = format!("{command:?}");
    let client_key_len = client_key.map(|k| k.len).unwrap_or(0);

    if cfg.log_client_key {
        // Opt-in: include a truncated hex prefix of the client key. Useful for
        // debugging access patterns; still never the full key material.
        let key_prefix = client_key
            .filter(|k| k.prefix_len > 0)
            .map(|k| k.prefix_hex())
            .unwrap_or_else(|| "-".to_string());
        info!(
            request_id,
            tenant = %tenant_hex,
            key_id = %key_id,
            cmd = %cmd,
            client_key_len = client_key_len,
            client_key_prefix = %key_prefix,
            status = ?status,
            duration_us = duration.as_micros(),
            "audit"
        );
    } else {
        info!(
            request_id,
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

/// Structured audit line for security-relevant admin / lifecycle events
/// (key revoke, tenant drop, replica promote, …). Always emitted — not gated
/// by `[security.audit].enabled` (those events are rare and operator-critical).
pub fn log_security_event(event: &str, fields: &[(&str, &str)]) {
    let mut pairs = String::new();
    for (i, (k, v)) in fields.iter().enumerate() {
        if i > 0 {
            pairs.push(' ');
        }
        pairs.push_str(k);
        pairs.push('=');
        pairs.push_str(v);
    }
    info!(event = %event, detail = %pairs, "security_event");
}

mod hex {
    pub fn encode(bytes: [u8; 16]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}
