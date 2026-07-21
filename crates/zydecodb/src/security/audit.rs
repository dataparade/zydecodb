use super::session::SessionState;
use crate::config::AuditConfig;
use std::time::Duration;
use tracing::info;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::Command;

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
