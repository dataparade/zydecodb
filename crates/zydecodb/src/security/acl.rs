//! Prefix ACL checks shared by the raw-KV and document dispatch paths.

use crate::security::SessionState;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::ResponseEnvelope;

/// Deny when the session has `allowed_prefixes` and `client_key` matches none.
pub fn check_key_prefix_acl(session: &SessionState, client_key: &[u8]) -> Option<ResponseEnvelope> {
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

/// Deny when the session has `allowed_prefixes` and `collection` matches none.
///
/// Matching rules (either is enough):
/// - `collection` starts with the configured prefix (same rule as KV keys)
/// - `collection` equals the prefix with a trailing `:` stripped, so a KV-style
///   prefix like `events:` still allows the document collection `events`
pub fn check_collection_prefix_acl(
    session: &SessionState,
    collection: &str,
) -> Option<ResponseEnvelope> {
    if session.allowed_prefixes.is_empty() {
        return None;
    }
    let allowed = session.allowed_prefixes.iter().any(|p| {
        collection.as_bytes().starts_with(p.as_bytes()) || collection == p.trim_end_matches(':')
    });
    if allowed {
        None
    } else {
        Some(ResponseEnvelope::error(
            Status::Forbidden,
            "collection prefix not allowed",
        ))
    }
}
