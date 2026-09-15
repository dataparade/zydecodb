//! Prefix ACL checks shared by the raw-KV and document dispatch paths.

use crate::security::SessionState;
use zydecodb_engine::errors::Status;
use zydecodb_engine::frame::ResponseEnvelope;

/// True when a raw-KV client key would alias the document layer's internal
/// keyspace. Document bodies live at `prefix|'d'|u32 BE collection_id|doc_id`
/// and index entries at `prefix|'i'|u32 BE collection_id|...`; both share the
/// tenant `prefix` with raw KV, so a raw key starting with `d` or `i` followed
/// by a 0x00 byte (the leading byte of every real collection id) lands inside
/// that range. Writing there lets a KV client plant bytes that the document
/// reader later decodes as a ZDoc body. Natural keys like `device:1` or
/// `id-42` are unaffected because their second byte is never 0x00.
pub fn reserved_client_key(client_key: &[u8]) -> bool {
    client_key.len() >= 2
        && (client_key[0] == zydecodb_document::keys::REC_DOC
            || client_key[0] == zydecodb_document::keys::REC_INDEX)
        && client_key[1] == 0x00
}

/// Deny raw-KV writes to keys that alias the document layer (see
/// [`reserved_client_key`]). Reads stay unrestricted: reading the range leaks
/// nothing a `Find` would not, and the danger is only in writing to it.
pub fn check_reserved_client_key(client_key: &[u8]) -> Option<ResponseEnvelope> {
    if reserved_client_key(client_key) {
        Some(ResponseEnvelope::error(
            Status::InvalidKey,
            "key prefix reserved: raw keys starting with 'd' or 'i' followed by 0x00 \
             alias the document/index keyspace",
        ))
    } else {
        None
    }
}

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
