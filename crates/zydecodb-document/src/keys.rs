//! Document and index key layout.
//!
//! All keys live inside the caller-provided storage `prefix` (`KS_USER` plus an
//! optional 16-byte tenant), so they pass the engine's `validate_user_key` and
//! inherit existing tenancy/ACL isolation. Within the prefix a one-byte record
//! discriminator separates document bodies from index entries:
//!
//! ```text
//! doc:   prefix | 'd' | collection_id(4) | doc_id
//! index: prefix | 'i' | collection_id(4) | index_id(4) | encoded_fields | doc_id
//! ```
//!
//! `collection_id`/`index_id` are fixed 4-byte big-endian so the byte layout is
//! unambiguous; `encoded_fields` is the prefix-free order-preserving encoding
//! (see [`crate::encoding`]); the trailing `doc_id` makes index keys unique when
//! several documents share a field value.

pub const REC_DOC: u8 = b'd';
pub const REC_INDEX: u8 = b'i';

/// `prefix | 'd' | collection_id` — the range under which all docs of a
/// collection live.
pub fn doc_prefix(prefix: &[u8], collection_id: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + 1 + 4);
    k.extend_from_slice(prefix);
    k.push(REC_DOC);
    k.extend_from_slice(&collection_id.to_be_bytes());
    k
}

/// Full key for one document body.
pub fn doc_key(prefix: &[u8], collection_id: u32, doc_id: &[u8]) -> Vec<u8> {
    let mut k = doc_prefix(prefix, collection_id);
    k.extend_from_slice(doc_id);
    k
}

/// Extract the doc_id suffix from a full document key, given the storage prefix
/// length. Returns an empty slice if the key is shorter than the fixed header.
pub fn doc_id_from_doc_key(prefix_len: usize, key: &[u8]) -> Vec<u8> {
    let start = prefix_len + 1 + 4;
    key.get(start..).unwrap_or(&[]).to_vec()
}

/// `prefix | 'i' | collection_id | index_id` — the range under which all
/// entries of one index live.
pub fn index_prefix(prefix: &[u8], collection_id: u32, index_id: u32) -> Vec<u8> {
    let mut k = Vec::with_capacity(prefix.len() + 1 + 4 + 4);
    k.extend_from_slice(prefix);
    k.push(REC_INDEX);
    k.extend_from_slice(&collection_id.to_be_bytes());
    k.extend_from_slice(&index_id.to_be_bytes());
    k
}

/// Full key for one index entry.
pub fn index_key(
    prefix: &[u8],
    collection_id: u32,
    index_id: u32,
    encoded_fields: &[u8],
    doc_id: &[u8],
) -> Vec<u8> {
    let mut k = index_prefix(prefix, collection_id, index_id);
    k.extend_from_slice(encoded_fields);
    k.extend_from_slice(doc_id);
    k
}

/// Smallest byte string strictly greater than every key having `prefix` as a
/// prefix (the exclusive upper bound for a prefix scan). An all-`0xFF` prefix
/// returns empty, which the engine's scan treats as unbounded.
pub fn prefix_upper_bound(prefix: &[u8]) -> Vec<u8> {
    let mut out = prefix.to_vec();
    while let Some(&last) = out.last() {
        if last == 0xFF {
            out.pop();
        } else {
            *out.last_mut().unwrap() = last + 1;
            return out;
        }
    }
    Vec::new()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doc_round_trip_extracts_id() {
        let prefix = b"\x01tenant";
        let key = doc_key(prefix, 3, b"user-42");
        assert_eq!(doc_id_from_doc_key(prefix.len(), &key), b"user-42");
    }

    #[test]
    fn index_keys_with_same_field_differ_by_doc_id() {
        let prefix = b"\x01";
        let a = index_key(prefix, 1, 1, b"\x03alice\x00\x00", b"doc1");
        let b = index_key(prefix, 1, 1, b"\x03alice\x00\x00", b"doc2");
        assert_ne!(a, b);
    }

    #[test]
    fn prefix_upper_bound_bumps_last_byte() {
        assert_eq!(prefix_upper_bound(b"abc"), b"abd".to_vec());
        assert_eq!(prefix_upper_bound(b"a\xFF"), b"b".to_vec());
        assert_eq!(prefix_upper_bound(b"\xFF\xFF"), Vec::<u8>::new());
    }
}
