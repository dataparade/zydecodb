//! Key types and keyspace partitioning.
//!
//! `InternalKey` is the key type that lives forever. Its `seq` field is just an
//! ordering tiebreaker in v1 but becomes the MVCC version in Sprint 3 — which is
//! why the read path already seeks `(user_key, seq DESC)`.

use std::cmp::Ordering;

/// Keyspace prefix bytes. User data is `0x01`; everything under `0x00` is the
/// reserved system keyspace, which user writes can never reach (the user path
/// enforces `KS_USER`). The engine treats system records as opaque — what lives
/// inside the system keyspace is defined by the embedder, not the engine. See
/// [`crate::engine::Engine::sys_get`] / [`crate::engine::Engine::sys_put`].
pub const KS_SYSTEM: u8 = 0x00;
pub const KS_USER: u8 = 0x01;

/// Operational limits. Compile-time constants in v1 (not configurable).
pub const MAX_KEY_BYTES: usize = 64 * 1024; // 64 KB
pub const MAX_VALUE_BYTES: usize = 16 * 1024 * 1024; // 16 MB
pub const MAX_IN_FLIGHT_WAL_BYTES: usize = 256 * 1024 * 1024; // 256 MB
pub const MAX_IMMUTABLE_MEMTABLES: usize = 4;
pub const MEMTABLE_FLUSH_THRESHOLD: usize = 64 * 1024 * 1024; // 64 MB
pub const SSTABLE_BLOCK_SIZE: usize = 16 * 1024; // 16 KB
pub const WAL_SEGMENT_SIZE: usize = 64 * 1024 * 1024; // 64 MB
pub const MAX_BATCH_KEYS: usize = 1024;

/// Entry kind. `kind` expands in later sprints (SnapshotMarker, SchemaDef, IndexEntry).
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EntryKind {
    Value = 0x01,
    Tombstone = 0x02,
}

impl EntryKind {
    pub fn as_u8(self) -> u8 {
        self as u8
    }

    pub fn from_u8(b: u8) -> Option<EntryKind> {
        match b {
            0x01 => Some(EntryKind::Value),
            0x02 => Some(EntryKind::Tombstone),
            _ => None,
        }
    }
}

/// The internal key: a user key plus a monotonic sequence and an entry kind.
///
/// Sort order is `user_key ASC, seq DESC`. This makes a point GET a single seek
/// to `(user_key, u64::MAX)` returning the newest entry first, and makes future
/// MVCC snapshot reads a seek to `(user_key, snapshot_seq)`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: u64,
    pub kind: EntryKind,
}

impl InternalKey {
    pub fn new(user_key: Vec<u8>, seq: u64, kind: EntryKind) -> Self {
        InternalKey {
            user_key,
            seq,
            kind,
        }
    }

    /// The keyspace prefix byte (first byte of the user key), if present.
    pub fn keyspace(&self) -> Option<u8> {
        self.user_key.first().copied()
    }
}

impl Ord for InternalKey {
    fn cmp(&self, other: &Self) -> Ordering {
        // user_key ascending, then seq DESCENDING (newer entries sort first).
        self.user_key
            .cmp(&other.user_key)
            .then_with(|| other.seq.cmp(&self.seq))
            .then_with(|| self.kind.as_u8().cmp(&other.kind.as_u8()))
    }
}

impl PartialOrd for InternalKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Validate that a user key is acceptable for a v1 user write:
/// non-empty, within size limit, and in the user keyspace (`0x01` prefix).
pub fn validate_user_key(user_key: &[u8]) -> Result<(), crate::errors::EngineError> {
    use crate::errors::EngineError;
    if user_key.is_empty() {
        return Err(EngineError::InvalidKey("empty key".into()));
    }
    if user_key.len() > MAX_KEY_BYTES {
        return Err(EngineError::InvalidKey(format!(
            "key length {} exceeds MAX_KEY_BYTES {}",
            user_key.len(),
            MAX_KEY_BYTES
        )));
    }
    if user_key[0] != KS_USER {
        return Err(EngineError::InvalidKey("reserved keyspace".into()));
    }
    Ok(())
}

/// Validate a system key: non-empty, within size limit, and in the system
/// keyspace (`0x00` prefix). Used by the engine's `sys_*` methods, which store
/// caller-defined opaque metadata that user writes can never reach.
pub fn validate_system_key(key: &[u8]) -> Result<(), crate::errors::EngineError> {
    use crate::errors::EngineError;
    if key.is_empty() {
        return Err(EngineError::InvalidKey("empty system key".into()));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(EngineError::InvalidKey(format!(
            "system key length {} exceeds MAX_KEY_BYTES {}",
            key.len(),
            MAX_KEY_BYTES
        )));
    }
    if key[0] != KS_SYSTEM {
        return Err(EngineError::InvalidKey("not a system key".into()));
    }
    Ok(())
}

pub fn validate_value(value: &[u8]) -> Result<(), crate::errors::EngineError> {
    use crate::errors::EngineError;
    if value.len() > MAX_VALUE_BYTES {
        return Err(EngineError::InvalidValue(format!(
            "value length {} exceeds MAX_VALUE_BYTES {}",
            value.len(),
            MAX_VALUE_BYTES
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ik(key: &[u8], seq: u64, kind: EntryKind) -> InternalKey {
        InternalKey::new(key.to_vec(), seq, kind)
    }

    #[test]
    fn newer_seq_sorts_first_for_same_key() {
        let older = ik(b"foo", 1, EntryKind::Value);
        let newer = ik(b"foo", 2, EntryKind::Value);
        assert!(newer < older, "newer seq must sort before older seq");
    }

    #[test]
    fn user_keys_sort_ascending() {
        let a = ik(b"aaa", 5, EntryKind::Value);
        let b = ik(b"bbb", 1, EntryKind::Value);
        assert!(a < b);
    }

    #[test]
    fn tombstone_and_value_same_key_seq_are_ordered_deterministically() {
        let v = ik(b"k", 7, EntryKind::Value);
        let t = ik(b"k", 7, EntryKind::Tombstone);
        // Value (0x01) sorts before Tombstone (0x02) at equal key+seq.
        assert!(v < t);
    }

    #[test]
    fn entrykind_round_trips() {
        assert_eq!(EntryKind::from_u8(0x01), Some(EntryKind::Value));
        assert_eq!(EntryKind::from_u8(0x02), Some(EntryKind::Tombstone));
        assert_eq!(EntryKind::from_u8(0x00), None);
        assert_eq!(EntryKind::Value.as_u8(), 0x01);
        assert_eq!(EntryKind::Tombstone.as_u8(), 0x02);
    }

    #[test]
    fn validate_user_key_rejects_bad_keys() {
        assert!(validate_user_key(b"").is_err());
        assert!(validate_user_key(&[KS_SYSTEM, 1, 2]).is_err());
        assert!(validate_user_key(&[KS_USER, 1, 2]).is_ok());
        let too_big = vec![KS_USER; MAX_KEY_BYTES + 1];
        assert!(validate_user_key(&too_big).is_err());
    }

    #[test]
    fn validate_value_rejects_oversize() {
        assert!(validate_value(&[0u8; 10]).is_ok());
        let too_big = vec![0u8; MAX_VALUE_BYTES + 1];
        assert!(validate_value(&too_big).is_err());
    }
}
