//! Manifest: append-only engine state of record.
//!
//! Record framing: `[1] record_type [4] record_length (u32 BE) [N] payload [4] CRC32`.
//!
//! v1 record types: SSTABLE_ADD, SSTABLE_REMOVE, SEQ_CHECKPOINT, WAL_TRUNCATE.

use crate::errors::{EngineError, EngineResult};
use std::path::Path;

pub const REC_SSTABLE_ADD: u8 = 0x01;
pub const REC_SSTABLE_REMOVE: u8 = 0x02;
pub const REC_SEQ_CHECKPOINT: u8 = 0x03;
pub const REC_WAL_TRUNCATE: u8 = 0x04;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SstableMeta {
    pub id: u64,
    pub level: u8,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub min_seq: u64,
    pub max_seq: u64,
    pub size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ManifestRecord {
    SstableAdd(SstableMeta),
    SstableRemove { id: u64 },
    SeqCheckpoint { last_durable_seq: u64 },
    WalTruncate { up_to_segment_id: u64 },
}

impl ManifestRecord {
    fn payload(&self) -> Vec<u8> {
        match self {
            ManifestRecord::SstableAdd(m) => {
                let mut p = Vec::new();
                p.extend_from_slice(&m.id.to_be_bytes());
                p.push(m.level);
                p.extend_from_slice(&(m.min_key.len() as u32).to_be_bytes());
                p.extend_from_slice(&m.min_key);
                p.extend_from_slice(&(m.max_key.len() as u32).to_be_bytes());
                p.extend_from_slice(&m.max_key);
                p.extend_from_slice(&m.min_seq.to_be_bytes());
                p.extend_from_slice(&m.max_seq.to_be_bytes());
                p.extend_from_slice(&m.size_bytes.to_be_bytes());
                p
            }
            ManifestRecord::SstableRemove { id } => id.to_be_bytes().to_vec(),
            ManifestRecord::SeqCheckpoint { last_durable_seq } => {
                last_durable_seq.to_be_bytes().to_vec()
            }
            ManifestRecord::WalTruncate { up_to_segment_id } => {
                up_to_segment_id.to_be_bytes().to_vec()
            }
        }
    }

    fn type_byte(&self) -> u8 {
        match self {
            ManifestRecord::SstableAdd(_) => REC_SSTABLE_ADD,
            ManifestRecord::SstableRemove { .. } => REC_SSTABLE_REMOVE,
            ManifestRecord::SeqCheckpoint { .. } => REC_SEQ_CHECKPOINT,
            ManifestRecord::WalTruncate { .. } => REC_WAL_TRUNCATE,
        }
    }

    /// Encode one framed record with CRC.
    pub fn encode(&self) -> Vec<u8> {
        let payload = self.payload();
        let mut buf = Vec::with_capacity(1 + 4 + payload.len() + 4);
        buf.push(self.type_byte());
        buf.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        buf.extend_from_slice(&payload);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf
    }

    /// Decode one record from the front. Returns `(record, consumed)` or None on
    /// short/torn tail.
    pub fn decode_one(buf: &[u8]) -> EngineResult<Option<(ManifestRecord, usize)>> {
        if buf.len() < 5 {
            return Ok(None);
        }
        let rtype = buf[0];
        let len = u32::from_be_bytes(buf[1..5].try_into().unwrap()) as usize;
        let total = 5 + len + 4;
        if buf.len() < total {
            return Ok(None);
        }
        let body_end = 5 + len;
        let stored_crc = u32::from_be_bytes(buf[body_end..total].try_into().unwrap());
        if crc32fast::hash(&buf[..body_end]) != stored_crc {
            return Err(EngineError::Io("manifest: CRC mismatch".into()));
        }
        let p = &buf[5..body_end];
        let rec = match rtype {
            REC_SSTABLE_ADD => ManifestRecord::SstableAdd(decode_sstable_meta(p)?),
            REC_SSTABLE_REMOVE => ManifestRecord::SstableRemove {
                id: read_u64(p, 0)?,
            },
            REC_SEQ_CHECKPOINT => ManifestRecord::SeqCheckpoint {
                last_durable_seq: read_u64(p, 0)?,
            },
            REC_WAL_TRUNCATE => ManifestRecord::WalTruncate {
                up_to_segment_id: read_u64(p, 0)?,
            },
            other => {
                // Unknown record type means the manifest was written by an
                // engine version that introduced a record type this build does
                // not know how to interpret. This is distinct from a torn tail
                // (CRC mismatch / short buffer) — the bytes are intact, just
                // version-incompatible — so we surface it as a hard
                // UnsupportedFormat refusal. `replay` relies on this variant
                // to know it must propagate instead of silently truncating.
                return Err(EngineError::UnsupportedFormat(format!(
                    "manifest: unknown record type 0x{:02x}",
                    other
                )));
            }
        };
        Ok(Some((rec, total)))
    }
}

fn decode_sstable_meta(p: &[u8]) -> EngineResult<SstableMeta> {
    let mut off = 0;
    let id = read_u64(p, off)?;
    off += 8;
    let level = *p.get(off).ok_or_else(short)?;
    off += 1;
    let (min_key, n) = read_bytes(p, off)?;
    off += n;
    let (max_key, n) = read_bytes(p, off)?;
    off += n;
    let min_seq = read_u64(p, off)?;
    off += 8;
    let max_seq = read_u64(p, off)?;
    off += 8;
    let size_bytes = read_u64(p, off)?;
    Ok(SstableMeta {
        id,
        level,
        min_key,
        max_key,
        min_seq,
        max_seq,
        size_bytes,
    })
}

fn short() -> EngineError {
    EngineError::Io("manifest: short record payload".into())
}

fn read_u64(p: &[u8], off: usize) -> EngineResult<u64> {
    p.get(off..off + 8)
        .map(|s| u64::from_be_bytes(s.try_into().unwrap()))
        .ok_or_else(short)
}

fn read_bytes(p: &[u8], off: usize) -> EngineResult<(Vec<u8>, usize)> {
    let len = p
        .get(off..off + 4)
        .map(|s| u32::from_be_bytes(s.try_into().unwrap()) as usize)
        .ok_or_else(short)?;
    let start = off + 4;
    let bytes = p.get(start..start + len).ok_or_else(short)?.to_vec();
    Ok((bytes, 4 + len))
}

/// The reconstructed state after replaying a manifest.
#[derive(Debug, Default)]
pub struct ManifestState {
    pub live_sstables: Vec<SstableMeta>,
    pub last_durable_seq: u64,
    pub wal_truncated_up_to: u64,
}

/// Replay a manifest byte stream into engine state.
///
/// Outcomes:
/// - **Clean record decoded** → apply it and advance.
/// - **Short tail** (`Ok(None)`) → the last write didn't complete; stop and
///   return the state accumulated so far. This is the torn-tail tolerance the
///   write path relies on (manifest appends are not group-committed).
/// - **CRC mismatch** (`EngineError::Io`) → also treated as a torn tail.
///   `decode_one` returns this when a record's stored CRC doesn't match its
///   computed CRC, which can only happen on a partial write.
/// - **Unknown record type** (`EngineError::UnsupportedFormat`) → bytes are
///   intact but written by an engine version this build doesn't understand.
///   Refuse loudly. Returning Ok here would silently truncate live catalog
///   state (the original bug this taxonomy fixes).
pub fn replay(buf: &[u8]) -> EngineResult<ManifestState> {
    let mut state = ManifestState::default();
    let mut offset = 0;
    while offset < buf.len() {
        match ManifestRecord::decode_one(&buf[offset..]) {
            Ok(Some((rec, consumed))) => {
                apply(&mut state, rec);
                offset += consumed;
            }
            Ok(None) => break,                // short tail
            Err(EngineError::Io(_)) => break, // torn write (CRC mismatch)
            Err(e @ EngineError::UnsupportedFormat(_)) => return Err(e),
            Err(e) => return Err(e), // any other surprise error: propagate
        }
    }
    Ok(state)
}

fn apply(state: &mut ManifestState, rec: ManifestRecord) {
    match rec {
        ManifestRecord::SstableAdd(m) => {
            state.last_durable_seq = state.last_durable_seq.max(m.max_seq);
            state.live_sstables.push(m);
        }
        ManifestRecord::SstableRemove { id } => {
            state.live_sstables.retain(|m| m.id != id);
        }
        ManifestRecord::SeqCheckpoint { last_durable_seq } => {
            state.last_durable_seq = state.last_durable_seq.max(last_durable_seq);
        }
        ManifestRecord::WalTruncate { up_to_segment_id } => {
            state.wal_truncated_up_to = state.wal_truncated_up_to.max(up_to_segment_id);
        }
    }
}

/// Read and replay a manifest file from disk. Missing file => empty state.
pub fn load(path: &Path) -> EngineResult<ManifestState> {
    if !path.exists() {
        return Ok(ManifestState::default());
    }
    let buf = std::fs::read(path)?;
    replay(&buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta(id: u64, max_seq: u64) -> SstableMeta {
        SstableMeta {
            id,
            level: 0,
            min_key: b"\x01a".to_vec(),
            max_key: b"\x01z".to_vec(),
            min_seq: 1,
            max_seq,
            size_bytes: 4096,
        }
    }

    #[test]
    fn sstable_add_round_trips() {
        let rec = ManifestRecord::SstableAdd(meta(1, 100));
        let bytes = rec.encode();
        let (back, n) = ManifestRecord::decode_one(&bytes).unwrap().unwrap();
        assert_eq!(rec, back);
        assert_eq!(n, bytes.len());
    }

    #[test]
    fn all_record_types_round_trip() {
        let recs = vec![
            ManifestRecord::SstableAdd(meta(2, 50)),
            ManifestRecord::SstableRemove { id: 2 },
            ManifestRecord::SeqCheckpoint {
                last_durable_seq: 123,
            },
            ManifestRecord::WalTruncate {
                up_to_segment_id: 3,
            },
        ];
        for r in recs {
            let bytes = r.encode();
            let (back, _) = ManifestRecord::decode_one(&bytes).unwrap().unwrap();
            assert_eq!(r, back);
        }
    }

    #[test]
    fn replay_reconstructs_live_set() {
        let mut stream = Vec::new();
        stream.extend_from_slice(&ManifestRecord::SstableAdd(meta(1, 10)).encode());
        stream.extend_from_slice(&ManifestRecord::SstableAdd(meta(2, 20)).encode());
        stream.extend_from_slice(&ManifestRecord::SstableRemove { id: 1 }.encode());
        stream.extend_from_slice(
            &ManifestRecord::SeqCheckpoint {
                last_durable_seq: 25,
            }
            .encode(),
        );
        let state = replay(&stream).expect("clean stream replays without error");
        assert_eq!(state.live_sstables.len(), 1);
        assert_eq!(state.live_sstables[0].id, 2);
        assert_eq!(state.last_durable_seq, 25);
    }

    #[test]
    fn replay_tolerates_torn_tail() {
        let mut stream = ManifestRecord::SstableAdd(meta(1, 10)).encode();
        stream.extend_from_slice(&[0x01, 0x00, 0x00]); // partial record
        let state = replay(&stream).expect("torn tail must not be an error");
        assert_eq!(state.live_sstables.len(), 1);
    }

    #[test]
    fn replay_rejects_unknown_record_type() {
        // A clean SstableAdd followed by a framed-but-unknown record type
        // must surface as UnsupportedFormat — NOT silently truncate the live
        // set to just the first record. That silent truncation was the bug
        // this taxonomy fixes.
        let mut stream = ManifestRecord::SstableAdd(meta(1, 10)).encode();
        let payload: &[u8] = &[];
        let rtype: u8 = 0xFF;
        let mut framed = Vec::new();
        framed.push(rtype);
        framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        framed.extend_from_slice(payload);
        let crc = crc32fast::hash(&framed);
        framed.extend_from_slice(&crc.to_be_bytes());
        stream.extend_from_slice(&framed);

        let err = replay(&stream).expect_err("unknown record type must propagate");
        assert!(
            matches!(err, EngineError::UnsupportedFormat(_)),
            "expected UnsupportedFormat, got {:?}",
            err
        );
    }

    #[test]
    fn crc_mismatch_detected() {
        let mut bytes = ManifestRecord::SstableRemove { id: 5 }.encode();
        let n = bytes.len();
        bytes[n - 5] ^= 0xFF;
        assert!(ManifestRecord::decode_one(&bytes).is_err());
    }
}
