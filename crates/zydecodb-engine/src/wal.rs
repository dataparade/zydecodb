//! Segmented Write-Ahead Log.
//!
//! On-disk segment layout (WAL format v2 — the engine stores opaque key bytes,
//! so a record carries no caller-side routing field; any routing prefix the
//! caller cares about lives inside the key itself):
//! ```text
//! [8 bytes] first_seq
//! [1 byte ] WAL_FORMAT_VERSION
//! repeated WAL entries:
//!   [1]  command (0x01=PUT, 0x02=DEL_TOMBSTONE)
//!   [8]  seq (u64 BE)
//!   [8]  expires_at
//!   [4]  key_len (u32 BE)
//!   [4]  value_len (u32 BE)
//!   [K]  key
//!   [V]  value
//!   [4]  CRC32 of the entry (bytes from command..value inclusive)
//! ```
//!
//! Append-only, fsync after every entry. Files are rolled at `WAL_SEGMENT_SIZE`.
//! This module owns the codec and the directory bookkeeping; file I/O is driven
//! by the engine.
//!
//! A batch (`WAL_BATCH`) is encoded as a SINGLE self-framed record with one
//! trailing CRC over the whole record. This makes a multi-key write atomic on a
//! torn crash: either the record's CRC validates (every op replays) or it does
//! not (the torn tail is truncated and NO op replays). There is no way to
//! recover a partial batch.

use crate::entry::Entry;
use crate::errors::{EngineError, EngineResult};
use crate::keys::{EntryKind, InternalKey, WAL_SEGMENT_SIZE};
use std::path::{Path, PathBuf};

pub const WAL_PUT: u8 = 0x01;
pub const WAL_DEL: u8 = 0x02;
/// Atomic multi-key batch record (see module docs). Followed by a u64 batch
/// seq, a u32 op count, the ops, and one trailing CRC32 over the whole record.
pub const WAL_BATCH: u8 = 0x03;
const ENTRY_FIXED: usize = 1 + 8 + 8 + 4 + 4; // = 25, before key/value/crc
/// Batch record header before the ops: command + seq + op count.
const BATCH_HEADER: usize = 1 + 8 + 4;
/// Per-op fixed prefix inside a batch: subcommand + expires_at + klen + vlen.
const BATCH_OP_FIXED: usize = 1 + 8 + 4 + 4;

/// WAL on-disk format version, stored as one byte after `first_seq` in each
/// segment header. v1 carried an extra 16-byte per-record field; v2 dropped it.
/// Recovery rejects segments with an unknown version.
pub const WAL_FORMAT_VERSION: u8 = 0x02;
/// Segment header: 8-byte first_seq + 1-byte format version.
pub const SEGMENT_HEADER_LEN: usize = 9;

/// A single logical WAL record (pre-serialization).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalRecord {
    pub command: u8,
    pub seq: u64,
    pub expires_at: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

impl WalRecord {
    pub fn put(seq: u64, expires_at: u64, key: Vec<u8>, value: Vec<u8>) -> Self {
        WalRecord {
            command: WAL_PUT,
            seq,
            expires_at,
            key,
            value,
        }
    }

    pub fn del(seq: u64, key: Vec<u8>) -> Self {
        WalRecord {
            command: WAL_DEL,
            seq,
            expires_at: 0,
            key,
            value: Vec::new(),
        }
    }

    /// Serialize to bytes including the trailing CRC32.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(ENTRY_FIXED + self.key.len() + self.value.len() + 4);
        buf.push(self.command);
        buf.extend_from_slice(&self.seq.to_be_bytes());
        buf.extend_from_slice(&self.expires_at.to_be_bytes());
        buf.extend_from_slice(&(self.key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&(self.value.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.key);
        buf.extend_from_slice(&self.value);
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf
    }

    /// Try to decode one entry from the front of `buf`. Dispatches on the
    /// leading command byte to a single record (`WAL_PUT` / `WAL_DEL`) or a
    /// batch (`WAL_BATCH`). Returns the decoded entry and bytes consumed, or:
    /// - `Ok(None)` if the buffer is too short (need more bytes / torn tail)
    /// - `Err` if a CRC mismatch or bad command indicates corruption (torn tail)
    pub fn decode_one(buf: &[u8]) -> EngineResult<Option<(WalEntry, usize)>> {
        match buf.first() {
            None => Ok(None),
            Some(&WAL_BATCH) => decode_batch(buf),
            Some(&WAL_PUT) | Some(&WAL_DEL) => decode_single(buf),
            Some(_) => Err(EngineError::Io("WAL: bad command byte".into())),
        }
    }

    /// Convert this record into a memtable `(InternalKey, Entry)` pair.
    pub fn to_memtable_pair(&self) -> (InternalKey, Entry) {
        match self.command {
            WAL_DEL => (
                InternalKey::new(self.key.clone(), self.seq, EntryKind::Tombstone),
                Entry::tombstone(),
            ),
            _ => (
                InternalKey::new(self.key.clone(), self.seq, EntryKind::Value),
                Entry::value(
                    self.value.clone(),
                    if self.expires_at == 0 {
                        None
                    } else {
                        Some(self.expires_at)
                    },
                ),
            ),
        }
    }
}

/// One operation inside a batch WAL record. `command` is `WAL_PUT` or
/// `WAL_DEL`; the batch seq is shared by every op and lives on [`WalBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalOp {
    pub command: u8,
    pub expires_at: u64,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

/// A batch of operations committed atomically as one self-framed WAL record
/// (one CRC). See module docs for the all-or-nothing recovery guarantee.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WalBatch {
    pub seq: u64,
    pub ops: Vec<WalOp>,
}

impl WalBatch {
    /// Serialize to bytes: `[WAL_BATCH][seq][count]` then each op
    /// `[cmd][expires_at][klen][vlen][key][value]`, then one trailing CRC32 over
    /// the whole record.
    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(BATCH_HEADER + 4);
        buf.push(WAL_BATCH);
        buf.extend_from_slice(&self.seq.to_be_bytes());
        buf.extend_from_slice(&(self.ops.len() as u32).to_be_bytes());
        for op in &self.ops {
            buf.push(op.command);
            buf.extend_from_slice(&op.expires_at.to_be_bytes());
            buf.extend_from_slice(&(op.key.len() as u32).to_be_bytes());
            buf.extend_from_slice(&(op.value.len() as u32).to_be_bytes());
            buf.extend_from_slice(&op.key);
            buf.extend_from_slice(&op.value);
        }
        let crc = crc32fast::hash(&buf);
        buf.extend_from_slice(&crc.to_be_bytes());
        buf
    }
}

/// A decoded WAL entry: either a single record or an atomic batch. Both replay
/// into one or more memtable pairs sharing the entry's seq.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WalEntry {
    Single(WalRecord),
    Batch(WalBatch),
}

impl WalEntry {
    /// The seq of this entry (the batch seq for a batch, shared by all its ops).
    pub fn seq(&self) -> u64 {
        match self {
            WalEntry::Single(r) => r.seq,
            WalEntry::Batch(b) => b.seq,
        }
    }

    /// Expand into the memtable `(InternalKey, Entry)` pairs it produces. A
    /// single record yields one pair; a batch yields one per op, all sharing
    /// the batch seq.
    pub fn into_memtable_pairs(self) -> Vec<(InternalKey, Entry)> {
        match self {
            WalEntry::Single(r) => vec![r.to_memtable_pair()],
            WalEntry::Batch(b) => {
                let seq = b.seq;
                b.ops
                    .into_iter()
                    .map(|op| op_to_memtable_pair(seq, op))
                    .collect()
            }
        }
    }
}

fn op_to_memtable_pair(seq: u64, op: WalOp) -> (InternalKey, Entry) {
    match op.command {
        WAL_DEL => (
            InternalKey::new(op.key, seq, EntryKind::Tombstone),
            Entry::tombstone(),
        ),
        _ => (
            InternalKey::new(op.key, seq, EntryKind::Value),
            Entry::value(
                op.value,
                if op.expires_at == 0 {
                    None
                } else {
                    Some(op.expires_at)
                },
            ),
        ),
    }
}

/// Decode one single (`WAL_PUT` / `WAL_DEL`) record from the front of `buf`.
fn decode_single(buf: &[u8]) -> EngineResult<Option<(WalEntry, usize)>> {
    if buf.len() < ENTRY_FIXED + 4 {
        return Ok(None);
    }
    let command = buf[0];
    let seq = u64::from_be_bytes(buf[1..9].try_into().unwrap());
    let expires_at = u64::from_be_bytes(buf[9..17].try_into().unwrap());
    let key_len = u32::from_be_bytes(buf[17..21].try_into().unwrap()) as usize;
    let value_len = u32::from_be_bytes(buf[21..25].try_into().unwrap()) as usize;
    let total = ENTRY_FIXED + key_len + value_len + 4;
    if buf.len() < total {
        return Ok(None);
    }
    let body_end = ENTRY_FIXED + key_len + value_len;
    let key = buf[ENTRY_FIXED..ENTRY_FIXED + key_len].to_vec();
    let value = buf[ENTRY_FIXED + key_len..body_end].to_vec();
    let stored_crc = u32::from_be_bytes(buf[body_end..total].try_into().unwrap());
    let computed = crc32fast::hash(&buf[..body_end]);
    if stored_crc != computed {
        return Err(EngineError::Io("WAL: CRC mismatch (torn write)".into()));
    }
    Ok(Some((
        WalEntry::Single(WalRecord {
            command,
            seq,
            expires_at,
            key,
            value,
        }),
        total,
    )))
}

/// Decode one atomic batch (`WAL_BATCH`) record from the front of `buf`. The
/// single trailing CRC covers every op, so a torn batch fails CRC as a whole
/// and replays no ops.
fn decode_batch(buf: &[u8]) -> EngineResult<Option<(WalEntry, usize)>> {
    if buf.len() < BATCH_HEADER {
        return Ok(None);
    }
    let seq = u64::from_be_bytes(buf[1..9].try_into().unwrap());
    let count = u32::from_be_bytes(buf[9..13].try_into().unwrap()) as usize;
    let mut offset = BATCH_HEADER;
    let mut ops = Vec::with_capacity(count.min(1024));
    for _ in 0..count {
        if buf.len() < offset + BATCH_OP_FIXED {
            return Ok(None); // short tail
        }
        let command = buf[offset];
        let expires_at = u64::from_be_bytes(buf[offset + 1..offset + 9].try_into().unwrap());
        let key_len = u32::from_be_bytes(buf[offset + 9..offset + 13].try_into().unwrap()) as usize;
        let value_len =
            u32::from_be_bytes(buf[offset + 13..offset + 17].try_into().unwrap()) as usize;
        let op_end = offset + BATCH_OP_FIXED + key_len + value_len;
        if buf.len() < op_end {
            return Ok(None); // short tail
        }
        if command != WAL_PUT && command != WAL_DEL {
            return Err(EngineError::Io("WAL: bad batch op command byte".into()));
        }
        let key = buf[offset + BATCH_OP_FIXED..offset + BATCH_OP_FIXED + key_len].to_vec();
        let value = buf[offset + BATCH_OP_FIXED + key_len..op_end].to_vec();
        ops.push(WalOp {
            command,
            expires_at,
            key,
            value,
        });
        offset = op_end;
    }
    let total = offset + 4;
    if buf.len() < total {
        return Ok(None);
    }
    let stored_crc = u32::from_be_bytes(buf[offset..total].try_into().unwrap());
    let computed = crc32fast::hash(&buf[..offset]);
    if stored_crc != computed {
        return Err(EngineError::Io(
            "WAL: batch CRC mismatch (torn write)".into(),
        ));
    }
    Ok(Some((WalEntry::Batch(WalBatch { seq, ops }), total)))
}

/// Decode all entries from a segment body (after the segment header). Stops at
/// the first torn/short tail, returning the entries decoded so far and whether
/// a torn tail was detected (so the caller can truncate).
pub fn replay_segment_body(body: &[u8]) -> (Vec<WalEntry>, bool) {
    let mut records = Vec::new();
    let mut offset = 0;
    let mut torn = false;
    while offset < body.len() {
        match WalRecord::decode_one(&body[offset..]) {
            Ok(Some((rec, consumed))) => {
                records.push(rec);
                offset += consumed;
            }
            Ok(None) => {
                // Short tail: trailing bytes that don't form a complete entry.
                if offset < body.len() {
                    torn = true;
                }
                break;
            }
            Err(_) => {
                // CRC mismatch / bad command: torn write. Stop here.
                torn = true;
                break;
            }
        }
    }
    (records, torn)
}

/// Segment file naming: `wal-00000001.log`.
pub fn segment_filename(id: u64) -> String {
    format!("wal-{:08}.log", id)
}

pub fn parse_segment_id(name: &str) -> Option<u64> {
    let stripped = name.strip_prefix("wal-")?.strip_suffix(".log")?;
    stripped.parse::<u64>().ok()
}

/// List WAL segment files in a directory, sorted by id ascending.
pub fn list_segments(dir: &Path) -> EngineResult<Vec<(u64, PathBuf)>> {
    let mut out = Vec::new();
    if !dir.exists() {
        return Ok(out);
    }
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = parse_segment_id(&name) {
            out.push((id, entry.path()));
        }
    }
    out.sort_by_key(|(id, _)| *id);
    Ok(out)
}

/// Whether an active segment of `current_size` should roll before appending
/// `next_entry_size` more bytes.
pub fn should_roll(current_size: usize, next_entry_size: usize) -> bool {
    current_size > 0 && current_size + next_entry_size > WAL_SEGMENT_SIZE
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unwrap_single(entry: WalEntry) -> WalRecord {
        match entry {
            WalEntry::Single(r) => r,
            WalEntry::Batch(_) => panic!("expected single record, got batch"),
        }
    }

    #[test]
    fn put_record_round_trips() {
        let rec = WalRecord::put(7, 999, b"\x01key".to_vec(), b"val".to_vec());
        let bytes = rec.encode();
        let (back, consumed) = WalRecord::decode_one(&bytes).unwrap().unwrap();
        assert_eq!(rec, unwrap_single(back));
        assert_eq!(consumed, bytes.len());
    }

    #[test]
    fn del_record_round_trips() {
        let rec = WalRecord::del(8, b"\x01gone".to_vec());
        let bytes = rec.encode();
        let (back, _) = WalRecord::decode_one(&bytes).unwrap().unwrap();
        let back = unwrap_single(back);
        assert_eq!(rec, back);
        assert_eq!(back.command, WAL_DEL);
    }

    #[test]
    fn batch_record_round_trips() {
        let batch = WalBatch {
            seq: 42,
            ops: vec![
                WalOp {
                    command: WAL_PUT,
                    expires_at: 0,
                    key: b"\x01doc".to_vec(),
                    value: b"body".to_vec(),
                },
                WalOp {
                    command: WAL_PUT,
                    expires_at: 123,
                    key: b"\x01idx".to_vec(),
                    value: b"doc".to_vec(),
                },
                WalOp {
                    command: WAL_DEL,
                    expires_at: 0,
                    key: b"\x01old".to_vec(),
                    value: Vec::new(),
                },
            ],
        };
        let bytes = batch.encode();
        let (back, consumed) = WalRecord::decode_one(&bytes).unwrap().unwrap();
        assert_eq!(consumed, bytes.len());
        match back {
            WalEntry::Batch(b) => assert_eq!(b, batch),
            WalEntry::Single(_) => panic!("expected batch, got single"),
        }
    }

    #[test]
    fn batch_crc_mismatch_detected() {
        let batch = WalBatch {
            seq: 1,
            ops: vec![WalOp {
                command: WAL_PUT,
                expires_at: 0,
                key: b"\x01k".to_vec(),
                value: b"v".to_vec(),
            }],
        };
        let mut bytes = batch.encode();
        let n = bytes.len();
        bytes[n - 6] ^= 0xFF; // corrupt a value byte before the CRC
        assert!(WalRecord::decode_one(&bytes).is_err());
    }

    #[test]
    fn batch_short_tail_returns_none() {
        let batch = WalBatch {
            seq: 1,
            ops: vec![WalOp {
                command: WAL_PUT,
                expires_at: 0,
                key: b"\x01k".to_vec(),
                value: b"v".to_vec(),
            }],
        };
        let bytes = batch.encode();
        // Truncate inside the record: must report "need more bytes", not error.
        assert!(WalRecord::decode_one(&bytes[..bytes.len() - 3])
            .unwrap()
            .is_none());
    }

    #[test]
    fn batch_expands_to_memtable_pairs_sharing_seq() {
        let batch = WalBatch {
            seq: 9,
            ops: vec![
                WalOp {
                    command: WAL_PUT,
                    expires_at: 0,
                    key: b"\x01a".to_vec(),
                    value: b"1".to_vec(),
                },
                WalOp {
                    command: WAL_DEL,
                    expires_at: 0,
                    key: b"\x01b".to_vec(),
                    value: Vec::new(),
                },
            ],
        };
        let pairs = WalEntry::Batch(batch).into_memtable_pairs();
        assert_eq!(pairs.len(), 2);
        assert!(pairs.iter().all(|(k, _)| k.seq == 9));
        assert_eq!(pairs[0].0.kind, EntryKind::Value);
        assert_eq!(pairs[1].0.kind, EntryKind::Tombstone);
        assert!(pairs[1].1.is_tombstone());
    }

    #[test]
    fn crc_mismatch_detected() {
        let rec = WalRecord::put(1, 0, b"\x01k".to_vec(), b"v".to_vec());
        let mut bytes = rec.encode();
        let n = bytes.len();
        bytes[n - 5] ^= 0xFF; // corrupt a value byte
        assert!(WalRecord::decode_one(&bytes).is_err());
    }

    #[test]
    fn short_buffer_returns_none() {
        let rec = WalRecord::put(1, 0, b"\x01k".to_vec(), b"v".to_vec());
        let bytes = rec.encode();
        assert!(WalRecord::decode_one(&bytes[..bytes.len() - 2])
            .unwrap()
            .is_none());
    }

    #[test]
    fn replay_multiple_records() {
        let mut body = Vec::new();
        for i in 1..=5u64 {
            body.extend_from_slice(
                &WalRecord::put(i, 0, vec![1u8, i as u8], vec![i as u8]).encode(),
            );
        }
        let (recs, torn) = replay_segment_body(&body);
        assert_eq!(recs.len(), 5);
        assert!(!torn);
        assert_eq!(recs[0].seq(), 1);
        assert_eq!(recs[4].seq(), 5);
    }

    #[test]
    fn replay_detects_torn_tail() {
        let mut body = Vec::new();
        body.extend_from_slice(&WalRecord::put(1, 0, vec![1u8, 2], vec![3]).encode());
        body.extend_from_slice(&[0xAB, 0xCD]); // partial trailing garbage
        let (recs, torn) = replay_segment_body(&body);
        assert_eq!(recs.len(), 1);
        assert!(torn);
    }

    #[test]
    fn segment_name_round_trips() {
        assert_eq!(segment_filename(1), "wal-00000001.log");
        assert_eq!(parse_segment_id("wal-00000042.log"), Some(42));
        assert_eq!(parse_segment_id("not-a-segment"), None);
    }

    #[test]
    fn to_memtable_pair_maps_kinds() {
        let put = WalRecord::put(1, 0, vec![1, 2], vec![3]);
        let (k, e) = put.to_memtable_pair();
        assert_eq!(k.kind, EntryKind::Value);
        assert!(!e.is_tombstone());

        let del = WalRecord::del(2, vec![1, 2]);
        let (k, e) = del.to_memtable_pair();
        assert_eq!(k.kind, EntryKind::Tombstone);
        assert!(e.is_tombstone());
    }
}
