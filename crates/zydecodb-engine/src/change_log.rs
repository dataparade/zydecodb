//! Retained WAL archive for logical change streams.
//!
//! When enabled, each sealed WAL segment is hardlinked (copy fallback) into
//! `archive_dir` and recorded in an fsynced manifest *before* the live WAL
//! segment may be unlinked. This is distinct from operator WAL shipping
//! (`shipping.rs`): archives are for authorized document change-stream resume,
//! not replica catch-up.

use crate::errors::{EngineError, EngineResult};
use crate::wal::{self, WalEntry, WAL_DEL, WAL_PUT};
use crc32fast::Hasher;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

pub const MANIFEST_NAME: &str = "manifest.json";
pub const TOKEN_VERSION: u8 = 1;

#[derive(Debug, Clone)]
pub struct ChangeLogConfig {
    pub archive_dir: PathBuf,
    pub retention_secs: u64,
    pub retention_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ArchiveSegment {
    pub segment_id: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub sealed_unix_ms: u64,
    pub size_bytes: u64,
    pub file_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ChangeLogManifest {
    /// Stable 16-byte database id (hex). Bound into resume tokens.
    pub database_id_hex: String,
    pub segments: Vec<ArchiveSegment>,
}

impl ChangeLogManifest {
    pub fn new(database_id: [u8; 16]) -> Self {
        Self {
            database_id_hex: hex_encode(&database_id),
            segments: Vec::new(),
        }
    }

    pub fn database_id(&self) -> EngineResult<[u8; 16]> {
        hex_decode_16(&self.database_id_hex)
    }

    pub fn earliest_seq(&self) -> Option<u64> {
        self.segments.iter().map(|s| s.min_seq).min()
    }

    pub fn latest_seq(&self) -> Option<u64> {
        self.segments.iter().map(|s| s.max_seq).max()
    }

    pub fn total_bytes(&self) -> u64 {
        self.segments.iter().map(|s| s.size_bytes).sum()
    }

    pub fn contains_segment(&self, segment_id: u64) -> bool {
        self.segments.iter().any(|s| s.segment_id == segment_id)
    }
}

/// Opaque resume token bound to database, tenant prefix, and collection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumeToken {
    pub database_id: [u8; 16],
    pub tenant_prefix: Vec<u8>,
    pub collection_id: u32,
    pub seq: u64,
    pub op_ordinal: u32,
}

impl ResumeToken {
    pub fn encode(&self) -> Vec<u8> {
        let mut body = Vec::new();
        body.push(TOKEN_VERSION);
        body.extend_from_slice(&self.database_id);
        body.extend_from_slice(&(self.tenant_prefix.len() as u16).to_be_bytes());
        body.extend_from_slice(&self.tenant_prefix);
        body.extend_from_slice(&self.collection_id.to_be_bytes());
        body.extend_from_slice(&self.seq.to_be_bytes());
        body.extend_from_slice(&self.op_ordinal.to_be_bytes());
        let mut hasher = Hasher::new();
        hasher.update(&body);
        let crc = hasher.finalize();
        body.extend_from_slice(&crc.to_be_bytes());
        body
    }

    pub fn decode(bytes: &[u8]) -> EngineResult<Self> {
        if bytes.len() < 1 + 16 + 2 + 4 + 8 + 4 + 4 {
            return Err(EngineError::Protocol("resume token truncated".into()));
        }
        if bytes[0] != TOKEN_VERSION {
            return Err(EngineError::Protocol(format!(
                "unsupported resume token version {}",
                bytes[0]
            )));
        }
        let (body, crc_bytes) = bytes.split_at(bytes.len() - 4);
        let mut hasher = Hasher::new();
        hasher.update(body);
        let expected = hasher.finalize();
        let actual = u32::from_be_bytes(crc_bytes.try_into().unwrap());
        if expected != actual {
            return Err(EngineError::Protocol(
                "resume token checksum mismatch".into(),
            ));
        }
        let mut pos = 1usize;
        let mut database_id = [0u8; 16];
        database_id.copy_from_slice(&body[pos..pos + 16]);
        pos += 16;
        let prefix_len = u16::from_be_bytes(body[pos..pos + 2].try_into().unwrap()) as usize;
        pos += 2;
        if body.len() < pos + prefix_len + 4 + 8 + 4 {
            return Err(EngineError::Protocol("resume token truncated".into()));
        }
        let tenant_prefix = body[pos..pos + prefix_len].to_vec();
        pos += prefix_len;
        let collection_id = u32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
        pos += 4;
        let seq = u64::from_be_bytes(body[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let op_ordinal = u32::from_be_bytes(body[pos..pos + 4].try_into().unwrap());
        Ok(ResumeToken {
            database_id,
            tenant_prefix,
            collection_id,
            seq,
            op_ordinal,
        })
    }
}

/// One logical document-body change decoded from the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogicalChange {
    pub seq: u64,
    pub op_ordinal: u32,
    pub collection_id: u32,
    pub doc_id: Vec<u8>,
    pub kind: LogicalChangeKind,
    /// Stored value including value_kind byte (empty for deletes).
    pub stored_value: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogicalChangeKind {
    Upsert,
    Delete,
}

/// Load or create the archive manifest. Creates `archive_dir` when missing.
pub fn open_manifest(
    cfg: &ChangeLogConfig,
    database_id: [u8; 16],
) -> EngineResult<ChangeLogManifest> {
    fs::create_dir_all(&cfg.archive_dir)?;
    let path = cfg.archive_dir.join(MANIFEST_NAME);
    if !path.exists() {
        let manifest = ChangeLogManifest::new(database_id);
        persist_manifest(cfg, &manifest)?;
        return Ok(manifest);
    }
    let text = fs::read_to_string(&path)?;
    let manifest: ChangeLogManifest = serde_json::from_str(&text)
        .map_err(|e| EngineError::Io(format!("change_log manifest: {e}")))?;
    Ok(manifest)
}

/// Atomically rewrite the manifest (temp + rename + fsync).
pub fn persist_manifest(cfg: &ChangeLogConfig, manifest: &ChangeLogManifest) -> EngineResult<()> {
    fs::create_dir_all(&cfg.archive_dir)?;
    let path = cfg.archive_dir.join(MANIFEST_NAME);
    let tmp = cfg.archive_dir.join("manifest.json.tmp");
    let bytes = serde_json::to_vec_pretty(manifest)
        .map_err(|e| EngineError::Io(format!("change_log manifest encode: {e}")))?;
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    crate::failpoints::failpoint_result(crate::failpoints::CHANGELOG_BEFORE_MANIFEST_RENAME)?;
    fs::rename(&tmp, &path)?;
    // Directory fsync so the rename is durable.
    {
        let dir = File::open(&cfg.archive_dir)?;
        dir.sync_all()?;
    }
    crate::failpoints::failpoint_result(crate::failpoints::CHANGELOG_AFTER_MANIFEST_RENAME)?;
    Ok(())
}

/// Archive a sealed WAL segment if not already present. Hardlink with copy fallback.
pub fn archive_segment(
    cfg: &ChangeLogConfig,
    manifest: &mut ChangeLogManifest,
    wal_dir: &Path,
    segment_id: u64,
    max_seq: u64,
) -> EngineResult<()> {
    if manifest.contains_segment(segment_id) {
        return Ok(());
    }
    let src_name = wal::segment_filename(segment_id);
    let src = wal_dir.join(&src_name);
    if !src.exists() {
        return Err(EngineError::Io(format!(
            "change_log: WAL segment {} missing at {}",
            segment_id,
            src.display()
        )));
    }
    let (entries, outcome) = read_segment_entries(&src)?;
    if outcome != wal::ReplayOutcome::Clean {
        return Err(EngineError::Io(format!(
            "change_log: refusing to archive damaged segment {}",
            segment_id
        )));
    }
    let min_seq = entries.iter().map(|e| e.seq()).min().unwrap_or(max_seq);
    let observed_max = entries.iter().map(|e| e.seq()).max().unwrap_or(max_seq);
    let max_seq = max_seq.max(observed_max);
    let size_bytes = fs::metadata(&src)?.len();
    let dst = cfg.archive_dir.join(&src_name);
    crate::failpoints::failpoint_result(crate::failpoints::CHANGELOG_BEFORE_ARCHIVE)?;
    hardlink_or_copy(&src, &dst)?;
    // Ensure archived bytes are durable before recording the manifest.
    {
        let f = OpenOptions::new().write(true).open(&dst)?;
        f.sync_all()?;
    }
    crate::failpoints::failpoint_result(crate::failpoints::CHANGELOG_AFTER_ARCHIVE)?;
    // Publish to disk BEFORE the in-memory manifest claims the segment:
    // `wal_segment_safe_to_unlink` trusts the in-memory state, so it must
    // never run ahead of the durable manifest. If persist fails, the live
    // WAL stays put and a later retry re-archives (hardlink_or_copy removes
    // any stale destination first, so the retry is idempotent).
    let mut candidate = manifest.clone();
    candidate.segments.push(ArchiveSegment {
        segment_id,
        min_seq,
        max_seq,
        sealed_unix_ms: now_ms(),
        size_bytes,
        file_name: src_name,
    });
    candidate.segments.sort_by_key(|s| s.segment_id);
    persist_manifest(cfg, &candidate)?;
    *manifest = candidate;
    Ok(())
}

/// Whether the archive already contains `segment_id` (safe to unlink live WAL).
pub fn is_archived(manifest: &ChangeLogManifest, segment_id: u64) -> bool {
    manifest.contains_segment(segment_id)
}

/// Drop oldest sealed archives until within retention time/bytes.
pub fn prune(cfg: &ChangeLogConfig, manifest: &mut ChangeLogManifest) -> EngineResult<usize> {
    let now = now_ms();
    let mut removed = 0usize;
    loop {
        if manifest.segments.is_empty() {
            break;
        }
        let over_bytes = cfg.retention_bytes > 0 && manifest.total_bytes() > cfg.retention_bytes;
        let oldest_age_ms = now.saturating_sub(manifest.segments[0].sealed_unix_ms);
        let over_time =
            cfg.retention_secs > 0 && oldest_age_ms > cfg.retention_secs.saturating_mul(1000);
        if !over_bytes && !over_time {
            break;
        }
        // Keep at least one segment so earliest_seq remains defined when streams are idle.
        if manifest.segments.len() == 1 && !over_time {
            break;
        }
        let victim = manifest.segments.remove(0);
        let path = cfg.archive_dir.join(&victim.file_name);
        let _ = fs::remove_file(&path);
        removed += 1;
    }
    if removed > 0 {
        persist_manifest(cfg, manifest)?;
    }
    Ok(removed)
}

/// Reconcile: archive any sealed WAL segments present in `wal_dir` that are
/// missing from the manifest. Call before opening a new active segment.
pub fn reconcile_sealed_wal(
    cfg: &ChangeLogConfig,
    manifest: &mut ChangeLogManifest,
    wal_dir: &Path,
    active_wal_id: Option<u64>,
) -> EngineResult<()> {
    let segments = wal::list_segments(wal_dir)?;
    for (id, path) in segments {
        if Some(id) == active_wal_id {
            continue;
        }
        if manifest.contains_segment(id) {
            continue;
        }
        let (entries, outcome) = read_segment_entries(&path)?;
        if outcome == wal::ReplayOutcome::Corruption {
            return Err(EngineError::Io(format!(
                "change_log: sealed WAL segment {id} is corrupted"
            )));
        }
        let max_seq = entries.iter().map(|e| e.seq()).max().unwrap_or(0);
        archive_segment(cfg, manifest, wal_dir, id, max_seq)?;
    }
    Ok(())
}

/// Iterate logical document-body changes from archived segments then the
/// active WAL file (if provided), starting strictly after `(after_seq, after_ord)`.
pub fn iter_logical_changes_after(
    cfg: &ChangeLogConfig,
    manifest: &ChangeLogManifest,
    active_wal_path: Option<&Path>,
    tenant_prefix: &[u8],
    collection_id: u32,
    after_seq: u64,
    after_ord: u32,
) -> EngineResult<Vec<LogicalChange>> {
    let mut out = Vec::new();
    let doc_header_len = tenant_prefix.len() + 1 + 4;
    let mut push_entry = |entry: WalEntry| {
        let seq = entry.seq();
        let ops: Vec<(u8, Vec<u8>, Vec<u8>)> = match entry {
            WalEntry::Single(r) => vec![(r.command, r.key, r.value)],
            WalEntry::Batch(b) => b
                .ops
                .into_iter()
                .map(|op| (op.command, op.key, op.value))
                .collect(),
        };
        for (ordinal, (command, key, value)) in ops.into_iter().enumerate() {
            let ord = ordinal as u32;
            if seq < after_seq || (seq == after_seq && ord <= after_ord) {
                continue;
            }
            if !is_doc_body_key(&key, tenant_prefix, collection_id) {
                continue;
            }
            let doc_id = key[doc_header_len..].to_vec();
            let kind = match command {
                WAL_PUT => LogicalChangeKind::Upsert,
                WAL_DEL => LogicalChangeKind::Delete,
                _ => continue,
            };
            out.push(LogicalChange {
                seq,
                op_ordinal: ord,
                collection_id,
                doc_id,
                kind,
                stored_value: if kind == LogicalChangeKind::Upsert {
                    value
                } else {
                    Vec::new()
                },
            });
        }
    };

    for seg in &manifest.segments {
        if seg.max_seq < after_seq {
            continue;
        }
        let path = cfg.archive_dir.join(&seg.file_name);
        let (entries, outcome) = read_segment_entries(&path)?;
        if outcome == wal::ReplayOutcome::Corruption {
            return Err(EngineError::Io(format!(
                "change_log: archived segment {} corrupted",
                seg.segment_id
            )));
        }
        for entry in entries {
            push_entry(entry);
        }
    }
    if let Some(active) = active_wal_path {
        if active.exists() {
            let (entries, _outcome) = read_segment_entries(active)?;
            // Active may have a torn tail; use the valid prefix only.
            for entry in entries {
                push_entry(entry);
            }
        }
    }
    out.sort_by(|a, b| a.seq.cmp(&b.seq).then(a.op_ordinal.cmp(&b.op_ordinal)));
    Ok(out)
}

fn is_doc_body_key(key: &[u8], tenant_prefix: &[u8], collection_id: u32) -> bool {
    if key.len() < tenant_prefix.len() + 1 + 4 {
        return false;
    }
    if !key.starts_with(tenant_prefix) {
        return false;
    }
    let rest = &key[tenant_prefix.len()..];
    if rest[0] != b'd' {
        return false;
    }
    let id = u32::from_be_bytes(rest[1..5].try_into().unwrap());
    id == collection_id
}

fn read_segment_entries(path: &Path) -> EngineResult<(Vec<WalEntry>, wal::ReplayOutcome)> {
    let mut f = File::open(path)?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    if buf.len() < wal::SEGMENT_HEADER_LEN {
        return Ok((Vec::new(), wal::ReplayOutcome::TornTail));
    }
    let version = buf[8];
    if version != wal::WAL_FORMAT_VERSION {
        return Err(EngineError::Io(format!(
            "change_log: unsupported WAL format version {version}"
        )));
    }
    Ok(wal::replay_segment_body(&buf[wal::SEGMENT_HEADER_LEN..]))
}

fn hardlink_or_copy(src: &Path, dst: &Path) -> EngineResult<()> {
    let _ = fs::remove_file(dst);
    match fs::hard_link(src, dst) {
        Ok(()) => Ok(()),
        Err(e) => {
            // Prefer copy on EXDEV / unsupported; also fall back for other
            // hardlink failures so archival still succeeds.
            let _ = e;
            fs::copy(src, dst).map(|_| ()).map_err(|copy_err| {
                EngineError::Io(format!("change_log archive copy failed: {copy_err}"))
            })
        }
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0xf) as usize] as char);
    }
    out
}

fn hex_decode_16(s: &str) -> EngineResult<[u8; 16]> {
    if s.len() != 32 {
        return Err(EngineError::Io("change_log: bad database_id hex".into()));
    }
    let mut out = [0u8; 16];
    for i in 0..16 {
        let byte = u8::from_str_radix(&s[i * 2..i * 2 + 2], 16)
            .map_err(|_| EngineError::Io("change_log: bad database_id hex".into()))?;
        out[i] = byte;
    }
    Ok(out)
}

/// Derive a stable 16-byte database id from the data directory path.
pub fn database_id_from_data_dir(data_dir: &Path) -> [u8; 16] {
    let mut hasher = Sha256::new();
    hasher.update(data_dir.to_string_lossy().as_bytes());
    let digest = hasher.finalize();
    let mut id = [0u8; 16];
    id.copy_from_slice(&digest[..16]);
    id
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::{WalBatch, WalOp, WalRecord};
    use tempfile::TempDir;

    fn write_segment(dir: &Path, id: u64, records: &[WalRecord]) {
        let path = dir.join(wal::segment_filename(id));
        let mut f = File::create(&path).unwrap();
        f.write_all(&1u64.to_be_bytes()).unwrap();
        f.write_all(&[wal::WAL_FORMAT_VERSION]).unwrap();
        for r in records {
            f.write_all(&r.encode()).unwrap();
        }
        f.sync_all().unwrap();
    }

    #[test]
    fn resume_token_round_trip() {
        let token = ResumeToken {
            database_id: [7u8; 16],
            tenant_prefix: b"\x01tenant".to_vec(),
            collection_id: 3,
            seq: 99,
            op_ordinal: 2,
        };
        let encoded = token.encode();
        assert_eq!(ResumeToken::decode(&encoded).unwrap(), token);
        let mut bad = encoded.clone();
        bad[5] ^= 0xff;
        assert!(ResumeToken::decode(&bad).is_err());
    }

    #[test]
    fn archive_before_manifest_and_logical_decode() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        let archive_dir = tmp.path().join("archive");
        fs::create_dir_all(&wal_dir).unwrap();
        let cfg = ChangeLogConfig {
            archive_dir: archive_dir.clone(),
            retention_secs: 3600,
            retention_bytes: 1024 * 1024,
        };
        let db_id = [1u8; 16];
        let mut manifest = open_manifest(&cfg, db_id).unwrap();

        let prefix = b"\x01";
        let mut doc_key = prefix.to_vec();
        doc_key.push(b'd');
        doc_key.extend_from_slice(&5u32.to_be_bytes());
        doc_key.extend_from_slice(b"doc1");
        let mut idx_key = prefix.to_vec();
        idx_key.push(b'i');
        idx_key.extend_from_slice(&5u32.to_be_bytes());
        idx_key.extend_from_slice(&1u32.to_be_bytes());
        idx_key.extend_from_slice(b"x");
        idx_key.extend_from_slice(b"doc1");

        let batch = WalBatch {
            seq: 10,
            ops: vec![
                WalOp {
                    command: WAL_PUT,
                    expires_at: 0,
                    key: doc_key.clone(),
                    value: vec![0x01, 1, 2, 3],
                },
                WalOp {
                    command: WAL_PUT,
                    expires_at: 0,
                    key: idx_key,
                    value: b"doc1".to_vec(),
                },
            ],
        };
        let path = wal_dir.join(wal::segment_filename(1));
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(&1u64.to_be_bytes()).unwrap();
            f.write_all(&[wal::WAL_FORMAT_VERSION]).unwrap();
            f.write_all(&batch.encode()).unwrap();
            f.sync_all().unwrap();
        }

        archive_segment(&cfg, &mut manifest, &wal_dir, 1, 10).unwrap();
        assert!(is_archived(&manifest, 1));
        assert!(archive_dir.join(wal::segment_filename(1)).exists());

        let changes = iter_logical_changes_after(&cfg, &manifest, None, prefix, 5, 0, 0).unwrap();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].seq, 10);
        assert_eq!(changes[0].op_ordinal, 0);
        assert_eq!(changes[0].doc_id, b"doc1");
        assert_eq!(changes[0].kind, LogicalChangeKind::Upsert);

        // Exclusive resume after the event.
        let after = iter_logical_changes_after(&cfg, &manifest, None, prefix, 5, 10, 0).unwrap();
        assert!(after.is_empty());
    }

    #[test]
    fn prune_removes_old_segments() {
        let tmp = TempDir::new().unwrap();
        let wal_dir = tmp.path().join("wal");
        let archive_dir = tmp.path().join("archive");
        fs::create_dir_all(&wal_dir).unwrap();
        let cfg = ChangeLogConfig {
            archive_dir,
            retention_secs: 1,
            retention_bytes: u64::MAX,
        };
        let mut manifest = open_manifest(&cfg, [2u8; 16]).unwrap();
        write_segment(
            &wal_dir,
            1,
            &[WalRecord::put(
                1,
                0,
                b"\x01d\0\0\0\x01a".to_vec(),
                b"v".to_vec(),
            )],
        );
        write_segment(
            &wal_dir,
            2,
            &[WalRecord::put(
                2,
                0,
                b"\x01d\0\0\0\x01b".to_vec(),
                b"v".to_vec(),
            )],
        );
        archive_segment(&cfg, &mut manifest, &wal_dir, 1, 1).unwrap();
        // Backdate first segment.
        manifest.segments[0].sealed_unix_ms = 1;
        persist_manifest(&cfg, &manifest).unwrap();
        archive_segment(&cfg, &mut manifest, &wal_dir, 2, 2).unwrap();
        let removed = prune(&cfg, &mut manifest).unwrap();
        assert!(removed >= 1);
        assert!(!manifest.contains_segment(1));
    }
}
