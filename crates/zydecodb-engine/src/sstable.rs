//! Block-based SSTable writer and reader.
//!
//! File layout:
//! ```text
//! [Data block 0] ... [Data block N]
//! [Bloom block]   (reserved/optional)
//! [Index block]   (first_key_of_block -> offset, length) per data block
//! [Footer - 40 bytes]
//!   [8] index_offset
//!   [8] index_length
//!   [8] bloom_offset (0 if absent)
//!   [8] bloom_length (0 if absent)
//!   [4] magic 0x50524144 ("PRAD")
//!   [4] format version (0x00000001 or 0x00000002)
//! ```
//!
//! Data block entry: `[1] kind [8] seq [4] key_len [4] value_len [8] expires_at [K] key [V] value`.
//! Entries are globally sorted by InternalKey (user_key ASC, seq DESC).
//!
//! Integrity: in format v2 each block (data, bloom, index) carries a trailing
//! CRC32 over its body, verified on read so silent bit-rot at rest surfaces as
//! an `Io` error instead of being served as truth or panicking on decode. The
//! block `length` recorded in the index/footer includes the 4-byte trailer.
//! Format v1 files (no per-block CRC) remain readable without verification, so
//! existing data keeps working and is rewritten to v2 by normal compaction.

use crate::block_cache::{BlockCache, BlockKey, CachedBlock};
use crate::bloom::BloomFilter;
use crate::entry::Entry;
use crate::errors::{EngineError, EngineResult};
use crate::keys::{EntryKind, InternalKey, SSTABLE_BLOCK_SIZE};
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub const MAGIC: u32 = 0x5052_4144; // "PRAD"
/// Current write format: per-block CRC32 trailers (see module docs).
pub const FORMAT_VERSION: u32 = 0x0000_0002;
/// Legacy format: no per-block checksums. Still readable (no verification).
pub const FORMAT_VERSION_V1: u32 = 0x0000_0001;
pub const FOOTER_LEN: usize = 40;
/// Length of the trailing CRC32 appended to each block in format v2.
const BLOCK_CRC_LEN: usize = 4;

/// Append a CRC32 over `bytes[body_start..]` (the block body just written).
/// The block's recorded length then includes this 4-byte trailer.
fn append_block_crc(bytes: &mut Vec<u8>, body_start: usize) {
    let crc = crc32fast::hash(&bytes[body_start..]);
    bytes.extend_from_slice(&crc.to_be_bytes());
}

/// Verify and strip a block's CRC32 trailer, returning the body. For v1 files
/// (no trailer) the raw bytes are returned unverified. `what` names the block
/// kind for error messages (data / index / bloom).
fn verify_block_crc(version: u32, raw: &[u8], what: &str) -> EngineResult<Vec<u8>> {
    if version < FORMAT_VERSION {
        return Ok(raw.to_vec());
    }
    if raw.len() < BLOCK_CRC_LEN {
        return Err(EngineError::Io(format!(
            "sstable: {what} block too short for checksum"
        )));
    }
    let split = raw.len() - BLOCK_CRC_LEN;
    let stored = u32::from_be_bytes(raw[split..].try_into().unwrap());
    let computed = crc32fast::hash(&raw[..split]);
    if stored != computed {
        return Err(EngineError::Io(format!(
            "sstable: {what} block checksum mismatch (corruption)"
        )));
    }
    Ok(raw[..split].to_vec())
}

/// Serialize one data-block entry.
fn encode_entry(key: &InternalKey, entry: &Entry) -> Vec<u8> {
    let value = entry.value.as_deref().unwrap_or(&[]);
    let expires_at = entry.expires_at.unwrap_or(0);
    let mut buf = Vec::with_capacity(25 + key.user_key.len() + value.len());
    buf.push(key.kind.as_u8());
    buf.extend_from_slice(&key.seq.to_be_bytes());
    buf.extend_from_slice(&(key.user_key.len() as u32).to_be_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(&expires_at.to_be_bytes());
    buf.extend_from_slice(&key.user_key);
    buf.extend_from_slice(value);
    buf
}

/// Decode one data-block entry from the front of `buf`. Returns `(key, entry, consumed)`.
fn decode_entry(buf: &[u8]) -> EngineResult<(InternalKey, Entry, usize)> {
    const FIXED: usize = 1 + 8 + 4 + 4 + 8;
    if buf.len() < FIXED {
        return Err(EngineError::Io("sstable: short entry header".into()));
    }
    let kind = EntryKind::from_u8(buf[0])
        .ok_or_else(|| EngineError::Io("sstable: bad entry kind".into()))?;
    let seq = u64::from_be_bytes(buf[1..9].try_into().unwrap());
    let key_len = u32::from_be_bytes(buf[9..13].try_into().unwrap()) as usize;
    let value_len = u32::from_be_bytes(buf[13..17].try_into().unwrap()) as usize;
    let expires_at = u64::from_be_bytes(buf[17..25].try_into().unwrap());
    let total = FIXED + key_len + value_len;
    if buf.len() < total {
        return Err(EngineError::Io("sstable: truncated entry".into()));
    }
    let user_key = buf[FIXED..FIXED + key_len].to_vec();
    let value = buf[FIXED + key_len..total].to_vec();
    let key = InternalKey::new(user_key, seq, kind);
    let entry = match kind {
        EntryKind::Tombstone => Entry::tombstone(),
        EntryKind::Value => Entry::value(
            value,
            if expires_at == 0 {
                None
            } else {
                Some(expires_at)
            },
        ),
    };
    Ok((key, entry, total))
}

/// Index block entry: first key of a data block plus its offset and length.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    first_key: Vec<u8>, // encoded InternalKey ordering key (user_key only is insufficient; store full)
    first_user_key: Vec<u8>,
    first_seq: u64,
    offset: u64,
    length: u64,
}

/// The serialized form of a complete SSTable in memory, ready to write to disk.
pub struct SstableBytes {
    pub bytes: Vec<u8>,
    pub min_key: Vec<u8>,
    pub max_key: Vec<u8>,
    pub min_seq: u64,
    pub max_seq: u64,
    pub num_entries: u64,
}

/// Build an SSTable from sorted `(InternalKey, Entry)` pairs. Caller MUST pass
/// them already sorted by InternalKey ordering (user_key ASC, seq DESC).
pub fn build(sorted: &[(InternalKey, Entry)], with_bloom: bool) -> SstableBytes {
    let mut bytes = Vec::new();
    let mut index: Vec<IndexEntry> = Vec::new();
    let mut bloom_keys: Vec<Vec<u8>> = Vec::new();

    let mut min_key = Vec::new();
    let mut max_key = Vec::new();
    let mut min_seq = u64::MAX;
    let mut max_seq = 0u64;

    let mut i = 0;
    while i < sorted.len() {
        let block_start = bytes.len();
        let first = &sorted[i].0;
        let first_user_key = first.user_key.clone();
        let first_seq = first.seq;

        // Pack entries into this block until it exceeds SSTABLE_BLOCK_SIZE.
        let mut block_bytes_len = 0usize;
        while i < sorted.len() {
            let (k, e) = &sorted[i];
            let enc = encode_entry(k, e);
            block_bytes_len += enc.len();
            bytes.extend_from_slice(&enc);

            if min_key.is_empty() || k.user_key < min_key {
                min_key = k.user_key.clone();
            }
            if k.user_key > max_key {
                max_key = k.user_key.clone();
            }
            min_seq = min_seq.min(k.seq);
            max_seq = max_seq.max(k.seq);
            if with_bloom {
                bloom_keys.push(k.user_key.clone());
            }

            i += 1;
            if block_bytes_len >= SSTABLE_BLOCK_SIZE {
                break;
            }
        }

        // v2: CRC32 trailer over the block body; recorded length includes it.
        append_block_crc(&mut bytes, block_start);
        index.push(IndexEntry {
            first_key: first_user_key.clone(),
            first_user_key,
            first_seq,
            offset: block_start as u64,
            length: (bytes.len() - block_start) as u64,
        });
    }

    // Bloom block (with CRC32 trailer).
    let (bloom_offset, bloom_length) = if with_bloom && !bloom_keys.is_empty() {
        let bf = BloomFilter::build(&bloom_keys);
        let enc = bf.encode();
        let off = bytes.len() as u64;
        bytes.extend_from_slice(&enc);
        append_block_crc(&mut bytes, off as usize);
        (off, bytes.len() as u64 - off)
    } else {
        (0, 0)
    };

    // Index block (with CRC32 trailer).
    let index_offset = bytes.len() as u64;
    let index_bytes = encode_index(&index);
    bytes.extend_from_slice(&index_bytes);
    append_block_crc(&mut bytes, index_offset as usize);
    let index_length = bytes.len() as u64 - index_offset;

    // Footer.
    bytes.extend_from_slice(&index_offset.to_be_bytes());
    bytes.extend_from_slice(&index_length.to_be_bytes());
    bytes.extend_from_slice(&bloom_offset.to_be_bytes());
    bytes.extend_from_slice(&bloom_length.to_be_bytes());
    bytes.extend_from_slice(&MAGIC.to_be_bytes());
    bytes.extend_from_slice(&FORMAT_VERSION.to_be_bytes());

    SstableBytes {
        bytes,
        min_key,
        max_key,
        min_seq: if min_seq == u64::MAX { 0 } else { min_seq },
        max_seq,
        num_entries: sorted.len() as u64,
    }
}

fn encode_index(index: &[IndexEntry]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&(index.len() as u32).to_be_bytes());
    for e in index {
        buf.extend_from_slice(&(e.first_user_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&e.first_user_key);
        buf.extend_from_slice(&e.first_seq.to_be_bytes());
        buf.extend_from_slice(&e.offset.to_be_bytes());
        buf.extend_from_slice(&e.length.to_be_bytes());
    }
    buf
}

fn decode_index(buf: &[u8]) -> EngineResult<Vec<IndexEntry>> {
    if buf.len() < 4 {
        return Err(EngineError::Io("sstable: short index".into()));
    }
    let count = u32::from_be_bytes(buf[0..4].try_into().unwrap()) as usize;
    let mut off = 4;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let klen = u32::from_be_bytes(
            buf.get(off..off + 4)
                .ok_or_else(|| EngineError::Io("sstable: index trunc".into()))?
                .try_into()
                .unwrap(),
        ) as usize;
        off += 4;
        let first_user_key = buf
            .get(off..off + klen)
            .ok_or_else(|| EngineError::Io("sstable: index key trunc".into()))?
            .to_vec();
        off += klen;
        let first_seq = read_u64(buf, off)?;
        off += 8;
        let offset = read_u64(buf, off)?;
        off += 8;
        let length = read_u64(buf, off)?;
        off += 8;
        out.push(IndexEntry {
            first_key: first_user_key.clone(),
            first_user_key,
            first_seq,
            offset,
            length,
        });
    }
    Ok(out)
}

fn read_u64(buf: &[u8], off: usize) -> EngineResult<u64> {
    buf.get(off..off + 8)
        .map(|s| u64::from_be_bytes(s.try_into().unwrap()))
        .ok_or_else(|| EngineError::Io("sstable: u64 trunc".into()))
}

/// A parsed SSTable footer.
#[derive(Debug, Clone, Copy)]
pub struct Footer {
    pub index_offset: u64,
    pub index_length: u64,
    pub bloom_offset: u64,
    pub bloom_length: u64,
    /// On-disk format version (1 = no per-block CRC, 2 = per-block CRC).
    pub version: u32,
}

pub fn parse_footer(file_tail: &[u8]) -> EngineResult<Footer> {
    if file_tail.len() < FOOTER_LEN {
        return Err(EngineError::Io("sstable: file too small for footer".into()));
    }
    let f = &file_tail[file_tail.len() - FOOTER_LEN..];
    let index_offset = u64::from_be_bytes(f[0..8].try_into().unwrap());
    let index_length = u64::from_be_bytes(f[8..16].try_into().unwrap());
    let bloom_offset = u64::from_be_bytes(f[16..24].try_into().unwrap());
    let bloom_length = u64::from_be_bytes(f[24..32].try_into().unwrap());
    let magic = u32::from_be_bytes(f[32..36].try_into().unwrap());
    let version = u32::from_be_bytes(f[36..40].try_into().unwrap());
    if magic != MAGIC {
        return Err(EngineError::Io("sstable: bad magic".into()));
    }
    if version != FORMAT_VERSION && version != FORMAT_VERSION_V1 {
        return Err(EngineError::Io(format!(
            "sstable: unsupported format version 0x{version:08x}"
        )));
    }
    Ok(Footer {
        index_offset,
        index_length,
        bloom_offset,
        bloom_length,
        version,
    })
}

/// Where a reader's bytes live. `File` is the production path (data blocks
/// are fetched on demand via `pread` and cached); `Memory` keeps the whole
/// thing in a `Vec<u8>` and is used by unit tests + the in-memory build
/// path so existing tests don't need a tempdir to exercise the reader.
enum Source {
    File {
        /// Retained for diagnostics / error context; I/O uses `file`.
        #[allow(dead_code)]
        path: PathBuf,
        /// Kept open for the reader lifetime so block-cache misses are
        /// `pread` only — not `open`+`pread`+`close` per miss.
        file: Arc<File>,
        cache: Arc<BlockCache>,
        sstable_id: u64,
        /// On-disk format version; gates per-block CRC verification.
        version: u32,
        /// Decoded at open and pinned on the reader (not in [`BlockCache`]).
        index: Arc<Vec<IndexEntry>>,
        bloom: Option<Arc<BloomFilter>>,
    },
    Memory {
        data: Vec<u8>,
        /// On-disk format version; gates per-block CRC verification.
        version: u32,
        index: Arc<Vec<IndexEntry>>,
        bloom: Option<Arc<BloomFilter>>,
    },
}

/// A reader for a single SSTable.
///
/// File-backed readers keep an open fd plus footer metadata; index, bloom, and
/// data blocks are charged to the shared [`BlockCache`] (RocksDB-style
/// `cache_index_and_filter_blocks`). In-memory readers (unit tests) pin
/// decoded metadata locally.
pub struct SstableReader {
    source: Source,
}

fn read_file_range(file: &File, offset: u64, len: usize) -> EngineResult<Vec<u8>> {
    use std::os::unix::fs::FileExt;
    let mut buf = vec![0u8; len];
    file.read_exact_at(&mut buf, offset)?;
    Ok(buf)
}

impl SstableReader {
    /// In-memory reader. Used by unit tests and any caller that already
    /// holds the full bytes. Does NOT consult the block cache; the bytes
    /// live inside the reader for its lifetime.
    pub fn open(data: Vec<u8>) -> EngineResult<SstableReader> {
        let footer = parse_footer(&data)?;
        let idx_start = footer.index_offset as usize;
        let idx_end = idx_start + footer.index_length as usize;
        if idx_end > data.len() {
            return Err(EngineError::Io("sstable: index out of bounds".into()));
        }
        let idx_body = verify_block_crc(footer.version, &data[idx_start..idx_end], "index")?;
        let index = decode_index(&idx_body)?;
        let bloom = if footer.bloom_length > 0 {
            let bs = footer.bloom_offset as usize;
            let be = bs + footer.bloom_length as usize;
            if be > data.len() {
                return Err(EngineError::Io("sstable: bloom out of bounds".into()));
            }
            let bloom_body = verify_block_crc(footer.version, &data[bs..be], "bloom")?;
            BloomFilter::decode(&bloom_body)
        } else {
            None
        };
        Ok(SstableReader {
            source: Source::Memory {
                data,
                version: footer.version,
                index: Arc::new(index),
                bloom: bloom.map(Arc::new),
            },
        })
    }

    /// File-backed reader. Index and bloom are decoded once at open and pinned
    /// on the reader; only data blocks use the shared block cache. The file
    /// descriptor stays open for subsequent `pread`s.
    pub fn open_from_path(
        path: &Path,
        sstable_id: u64,
        cache: Arc<BlockCache>,
    ) -> EngineResult<SstableReader> {
        use std::io::{Read, Seek, SeekFrom};
        let mut f = File::open(path)?;
        let file_len = f.seek(SeekFrom::End(0))?;
        if (file_len as usize) < FOOTER_LEN {
            return Err(EngineError::Io("sstable: file too small for footer".into()));
        }
        f.seek(SeekFrom::End(-(FOOTER_LEN as i64)))?;
        let mut footer_buf = vec![0u8; FOOTER_LEN];
        f.read_exact(&mut footer_buf)?;
        let footer = parse_footer(&footer_buf)?;

        if footer.index_offset + footer.index_length > file_len {
            return Err(EngineError::Io("sstable: index out of bounds".into()));
        }
        let idx_buf = read_file_range(&f, footer.index_offset, footer.index_length as usize)?;
        let idx_body = verify_block_crc(footer.version, &idx_buf, "index")?;
        let index = Arc::new(decode_index(&idx_body)?);

        let bloom = if footer.bloom_length > 0 {
            if footer.bloom_offset + footer.bloom_length > file_len {
                return Err(EngineError::Io("sstable: bloom out of bounds".into()));
            }
            let bloom_buf = read_file_range(&f, footer.bloom_offset, footer.bloom_length as usize)?;
            let bloom_body = verify_block_crc(footer.version, &bloom_buf, "bloom")?;
            BloomFilter::decode(&bloom_body).map(Arc::new)
        } else {
            None
        };

        Ok(SstableReader {
            source: Source::File {
                path: path.to_path_buf(),
                file: Arc::new(f),
                cache,
                sstable_id,
                version: footer.version,
                index,
                bloom,
            },
        })
    }

    fn index_arc(&self) -> Arc<Vec<IndexEntry>> {
        match &self.source {
            Source::Memory { index, .. } | Source::File { index, .. } => Arc::clone(index),
        }
    }

    pub fn has_bloom(&self) -> bool {
        match &self.source {
            Source::Memory { bloom, .. } | Source::File { bloom, .. } => bloom.is_some(),
        }
    }

    /// On-disk format version of this table (see [`FORMAT_VERSION`]). Used by
    /// startup logging and the `admin upgrade` path to report how much data is
    /// still in a legacy format.
    pub fn format_version(&self) -> u32 {
        match &self.source {
            Source::Memory { version, .. } | Source::File { version, .. } => *version,
        }
    }

    /// Returns false if the bloom filter proves the key is absent.
    pub fn might_contain(&self, user_key: &[u8]) -> bool {
        match &self.source {
            Source::Memory { bloom, .. } | Source::File { bloom, .. } => match bloom {
                Some(bf) => bf.maybe_contains(user_key),
                None => true,
            },
        }
    }

    /// Fetch a data block by index entry. Uses the cache for file-backed
    /// readers and a direct slice for in-memory readers.
    ///
    /// `cache_owner` attributes the insert to a tenant for FairDB cache floors
    /// (Phase 5a). Compaction passes `None` with `populate_cache=false`.
    pub(crate) fn read_block(
        &self,
        idx: &IndexEntry,
        populate_cache: bool,
        cache_owner: Option<crate::tenant_fair::TenantId>,
    ) -> EngineResult<BlockBytes> {
        match &self.source {
            Source::Memory { data, version, .. } => {
                let start = idx.offset as usize;
                let end = start + idx.length as usize;
                if end > data.len() {
                    return Err(EngineError::Io("sstable: block out of bounds".into()));
                }
                let body = verify_block_crc(*version, &data[start..end], "data")?;
                Ok(BlockBytes::Borrowed(body))
            }
            Source::File {
                file,
                cache,
                sstable_id,
                version,
                ..
            } => {
                let key = BlockKey::data(*sstable_id, idx.offset);
                if populate_cache {
                    // Cache holds the verified, trailer-stripped body, so a hit
                    // needs no re-verification.
                    if let Some(hit) = cache.get(key) {
                        return Ok(BlockBytes::Cached(hit));
                    }
                }
                // Compaction uses populate_cache=false (RocksDB fill_cache=false):
                // read directly without touching user-facing cache stats.
                use std::os::unix::fs::FileExt;
                let mut buf = vec![0u8; idx.length as usize];
                file.read_exact_at(&mut buf, idx.offset)?;
                // Verify (and strip the CRC trailer) BEFORE caching so a
                // corrupt block can never be served or persisted in the cache.
                let body = verify_block_crc(*version, &buf, "data")?;
                let arc = Arc::new(body);
                if populate_cache {
                    cache.insert_for_tenant(key, arc.clone(), cache_owner);
                } else {
                    cache.record_compaction_read();
                }
                Ok(BlockBytes::Cached(arc))
            }
        }
    }

    /// Point lookup. Returns the newest entry for `user_key`, or None.
    pub fn get_latest(&self, user_key: &[u8]) -> EngineResult<Option<(InternalKey, Entry)>> {
        let index = match &self.source {
            Source::Memory { index, .. } | Source::File { index, .. } => index.as_slice(),
        };
        let block_idx = Self::candidate_block(index, user_key);
        let Some(bi) = block_idx else {
            return Ok(None);
        };
        let owner = crate::tenant_fair::tenant_from_user_key(user_key);
        for entry in &index[bi..] {
            if entry.first_user_key.as_slice() > user_key {
                break;
            }
            let block = self.read_block(entry, true, owner)?;
            if let Some(found) = scan_block_for_latest(block.as_slice(), user_key)? {
                return Ok(Some(found));
            }
        }
        Ok(None)
    }

    fn candidate_block(index: &[IndexEntry], user_key: &[u8]) -> Option<usize> {
        if index.is_empty() {
            return None;
        }
        let mut lo = 0isize;
        let mut hi = index.len() as isize - 1;
        let mut ans = None;
        while lo <= hi {
            let mid = ((lo + hi) / 2) as usize;
            if index[mid].first_user_key.as_slice() <= user_key {
                ans = Some(mid);
                lo = mid as isize + 1;
            } else {
                hi = mid as isize - 1;
            }
        }
        ans.or(Some(0))
    }

    fn range_start_block(index: &[IndexEntry], lo: &[u8]) -> usize {
        if index.is_empty() {
            return 0;
        }
        let mut lo_idx = 0isize;
        let mut hi_idx = index.len() as isize - 1;
        let mut ans: Option<usize> = None;
        while lo_idx <= hi_idx {
            let mid = ((lo_idx + hi_idx) / 2) as usize;
            if index[mid].first_user_key.as_slice() <= lo {
                ans = Some(mid);
                lo_idx = mid as isize + 1;
            } else {
                hi_idx = mid as isize - 1;
            }
        }
        ans.unwrap_or(0)
    }

    /// Full ordered scan over all entries. Used by compaction (which now
    /// prefers `range_iter`, but this is preserved for tests + simple use).
    pub fn scan_all(&self) -> EngineResult<Vec<(InternalKey, Entry)>> {
        let index = self.index_arc();
        let mut out = Vec::new();
        for entry in index.iter() {
            let block = self.read_block(entry, false, None)?;
            let mut data = block.as_slice();
            while !data.is_empty() {
                let (k, e, consumed) = decode_entry(data)?;
                out.push((k, e));
                data = &data[consumed..];
            }
        }
        Ok(out)
    }

    /// Iterator over entries in user-key range `[lo, hi)`. Streams blocks
    /// from cache/disk; does NOT materialize the full table.
    pub fn range_iter(self: Arc<Self>, lo: Vec<u8>, hi: Vec<u8>) -> EngineResult<SstableRangeIter> {
        let index = self.index_arc();
        let start_block = Self::range_start_block(&index, &lo);
        Ok(SstableRangeIter {
            reader: self,
            index,
            lo,
            hi,
            block_idx: start_block,
            current: Vec::new(),
            cursor: 0,
            exhausted: false,
        })
    }

    /// Iterator over every entry in the SSTable in InternalKey order.
    pub fn full_iter(self: Arc<Self>) -> EngineResult<SstableRangeIter> {
        let index = self.index_arc();
        Ok(SstableRangeIter {
            reader: self,
            index,
            lo: Vec::new(),
            hi: Vec::new(),
            block_idx: 0,
            current: Vec::new(),
            cursor: 0,
            exhausted: false,
        })
    }
}

/// Bytes returned by `read_block`. The cached variant holds an Arc so the
/// caller can decode without copying; the borrowed variant holds an owned
/// `Vec<u8>` carved from the in-memory `Source::Memory` reader.
pub(crate) enum BlockBytes {
    Cached(CachedBlock),
    Borrowed(Vec<u8>),
}

impl BlockBytes {
    fn as_slice(&self) -> &[u8] {
        match self {
            BlockBytes::Cached(a) => a.as_slice(),
            BlockBytes::Borrowed(v) => v.as_slice(),
        }
    }
}

/// Streaming iterator over a key range in an SSTable. Reads one block at a
/// time through the block cache; never materializes the whole table.
///
/// `hi` is exclusive on the user key. An empty `hi` means "no upper bound"
/// (full-table iteration).
pub struct SstableRangeIter {
    reader: Arc<SstableReader>,
    index: Arc<Vec<IndexEntry>>,
    lo: Vec<u8>,
    hi: Vec<u8>,
    block_idx: usize,
    /// Decoded entries pending yield from the current block.
    current: Vec<(InternalKey, Entry)>,
    cursor: usize,
    exhausted: bool,
}

impl SstableRangeIter {
    fn load_next_block(&mut self) -> EngineResult<bool> {
        loop {
            if self.block_idx >= self.index.len() {
                return Ok(false);
            }
            let idx = self.index[self.block_idx].clone();
            self.block_idx += 1;

            // Stop early if this block starts beyond hi (when hi is bounded).
            if !self.hi.is_empty() && idx.first_user_key.as_slice() >= self.hi.as_slice() {
                return Ok(false);
            }

            // Scans use fill_cache=false (compaction + long ranges must not thrash).
            let block = self.reader.read_block(&idx, false, None)?;
            let mut data = block.as_slice();
            let mut pairs: Vec<(InternalKey, Entry)> = Vec::new();
            while !data.is_empty() {
                let (k, e, consumed) = decode_entry(data)?;
                data = &data[consumed..];
                // Filter by [lo, hi). Range scans can start mid-block when
                // lo falls inside a block; skip earlier keys.
                if k.user_key.as_slice() < self.lo.as_slice() {
                    continue;
                }
                if !self.hi.is_empty() && k.user_key.as_slice() >= self.hi.as_slice() {
                    self.exhausted = true;
                    break;
                }
                pairs.push((k, e));
            }
            self.current = pairs;
            self.cursor = 0;
            if !self.current.is_empty() {
                return Ok(true);
            }
            if self.exhausted {
                return Ok(false);
            }
            // Block had nothing in range; advance to the next.
        }
    }
}

impl crate::iter::EntryIterator for SstableRangeIter {
    fn next(&mut self) -> EngineResult<Option<(InternalKey, Entry)>> {
        if self.cursor >= self.current.len() {
            if self.exhausted {
                return Ok(None);
            }
            if !self.load_next_block()? {
                return Ok(None);
            }
        }
        let pair = self.current[self.cursor].clone();
        self.cursor += 1;
        Ok(Some(pair))
    }
}

/// Scan a single data block for the newest entry matching `user_key`.
fn scan_block_for_latest(
    mut block: &[u8],
    user_key: &[u8],
) -> EngineResult<Option<(InternalKey, Entry)>> {
    // Entries are sorted user_key ASC, seq DESC, so the FIRST matching entry is
    // the newest version of that key.
    while !block.is_empty() {
        let (k, e, consumed) = decode_entry(block)?;
        if k.user_key.as_slice() == user_key {
            return Ok(Some((k, e)));
        }
        if k.user_key.as_slice() > user_key {
            // Sorted: no further matches possible.
            return Ok(None);
        }
        block = &block[consumed..];
    }
    Ok(None)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uk(k: &[u8]) -> Vec<u8> {
        let mut v = vec![crate::keys::KS_USER];
        v.extend_from_slice(k);
        v
    }

    fn sorted_pairs(pairs: Vec<(InternalKey, Entry)>) -> Vec<(InternalKey, Entry)> {
        let mut p = pairs;
        p.sort_by(|a, b| a.0.cmp(&b.0));
        p
    }

    #[test]
    fn build_and_read_back_single_block() {
        let pairs = sorted_pairs(vec![
            (
                InternalKey::new(uk(b"a"), 1, EntryKind::Value),
                Entry::value(b"1".to_vec(), None),
            ),
            (
                InternalKey::new(uk(b"b"), 2, EntryKind::Value),
                Entry::value(b"2".to_vec(), None),
            ),
            (
                InternalKey::new(uk(b"c"), 3, EntryKind::Value),
                Entry::value(b"3".to_vec(), None),
            ),
        ]);
        let sst = build(&pairs, false);
        let reader = SstableReader::open(sst.bytes).unwrap();
        let (_, e) = reader.get_latest(&uk(b"b")).unwrap().unwrap();
        assert_eq!(e.value.as_deref(), Some(b"2".as_ref()));
        assert!(reader.get_latest(&uk(b"zzz")).unwrap().is_none());
    }

    #[test]
    fn footer_has_magic() {
        let pairs = sorted_pairs(vec![(
            InternalKey::new(uk(b"a"), 1, EntryKind::Value),
            Entry::value(b"1".to_vec(), None),
        )]);
        let sst = build(&pairs, false);
        let footer = parse_footer(&sst.bytes).unwrap();
        assert!(footer.index_length > 0);
        // last 8 bytes = magic(4) + version(4)
        let n = sst.bytes.len();
        let magic = u32::from_be_bytes(sst.bytes[n - 8..n - 4].try_into().unwrap());
        assert_eq!(magic, MAGIC);
    }

    #[test]
    fn newest_seq_wins_within_table() {
        let pairs = sorted_pairs(vec![
            (
                InternalKey::new(uk(b"k"), 1, EntryKind::Value),
                Entry::value(b"old".to_vec(), None),
            ),
            (
                InternalKey::new(uk(b"k"), 5, EntryKind::Value),
                Entry::value(b"new".to_vec(), None),
            ),
        ]);
        let sst = build(&pairs, false);
        let reader = SstableReader::open(sst.bytes).unwrap();
        let (k, e) = reader.get_latest(&uk(b"k")).unwrap().unwrap();
        assert_eq!(k.seq, 5);
        assert_eq!(e.value.as_deref(), Some(b"new".as_ref()));
    }

    #[test]
    fn tombstone_readable_as_tombstone() {
        let pairs = sorted_pairs(vec![(
            InternalKey::new(uk(b"d"), 9, EntryKind::Tombstone),
            Entry::tombstone(),
        )]);
        let sst = build(&pairs, false);
        let reader = SstableReader::open(sst.bytes).unwrap();
        let (k, e) = reader.get_latest(&uk(b"d")).unwrap().unwrap();
        assert_eq!(k.kind, EntryKind::Tombstone);
        assert!(e.is_tombstone());
    }

    #[test]
    fn many_keys_across_multiple_blocks() {
        let mut pairs = Vec::new();
        for i in 0..5000u32 {
            let key = format!("key{:06}", i);
            pairs.push((
                InternalKey::new(uk(key.as_bytes()), i as u64 + 1, EntryKind::Value),
                Entry::value(vec![b'v'; 64], None),
            ));
        }
        let pairs = sorted_pairs(pairs);
        let sst = build(&pairs, true);
        let reader = SstableReader::open(sst.bytes).unwrap();
        assert!(reader.has_bloom());
        // spot-check several keys
        for i in [0u32, 1, 2500, 4999] {
            let key = format!("key{:06}", i);
            let got = reader.get_latest(&uk(key.as_bytes())).unwrap();
            assert!(got.is_some(), "missing {}", key);
        }
        // absent key
        assert!(reader.get_latest(&uk(b"nope")).unwrap().is_none());
    }

    #[test]
    fn bloom_filters_absent_keys() {
        let mut pairs = Vec::new();
        for i in 0..1000u32 {
            let key = format!("k{:05}", i);
            pairs.push((
                InternalKey::new(uk(key.as_bytes()), i as u64 + 1, EntryKind::Value),
                Entry::value(b"v".to_vec(), None),
            ));
        }
        let pairs = sorted_pairs(pairs);
        let sst = build(&pairs, true);
        let reader = SstableReader::open(sst.bytes).unwrap();
        assert!(!reader.might_contain(&uk(b"definitely-not-here-xyz")));
    }

    #[test]
    fn scan_all_returns_everything_sorted() {
        let pairs = sorted_pairs(vec![
            (
                InternalKey::new(uk(b"a"), 1, EntryKind::Value),
                Entry::value(b"1".to_vec(), None),
            ),
            (
                InternalKey::new(uk(b"b"), 2, EntryKind::Value),
                Entry::value(b"2".to_vec(), None),
            ),
        ]);
        let sst = build(&pairs, false);
        let reader = SstableReader::open(sst.bytes).unwrap();
        let all = reader.scan_all().unwrap();
        assert_eq!(all.len(), 2);
        assert_eq!(all[0].0.user_key, uk(b"a"));
    }

    #[test]
    fn range_iter_yields_keys_within_bounds_in_order() {
        use crate::iter::EntryIterator;
        let mut pairs = Vec::new();
        for i in 0..500u32 {
            let key = format!("k{:04}", i);
            pairs.push((
                InternalKey::new(uk(key.as_bytes()), i as u64 + 1, EntryKind::Value),
                Entry::value(b"v".to_vec(), None),
            ));
        }
        let pairs = sorted_pairs(pairs);
        let sst = build(&pairs, false);
        let reader = std::sync::Arc::new(SstableReader::open(sst.bytes).unwrap());
        let mut it = reader.range_iter(uk(b"k0100"), uk(b"k0150")).unwrap();
        let mut count = 0;
        let mut last: Option<Vec<u8>> = None;
        while let Some((k, _)) = it.next().unwrap() {
            assert!(k.user_key.as_slice() >= uk(b"k0100").as_slice());
            assert!(k.user_key.as_slice() < uk(b"k0150").as_slice());
            if let Some(prev) = &last {
                assert!(prev.as_slice() < k.user_key.as_slice());
            }
            last = Some(k.user_key.clone());
            count += 1;
        }
        assert_eq!(count, 50);
    }

    #[test]
    fn full_iter_yields_everything_in_order() {
        use crate::iter::EntryIterator;
        let pairs = sorted_pairs(vec![
            (
                InternalKey::new(uk(b"a"), 1, EntryKind::Value),
                Entry::value(b"1".to_vec(), None),
            ),
            (
                InternalKey::new(uk(b"b"), 2, EntryKind::Value),
                Entry::value(b"2".to_vec(), None),
            ),
            (
                InternalKey::new(uk(b"c"), 3, EntryKind::Value),
                Entry::value(b"3".to_vec(), None),
            ),
        ]);
        let sst = build(&pairs, false);
        let reader = std::sync::Arc::new(SstableReader::open(sst.bytes).unwrap());
        let mut it = reader.full_iter().unwrap();
        let mut got = Vec::new();
        while let Some((k, _)) = it.next().unwrap() {
            got.push(k.user_key);
        }
        assert_eq!(got, vec![uk(b"a"), uk(b"b"), uk(b"c")]);
    }

    #[test]
    fn file_backed_reader_via_block_cache() {
        let pairs = sorted_pairs(vec![
            (
                InternalKey::new(uk(b"a"), 1, EntryKind::Value),
                Entry::value(b"1".to_vec(), None),
            ),
            (
                InternalKey::new(uk(b"b"), 2, EntryKind::Value),
                Entry::value(b"2".to_vec(), None),
            ),
        ]);
        let sst = build(&pairs, true);
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("test.sst");
        std::fs::write(&path, &sst.bytes).unwrap();

        let cache = crate::block_cache::BlockCache::new(1024 * 1024);
        let reader = SstableReader::open_from_path(&path, 42, cache.clone()).unwrap();
        assert!(reader.has_bloom());
        let (_, e) = reader.get_latest(&uk(b"b")).unwrap().unwrap();
        assert_eq!(e.value.as_deref(), Some(b"2".as_ref()));
        // First get triggered a miss + insert; a second get should hit.
        let stats_before = cache.stats();
        let _ = reader.get_latest(&uk(b"a")).unwrap();
        let stats_after = cache.stats();
        assert!(stats_after.hits >= stats_before.hits);
    }

    #[test]
    fn bad_magic_rejected() {
        let pairs = sorted_pairs(vec![(
            InternalKey::new(uk(b"a"), 1, EntryKind::Value),
            Entry::value(b"1".to_vec(), None),
        )]);
        let mut sst = build(&pairs, false);
        let n = sst.bytes.len();
        sst.bytes[n - 8] ^= 0xFF;
        assert!(SstableReader::open(sst.bytes).is_err());
    }
}
