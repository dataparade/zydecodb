//! Format-version refusal.
//!
//! Each on-disk format the engine reads carries a version tag. When that tag
//! does not match the version the running engine expects, the engine MUST
//! refuse to proceed rather than silently misparse old bytes. This file pins
//! that contract for every versioned surface.
//!
//! Why generate the fixtures inline instead of checking in binary files? Two
//! reasons:
//!   1. The bytes are documented next to the test that consumes them. A
//!      reviewer can see exactly what shape "v1 WAL segment" means without
//!      opening a hex editor on a `.bin` file.
//!   2. When a real format bump (v2 -> v3) happens, the old v2 layout will be
//!      reproduced from the existing code at the time of the bump and checked
//!      in as a fixture. *That* is when `tests/fixtures/` starts earning its
//!      keep. Today we only have v1 -> v2 (WAL) historically and the v1 layout
//!      no longer exists in code, so generating it inline is the only honest
//!      option.
//!
//! Coverage gap explicitly called out:
//!   - The manifest has no header version byte today. Per-record-type rejection
//!     is the only versioning mechanism — when a future engine adds a new
//!     record type, older engines refuse with [`EngineError::UnsupportedFormat`]
//!     thanks to the decoder + `replay` contract pinned below. This is
//!     sufficient for forward-compatibility refusal but does NOT cover the
//!     case where the framing itself changes (e.g. record-length width). A
//!     proper header version byte should be added before 0.1.0.

use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::errors::EngineError;
use zydecodb_engine::manifest;
use zydecodb_engine::sstable::{self, SstableReader};
use zydecodb_engine::wal;

fn open(dir: &TempDir) -> Result<Engine, EngineError> {
    Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir: dir.path().join("data/wal"),
        ..Default::default()
    })
}

// ---------- WAL ----------

/// Build the bytes of a v1-shaped WAL segment: 8-byte first_seq followed by a
/// 0x01 version byte. The body is empty; the version mismatch alone must be
/// fatal.
fn synthesize_v1_wal_segment() -> Vec<u8> {
    let first_seq: u64 = 1;
    let mut buf = Vec::new();
    buf.extend_from_slice(&first_seq.to_be_bytes());
    buf.push(0x01); // historical v1 version byte; current is 0x02
    buf
}

#[test]
fn wal_v1_segment_is_rejected_with_clear_message() {
    let dir = TempDir::new().expect("tempdir");
    let wal_dir = dir.path().join("data/wal");
    std::fs::create_dir_all(&wal_dir).unwrap();
    // The engine scans for files named `wal-NNNNNNNN.log`.
    let seg_path = wal_dir.join(wal::segment_filename(1));
    std::fs::write(&seg_path, synthesize_v1_wal_segment()).unwrap();

    let err = match open(&dir) {
        Ok(_) => panic!("opening with a v1 segment must fail"),
        Err(e) => e,
    };
    let EngineError::Io(msg) = &err else {
        panic!("expected EngineError::Io, got {:?}", err);
    };
    // Frozen substring — recovery scripts grep on it.
    assert!(
        msg.contains("unsupported segment format version"),
        "error message must mention the unsupported version contract; got: {msg}"
    );
    // Specifically calls out the bad byte and the expected byte.
    assert!(msg.contains("0x01"), "must report observed version: {msg}");
    assert!(msg.contains("0x02"), "must report expected version: {msg}");
}

#[test]
fn wal_current_format_constant_is_pinned() {
    // If you change this constant, you MUST add a fixture for the prior
    // version. This assertion exists to force that conversation in code review.
    assert_eq!(wal::WAL_FORMAT_VERSION, 0x02);
    assert_eq!(wal::SEGMENT_HEADER_LEN, 9);
}

// ---------- SSTable ----------

/// Build a tail-only byte vector that looks like a valid SSTable footer except
/// the version field has been bumped to a hypothetical future v2 (0x02).
fn synthesize_future_sstable_footer() -> Vec<u8> {
    let mut bytes = vec![0u8; sstable::FOOTER_LEN - 8];
    bytes.extend_from_slice(&sstable::MAGIC.to_be_bytes());
    // FORMAT_VERSION + 1 — a version the engine does not know how to read yet.
    let future_version: u32 = sstable::FORMAT_VERSION + 1;
    bytes.extend_from_slice(&future_version.to_be_bytes());
    bytes
}

#[test]
fn sstable_future_version_is_rejected() {
    let bytes = synthesize_future_sstable_footer();
    let err = match SstableReader::open(bytes) {
        Ok(_) => panic!("future-version SSTable must fail"),
        Err(e) => e,
    };
    let EngineError::Io(msg) = err else {
        panic!("expected EngineError::Io");
    };
    assert!(
        msg.contains("unsupported format version"),
        "must surface the version contract; got: {msg}"
    );
}

#[test]
fn sstable_bad_magic_is_rejected() {
    // Bytes that are footer-sized but whose magic is wrong — the engine must
    // reject this before even looking at the version field, because seeing a
    // wrong magic at the end of a file means it isn't ours.
    let mut bytes = vec![0u8; sstable::FOOTER_LEN];
    let len = bytes.len();
    bytes[len - 8..len - 4].copy_from_slice(&0xDEAD_BEEFu32.to_be_bytes());
    bytes[len - 4..].copy_from_slice(&sstable::FORMAT_VERSION.to_be_bytes());
    let err = match SstableReader::open(bytes) {
        Ok(_) => panic!("bad magic must fail"),
        Err(e) => e,
    };
    let EngineError::Io(msg) = err else {
        panic!("expected EngineError::Io");
    };
    assert!(
        msg.contains("bad magic"),
        "must surface bad magic; got: {msg}"
    );
}

#[test]
fn sstable_current_format_constants_are_pinned() {
    // If you bump FORMAT_VERSION you MUST keep a read path (and test) for every
    // prior version still in the supported range. v1 -> v2 added per-block
    // CRC32 trailers; v1 stays readable (see sstable_v1_reads_without_checksums).
    assert_eq!(sstable::MAGIC, 0x5052_4144); // "PRAD"
    assert_eq!(sstable::FORMAT_VERSION, 0x0000_0002);
    assert_eq!(sstable::FORMAT_VERSION_V1, 0x0000_0001);
    assert_eq!(sstable::FOOTER_LEN, 40);
}

/// Hand-build a v1-format SSTable (no per-block CRC trailers, footer version
/// 0x01): one data block with two entries, a one-entry index, and the footer.
/// The v1 layout no longer exists in code, so it is synthesized inline (same
/// rationale as the WAL v1 fixture above).
fn synthesize_v1_sstable() -> Vec<u8> {
    fn encode_entry_v1(seq: u64, user_key: &[u8], value: &[u8]) -> Vec<u8> {
        let mut b = Vec::new();
        b.push(0x01); // EntryKind::Value
        b.extend_from_slice(&seq.to_be_bytes());
        b.extend_from_slice(&(user_key.len() as u32).to_be_bytes());
        b.extend_from_slice(&(value.len() as u32).to_be_bytes());
        b.extend_from_slice(&0u64.to_be_bytes()); // expires_at
        b.extend_from_slice(user_key);
        b.extend_from_slice(value);
        b
    }
    let key_a: &[u8] = b"\x01a";
    let key_b: &[u8] = b"\x01b";

    // Single data block at offset 0, no CRC trailer (v1).
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&encode_entry_v1(1, key_a, b"1"));
    bytes.extend_from_slice(&encode_entry_v1(2, key_b, b"2"));
    let block_len = bytes.len() as u64;

    // Index: count, then [klen][first_user_key][first_seq][offset][length].
    let index_offset = bytes.len() as u64;
    bytes.extend_from_slice(&1u32.to_be_bytes());
    bytes.extend_from_slice(&(key_a.len() as u32).to_be_bytes());
    bytes.extend_from_slice(key_a);
    bytes.extend_from_slice(&1u64.to_be_bytes()); // first_seq
    bytes.extend_from_slice(&0u64.to_be_bytes()); // block offset
    bytes.extend_from_slice(&block_len.to_be_bytes());
    let index_length = bytes.len() as u64 - index_offset;

    // Footer (v1: no per-block CRC).
    bytes.extend_from_slice(&index_offset.to_be_bytes());
    bytes.extend_from_slice(&index_length.to_be_bytes());
    bytes.extend_from_slice(&0u64.to_be_bytes()); // bloom_offset
    bytes.extend_from_slice(&0u64.to_be_bytes()); // bloom_length
    bytes.extend_from_slice(&sstable::MAGIC.to_be_bytes());
    bytes.extend_from_slice(&sstable::FORMAT_VERSION_V1.to_be_bytes());
    bytes
}

#[test]
fn sstable_v1_reads_without_checksums() {
    // Backward compatibility: a v1 file (no per-block CRC) must open and read
    // correctly, with verification simply skipped. This is what keeps existing
    // on-disk data working across the v2 upgrade.
    let reader = SstableReader::open(synthesize_v1_sstable()).expect("v1 sstable must open");
    let all = reader.scan_all().expect("v1 scan must succeed");
    assert_eq!(all.len(), 2);
    assert_eq!(all[0].0.user_key, b"\x01a");
    assert_eq!(all[1].0.user_key, b"\x01b");
    let (_, e) = reader
        .get_latest(b"\x01b")
        .expect("v1 get must succeed")
        .expect("key b present");
    assert_eq!(e.value.as_deref(), Some(b"2".as_ref()));
}

#[test]
fn sstable_v2_data_block_corruption_is_detected() {
    // A bit flip in a v2 data block must surface as an Io error on read, not be
    // served as a wrong value and not panic on decode.
    let pairs = vec![
        (
            zydecodb_engine::keys::InternalKey::new(
                b"\x01a".to_vec(),
                1,
                zydecodb_engine::keys::EntryKind::Value,
            ),
            zydecodb_engine::entry::Entry::value(b"hello".to_vec(), None),
        ),
        (
            zydecodb_engine::keys::InternalKey::new(
                b"\x01b".to_vec(),
                2,
                zydecodb_engine::keys::EntryKind::Value,
            ),
            zydecodb_engine::entry::Entry::value(b"world".to_vec(), None),
        ),
    ];
    let sst = sstable::build(&pairs, false);
    let mut bytes = sst.bytes;
    // Flip a byte inside data block 0 (block starts at offset 0; offset 0 is the
    // entry kind byte, well inside the block body that the CRC covers).
    bytes[0] ^= 0xFF;
    let reader = SstableReader::open(bytes).expect("footer/index still parse");
    let err = reader
        .scan_all()
        .expect_err("corrupted data block must error, not return a value");
    let EngineError::Io(msg) = err else {
        panic!("expected EngineError::Io");
    };
    assert!(
        msg.contains("checksum mismatch"),
        "must surface the checksum contract; got: {msg}"
    );
}

// ---------- Manifest ----------

/// Construct a framed manifest record with the given type byte and empty
/// payload. CRC is computed correctly so the framing itself is well-formed —
/// only the type byte is unrecognized.
fn framed_record_with_type(rtype: u8) -> Vec<u8> {
    let payload: &[u8] = &[];
    let mut framed = Vec::new();
    framed.push(rtype);
    framed.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    framed.extend_from_slice(payload);
    let crc = crc32fast::hash(&framed);
    framed.extend_from_slice(&crc.to_be_bytes());
    framed
}

#[test]
fn manifest_unknown_record_type_is_rejected_by_decoder() {
    let framed = framed_record_with_type(0xFF);
    let err = manifest::ManifestRecord::decode_one(&framed)
        .expect_err("unknown record type must fail at the decoder");
    // Distinct from EngineError::Io (which means torn write). The wire
    // contract is that this surfaces Status::UnsupportedFormat (0x0A) so
    // operators can tell "your binary is too old" apart from "your disk is
    // dying."
    let EngineError::UnsupportedFormat(msg) = err else {
        panic!("expected EngineError::UnsupportedFormat, got different variant");
    };
    assert!(
        msg.contains("unknown record type"),
        "must surface the unknown-type contract; got: {msg}"
    );
}

#[test]
fn manifest_replay_refuses_unknown_record_type_loudly() {
    // The original bug: replay swallowed decode errors and stopped, which is
    // indistinguishable from a torn tail. That meant a manifest written by a
    // newer engine version (with an added record type) silently truncated
    // catalog state on an older engine. This test pins the FIXED behavior:
    // replay propagates an UnsupportedFormat error so Engine::open fails
    // loudly instead of opening with a partial live set.
    let framed = framed_record_with_type(0xFF);
    let err =
        manifest::replay(&framed).expect_err("unknown record type must propagate out of replay");
    assert!(
        matches!(err, EngineError::UnsupportedFormat(_)),
        "expected UnsupportedFormat; got {:?}",
        err
    );
}

#[test]
fn manifest_replay_still_tolerates_torn_tail() {
    // The fix must NOT regress torn-tail tolerance. A partial trailing record
    // (CRC will mismatch or buffer will be short) is the normal "process
    // crashed mid-append" condition and must stay silent.
    let mut stream = Vec::new();
    // One valid SeqCheckpoint record, then 3 garbage bytes (too short to form
    // a header at all).
    let rec = manifest::ManifestRecord::SeqCheckpoint {
        last_durable_seq: 42,
    };
    stream.extend_from_slice(&rec.encode());
    stream.extend_from_slice(&[0x01, 0x02, 0x03]);

    let state = manifest::replay(&stream).expect("torn tail must not be an error");
    // The valid prefix was applied.
    assert_eq!(state.last_durable_seq, 42);
}

#[test]
fn manifest_replay_refuses_unknown_type_even_after_valid_prefix() {
    // The dangerous variant of the bug: a manifest with N valid records and
    // an Nth+1 unknown record. The fix must propagate the error so the engine
    // refuses to open with a partial live set — not return Ok with the first
    // N records applied.
    let mut stream = Vec::new();
    let rec = manifest::ManifestRecord::SeqCheckpoint {
        last_durable_seq: 99,
    };
    stream.extend_from_slice(&rec.encode());
    stream.extend_from_slice(&framed_record_with_type(0xFF));

    let err = manifest::replay(&stream)
        .expect_err("unknown-type tail must turn replay into a hard refusal");
    assert!(matches!(err, EngineError::UnsupportedFormat(_)));
}

// ---------- Fixtures directory ----------

/// The plan calls for a `tests/fixtures/` directory that grows over time as
/// real format bumps land. We materialize an empty marker so the directory is
/// version-controlled now and the path is reserved.
#[test]
fn fixtures_directory_is_a_format_history_archive() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("fixtures");
    assert!(
        fixtures.is_dir(),
        "expected {} to exist as a checked-in directory",
        fixtures.display()
    );
}
