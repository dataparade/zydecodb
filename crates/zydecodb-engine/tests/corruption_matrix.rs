//! Structural-corruption matrix (the SQLite "malformed database" discipline).
//!
//! For each on-disk artifact (SSTable, manifest stream, WAL segment) we build a
//! well-formed instance, then walk every byte applying a small set of
//! mutations. For every mutation the reader/replayer must:
//!   - never panic and never read out of bounds (we run under `catch_unwind`),
//!   - never serve a *wrong* value: a successful read returns either the exact
//!     original data or a valid prefix of it; otherwise it surfaces a typed
//!     `EngineError`.
//!
//! This complements the targeted single-flip cases in `format_versions.rs` by
//! exhaustively sweeping the structural regions instead of one hand-picked byte.

use std::panic::{catch_unwind, AssertUnwindSafe};
use zydecodb_engine::entry::Entry;
use zydecodb_engine::keys::{EntryKind, InternalKey};
use zydecodb_engine::manifest::{self, ManifestRecord, ManifestState, SstableMeta};
use zydecodb_engine::sstable::{self, SstableReader};
use zydecodb_engine::wal::{self, ReplayOutcome, WalEntry, WalRecord};

/// The byte mutations applied at each offset. A single XOR flip plus the two
/// extremes catches sign/length/offset corruption that a lone flip can miss.
fn mutate(orig: u8) -> Vec<u8> {
    let mut out = vec![orig ^ 0xFF, 0x00, 0xFF];
    out.retain(|&b| b != orig);
    out
}

/// Run `check` for every (offset, mutation) over `good`, asserting the closure
/// never panics. `check` receives the corrupted bytes and the offset.
fn sweep(good: &[u8], label: &str, mut check: impl FnMut(Vec<u8>, usize)) {
    for i in 0..good.len() {
        for m in mutate(good[i]) {
            let mut corrupted = good.to_vec();
            corrupted[i] = m;
            let res = catch_unwind(AssertUnwindSafe(|| check(corrupted, i)));
            assert!(
                res.is_ok(),
                "{label}: corruption at byte {i} (-> 0x{m:02x}) caused a panic"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// SSTable

fn ik(key: &[u8], seq: u64) -> InternalKey {
    InternalKey::new(key.to_vec(), seq, EntryKind::Value)
}

fn build_sstable() -> (Vec<u8>, Vec<(InternalKey, Entry)>) {
    let pairs = vec![
        (
            ik(b"\x01alpha", 1),
            Entry::value(b"first-value".to_vec(), None),
        ),
        (
            ik(b"\x01bravo", 2),
            Entry::value(b"second-value".to_vec(), None),
        ),
        (
            ik(b"\x01charlie", 3),
            Entry::value(b"third-value".to_vec(), None),
        ),
    ];
    let built = sstable::build(&pairs, true);
    (built.bytes, pairs)
}

#[test]
fn sstable_corruption_never_panics_or_serves_wrong_values() {
    let (good, pairs) = build_sstable();
    let reference = SstableReader::open(good.clone())
        .expect("reference opens")
        .scan_all()
        .expect("reference scans");

    sweep(&good, "sstable", |bytes, _i| {
        // A corrupted SSTable must either fail to open/read with a typed error,
        // or — if it opens — return exactly the original data. A wrong value
        // (or a false miss) served without an error is the unacceptable outcome.
        let Ok(reader) = SstableReader::open(bytes) else {
            return; // typed open error: acceptable
        };
        if let Ok(scan) = reader.scan_all() {
            assert_eq!(
                scan, reference,
                "sstable served wrong scan after corruption"
            );
        }
        // Point reads exercise the footer/index/bloom/data path. Because the
        // index and bloom are CRC-protected at open, a reader that opened must
        // never lose a present key: get_latest returns the right entry or a
        // typed error, never a false `None`.
        for (key, entry) in &pairs {
            match reader.get_latest(&key.user_key) {
                Ok(Some((_, got))) => {
                    assert_eq!(
                        &got, entry,
                        "sstable served wrong point value after corruption"
                    )
                }
                Ok(None) => panic!("sstable lost a present key after a corruption it accepted"),
                Err(_) => {} // typed read error: acceptable
            }
        }
    });
}

// ---------------------------------------------------------------------------
// Manifest

fn meta(id: u64) -> SstableMeta {
    SstableMeta {
        id,
        level: 0,
        min_key: vec![1, 2, 3],
        max_key: vec![9, 9, 9],
        min_seq: id * 10,
        max_seq: id * 10 + 5,
        size_bytes: 4096,
    }
}

fn manifest_records() -> Vec<ManifestRecord> {
    vec![
        ManifestRecord::SstableAdd(meta(1)),
        ManifestRecord::SstableAdd(meta(2)),
        ManifestRecord::SeqCheckpoint {
            last_durable_seq: 100,
        },
        ManifestRecord::SstableRemove { id: 1 },
        ManifestRecord::WalTruncate {
            up_to_segment_id: 7,
        },
    ]
}

fn manifest_stream(records: &[ManifestRecord]) -> Vec<u8> {
    let mut out = Vec::new();
    for r in records {
        out.extend_from_slice(&r.encode());
    }
    out
}

/// Every state reachable by replaying a prefix of `records`. A single-byte flip
/// always breaks a record's CRC (or its framing), so replay applies some clean
/// prefix and then stops -- the resulting state must be one of these.
fn prefix_states(records: &[ManifestRecord]) -> Vec<ManifestState> {
    (0..=records.len())
        .map(|n| manifest::replay(&manifest_stream(&records[..n])).expect("prefix replays"))
        .collect()
}

#[test]
fn manifest_corruption_never_panics_and_yields_only_prefix_states() {
    let records = manifest_records();
    let good = manifest_stream(&records);
    let prefixes = prefix_states(&records);

    sweep(&good, "manifest", |bytes, _i| {
        // replay either refuses (typed Err) or returns a state equal to some
        // clean prefix of the original record sequence -- never a state that
        // never legitimately existed.
        if let Ok(state) = manifest::replay(&bytes) {
            // ManifestState isn't PartialEq; compare its public fields.
            let matches_prefix = prefixes.iter().any(|p| {
                p.live_sstables == state.live_sstables
                    && p.last_durable_seq == state.last_durable_seq
                    && p.wal_truncated_up_to == state.wal_truncated_up_to
            });
            assert!(
                matches_prefix,
                "manifest replay produced a non-prefix state after corruption"
            );
        }
    });
}

// ---------------------------------------------------------------------------
// WAL segment

fn wal_segment(records: &[WalRecord]) -> Vec<u8> {
    let mut buf = Vec::new();
    buf.extend_from_slice(&1u64.to_be_bytes()); // first_seq
    buf.push(wal::WAL_FORMAT_VERSION);
    for r in records {
        buf.extend_from_slice(&r.encode());
    }
    buf
}

/// Mirror `Engine::read_segment`'s header handling, then replay the body. Err
/// signals an unreadable header (bad/short length or version mismatch).
fn replay_full_segment(bytes: &[u8]) -> Result<(Vec<WalEntry>, ReplayOutcome), ()> {
    if bytes.len() < wal::SEGMENT_HEADER_LEN {
        return Ok((Vec::new(), ReplayOutcome::Clean));
    }
    if bytes[8] != wal::WAL_FORMAT_VERSION {
        return Err(()); // version refusal (also covered in format_versions.rs)
    }
    Ok(wal::replay_segment_body(&bytes[wal::SEGMENT_HEADER_LEN..]))
}

#[test]
fn wal_segment_corruption_never_panics_and_decodes_only_prefixes() {
    let records = vec![
        WalRecord::put(1, 0, b"\x01a".to_vec(), b"one".to_vec()),
        WalRecord::put(2, 7, b"\x01b".to_vec(), b"two".to_vec()),
        WalRecord::del(3, b"\x01c".to_vec()),
    ];
    let good = wal_segment(&records);
    let reference: Vec<WalEntry> = wal::replay_segment_body(&good[wal::SEGMENT_HEADER_LEN..]).0;

    sweep(&good, "wal", |bytes, _i| {
        if let Ok((decoded, _outcome)) = replay_full_segment(&bytes) {
            // Whatever decoded must be a prefix of the original entries: replay
            // stops at the first damaged record, never fabricating one.
            assert!(
                decoded.len() <= reference.len() && decoded[..] == reference[..decoded.len()],
                "wal replay produced a non-prefix entry list after corruption"
            );
        }
    });
}
