//! Systematic WAL torn-write matrix at every frame boundary and mid-frame.
//!
//! Builds a segment body with several complete `WalRecord` frames, then truncates
//! and/or flips bytes at each boundary. Replay must never panic, never apply
//! garbage past the last intact frame, and classify short/corrupt tails as
//! `TornTail` (or `Corruption` when intact frames follow the damage).

use std::panic::{catch_unwind, AssertUnwindSafe};
use tempfile::TempDir;
use zydecodb_engine::engine::{Engine, EngineConfig};
use zydecodb_engine::keys::KS_USER;
use zydecodb_engine::wal::{self, ReplayOutcome, WalRecord};

fn uk(k: &[u8]) -> Vec<u8> {
    let mut v = vec![KS_USER];
    v.extend_from_slice(k);
    v
}

fn rec(seq: u64, key: &[u8], val: &[u8]) -> Vec<u8> {
    WalRecord::put(seq, 0, uk(key), val.to_vec()).encode()
}

/// Multi-frame body (no segment header) plus cumulative end offsets.
fn build_body() -> (Vec<u8>, Vec<usize>) {
    let frames = [
        rec(1, b"a", b"v1"),
        rec(2, b"bb", b"v22"),
        rec(3, b"ccc", b"v333"),
        rec(4, b"dddd", b"v4444"),
        rec(5, b"eeeee", b"v55555"),
    ];
    let mut body = Vec::new();
    let mut ends = Vec::new();
    for f in &frames {
        body.extend_from_slice(f);
        ends.push(body.len());
    }
    (body, ends)
}

#[test]
fn torn_at_every_frame_boundary_and_mid_frame() {
    let (body, ends) = build_body();
    assert_eq!(ends.len(), 5);

    // Truncate exactly at each frame boundary.
    for (i, &end) in ends.iter().enumerate() {
        let truncated = &body[..end];
        let (entries, outcome) = wal::replay_segment_body(truncated);
        assert_eq!(entries.len(), i + 1, "boundary truncate at frame {}", i + 1);
        assert_eq!(outcome, ReplayOutcome::Clean);
        for (j, e) in entries.iter().enumerate() {
            assert_eq!(e.seq(), (j as u64) + 1);
        }
    }

    let (entries, outcome) = wal::replay_segment_body(&[]);
    assert!(entries.is_empty());
    assert_eq!(outcome, ReplayOutcome::Clean);

    // Truncate mid-frame.
    let mut starts = vec![0usize];
    starts.extend(ends.iter().copied().take(ends.len() - 1));
    for (frame_idx, (&start, &end)) in starts.iter().zip(ends.iter()).enumerate() {
        let span = end - start;
        let mid_offsets = [start + 1, start + span / 4, start + span / 2, end - 1];
        for off in mid_offsets {
            if off <= start || off >= end {
                continue;
            }
            let truncated = &body[..off];
            let result = catch_unwind(AssertUnwindSafe(|| wal::replay_segment_body(truncated)));
            assert!(
                result.is_ok(),
                "panic on mid-frame truncate frame={} off={}",
                frame_idx + 1,
                off
            );
            let (entries, outcome) = result.unwrap();
            assert_eq!(
                entries.len(),
                frame_idx,
                "mid-frame truncate keeps only prior complete frames (frame {} off {})",
                frame_idx + 1,
                off
            );
            assert_eq!(outcome, ReplayOutcome::TornTail);
            for (j, e) in entries.iter().enumerate() {
                assert_eq!(e.seq(), (j as u64) + 1);
            }
        }
    }

    // Flip a byte in each trailing frame prefix: must not report Clean garbage.
    for (i, &end) in ends.iter().enumerate() {
        if end < 4 {
            continue;
        }
        let mut corrupted = body[..end].to_vec();
        let flip_at = corrupted.len() - 2;
        corrupted[flip_at] ^= 0xFF;
        let result = catch_unwind(AssertUnwindSafe(|| wal::replay_segment_body(&corrupted)));
        assert!(result.is_ok(), "panic on flipped frame {}", i + 1);
        let (entries, outcome) = result.unwrap();
        assert!(
            matches!(outcome, ReplayOutcome::TornTail | ReplayOutcome::Corruption),
            "flipped frame {} must not report Clean",
            i + 1
        );
        assert!(
            entries.len() <= i,
            "must not return the damaged frame as valid (got {} entries for frame {})",
            entries.len(),
            i + 1
        );
        for (j, e) in entries.iter().enumerate() {
            assert_eq!(e.seq(), (j as u64) + 1);
        }
    }
}

#[test]
fn engine_open_active_torn_tail_keeps_prefix() {
    let dir = TempDir::new().unwrap();
    let wal_dir = dir.path().join("data/wal");
    std::fs::create_dir_all(&wal_dir).unwrap();

    let (body, ends) = build_body();
    let mut seg = Vec::new();
    seg.extend_from_slice(&1u64.to_be_bytes());
    seg.push(wal::WAL_FORMAT_VERSION);
    // Four complete frames + half of the fifth.
    let half = ends[3] + (ends[4] - ends[3]) / 2;
    seg.extend_from_slice(&body[..half]);

    let path = wal_dir.join(wal::segment_filename(1));
    std::fs::write(&path, &seg).unwrap();

    let eng = Engine::open(EngineConfig {
        data_dir: dir.path().join("data"),
        wal_dir,
        ..Default::default()
    })
    .expect("active torn tail must open");

    assert!(eng.get(&uk(b"a")).unwrap().is_some());
    assert!(eng.get(&uk(b"dddd")).unwrap().is_some());
    assert!(eng.get(&uk(b"eeeee")).unwrap().is_none());
}
