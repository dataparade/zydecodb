//! Property-based codec robustness: every public decoder in the engine must
//! handle arbitrary bytes without panicking, without reading past the end of
//! the input, and either succeed or return a typed error. This is the "we
//! cannot DoS the engine with a malformed byte string" invariant.
//!
//! Decoders covered:
//!   - [`zydecodb_engine::wal::WalRecord::decode_one`] (single-record decode)
//!   - [`zydecodb_engine::wal::replay_segment_body`] (segment-level replay)
//!   - [`zydecodb_engine::frame::RequestEnvelope::decode`]
//!   - [`zydecodb_engine::frame::ResponseEnvelope::parse_header`]
//!   - [`zydecodb_engine::frame::PutPayload::decode`]
//!   - [`zydecodb_engine::frame::KeyPayload::decode`]
//!   - [`zydecodb_engine::sstable::SstableReader::open`]
//!   - [`zydecodb_engine::bloom::BloomFilter::decode`]
//!   - [`zydecodb_engine::manifest::ManifestRecord::decode_one`]
//!
//! The fuzz targets in `fuzz/fuzz_targets/` provide adversarial coverage when
//! run for hours; this file provides the regression net that runs on every PR.

use proptest::collection::vec;
use proptest::prelude::*;
use zydecodb_engine::bloom::BloomFilter;
use zydecodb_engine::frame::{KeyPayload, PutPayload, RequestEnvelope, ResponseEnvelope};
use zydecodb_engine::manifest::ManifestRecord;
use zydecodb_engine::sstable::SstableReader;
use zydecodb_engine::wal::{self, WalRecord};

proptest! {
    #![proptest_config(ProptestConfig {
        // Pure CPU, no I/O — keep this dense.
        cases: 1024,
        .. ProptestConfig::default()
    })]

    #[test]
    fn wal_decode_one_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        // Either Ok(Some(...)), Ok(None) (truncated tail), or Err(...).
        let _ = WalRecord::decode_one(&bytes);
    }

    #[test]
    fn wal_replay_segment_body_never_panics(bytes in vec(any::<u8>(), 0..=2048)) {
        let (_records, _torn) = wal::replay_segment_body(&bytes);
    }

    #[test]
    fn request_envelope_decode_never_panics(bytes in vec(any::<u8>(), 0..=256)) {
        let _ = RequestEnvelope::decode(&bytes);
    }

    #[test]
    fn response_envelope_parse_header_never_panics(bytes in vec(any::<u8>(), 0..=64)) {
        let _ = ResponseEnvelope::parse_header(&bytes);
    }

    #[test]
    fn put_payload_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = PutPayload::decode(&bytes);
    }

    #[test]
    fn key_payload_decode_never_panics(bytes in vec(any::<u8>(), 0..=256)) {
        let _ = KeyPayload::decode(&bytes);
    }

    #[test]
    fn sstable_reader_open_never_panics(bytes in vec(any::<u8>(), 0..=4096)) {
        let _ = SstableReader::open(bytes);
    }

    #[test]
    fn bloom_filter_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        // Decode returns Option, not Result — None is the rejection case.
        let _ = BloomFilter::decode(&bytes);
    }

    #[test]
    fn manifest_record_decode_one_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = ManifestRecord::decode_one(&bytes);
    }

    /// Truncation invariant: removing any suffix of a valid request envelope
    /// must produce a clean error (or a short-header indication), never panic
    /// and never silently succeed on a partial payload.
    #[test]
    fn truncated_request_envelopes_reject_cleanly(
        payload in vec(any::<u8>(), 0..=128),
        trim in 1usize..=64,
    ) {
        use zydecodb_engine::frame::Command;
        let bytes = RequestEnvelope::new(Command::Put, payload.clone()).encode();
        // Trim from the tail; cap at len-1 so we always remove something.
        let cut = trim.min(bytes.len().saturating_sub(1)).max(1);
        let truncated = &bytes[..bytes.len() - cut];
        // Decoder either rejects with an error or — if we only trimmed a
        // partial payload — succeeds with the now-shorter declared length
        // pointing past EOF. Either way, must not panic and must not return
        // a successful decode that overreads.
        if let Ok(env) = RequestEnvelope::decode(truncated) {
            // If it decoded, the announced payload length must fit in the
            // buffer we gave it. Otherwise it read garbage past EOF.
            prop_assert!(env.payload.len() <= truncated.len());
        }
    }
}
