//! Property-based codec robustness for the document wire layer: every public
//! decoder must handle arbitrary bytes without panicking, without reading past
//! the end of the input, and either succeed or return a typed error. These
//! payloads are attacker-reachable (document commands arrive straight off the
//! wire), so a malformed byte string must never be able to crash the server.
//!
//! This mirrors the engine's `proptest_codecs.rs` for the raw-KV/frame layer.

use proptest::collection::vec;
use proptest::prelude::*;
use zydecodb_document::wire::{
    decode_query_page, CountPayload, DeletePayload, DocDelPayload, DocPutPayload, FindPayload,
    IndexDefPayload, QueryPayload, UpdatePayload,
};

proptest! {
    #![proptest_config(ProptestConfig {
        // Pure CPU, no I/O — keep this dense.
        cases: 1024,
        .. ProptestConfig::default()
    })]

    #[test]
    fn doc_put_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = DocPutPayload::decode(&bytes);
    }

    #[test]
    fn doc_del_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = DocDelPayload::decode(&bytes);
    }

    #[test]
    fn index_def_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = IndexDefPayload::decode(&bytes);
    }

    #[test]
    fn query_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = QueryPayload::decode(&bytes);
    }

    #[test]
    fn find_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = FindPayload::decode(&bytes);
    }

    #[test]
    fn update_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = UpdatePayload::decode(&bytes);
    }

    #[test]
    fn delete_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = DeletePayload::decode(&bytes);
    }

    #[test]
    fn count_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = CountPayload::decode(&bytes);
    }

    #[test]
    fn query_page_decode_never_panics(bytes in vec(any::<u8>(), 0..=512)) {
        let _ = decode_query_page(&bytes);
    }
}
