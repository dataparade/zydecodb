//! The ZydecoDB document layer.
//!
//! A thin, JSON-first document store layered on the KV engine. It owns:
//! - a persisted collection/index **catalog** (one blob in `KS_SYSTEM`);
//! - **order-preserving** index-key encoding so range scans return rows in
//!   logical field order;
//! - **atomic** document writes that maintain every secondary index in a single
//!   [`zydecodb_engine::engine::Engine::write_batch`] (one WAL record);
//! - an index-range **query** with an opaque pagination cursor.
//!
//! The document body is stored as `[value_kind][payload]`. v1 uses
//! `value_kind = 0x00` (Raw / JSON); the index-key encoder is format-agnostic,
//! so a FlatBuffer extractor (`0x01`) can be added later without touching the
//! encoding, key layout, or storage.

pub mod binary;
pub mod catalog;
pub mod encoding;
pub mod error;
pub mod filter;
pub mod keys;
pub mod planner;
pub mod query;
pub mod store;
pub mod update;
pub mod wire;

pub use catalog::{Catalog, CollectionMeta, IndexMeta, SharedCatalog};
pub use error::{DocError, DocResult};
