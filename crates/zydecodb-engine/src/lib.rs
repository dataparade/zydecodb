//! ZydecoDB storage core — WAL, memtable, SSTables, compaction, crash recovery.
//!
//! Used by the `zydecodb` server binary. Wire framing lives in [`frame`];
//! transport and CLI live in the `zydecodb` crate.

pub mod apply_worker;
pub mod block_cache;
pub mod bloom;
pub mod compaction;
pub mod compaction_worker;
pub mod engine;
pub mod entry;
pub mod errors;
pub mod failpoints;
pub mod flush_worker;
pub mod frame;
pub mod iter;
pub mod keys;
pub mod manifest;
pub mod memtable;
pub mod metrics;
pub mod owned_snapshot;
pub mod policy;
pub mod reader_cache;
pub mod result_cache;
pub mod seq;
pub mod shipping;
pub mod snapshot;
pub mod sstable;
pub mod stats;
pub mod wal;
pub mod wal_sync;

/// Shared SSTable block cache. Constructed by the engine at open and
/// hung off `Arc`s in every `SstableReader`. Embedders rarely touch this
/// type directly; tune via [`engine::EngineConfig::block_cache_bytes`].
pub use block_cache::{BlockCache, BlockKey, CacheStats};
/// Snapshot-isolated read primitives. See [`snapshot`] module docs for
/// the v1 (scoped) vs v2 (long-lived) trade-off.
pub use owned_snapshot::SnapshotHandle;
pub use snapshot::{RangeIter, SnapshotView};
