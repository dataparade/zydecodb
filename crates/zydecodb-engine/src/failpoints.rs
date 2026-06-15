//! Internal failpoint helpers.
//!
//! The `fail` crate's macros expand to nothing unless the `failpoints` feature
//! is enabled, so release builds are byte-identical to a build without this
//! module. Tests turn individual points on with
//! [`fail::cfg`](https://docs.rs/fail/latest/fail/fn.cfg.html) — for example
//! `fail::cfg("wal::before_fsync", "return")` makes the named point return the
//! injected error on the next traversal.
//!
//! Every failpoint name used in the engine is declared here as a `pub const`
//! so a test can refer to `zydecodb_engine::failpoints::WAL_BEFORE_FSYNC`
//! instead of a stringly-typed literal.

/// Right before [`crate::engine::Engine`] writes WAL bytes to the page cache.
pub const WAL_BEFORE_APPEND: &str = "wal::before_append";
/// Right after WAL bytes have reached the page cache but before the in-memory
/// bookkeeping (`active_wal_size`, `last_buffered_seq`) is updated.
pub const WAL_AFTER_APPEND: &str = "wal::after_append";
/// Right before the group-commit `fsync(2)` call.
pub const WAL_BEFORE_FSYNC: &str = "wal::before_fsync";
/// Right after `fsync(2)` returned, before the durability watermark advances.
pub const WAL_AFTER_FSYNC: &str = "wal::after_fsync";
/// Test-only: advance the durability watermark without calling `fsync(2)`.
/// Resilience tests use this to simulate a lying fsync.
pub const WAL_LIE_FSYNC: &str = "wal::lie_fsync";
/// Right before rolling to a new WAL segment (the prior segment has been
/// fsynced and is about to be sealed and shipped).
pub const WAL_BEFORE_SEGMENT_ROLL: &str = "wal::before_segment_roll";
/// Right after the new WAL segment file's header has been fsynced.
pub const WAL_AFTER_SEGMENT_ROLL: &str = "wal::after_segment_roll";

/// Right before inserting a decoded record into the active memtable.
pub const ENGINE_BEFORE_MEMTABLE_INSERT: &str = "engine::before_memtable_insert";
/// Right after the memtable insert returned (counters about to update).
pub const ENGINE_AFTER_MEMTABLE_INSERT: &str = "engine::after_memtable_insert";

/// Right before writing the in-memory SSTable bytes to the .tmp file.
pub const SSTABLE_BEFORE_TMP_WRITE: &str = "sstable::before_tmp_write";
/// Right after the .tmp file has been written and fsynced, before rename.
pub const SSTABLE_AFTER_TMP_WRITE: &str = "sstable::after_tmp_write";
/// Right before the atomic rename(2) that publishes the SSTable.
pub const SSTABLE_BEFORE_RENAME: &str = "sstable::before_rename";
/// Right after rename(2) but before the directory fsync that makes the rename
/// itself durable. A crash here is the canonical "lost SSTable" scenario the
/// recovery path must tolerate.
pub const SSTABLE_AFTER_RENAME: &str = "sstable::after_rename";

/// Right before appending a manifest record (SSTABLE_ADD or WAL_TRUNCATE).
pub const MANIFEST_BEFORE_APPEND: &str = "manifest::before_append";
/// Right after the manifest fsync — the catalog change is now durable.
pub const MANIFEST_AFTER_FSYNC: &str = "manifest::after_fsync";

/// Right before atomically renaming a compaction output SSTable into place.
pub const COMPACTION_BEFORE_RENAME: &str = "compaction::before_rename";
/// Right before appending the SstableAdd/SstableRemove manifest batch for
/// a compaction.
pub const COMPACTION_BEFORE_MANIFEST: &str = "compaction::before_manifest";
/// Right after the compaction manifest fsync but before the input files
/// are unlinked. Legacy name; apply-thread path uses
/// [`APPLY_AFTER_PUBLISH_BEFORE_UNLINK`].
pub const COMPACTION_AFTER_MANIFEST_BEFORE_UNLINK: &str =
    "compaction::after_manifest_before_unlink";

/// Apply thread: manifest fsync completed, about to publish to owner ready queue.
pub const APPLY_AFTER_FSYNC_BEFORE_PUBLISH: &str = "apply::after_fsync_before_publish";

/// Owner thread: catalog swap done, about to unlink obsolete SSTables/WAL.
pub const APPLY_AFTER_PUBLISH_BEFORE_UNLINK: &str = "apply::after_publish_before_unlink";

/// Convenience: build the injected-error `Err` arm for a `fail_point!` site.
/// Centralizing this keeps every injection site producing the same shape of
/// `EngineError::Io("injected: <name>")` string, which the crash matrix tests
/// pattern-match on.
#[macro_export]
macro_rules! engine_fail_point {
    ($name:expr) => {
        ::fail::fail_point!($name, |_| {
            Err($crate::errors::EngineError::Io(format!(
                "injected failpoint: {}",
                $name
            )))
        });
    };
}

/// Expression-position failpoint check returning `EngineResult<()>`.
// `name` is consumed by `fail_point!`; with the `fail` feature disabled the
// macro expands to nothing, leaving the parameter unused.
#[allow(unused_variables)]
pub fn failpoint_result(name: &'static str) -> crate::errors::EngineResult<()> {
    ::fail::fail_point!(name, |_| {
        Err(crate::errors::EngineError::Io(format!(
            "injected failpoint: {}",
            name
        )))
    });
    Ok(())
}
