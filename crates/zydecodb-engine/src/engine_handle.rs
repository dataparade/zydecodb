//! Factorized shared engine handle (Phase 4 DFS prerequisite).
//!
//! Replaces a bare `Arc<Mutex<Engine>>` as the server's shared state so that
//! block-cache, fair-share accounting, and WAL group-commit fsync each have
//! their own lock domain and never require the write mutex.
//!
//! | Domain | Lock |
//! |--------|------|
//! | Write (memtable, WAL append, SST catalog publish) | `write: Mutex<Engine>` |
//! | Block cache | interior mutex on [`BlockCache`] |
//! | Fair-share pools / stalls | interior mutex on [`FairShareState`] |
//! | WAL durability (group commit) | [`WalSync`] |
//!
//! Callers that only need durability or cache stats must use the dedicated
//! accessors — do not take `write()` for those paths.

use crate::block_cache::BlockCache;
use crate::engine::Engine;
use crate::tenant_fair::FairShareState;
use crate::wal_sync::WalSync;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// Shared engine with per-resource lock domains.
pub struct EngineHandle {
    write: Mutex<Engine>,
    block_cache: Arc<BlockCache>,
    fair: Arc<FairShareState>,
    wal_sync: Arc<WalSync>,
}

impl EngineHandle {
    /// Wrap a freshly opened [`Engine`], cloning out concurrent resource Arcs.
    pub fn new(engine: Engine) -> Arc<Self> {
        let block_cache = engine.block_cache_arc();
        let fair = engine.fair_share();
        let wal_sync = engine.wal_sync();
        Arc::new(EngineHandle {
            write: Mutex::new(engine),
            block_cache,
            fair,
            wal_sync,
        })
    }

    /// Acquire the write-domain mutex (memtable / WAL append / catalog).
    pub fn write(&self) -> MutexGuard<'_, Engine> {
        self.write.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Fallible write lock (preserves poison observability for callers that care).
    pub fn try_write(&self) -> Result<MutexGuard<'_, Engine>, PoisonError<MutexGuard<'_, Engine>>> {
        self.write.lock()
    }

    /// Block cache — safe to use without the write mutex.
    pub fn block_cache(&self) -> &Arc<BlockCache> {
        &self.block_cache
    }

    /// δ-fair accounting — safe to use without the write mutex.
    pub fn fair(&self) -> &Arc<FairShareState> {
        &self.fair
    }

    /// WAL durability handle for the commit coordinator — never needs `write()`.
    pub fn wal_sync(&self) -> &Arc<WalSync> {
        &self.wal_sync
    }
}
