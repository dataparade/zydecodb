//! Factorized shared engine handle (Phase 4 DFS prerequisite).
//!
//! Replaces a bare `Arc<Mutex<Engine>>` as the server's shared state so that
//! block-cache, fair-share accounting, and WAL group-commit fsync each have
//! their own lock domain and never require the write mutex.
//!
//! | Domain | Lock |
//! |--------|------|
//! | Write (memtable, WAL append, SST catalog publish) | `write: RwLock<Engine>` (exclusive) |
//! | Read snapshot pin (memtable Arc + SST list) | `write: RwLock<Engine>` (shared) |
//! | Block cache | interior mutex on [`BlockCache`] |
//! | Fair-share pools / stalls | interior mutex on [`FairShareState`] |
//! | WAL durability (group commit) | [`WalSync`] |
//!
//! Callers that only need durability or cache stats must use the dedicated
//! accessors — do not take `write()` for those paths. Snapshot acquisition and
//! other `&Engine` reads must use [`EngineHandle::read`] so concurrent readers
//! do not serialize behind writers for the whole SST I/O.

use crate::block_cache::BlockCache;
use crate::engine::Engine;
use crate::tenant_fair::FairShareState;
use crate::wal_sync::WalSync;
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard, TryLockError};

/// Shared engine with per-resource lock domains.
pub struct EngineHandle {
    write: RwLock<Engine>,
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
            write: RwLock::new(engine),
            block_cache,
            fair,
            wal_sync,
        })
    }

    /// Shared read lock for snapshot capture and other `&Engine` paths.
    /// Hold only long enough to clone Arcs / pin SST ids — not across I/O.
    pub fn read(&self) -> RwLockReadGuard<'_, Engine> {
        self.write.read().unwrap_or_else(|e| e.into_inner())
    }

    /// Acquire the exclusive write-domain lock (memtable / WAL append / catalog).
    pub fn write(&self) -> RwLockWriteGuard<'_, Engine> {
        self.write.write().unwrap_or_else(|e| e.into_inner())
    }

    /// Non-blocking exclusive write lock. Returns `WouldBlock` when another
    /// writer (or reader) holds the lock — callers must skip or retry later.
    /// Preserves poison observability via [`TryLockError::Poisoned`].
    pub fn try_write(
        &self,
    ) -> Result<RwLockWriteGuard<'_, Engine>, TryLockError<RwLockWriteGuard<'_, Engine>>> {
        self.write.try_write()
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
