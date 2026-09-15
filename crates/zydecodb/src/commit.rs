//! Commit coordinator: turns the engine's buffered group-commit WAL into a
//! safe, batched durability contract for the threaded server.
//!
//! The engine buffers WAL appends (`group_commit = true`) and exposes
//! [`Engine::sync_wal`], which fsyncs everything buffered so far and returns the
//! highest durable sequence number. Connection threads do the fast part of a
//! write under the engine lock (WAL append + memtable insert), then ask the
//! coordinator to make their assigned `seq` durable. A single coordinator thread
//! performs the fsync, so many concurrent writers collapse into one fsync — real
//! group commit, without needing concurrent access to the single-owner engine.
//!
//! Durability modes:
//! - [`DurabilityMode::Sync`]: a write is acknowledged only after its `seq` has
//!   been fsynced. The coordinator syncs as soon as a waiter appears, batching
//!   whatever else is already buffered. Safe against power loss by default.
//! - [`DurabilityMode::Periodic`]: the coordinator fsyncs on a fixed interval;
//!   writes are acknowledged right after the buffered append, so at most one
//!   interval of acknowledged writes can be lost on power loss (bounded-loss,
//!   like Redis `appendfsync everysec`).
//!
//! A per-request `relaxed` flag lets an individual write opt out of the
//! durability wait even in `Sync` mode (ack-after-buffer for that write).
//!
//! Fsync failure is sticky and fails closed. After `fsync(2)` reports an error
//! the kernel may already have dropped the dirty pages, so a later successful
//! fsync proves nothing about the writes buffered before it. The coordinator
//! therefore records the first failure, wakes every waiter with
//! [`CommitError`], and never syncs again; the server refuses new writes
//! (reads keep working) and `/readyz` reports 503 until the process restarts.

use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};
use tracing::error;
use zydecodb_engine::engine_handle::EngineHandle;
use zydecodb_engine::errors::{EngineError, Status};
use zydecodb_engine::frame::ResponseEnvelope;
use zydecodb_engine::wal_sync::WalSync;

/// The WAL fsync thread failed. The write this was returned for is not known
/// to be durable, and the coordinator refuses all further writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CommitError {
    /// The underlying I/O error text from the first failed fsync.
    pub reason: String,
}

impl fmt::Display for CommitError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "WAL fsync failed ({}); write not durable; writes refused until restart",
            self.reason
        )
    }
}

impl std::error::Error for CommitError {}

impl CommitError {
    /// Wire response for a write refused or unacknowledged because of a
    /// coordinator failure.
    pub fn to_response(&self) -> ResponseEnvelope {
        ResponseEnvelope::error(Status::IoError, &self.to_string())
    }
}

impl From<CommitError> for EngineError {
    fn from(e: CommitError) -> Self {
        EngineError::Io(e.to_string())
    }
}

/// How the server establishes durability for acknowledged writes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    /// Acknowledge a write only after its `seq` is fsynced (safe by default).
    Sync,
    /// Acknowledge after the buffered append; the coordinator fsyncs every
    /// `interval` (bounded data-loss window on power loss).
    Periodic { interval: Duration },
}

struct CommitState {
    /// Highest `seq` a `Sync`-mode waiter wants made durable.
    requested_seq: u64,
    /// Highest `seq` known fsynced to disk.
    synced_seq: u64,
    shutdown: bool,
    /// Set once by the first failed fsync; never cleared.
    failed: Option<String>,
}

/// Owns the single fsync thread and the condvars that connection threads wait
/// on for durability.
pub struct CommitCoordinator {
    /// Decoupled WAL durability handle. The coordinator fsyncs through this
    /// instead of `Arc<Mutex<Engine>>`, so the group-commit fsync never contends
    /// on the engine mutex with writers and snapshot captures.
    wal_sync: Arc<zydecodb_engine::wal_sync::WalSync>,
    mode: DurabilityMode,
    state: Mutex<CommitState>,
    /// Lock-free mirror of `state.failed.is_some()` so the per-request write
    /// gate in the server loop never takes the state mutex.
    failed: AtomicBool,
    /// Signaled when a `Sync`-mode waiter raises `requested_seq`, or on stop.
    work: Condvar,
    /// Signaled when `synced_seq` advances, on failure, or on stop.
    done: Condvar,
}

impl CommitCoordinator {
    /// Build a coordinator from the factorized handle's WAL-sync domain
    /// (never takes the write mutex).
    pub fn new(engine: &EngineHandle, mode: DurabilityMode) -> Arc<Self> {
        Self::from_wal_sync(Arc::clone(engine.wal_sync()), mode)
    }

    pub fn from_wal_sync(wal_sync: Arc<WalSync>, mode: DurabilityMode) -> Arc<Self> {
        Arc::new(CommitCoordinator {
            wal_sync,
            mode,
            state: Mutex::new(CommitState {
                requested_seq: 0,
                synced_seq: 0,
                shutdown: false,
                failed: None,
            }),
            failed: AtomicBool::new(false),
            work: Condvar::new(),
            done: Condvar::new(),
        })
    }

    /// Spawn the coordinator's dedicated fsync thread.
    pub fn spawn(self: &Arc<Self>) -> std::io::Result<JoinHandle<()>> {
        let me = Arc::clone(self);
        thread::Builder::new()
            .name("zydecodb-commit".into())
            .spawn(move || me.run())
    }

    pub fn mode(&self) -> DurabilityMode {
        self.mode
    }

    /// Make `seq` durable according to the configured mode. In `Sync` mode this
    /// blocks (unless `relaxed`) until `seq` is fsynced; in `Periodic` mode it
    /// returns immediately and the background tick provides durability.
    ///
    /// Returns `Err` once the fsync thread has failed: the write may not be on
    /// disk and the caller must not acknowledge it as durable. Relaxed and
    /// periodic writes get the same `Err` so no caller acks into a dead WAL.
    pub fn commit(&self, seq: u64, relaxed: bool) -> Result<(), CommitError> {
        match self.mode {
            DurabilityMode::Sync if !relaxed => self.await_durable(seq),
            _ => match self.failure() {
                Some(e) => Err(e),
                None => Ok(()),
            },
        }
    }

    /// `Some` once the fsync thread has hit an I/O error. Sticky until restart.
    pub fn failure(&self) -> Option<CommitError> {
        if !self.failed.load(Ordering::Acquire) {
            return None;
        }
        let st = self.state.lock().unwrap();
        st.failed.as_ref().map(|reason| CommitError {
            reason: reason.clone(),
        })
    }

    /// Lock-free check for the server's per-request write gate.
    pub fn is_failed(&self) -> bool {
        self.failed.load(Ordering::Acquire)
    }

    /// Highest sequence known fsynced (change streams must not emit beyond this).
    pub fn durable_seq(&self) -> u64 {
        self.state
            .lock()
            .unwrap()
            .synced_seq
            .max(self.wal_sync.synced_seq())
    }

    /// Block until `seq` is fsynced, or `timeout` elapses, or shutdown, or the
    /// fsync thread has failed. Returns true if `seq` is durable.
    pub fn wait_durable(&self, seq: u64, timeout: Duration) -> bool {
        let mut st = self.state.lock().unwrap();
        if st.synced_seq.max(self.wal_sync.synced_seq()) >= seq {
            return true;
        }
        if seq > st.requested_seq {
            st.requested_seq = seq;
        }
        self.work.notify_one();
        let deadline = Instant::now() + timeout;
        while st.synced_seq.max(self.wal_sync.synced_seq()) < seq
            && !st.shutdown
            && st.failed.is_none()
        {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (next, _) = self.done.wait_timeout(st, remaining).unwrap();
            st = next;
        }
        st.synced_seq.max(self.wal_sync.synced_seq()) >= seq
    }

    /// Block until the durable watermark advances past `seq`, then return.
    /// Used by change streams waiting for new fsynced writes.
    ///
    /// After an fsync failure the watermark can never advance, so this simply
    /// runs out its (bounded) `timeout`. It deliberately does not return early
    /// on failure: the watch loop calls it back-to-back, and an immediate
    /// return would turn every idle change stream into a busy loop.
    pub fn wait_durable_advance(&self, after_seq: u64, timeout: Duration) -> u64 {
        let mut st = self.state.lock().unwrap();
        let mut durable = st.synced_seq.max(self.wal_sync.synced_seq());
        if durable > after_seq || st.shutdown {
            return durable;
        }
        // Nudge the coordinator so periodic/sync modes keep moving.
        if after_seq + 1 > st.requested_seq {
            st.requested_seq = after_seq + 1;
        }
        self.work.notify_one();
        let deadline = Instant::now() + timeout;
        while durable <= after_seq && !st.shutdown {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            let (next, _) = self.done.wait_timeout(st, remaining).unwrap();
            st = next;
            durable = st.synced_seq.max(self.wal_sync.synced_seq());
        }
        durable
    }

    /// Block until `seq` is fsynced, or the coordinator is shutting down (in
    /// which case `Engine::shutdown` provides the final durability point), or
    /// the fsync thread has failed (`Err`: `seq` is not known to be durable).
    fn await_durable(&self, seq: u64) -> Result<(), CommitError> {
        let mut st = self.state.lock().unwrap();
        if st.synced_seq >= seq {
            return Ok(());
        }
        if let Some(reason) = &st.failed {
            return Err(CommitError {
                reason: reason.clone(),
            });
        }
        if seq > st.requested_seq {
            st.requested_seq = seq;
        }
        self.work.notify_one();
        while st.synced_seq < seq && !st.shutdown && st.failed.is_none() {
            st = self.done.wait(st).unwrap();
        }
        if st.synced_seq >= seq {
            return Ok(());
        }
        match &st.failed {
            Some(reason) => Err(CommitError {
                reason: reason.clone(),
            }),
            None => Ok(()),
        }
    }

    fn run(self: Arc<Self>) {
        match self.mode {
            DurabilityMode::Sync => self.run_sync(),
            DurabilityMode::Periodic { interval } => self.run_periodic(interval),
        }
    }

    fn run_sync(&self) {
        let mut st = self.state.lock().unwrap();
        loop {
            while !st.shutdown && st.requested_seq <= st.synced_seq {
                st = self.work.wait(st).unwrap();
            }
            if st.shutdown {
                return;
            }
            // Release the state lock before the fsync: the state lock and the
            // WAL-sync locks are never held together, so there is no lock-order
            // inversion with writers.
            drop(st);
            if !self.fsync_once() {
                return;
            }
            st = self.state.lock().unwrap();
        }
    }

    fn run_periodic(&self, interval: Duration) {
        let mut st = self.state.lock().unwrap();
        loop {
            if st.shutdown {
                return;
            }
            drop(st);
            if !self.fsync_once() {
                return;
            }
            st = self.state.lock().unwrap();
            if st.shutdown {
                return;
            }
            let (next, _) = self.work.wait_timeout(st, interval).unwrap();
            st = next;
        }
    }

    /// Fsync the WAL once (off the engine lock) and publish the new durable seq
    /// to any waiters. Strict-ack is preserved: `done` is notified only after
    /// `WalSync::sync` returns, i.e. after the fsync has actually completed.
    ///
    /// Returns `false` after an fsync error. The failure is recorded, every
    /// waiter is woken so it can return [`CommitError`], and the caller must
    /// stop the fsync loop: a later fsync cannot be trusted to cover the
    /// writes that were buffered when this one failed.
    fn fsync_once(&self) -> bool {
        match self.wal_sync.sync() {
            Ok(seq) => {
                let mut st = self.state.lock().unwrap();
                if seq > st.synced_seq {
                    st.synced_seq = seq;
                    self.done.notify_all();
                }
                true
            }
            Err(e) => {
                error!(
                    error = %e,
                    "WAL fsync failed in commit coordinator; refusing writes until restart"
                );
                let mut st = self.state.lock().unwrap();
                if st.failed.is_none() {
                    st.failed = Some(e.to_string());
                }
                self.failed.store(true, Ordering::Release);
                self.done.notify_all();
                false
            }
        }
    }

    /// Signal the coordinator to stop and wake every waiter. Call before joining
    /// connection threads so any in-flight `await_durable` returns.
    pub fn stop(&self) {
        let mut st = self.state.lock().unwrap();
        st.shutdown = true;
        self.work.notify_all();
        self.done.notify_all();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;
    use zydecodb_engine::engine::{Engine, EngineConfig};

    fn temp_engine() -> Arc<EngineHandle> {
        let dir = std::env::temp_dir().join(format!("zydeco-commit-{}", rand_suffix()));
        let engine = Engine::open(EngineConfig {
            data_dir: dir.join("data"),
            wal_dir: dir.join("wal"),
            ..Default::default()
        })
        .unwrap();
        EngineHandle::new(engine)
    }

    fn rand_suffix() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos() as u64
    }

    #[test]
    fn sync_mode_makes_write_durable_before_returning() {
        let engine = temp_engine();
        let coord = CommitCoordinator::new(&engine, DurabilityMode::Sync);
        let _h = coord.spawn().unwrap();

        let seq = {
            let mut e = engine.write();
            e.put(b"\x01k".to_vec(), b"v".to_vec(), 0).unwrap()
        };
        coord.commit(seq, false).unwrap();
        // After commit() returns in Sync mode, the seq must be fsynced.
        let synced = coord.state.lock().unwrap().synced_seq;
        assert!(synced >= seq);
        coord.stop();
    }

    /// An fsync error must wake the blocked Sync-mode waiter with an error
    /// (not hang it), stick, and refuse every later commit in any mode.
    #[cfg(unix)]
    #[test]
    fn fsync_failure_fails_closed() {
        use std::os::fd::OwnedFd;
        // fsync(2) on a pipe fails with EINVAL, giving a deterministic I/O
        // error without touching a filesystem.
        let (_reader, writer) = std::io::pipe().unwrap();
        let pipe_file = std::fs::File::from(OwnedFd::from(writer));

        let wal_sync = WalSync::new(0);
        wal_sync.set_active(Arc::new(pipe_file));
        let coord = CommitCoordinator::from_wal_sync(Arc::clone(&wal_sync), DurabilityMode::Sync);
        let thread = coord.spawn().unwrap();
        assert!(coord.failure().is_none());

        wal_sync.advance_buffered(1);
        let start = Instant::now();
        let err = coord.commit(1, false).unwrap_err();
        assert!(
            start.elapsed() < Duration::from_secs(5),
            "waiter did not return promptly after fsync failure"
        );
        assert!(err.to_string().contains("writes refused until restart"));
        assert!(coord.is_failed());
        assert_eq!(coord.failure(), Some(err.clone()));
        assert_eq!(err.to_response().status, Status::IoError);

        // Sticky: later commits of any flavor are refused without blocking.
        wal_sync.advance_buffered(2);
        assert_eq!(coord.commit(2, false), Err(err.clone()));
        assert_eq!(coord.commit(2, true), Err(err.clone()));
        assert!(!coord.wait_durable(2, Duration::from_millis(50)));
        // Already-durable seqs stay durable.
        assert!(coord.wait_durable(0, Duration::from_millis(10)));

        // The fsync thread has exited; stop/join must not hang.
        coord.stop();
        thread.join().unwrap();
    }

    #[cfg(unix)]
    #[test]
    fn periodic_mode_fsync_failure_is_reported_on_next_commit() {
        use std::os::fd::OwnedFd;
        let (_reader, writer) = std::io::pipe().unwrap();
        let wal_sync = WalSync::new(0);
        wal_sync.set_active(Arc::new(std::fs::File::from(OwnedFd::from(writer))));
        let coord = CommitCoordinator::from_wal_sync(
            Arc::clone(&wal_sync),
            DurabilityMode::Periodic {
                interval: Duration::from_millis(10),
            },
        );
        let thread = coord.spawn().unwrap();
        wal_sync.advance_buffered(1);
        let mut failed = false;
        for _ in 0..100 {
            if coord.is_failed() {
                failed = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(failed, "periodic tick never observed the fsync error");
        assert!(coord.commit(1, false).is_err());
        coord.stop();
        thread.join().unwrap();
    }

    #[test]
    fn relaxed_write_does_not_block() {
        let engine = temp_engine();
        let coord = CommitCoordinator::new(&engine, DurabilityMode::Sync);
        let _h = coord.spawn().unwrap();
        let seq = {
            let mut e = engine.write();
            e.put(b"\x01k".to_vec(), b"v".to_vec(), 0).unwrap()
        };
        let start = Instant::now();
        coord.commit(seq, true).unwrap();
        // Relaxed must return promptly without waiting on the fsync thread.
        assert!(start.elapsed() < Duration::from_millis(50));
        coord.stop();
    }

    #[test]
    fn periodic_mode_fsyncs_in_background() {
        let engine = temp_engine();
        let coord = CommitCoordinator::new(
            &engine,
            DurabilityMode::Periodic {
                interval: Duration::from_millis(20),
            },
        );
        let _h = coord.spawn().unwrap();
        let seq = {
            let mut e = engine.write();
            e.put(b"\x01k".to_vec(), b"v".to_vec(), 0).unwrap()
        };
        // commit() returns immediately in periodic mode (no wait).
        let start = Instant::now();
        coord.commit(seq, false).unwrap();
        assert!(start.elapsed() < Duration::from_millis(20));
        // The background tick makes it durable within a few intervals.
        let mut durable = false;
        for _ in 0..50 {
            if coord.state.lock().unwrap().synced_seq >= seq {
                durable = true;
                break;
            }
            thread::sleep(Duration::from_millis(10));
        }
        assert!(durable, "periodic coordinator never fsynced the write");
        coord.stop();
    }

    #[test]
    fn synced_writes_survive_reopen() {
        // Proves the committed write path (buffered append + coordinator fsync)
        // is recoverable: after a Sync-mode ack, reopening the engine from the
        // same directories replays the write.
        let dir = std::env::temp_dir().join(format!("zydeco-commit-reopen-{}", rand_suffix()));
        let data_dir = dir.join("data");
        let wal_dir = dir.join("wal");
        {
            let engine = EngineHandle::new(
                Engine::open(EngineConfig {
                    data_dir: data_dir.clone(),
                    wal_dir: wal_dir.clone(),
                    ..Default::default()
                })
                .unwrap(),
            );
            let coord = CommitCoordinator::new(&engine, DurabilityMode::Sync);
            let handle = coord.spawn().unwrap();
            let seq = {
                let mut e = engine.write();
                e.put(b"\x01durable".to_vec(), b"value".to_vec(), 0)
                    .unwrap()
            };
            coord.commit(seq, false).unwrap();
            coord.stop();
            // Join the coordinator so its engine Arc is released; otherwise the
            // background thread can outlive this block and keep the data_dir lock
            // held, blocking the reopen below. Drop without a clean shutdown to
            // mimic an abrupt exit.
            handle.join().unwrap();
            drop(coord);
        }
        let reopened = Engine::open(EngineConfig {
            data_dir,
            wal_dir,
            ..Default::default()
        })
        .unwrap();
        assert_eq!(
            reopened.get(b"\x01durable").unwrap().as_deref(),
            Some(&b"value"[..])
        );
    }
}
