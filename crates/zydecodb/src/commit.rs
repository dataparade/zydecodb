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

use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use tracing::error;
use zydecodb_engine::engine::Engine;

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
    /// Signaled when a `Sync`-mode waiter raises `requested_seq`, or on stop.
    work: Condvar,
    /// Signaled when `synced_seq` advances, or on stop.
    done: Condvar,
}

impl CommitCoordinator {
    pub fn new(engine: Arc<Mutex<Engine>>, mode: DurabilityMode) -> Arc<Self> {
        // Grab the WAL-sync handle once; the coordinator never needs the engine
        // mutex again, which is the whole point of decoupling the fsync.
        let wal_sync = engine.lock().unwrap().wal_sync();
        Arc::new(CommitCoordinator {
            wal_sync,
            mode,
            state: Mutex::new(CommitState {
                requested_seq: 0,
                synced_seq: 0,
                shutdown: false,
            }),
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
    pub fn commit(&self, seq: u64, relaxed: bool) {
        match self.mode {
            DurabilityMode::Sync if !relaxed => self.await_durable(seq),
            _ => {}
        }
    }

    /// Block until `seq` is fsynced, or the coordinator is shutting down (in
    /// which case `Engine::shutdown` provides the final durability point).
    fn await_durable(&self, seq: u64) {
        let mut st = self.state.lock().unwrap();
        if st.synced_seq >= seq {
            return;
        }
        if seq > st.requested_seq {
            st.requested_seq = seq;
        }
        self.work.notify_one();
        while st.synced_seq < seq && !st.shutdown {
            st = self.done.wait(st).unwrap();
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
            self.fsync_once();
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
            self.fsync_once();
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
    fn fsync_once(&self) {
        match self.wal_sync.sync() {
            Ok(seq) => {
                let mut st = self.state.lock().unwrap();
                if seq > st.synced_seq {
                    st.synced_seq = seq;
                    self.done.notify_all();
                }
            }
            Err(e) => error!(error = %e, "WAL fsync failed in commit coordinator"),
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

    fn temp_engine() -> Arc<Mutex<Engine>> {
        let dir = std::env::temp_dir().join(format!("zydeco-commit-{}", rand_suffix()));
        let engine = Engine::open(EngineConfig {
            data_dir: dir.join("data"),
            wal_dir: dir.join("wal"),
            ..Default::default()
        })
        .unwrap();
        Arc::new(Mutex::new(engine))
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
        let coord = CommitCoordinator::new(Arc::clone(&engine), DurabilityMode::Sync);
        let _h = coord.spawn().unwrap();

        let seq = {
            let mut e = engine.lock().unwrap();
            e.put(b"\x01k".to_vec(), b"v".to_vec(), 0).unwrap()
        };
        coord.commit(seq, false);
        // After commit() returns in Sync mode, the seq must be fsynced.
        let synced = coord.state.lock().unwrap().synced_seq;
        assert!(synced >= seq);
        coord.stop();
    }

    #[test]
    fn relaxed_write_does_not_block() {
        let engine = temp_engine();
        let coord = CommitCoordinator::new(Arc::clone(&engine), DurabilityMode::Sync);
        let _h = coord.spawn().unwrap();
        let seq = {
            let mut e = engine.lock().unwrap();
            e.put(b"\x01k".to_vec(), b"v".to_vec(), 0).unwrap()
        };
        let start = Instant::now();
        coord.commit(seq, true);
        // Relaxed must return promptly without waiting on the fsync thread.
        assert!(start.elapsed() < Duration::from_millis(50));
        coord.stop();
    }

    #[test]
    fn periodic_mode_fsyncs_in_background() {
        let engine = temp_engine();
        let coord = CommitCoordinator::new(
            Arc::clone(&engine),
            DurabilityMode::Periodic {
                interval: Duration::from_millis(20),
            },
        );
        let _h = coord.spawn().unwrap();
        let seq = {
            let mut e = engine.lock().unwrap();
            e.put(b"\x01k".to_vec(), b"v".to_vec(), 0).unwrap()
        };
        // commit() returns immediately in periodic mode (no wait).
        let start = Instant::now();
        coord.commit(seq, false);
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
            let engine = Arc::new(Mutex::new(
                Engine::open(EngineConfig {
                    data_dir: data_dir.clone(),
                    wal_dir: wal_dir.clone(),
                    ..Default::default()
                })
                .unwrap(),
            ));
            let coord = CommitCoordinator::new(Arc::clone(&engine), DurabilityMode::Sync);
            let handle = coord.spawn().unwrap();
            let seq = {
                let mut e = engine.lock().unwrap();
                e.put(b"\x01durable".to_vec(), b"value".to_vec(), 0)
                    .unwrap()
            };
            coord.commit(seq, false);
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
