//! Read-replica ingest: pull sha256-verified WAL segments shipped by a primary
//! into this node's WAL directory so the engine can replay them.
//!
//! The primary ships each sealed segment into a directory plus a `shipped.log`
//! line (`<id> <seal_seq> <sha256>`); an operator-supplied sidecar transports
//! that directory here. This module never does network I/O — it reads the local
//! `from` directory the sidecar delivers into.
//!
//! Each newly shipped segment is verified against its recorded sha256 before it
//! is installed, so a truncated or corrupted transfer is refused rather than
//! replayed. After [`Replica::sync`] installs new segments, the caller reopens
//! the engine (full WAL replay) to surface them — reusing the same recovery path
//! the primary uses on restart.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use zydecodb_engine::errors::{EngineError, EngineResult};
use zydecodb_engine::{shipping, wal};

/// File in `wal_dir` recording the highest seal sequence this replica has
/// installed, so an out-of-process `replica status` can report lag without
/// touching the running engine.
pub const REPLICA_STATE: &str = "replica.state";

/// File in `data_dir` holding this node's monotonic promotion epoch (term).
/// Absent means epoch 1 (never promoted).
pub const EPOCH: &str = "EPOCH";

/// File in a shipped stream recording the epoch of the primary feeding it. A
/// primary that observes a higher epoch here self-demotes (refuses to start).
pub const FENCE: &str = "FENCE";

/// Tracks which shipped segments have already been installed locally.
pub struct Replica {
    from: PathBuf,
    wal_dir: PathBuf,
    applied: BTreeSet<u64>,
    last_seq: u64,
    applied_max_seq: u64,
    /// When set, every `shipped.log` entry must carry a valid HMAC (see
    /// `shipping::verify_entry`); entries without one are refused.
    hmac_key: Option<Vec<u8>>,
}

/// Result of one [`Replica::sync`] pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct SyncOutcome {
    /// Segment ids installed during this pass.
    pub installed: Vec<u64>,
    /// Highest seal sequence observed across all applied segments.
    pub max_seq: u64,
}

impl SyncOutcome {
    pub fn made_progress(&self) -> bool {
        !self.installed.is_empty()
    }
}

impl Replica {
    pub fn new(from: PathBuf, wal_dir: PathBuf) -> Self {
        let applied_max_seq = read_replica_state(&wal_dir).unwrap_or(0);
        Replica {
            from,
            wal_dir,
            applied: BTreeSet::new(),
            last_seq: applied_max_seq,
            applied_max_seq,
            hmac_key: None,
        }
    }

    /// Require every shipped entry to carry a valid HMAC under `key`.
    pub fn with_hmac_key(mut self, key: Option<Vec<u8>>) -> Self {
        self.hmac_key = key;
        self
    }

    pub fn last_seq(&self) -> u64 {
        self.last_seq
    }

    /// Highest seal sequence this replica has installed (and persisted).
    pub fn applied_max_seq(&self) -> u64 {
        self.applied_max_seq
    }

    /// Read `shipped.log`, verify and install every segment not yet applied.
    /// Returns which segments were installed this pass. Segments are installed
    /// in shipped (ascending id) order; the first verification failure stops the
    /// pass so replay order is never violated (the segment is retried next pass,
    /// after the sidecar finishes delivering it).
    pub fn sync(&mut self) -> EngineResult<SyncOutcome> {
        std::fs::create_dir_all(&self.wal_dir)?;
        let entries = shipping::read_shipped_log(&self.from)?;
        let mut outcome = SyncOutcome::default();

        for entry in entries {
            self.last_seq = self.last_seq.max(entry.seal_seq);
            if self.applied.contains(&entry.segment_id) {
                continue;
            }

            let file_name = wal::segment_filename(entry.segment_id);
            let src = self.from.join(&file_name);
            if !src.exists() {
                // The sidecar logged the segment but hasn't delivered the bytes
                // yet; stop and retry on the next pass so order is preserved.
                break;
            }
            if !shipping::verify_entry(&src, &entry, self.hmac_key.as_deref())? {
                // A partial/corrupt/forged transfer: stop and retry once it
                // settles (or fail permanently if the manifest was tampered).
                break;
            }

            install_segment(&src, &self.wal_dir.join(&file_name))?;
            self.applied.insert(entry.segment_id);
            self.applied_max_seq = self.applied_max_seq.max(entry.seal_seq);
            outcome.installed.push(entry.segment_id);
            outcome.max_seq = outcome.max_seq.max(entry.seal_seq);
        }
        if outcome.made_progress() {
            // Best-effort: a failure to persist just makes `replica status`
            // under-report; the WAL segments themselves are already installed.
            let _ = write_replica_state(&self.wal_dir, self.applied_max_seq);
        }
        Ok(outcome)
    }
}

/// Persist the highest installed seal sequence (atomic temp + rename).
pub fn write_replica_state(wal_dir: &Path, seq: u64) -> EngineResult<()> {
    std::fs::create_dir_all(wal_dir)?;
    let tmp = wal_dir.join("replica.state.tmp");
    let dst = wal_dir.join(REPLICA_STATE);
    std::fs::write(&tmp, seq.to_string())?;
    std::fs::rename(&tmp, &dst)
        .map_err(|e| EngineError::Io(format!("replica.state rename: {}", e)))?;
    Ok(())
}

/// Read the persisted applied seal sequence (absent / unreadable -> None).
pub fn read_replica_state(wal_dir: &Path) -> Option<u64> {
    std::fs::read_to_string(wal_dir.join(REPLICA_STATE))
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// Read this node's promotion epoch from `data_dir/EPOCH` (absent -> 1).
pub fn read_epoch(data_dir: &Path) -> u64 {
    std::fs::read_to_string(data_dir.join(EPOCH))
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
        .unwrap_or(1)
}

/// Persist this node's promotion epoch (atomic temp + rename).
pub fn write_epoch(data_dir: &Path, epoch: u64) -> EngineResult<()> {
    std::fs::create_dir_all(data_dir)?;
    let tmp = data_dir.join("EPOCH.tmp");
    let dst = data_dir.join(EPOCH);
    std::fs::write(&tmp, epoch.to_string())?;
    std::fs::rename(&tmp, &dst).map_err(|e| EngineError::Io(format!("EPOCH rename: {}", e)))?;
    Ok(())
}

/// Read the fence epoch recorded in a shipped stream (absent -> None).
pub fn read_fence(dir: &Path) -> Option<u64> {
    std::fs::read_to_string(dir.join(FENCE))
        .ok()
        .and_then(|s| s.trim().parse::<u64>().ok())
}

/// Stamp the fence epoch into a shipped stream (atomic temp + rename).
pub fn write_fence(dir: &Path, epoch: u64) -> EngineResult<()> {
    std::fs::create_dir_all(dir)?;
    let tmp = dir.join("FENCE.tmp");
    let dst = dir.join(FENCE);
    std::fs::write(&tmp, epoch.to_string())?;
    std::fs::rename(&tmp, &dst).map_err(|e| EngineError::Io(format!("FENCE rename: {}", e)))?;
    Ok(())
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// A point-in-time view of replica health for `replica status`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatusReport {
    /// Seconds since the primary's last heartbeat; `None` if none was shipped.
    pub heartbeat_age_secs: Option<u64>,
    /// The primary's write position (from the heartbeat / shipped.log).
    pub primary_seq: u64,
    /// Highest seal sequence present in the shipped stream.
    pub shipped_high_seq: u64,
    /// Highest seal sequence this replica has installed.
    pub replica_applied_seq: u64,
    /// `primary_seq - replica_applied_seq`.
    pub seq_lag: u64,
    /// The replica has installed every shipped segment.
    pub caught_up: bool,
    /// The primary's heartbeat is present and within `max_stale_secs`.
    pub healthy: bool,
    /// Staleness threshold used to compute `healthy`.
    pub max_stale_secs: u64,
}

impl StatusReport {
    /// True when the primary looks alive (fresh heartbeat). Drives the CLI exit
    /// code so an orchestrator can use `replica status` as a health probe.
    pub fn is_ok(&self) -> bool {
        self.healthy
    }

    pub fn render_human(&self) -> String {
        let hb = match self.heartbeat_age_secs {
            Some(a) => format!("{}s ago", a),
            None => "none".to_string(),
        };
        format!(
            "primary_heartbeat: {}\nprimary_seq:       {}\nshipped_high_seq:  {}\napplied_seq:       {}\nseq_lag:           {}\ncaught_up:         {}\nhealthy:           {} (max_stale={}s)",
            hb,
            self.primary_seq,
            self.shipped_high_seq,
            self.replica_applied_seq,
            self.seq_lag,
            self.caught_up,
            self.healthy,
            self.max_stale_secs,
        )
    }

    pub fn render_json(&self) -> String {
        let age = match self.heartbeat_age_secs {
            Some(a) => a.to_string(),
            None => "null".to_string(),
        };
        format!(
            "{{\"heartbeat_age_secs\":{},\"primary_seq\":{},\"shipped_high_seq\":{},\"applied_seq\":{},\"seq_lag\":{},\"caught_up\":{},\"healthy\":{},\"max_stale_secs\":{}}}",
            age,
            self.primary_seq,
            self.shipped_high_seq,
            self.replica_applied_seq,
            self.seq_lag,
            self.caught_up,
            self.healthy,
            self.max_stale_secs,
        )
    }
}

/// Compute replica health from the shipped stream (`from`) and this replica's
/// persisted state (`wal_dir`). Pure file reads; safe to run alongside a live
/// replica.
pub fn status(from: &Path, wal_dir: &Path, max_stale_secs: u64) -> EngineResult<StatusReport> {
    let hb = shipping::read_heartbeat(from)?;
    let shipped = shipping::read_shipped_log(from)?;
    let shipped_high_seq = shipped.iter().map(|e| e.seal_seq).max().unwrap_or(0);
    let replica_applied_seq = read_replica_state(wal_dir).unwrap_or(0);

    let (heartbeat_age_secs, hb_seq) = match hb {
        Some(h) => (
            Some(now_millis().saturating_sub(h.unix_millis) / 1000),
            h.last_seal_seq,
        ),
        None => (None, 0),
    };
    let primary_seq = hb_seq.max(shipped_high_seq);
    let seq_lag = primary_seq.saturating_sub(replica_applied_seq);
    let caught_up = replica_applied_seq >= shipped_high_seq;
    let healthy = matches!(heartbeat_age_secs, Some(a) if a <= max_stale_secs);

    Ok(StatusReport {
        heartbeat_age_secs,
        primary_seq,
        shipped_high_seq,
        replica_applied_seq,
        seq_lag,
        caught_up,
        healthy,
        max_stale_secs,
    })
}

/// Result of a [`promote`] run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromoteOutcome {
    /// Segment ids drained from the stream during promotion.
    pub drained: Vec<u64>,
    /// This node's epoch before promotion.
    pub previous_epoch: u64,
    /// The new (bumped) epoch written to `data_dir/EPOCH`.
    pub new_epoch: u64,
    /// Highest seal sequence installed after draining.
    pub applied_max_seq: u64,
}

/// Promote a replica to primary: drain every delivered segment, then bump this
/// node's epoch past anything seen in the stream's fence. Offline and
/// file-only (no running server). The caller restarts `serve` without a
/// replication source afterwards; the new primary stamps its epoch into its
/// ship stream, fencing an old primary that re-attaches to the same stream.
pub fn promote(from: &Path, wal_dir: &Path, data_dir: &Path) -> EngineResult<PromoteOutcome> {
    let mut rep = Replica::new(from.to_path_buf(), wal_dir.to_path_buf());
    let mut drained = Vec::new();
    loop {
        let out = rep.sync()?;
        if !out.made_progress() {
            break;
        }
        drained.extend(out.installed);
    }

    let local_epoch = read_epoch(data_dir);
    let fence_epoch = read_fence(from).unwrap_or(0);
    let new_epoch = local_epoch.max(fence_epoch) + 1;
    write_epoch(data_dir, new_epoch)?;

    Ok(PromoteOutcome {
        drained,
        previous_epoch: local_epoch,
        new_epoch,
        applied_max_seq: rep.applied_max_seq(),
    })
}

/// Atomically install a verified segment into the WAL directory: write to a
/// temp file, fsync, then rename over any existing placeholder of the same id.
fn install_segment(src: &Path, dst: &Path) -> EngineResult<()> {
    let bytes = std::fs::read(src)?;
    let tmp = dst.with_extension("log.tmp");
    {
        use std::io::Write;
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, dst)
        .map_err(|e| EngineError::Io(format!("install segment {}: {}", dst.display(), e)))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use zydecodb_engine::shipping::{ship_segment, ShipMode};

    fn make_segment(dir: &Path, id: u64, contents: &[u8]) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(wal::segment_filename(id)), contents).unwrap();
    }

    #[test]
    fn sync_installs_verified_segments_in_order() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_wal = tmp.path().join("primary_wal");
        let ship = tmp.path().join("ship");
        let replica_wal = tmp.path().join("replica_wal");

        // Primary seals two segments and ships them.
        make_segment(&primary_wal, 1, b"segment-one");
        make_segment(&primary_wal, 2, b"segment-two");
        ship_segment(
            &primary_wal.join(wal::segment_filename(1)),
            &ship,
            1,
            10,
            ShipMode::Copy,
            None,
        )
        .unwrap();
        ship_segment(
            &primary_wal.join(wal::segment_filename(2)),
            &ship,
            2,
            20,
            ShipMode::Copy,
            None,
        )
        .unwrap();

        let mut replica = Replica::new(ship.clone(), replica_wal.clone());
        let out = replica.sync().unwrap();
        assert_eq!(out.installed, vec![1, 2]);
        assert_eq!(out.max_seq, 20);
        assert_eq!(replica.last_seq(), 20);
        assert_eq!(
            std::fs::read(replica_wal.join(wal::segment_filename(1))).unwrap(),
            b"segment-one"
        );

        // A second pass with nothing new is a no-op.
        let out2 = replica.sync().unwrap();
        assert!(!out2.made_progress());
    }

    #[test]
    fn sync_refuses_corrupt_segment() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_wal = tmp.path().join("primary_wal");
        let ship = tmp.path().join("ship");
        let replica_wal = tmp.path().join("replica_wal");

        make_segment(&primary_wal, 1, b"good-bytes");
        ship_segment(
            &primary_wal.join(wal::segment_filename(1)),
            &ship,
            1,
            5,
            ShipMode::Copy,
            None,
        )
        .unwrap();
        // Corrupt the shipped copy after its sha256 was recorded.
        std::fs::write(ship.join(wal::segment_filename(1)), b"tampered!!").unwrap();

        let mut replica = Replica::new(ship, replica_wal.clone());
        // verify_entry returns Err on hash mismatch — sync must not install.
        let err = replica.sync().unwrap_err().to_string();
        assert!(
            err.contains("hash mismatch") || err.contains("corrupt"),
            "unexpected error: {err}"
        );
        assert!(!replica_wal.join(wal::segment_filename(1)).exists());
    }

    #[test]
    fn sync_stops_at_first_undelivered_segment() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_wal = tmp.path().join("primary_wal");
        let ship = tmp.path().join("ship");
        let replica_wal = tmp.path().join("replica_wal");

        make_segment(&primary_wal, 1, b"one");
        make_segment(&primary_wal, 2, b"two");
        ship_segment(
            &primary_wal.join(wal::segment_filename(1)),
            &ship,
            1,
            1,
            ShipMode::Copy,
            None,
        )
        .unwrap();
        ship_segment(
            &primary_wal.join(wal::segment_filename(2)),
            &ship,
            2,
            2,
            ShipMode::Copy,
            None,
        )
        .unwrap();
        // Simulate segment 2's bytes not yet delivered (log line present, file gone).
        std::fs::remove_file(ship.join(wal::segment_filename(2))).unwrap();

        let mut replica = Replica::new(ship, replica_wal);
        let out = replica.sync().unwrap();
        assert_eq!(
            out.installed,
            vec![1],
            "must stop before the missing segment"
        );
    }

    #[test]
    fn sync_persists_applied_state() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_wal = tmp.path().join("primary_wal");
        let ship = tmp.path().join("ship");
        let replica_wal = tmp.path().join("replica_wal");

        make_segment(&primary_wal, 1, b"one");
        ship_segment(
            &primary_wal.join(wal::segment_filename(1)),
            &ship,
            1,
            7,
            ShipMode::Copy,
            None,
        )
        .unwrap();

        let mut replica = Replica::new(ship.clone(), replica_wal.clone());
        replica.sync().unwrap();
        assert_eq!(read_replica_state(&replica_wal), Some(7));

        // A fresh Replica recovers the persisted applied position.
        let replica2 = Replica::new(ship, replica_wal);
        assert_eq!(replica2.applied_max_seq(), 7);
    }

    #[test]
    fn epoch_defaults_to_one_and_round_trips() {
        let tmp = tempfile::TempDir::new().unwrap();
        let data = tmp.path().join("data");
        std::fs::create_dir_all(&data).unwrap();
        assert_eq!(read_epoch(&data), 1, "absent EPOCH means epoch 1");
        write_epoch(&data, 5).unwrap();
        assert_eq!(read_epoch(&data), 5);
    }

    #[test]
    fn status_reports_lag_and_staleness() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_wal = tmp.path().join("primary_wal");
        let ship = tmp.path().join("ship");
        let replica_wal = tmp.path().join("replica_wal");

        make_segment(&primary_wal, 1, b"one");
        ship_segment(
            &primary_wal.join(wal::segment_filename(1)),
            &ship,
            1,
            10,
            ShipMode::Copy,
            None,
        )
        .unwrap();
        // Replica has applied up to seq 10 (caught up to shipped).
        write_replica_state(&replica_wal, 10).unwrap();

        // Fresh heartbeat: primary at seq 25, alive now.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        shipping::write_heartbeat(&ship, now, 25).unwrap();

        let report = status(&ship, &replica_wal, 10).unwrap();
        assert_eq!(report.primary_seq, 25);
        assert_eq!(report.shipped_high_seq, 10);
        assert_eq!(report.replica_applied_seq, 10);
        assert_eq!(report.seq_lag, 15);
        assert!(report.caught_up, "applied all shipped segments");
        assert!(
            report.healthy && report.is_ok(),
            "fresh heartbeat is healthy"
        );

        // A stale heartbeat trips unhealthy (used for the non-zero exit code).
        shipping::write_heartbeat(&ship, now - 60_000, 25).unwrap();
        let stale = status(&ship, &replica_wal, 10).unwrap();
        assert!(!stale.healthy, "60s-old heartbeat exceeds 10s threshold");

        // No heartbeat at all is also unhealthy.
        std::fs::remove_file(ship.join(shipping::HEARTBEAT)).unwrap();
        let none = status(&ship, &replica_wal, 10).unwrap();
        assert_eq!(none.heartbeat_age_secs, None);
        assert!(!none.healthy);
    }

    #[test]
    fn promote_drains_then_bumps_epoch() {
        let tmp = tempfile::TempDir::new().unwrap();
        let primary_wal = tmp.path().join("primary_wal");
        let ship = tmp.path().join("ship");
        let replica_wal = tmp.path().join("replica_wal");
        let data = tmp.path().join("data");

        make_segment(&primary_wal, 1, b"one");
        make_segment(&primary_wal, 2, b"two");
        ship_segment(
            &primary_wal.join(wal::segment_filename(1)),
            &ship,
            1,
            10,
            ShipMode::Copy,
            None,
        )
        .unwrap();
        ship_segment(
            &primary_wal.join(wal::segment_filename(2)),
            &ship,
            2,
            20,
            ShipMode::Copy,
            None,
        )
        .unwrap();
        // The stream already carries a fence epoch of 3.
        write_fence(&ship, 3).unwrap();

        let out = promote(&ship, &replica_wal, &data).unwrap();
        assert_eq!(out.drained, vec![1, 2], "drains every delivered segment");
        assert_eq!(out.applied_max_seq, 20);
        // new_epoch = max(local 1, fence 3) + 1 = 4.
        assert_eq!(out.new_epoch, 4);
        assert_eq!(read_epoch(&data), 4, "epoch persisted to data_dir");
    }
}
