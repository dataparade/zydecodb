use super::*;

impl Engine {
    /// Open (or create) an engine rooted at the configured directories, running
    /// full recovery (manifest -> orphan cleanup -> WAL replay).
    pub fn open(cfg: EngineConfig) -> EngineResult<Engine> {
        std::fs::create_dir_all(&cfg.data_dir)?;
        std::fs::create_dir_all(&cfg.wal_dir)?;

        // Take the exclusive data_dir lock before touching any artifact. A second
        // process (or a stray offline CLI command) opening the same directory is
        // rejected here rather than racing on the manifest/WAL/SSTables.
        let data_dir_lock = Self::acquire_data_dir_lock(&cfg.data_dir)?;

        // Detect (and immediately clear) the clean-shutdown marker. We clear it
        // up front so that if THIS process later crashes, the next boot correctly
        // reports an unclean start rather than a stale clean one.
        let marker_path = cfg.data_dir.join(CLEAN_SHUTDOWN_MARKER);
        let clean_boot = marker_path.exists();
        if clean_boot {
            let _ = std::fs::remove_file(&marker_path);
            tracing::info!("clean-shutdown marker found; previous stop was graceful");
        } else {
            tracing::info!("no clean-shutdown marker; recovering via WAL replay");
        }

        // 1. Replay manifest.
        let manifest_path = cfg.data_dir.join("MANIFEST");
        let state = manifest::load(&manifest_path)?;
        tracing::info!(
            live_sstables = state.live_sstables.len(),
            last_durable_seq = state.last_durable_seq,
            "manifest replayed"
        );

        // 2. Delete orphan .sst files (Invariant R2).
        let live_ids: std::collections::HashSet<u64> =
            state.live_sstables.iter().map(|m| m.id).collect();
        Self::delete_orphan_sstables(&cfg.data_dir, &live_ids)?;

        // Construct shared caches before opening any reader. Block cache and
        // fair-share state are independent lock domains (Phase 4).
        let mut fair_cfg = cfg.fair.clone();
        fair_cfg.cache_total_bytes = cfg.block_cache_bytes as u64;
        // Respect an explicit fair memtable budget; only default to the flush
        // threshold when the config left the pool size unset/zero.
        if fair_cfg.memtable_total_bytes == 0 {
            fair_cfg.memtable_total_bytes = cfg.memtable_flush_threshold as u64;
        }
        let fair = std::sync::Arc::new(crate::tenant_fair::FairShareState::new(fair_cfg));
        let block_cache = crate::block_cache::BlockCache::new(cfg.block_cache_bytes);
        if cfg.fair.enabled {
            block_cache.with_fair(std::sync::Arc::clone(&fair));
        }
        let reader_cache = crate::reader_cache::ReaderCache::new(cfg.max_open_readers);
        let result_cache = if cfg.result_cache_bytes > 0 {
            Some(crate::result_cache::ResultCache::new(
                cfg.result_cache_bytes,
            ))
        } else {
            None
        };

        // Load live SSTables (oldest id first => newest last).
        let mut metas = state.live_sstables.clone();
        metas.sort_by_key(|m| m.id);
        let mut max_sstable_id = 0u64;
        let mut max_seq_seen = state.last_durable_seq;
        let mut sstables = Vec::new();
        let mut legacy_sstables = 0usize;
        for meta in metas {
            max_sstable_id = max_sstable_id.max(meta.id);
            max_seq_seen = max_seq_seen.max(meta.max_seq);
            let path = Self::sstable_path(&cfg.data_dir, meta.id);
            let reader = reader_cache.get_or_open(&path, meta.id, block_cache.clone())?;
            if reader.format_version() < crate::sstable::FORMAT_VERSION {
                legacy_sstables += 1;
            }
            sstables.push(LoadedSstable { meta, reader });
        }
        // Surface the on-disk format mix so operators can see when a `data_dir`
        // still holds pre-upgrade (legacy-format) SSTables. They remain readable;
        // `zydecodb admin upgrade` (or ongoing compaction) rewrites them forward.
        if legacy_sstables > 0 {
            tracing::warn!(
                legacy_sstables,
                total_sstables = sstables.len(),
                current_format = crate::sstable::FORMAT_VERSION,
                "on-disk SSTables include legacy-format files (readable; run `admin upgrade` to rewrite)"
            );
        } else if !sstables.is_empty() {
            tracing::info!(
                total_sstables = sstables.len(),
                format_version = crate::sstable::FORMAT_VERSION,
                "all SSTables at current on-disk format"
            );
        }

        // 3. Replay WAL segments (Invariant R1: skip seq <= max_seq of live SSTables).
        //    As a side effect of the scan, record each segment's max seq into
        //    `sealed_segment_max_seq` so post-recovery flushes can compute WAL
        //    coverage without re-reading any segment files. (All segments found
        //    here become "sealed" after open, because open_new_wal_segment will
        //    allocate a fresh active segment id beyond max_wal_id+1.)
        let mut active = Memtable::new();
        let segments = wal::list_segments(&cfg.wal_dir)?;
        // `active` is wrapped in Arc after replay (see Engine construction).
        let sstable_max_seq = sstables.iter().map(|s| s.meta.max_seq).max().unwrap_or(0);
        let mut replayed = 0usize;
        let mut max_wal_id = 0u64;
        let mut sealed_segment_max_seq: BTreeMap<u64, u64> = BTreeMap::new();
        // Only the highest-id segment was the active one at crash time, so it is
        // the only segment allowed to end with a torn tail. Any earlier segment
        // was sealed and fsynced complete before the roll, so damage there is
        // bit-rot, not a crash artifact.
        let active_wal_id_at_crash = segments.iter().map(|(id, _)| *id).max();
        for (id, path) in &segments {
            max_wal_id = max_wal_id.max(*id);
            let is_active = Some(*id) == active_wal_id_at_crash;
            let (records, outcome) = Self::read_segment(path)?;
            match outcome {
                wal::ReplayOutcome::Clean => {}
                wal::ReplayOutcome::TornTail if is_active => {
                    tracing::info!(segment = %path.display(), "truncated torn WAL tail on replay");
                    Self::truncate_torn_segment(path)?;
                }
                wal::ReplayOutcome::TornTail => {
                    // A sealed segment must be intact; an incomplete record here
                    // means the file was truncated/damaged after sealing.
                    return Err(EngineError::Io(format!(
                        "WAL: corruption detected in sealed segment {} \
                         (incomplete record in a segment that was sealed and \
                         fsynced; refusing to open to avoid silently dropping \
                         committed data)",
                        path.display()
                    )));
                }
                wal::ReplayOutcome::Corruption => {
                    // A damaged record with intact records after it: truncating
                    // would silently drop committed data, so refuse loudly.
                    return Err(EngineError::Io(format!(
                        "WAL: corruption detected in segment {} \
                         (a damaged record is followed by intact records; \
                         refusing to open to avoid silently dropping committed \
                         data)",
                        path.display()
                    )));
                }
            }
            // Capture this segment's max seq for the cache, regardless of
            // whether its records get replayed into the memtable (a segment
            // covered entirely by an SSTable is still on disk and still needs
            // a max_seq entry until the next flush truncates it).
            let seg_max = records.iter().map(|r| r.seq()).max().unwrap_or(0);
            if seg_max > 0 {
                sealed_segment_max_seq.insert(*id, seg_max);
            }
            for rec in records {
                let rec_seq = rec.seq();
                if rec_seq <= sstable_max_seq {
                    continue; // R1: already durable in an SSTable
                }
                // Point-in-time restore: stop replaying past the requested seq.
                if let Some(ceiling) = cfg.wal_replay_max_seq {
                    if rec_seq > ceiling {
                        continue;
                    }
                }
                max_seq_seen = max_seq_seen.max(rec_seq);
                // A batch expands to N pairs that all share the batch seq; a
                // single record yields one pair.
                for (k, e) in rec.into_memtable_pairs() {
                    active.insert(k, e);
                    replayed += 1;
                }
            }
        }
        tracing::info!(replayed, "WAL replay complete");

        // 4/5. Seed allocator to max(seq)+1.
        let seq = Arc::new(SeqAllocator::new(max_seq_seen + 1));

        // Open manifest for appending (shared with the apply worker).
        let manifest_file = Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&manifest_path)?,
        ));

        let next_sstable_id = max_sstable_id + 1;
        let active_wal_id = max_wal_id + 1;
        let next_sstable_id_atomic = Arc::new(AtomicU64::new(next_sstable_id));

        let compaction_scheduler = crate::compaction_worker::CompactionScheduler::new(
            cfg.data_dir.clone(),
            cfg.compaction,
            block_cache.clone(),
            next_sstable_id_atomic.clone(),
        );
        let flush_scheduler = crate::flush_worker::FlushScheduler::new();
        let manifest_sync_window = Arc::new(crate::apply_worker::ManifestSyncWindow::default());
        let apply_scheduler =
            crate::apply_worker::ApplyScheduler::new(manifest_file.clone(), manifest_sync_window);

        let mut engine = Engine {
            cfg,
            seq,
            active: Arc::new(active),
            immutable: VecDeque::new(),
            sstables,
            next_sstable_id,
            active_wal_id,
            active_wal: None,
            active_wal_size: 0,
            in_flight_wal_bytes: 0,
            sealed_segment_max_seq,
            wal_sync: crate::wal_sync::WalSync::new(max_seq_seen),
            group_commit: true,
            ship_dir: None,
            ship_mode: crate::shipping::ShipMode::Hardlink,
            ship_hmac_key: None,
            manifest_file,
            metrics: None,
            block_cache,
            reader_cache,
            result_cache,
            started: Instant::now(),
            clean_boot,
            policy: Arc::new(NoopWritePolicy),
            compaction_scheduler,
            flush_scheduler,
            apply_scheduler,
            next_sstable_id_atomic,
            pin_state: Arc::new(std::sync::Mutex::new(crate::owned_snapshot::PinState {
                pin_counts: BTreeMap::new(),
                live_snapshot_seqs: BTreeMap::new(),
                deferred_unlinks: Vec::new(),
            })),
            freeze_writes: false,
            dir_fsync_pending: false,
            apply_window_count: AtomicU64::new(0),
            apply_window_sum_ns: AtomicU64::new(0),
            apply_window_max_ns: AtomicU64::new(0),
            manifest_dirty: false,
            last_manifest_sync: Instant::now(),
            data_dir_lock,
            pending_write_slowdown: std::time::Duration::ZERO,
            fair,
        };
        engine.open_new_wal_segment()?;
        engine.refresh_topology_gauges();
        Ok(engine)
    }

    /// Shared block-cache handle (Phase 4: usable without the write mutex).
    pub fn block_cache_arc(&self) -> Arc<crate::block_cache::BlockCache> {
        Arc::clone(&self.block_cache)
    }

    pub fn with_metrics(mut self, metrics: Arc<crate::metrics::Metrics>) -> Self {
        if self.result_cache.is_some() {
            metrics.ensure_result_cache_registered();
        }
        metrics.last_shutdown_clean.set(self.clean_boot as i64);
        self.apply_scheduler.set_metrics(metrics.clone());
        self.wal_sync.set_metrics(Some(metrics.clone()));
        self.metrics = Some(metrics);
        self.refresh_topology_gauges();
        self
    }

    /// Shared WAL durability handle, so a commit coordinator can `fsync` the WAL
    /// without taking the engine mutex. See [`crate::wal_sync::WalSync`].
    pub fn wal_sync(&self) -> Arc<crate::wal_sync::WalSync> {
        Arc::clone(&self.wal_sync)
    }

    /// Toggle group commit. When false, every WAL append fsyncs inline (the
    /// pre-group-commit behavior). Defaults to true.
    pub fn with_group_commit(mut self, enabled: bool) -> Self {
        self.group_commit = enabled;
        self
    }

    /// Install a write-time policy (see [`crate::policy::WritePolicy`]). The
    /// policy is consulted around every user PUT/DEL. Defaults to a no-op.
    pub fn with_write_policy(mut self, policy: Arc<dyn WritePolicy>) -> Self {
        self.policy = policy;
        self
    }

    /// Enable WAL shipping: each sealed segment is hardlinked/copied into
    /// `ship_dir` for an operator sidecar to transport off-box. Empty path
    /// disables it.
    pub fn with_shipping(
        mut self,
        ship_dir: Option<PathBuf>,
        ship_mode: crate::shipping::ShipMode,
    ) -> Self {
        self.ship_dir = ship_dir;
        self.ship_mode = ship_mode;
        self
    }

    /// Set the HMAC key that authenticates each `shipped.log` entry. `None`
    /// keeps the legacy unauthenticated 3-field format (dev only).
    pub fn with_shipping_hmac_key(mut self, key: Option<Vec<u8>>) -> Self {
        self.ship_hmac_key = key;
        self
    }

    /// Whether group commit is enabled (writers buffer; a coordinator fsyncs).
    pub fn group_commit_enabled(&self) -> bool {
        self.group_commit
    }

    /// Whether this instance booted from a clean-shutdown marker.
    pub fn was_clean_shutdown(&self) -> bool {
        self.clean_boot
    }

    /// Drain flush/compaction workers and the apply queue (no new writes).
    pub fn drain_background_work(&mut self) -> EngineResult<()> {
        let _ = self.poll_flush()?;
        let _ = self.poll_compaction()?;
        self.drain_compaction()?;
        self.finish_pending_applies()?;
        Ok(())
    }

    /// Graceful shutdown: flush all in-memory data to durable SSTables (which also
    /// truncates the WAL via the manifest), then write a clean-shutdown marker so
    /// the next boot can skip WAL replay and verify a graceful stop. Idempotent.
    pub fn shutdown(&mut self) -> EngineResult<()> {
        self.force_flush()?;
        // Sync and ship the active segment so the sidecar has the final WAL bytes.
        self.sync_wal()?;
        self.ship_sealed_segment(self.active_wal_id);
        let marker_path = self.cfg.data_dir.join(CLEAN_SHUTDOWN_MARKER);
        let last_seq = self.seq.peek().saturating_sub(1);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&marker_path)?;
        f.write_all(&last_seq.to_be_bytes())?;
        f.sync_all()?;
        Self::fsync_dir(&self.cfg.data_dir)?;
        tracing::info!(last_seq, "clean shutdown complete; marker written");
        Ok(())
    }

    pub(crate) fn sstable_path(data_dir: &Path, id: u64) -> PathBuf {
        data_dir.join(format!("{:08}.sst", id))
    }

    /// Acquire an exclusive advisory lock on `data_dir/LOCK`. Returns the held
    /// file handle (drop releases the lock) or [`EngineError::Locked`] if another
    /// process holds it. Uses the stable `std::fs::File` lock API (Rust 1.89+).
    pub(crate) fn acquire_data_dir_lock(data_dir: &Path) -> EngineResult<File> {
        let lock_path = data_dir.join("LOCK");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)?;
        match file.try_lock() {
            Ok(()) => Ok(file),
            Err(std::fs::TryLockError::WouldBlock) => Err(EngineError::Locked(format!(
                "{} is already locked by another zydecodb process",
                data_dir.display()
            ))),
            Err(std::fs::TryLockError::Error(e)) => Err(EngineError::Io(e.to_string())),
        }
    }

    pub(crate) fn delete_orphan_sstables(
        data_dir: &Path,
        live_ids: &std::collections::HashSet<u64>,
    ) -> EngineResult<()> {
        for entry in std::fs::read_dir(data_dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(stripped) = name.strip_suffix(".sst") {
                if let Ok(id) = stripped.parse::<u64>() {
                    if !live_ids.contains(&id) {
                        tracing::info!(orphan = %name, "deleting orphan SSTable");
                        let _ = std::fs::remove_file(entry.path());
                    }
                }
            }
        }
        Ok(())
    }

    pub(crate) fn read_segment(
        path: &Path,
    ) -> EngineResult<(Vec<wal::WalEntry>, wal::ReplayOutcome)> {
        let mut f = File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        if buf.len() < wal::SEGMENT_HEADER_LEN {
            return Ok((Vec::new(), wal::ReplayOutcome::Clean));
        }
        // Header: [8] first_seq [1] format version. Reject unknown versions so a
        // v1 segment (with its old, wider record layout) is never misparsed as
        // v2.
        let version = buf[8];
        if version != wal::WAL_FORMAT_VERSION {
            return Err(EngineError::Io(format!(
                "WAL: unsupported segment format version 0x{:02x} (expected 0x{:02x})",
                version,
                wal::WAL_FORMAT_VERSION
            )));
        }
        let body = &buf[wal::SEGMENT_HEADER_LEN..];
        Ok(wal::replay_segment_body(body))
    }

    pub(crate) fn truncate_torn_segment(path: &Path) -> EngineResult<()> {
        // Re-read, find the valid prefix length, truncate the file to it.
        let mut f = File::open(path)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        if buf.len() < wal::SEGMENT_HEADER_LEN {
            return Ok(());
        }
        let body = &buf[wal::SEGMENT_HEADER_LEN..];
        let mut offset = 0;
        while let Ok(Some((_, consumed))) = WalRecord::decode_one(&body[offset..]) {
            offset += consumed;
        }
        let valid_len = wal::SEGMENT_HEADER_LEN + offset;
        let f = OpenOptions::new().write(true).open(path)?;
        f.set_len(valid_len as u64)?;
        f.sync_all()?;
        Ok(())
    }

    pub(crate) fn open_new_wal_segment(&mut self) -> EngineResult<()> {
        crate::engine_fail_point!(crate::failpoints::WAL_BEFORE_SEGMENT_ROLL);
        let first_seq = self.seq.peek();
        let name = wal::segment_filename(self.active_wal_id);
        let path = self.cfg.wal_dir.join(name);
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)?;
        f.write_all(&first_seq.to_be_bytes())?;
        f.write_all(&[wal::WAL_FORMAT_VERSION])?;
        f.sync_all()?;
        crate::engine_fail_point!(crate::failpoints::WAL_AFTER_SEGMENT_ROLL);
        // Share the fd with the durability state so the coordinator can fsync it
        // off the engine lock. Publishing here (after the prior segment was synced
        // in the rotation path) maintains the WalSync invariant.
        let f = Arc::new(f);
        self.wal_sync.set_active(Arc::clone(&f));
        self.active_wal = Some(f);
        self.active_wal_size = wal::SEGMENT_HEADER_LEN;
        tracing::info!(segment = %path.display(), first_seq, "opened new WAL segment");
        Ok(())
    }

    /// Append a record to the active WAL segment and fsync immediately. Used on
    /// the per-record durability path (group commit disabled) and for internal
    /// system writes that want their own durability point.
    /// Ship a freshly-sealed WAL segment off-box (hardlink/copy into `ship_dir`)
    /// and record it in `shipped.log`. Best-effort: a shipping failure is logged
    /// but does not fail the write path — the live WAL remains the source of
    /// truth and the sidecar can reconcile from `shipped.log`. No-op when
    /// shipping is disabled.
    pub(crate) fn ship_sealed_segment(&self, segment_id: u64) {
        let Some(ship_dir) = &self.ship_dir else {
            return;
        };
        let name = wal::segment_filename(segment_id);
        let src = self.cfg.wal_dir.join(&name);
        if let Err(e) = crate::shipping::ship_segment(
            &src,
            ship_dir,
            segment_id,
            self.wal_sync.synced_seq(),
            self.ship_mode,
            self.ship_hmac_key.as_deref(),
        ) {
            tracing::error!(error = %e, segment = segment_id, "WAL shipping failed");
        } else {
            tracing::info!(segment = segment_id, "shipped sealed WAL segment");
        }
    }

    pub(crate) fn append_wal(&mut self, rec: &WalRecord) -> EngineResult<()> {
        self.append_wal_buffered(rec)?;
        self.sync_wal()?;
        Ok(())
    }

    /// Append a record's bytes to the active WAL segment WITHOUT fsync. The bytes
    /// reach the OS page cache; durability is established later by [`sync_wal`].
    /// This is the write half of group commit: many records are buffered, then a
    /// single fsync makes the whole batch durable.
    pub(crate) fn append_wal_buffered(&mut self, rec: &WalRecord) -> EngineResult<()> {
        let bytes = rec.encode();
        self.append_bytes_buffered(&bytes, rec.seq)
    }

    /// Append pre-encoded WAL `bytes` carrying sequence `seq` to the active
    /// segment WITHOUT fsync, rolling the segment first if it would overflow.
    /// Shared by single-record appends and by atomic batch records (one
    /// self-framed record, one CRC, one seq).
    pub(crate) fn append_bytes_buffered(&mut self, bytes: &[u8], seq: u64) -> EngineResult<()> {
        if wal::should_roll(self.active_wal_size, bytes.len()) {
            // A new segment starts fully durable (its header is fsynced in
            // open_new_wal_segment), so the prior segment's tail must be synced
            // first to preserve ordering of the durability watermark. After the
            // sync the now-sealed segment is shipped off-box (if enabled).
            self.sync_wal()?;
            let sealed_id = self.active_wal_id;
            // The just-sealed segment's max seq is exactly the buffered watermark:
            // it's the highest seq appended, and the segment is non-empty
            // (should_roll requires current_size > 0). Cache it so subsequent
            // flushes can decide WAL coverage without re-reading the file.
            self.sealed_segment_max_seq
                .insert(sealed_id, self.wal_sync.buffered_seq());
            self.ship_sealed_segment(sealed_id);
            self.active_wal_id += 1;
            self.open_new_wal_segment()?;
            self.refresh_topology_gauges();
        }
        // The engine lock serializes writers, so a shared `&File` is safe here;
        // only the (concurrent) coordinator reads this fd to fsync, which is
        // independent of the append.
        let arc = self
            .active_wal
            .as_ref()
            .ok_or_else(|| EngineError::Io("no active WAL segment".into()))?;
        let mut f: &File = arc;
        crate::engine_fail_point!(crate::failpoints::WAL_BEFORE_APPEND);
        f.write_all(bytes)?;
        crate::engine_fail_point!(crate::failpoints::WAL_AFTER_APPEND);
        if let Some(m) = &self.metrics {
            m.wal_bytes_written_total.inc_by(bytes.len() as u64);
        }
        self.active_wal_size += bytes.len();
        self.in_flight_wal_bytes += bytes.len();
        // Publish the new buffered watermark AFTER the write_all above; the
        // Release in `advance_buffered` makes the bytes visible to a syncer that
        // observes this seq.
        self.wal_sync.advance_buffered(seq);
        Ok(())
    }
}
