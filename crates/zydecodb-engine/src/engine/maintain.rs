use super::*;

impl Engine {
    /// Force flush of active + immutable memtables, then drain compaction.
    pub fn flush(&mut self) -> EngineResult<()> {
        self.force_flush()
    }

    /// Run one compaction job synchronously if the planner finds work.
    pub fn compact_range(&mut self) -> EngineResult<bool> {
        self.compact_once()
    }

    /// Replay a sealed WAL segment already present in `wal_dir` into the live
    /// memtable without a full [`Engine::open`]. Used by replica catch-up.
    ///
    /// Flushes current memtables first so only seqs not yet in SSTables are
    /// applied. Reloading the document catalog is the caller's responsibility.
    pub fn apply_installed_wal_segment(&mut self, segment_id: u64) -> EngineResult<usize> {
        self.force_flush()?;
        self.drain_background_work()?;

        let path = self.cfg.wal_dir.join(wal::segment_filename(segment_id));
        if !path.exists() {
            return Err(EngineError::Io(format!(
                "WAL segment {} missing at {}",
                segment_id,
                path.display()
            )));
        }
        let (records, outcome) = Self::read_segment(&path)?;
        match outcome {
            wal::ReplayOutcome::Clean => {}
            wal::ReplayOutcome::TornTail | wal::ReplayOutcome::Corruption => {
                return Err(EngineError::Io(format!(
                    "WAL: sealed segment {} is damaged; refusing catch-up apply",
                    segment_id
                )));
            }
        }

        let sstable_max_seq = self
            .sstables
            .iter()
            .map(|s| s.meta.max_seq)
            .max()
            .unwrap_or(0);
        let seg_max = records.iter().map(|r| r.seq()).max().unwrap_or(0);
        if seg_max > 0 {
            self.sealed_segment_max_seq.insert(segment_id, seg_max);
        }

        let mut applied = 0usize;
        let mut max_seq_seen = self.current_seq();
        for rec in records {
            let rec_seq = rec.seq();
            if rec_seq <= sstable_max_seq {
                continue;
            }
            max_seq_seen = max_seq_seen.max(rec_seq);
            for (k, e) in rec.into_memtable_pairs() {
                self.active_mut().insert(k, e);
                applied += 1;
            }
        }
        self.seq.bump_to_at_least(max_seq_seen.saturating_add(1));
        self.wal_sync.set_watermarks(max_seq_seen);
        self.maybe_freeze();
        self.try_submit_flush();
        self.maybe_submit_compaction();
        self.update_gauges();
        Ok(applied)
    }

    /// Force a flush of the active memtable (used by tests and shutdown).
    pub fn force_flush(&mut self) -> EngineResult<()> {
        if self.active.is_empty() && self.immutable.is_empty() {
            return Ok(());
        }
        if !self.active.is_empty() {
            let frozen = std::mem::replace(&mut self.active, Arc::new(Memtable::new()));
            self.immutable.push_back(frozen);
        }
        self.drain_flush()?;
        self.drain_compaction()?;
        self.finish_pending_applies()?;
        self.maybe_sync_data_dir()?;
        Ok(())
    }

    pub(crate) fn drain_flush(&mut self) -> EngineResult<()> {
        loop {
            self.poll_flush()?;
            if let Some(err) = self.flush_scheduler.take_worker_failure() {
                if err.contains("injected failpoint") {
                    return Err(EngineError::Io(err));
                }
                tracing::warn!(error = %err, "flush worker error; continuing drain");
            }
            if self.flush_scheduler.is_worker_busy() {
                std::thread::sleep(std::time::Duration::from_millis(1));
                continue;
            }
            if !self.immutable.is_empty() && self.try_submit_flush() {
                continue;
            }
            if self.immutable.is_empty() {
                self.finish_pending_applies()?;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        Ok(())
    }

    pub fn poll_flush(&mut self) -> EngineResult<usize> {
        let results = self.flush_scheduler.poll_results();
        let mut applied = 0usize;
        for result in results {
            match result {
                Ok(r) => {
                    self.submit_flush_apply(r)?;
                    applied += 1;
                }
                Err(e) => {
                    tracing::error!(error = %e, "background flush failed");
                    self.flush_scheduler.note_worker_failed(e);
                }
            }
        }
        self.try_submit_flush();
        applied += self.drain_catalog_apply()?;
        Ok(applied)
    }

    /// Signal the compaction worker that work may be available. Non-blocking.
    pub fn request_compaction(&self) {
        self.compaction_scheduler.request_compaction();
    }

    /// Apply completed background compaction jobs. Returns the number of jobs
    /// applied. Call from idle loops, after flush, or in soak harnesses.
    pub fn poll_compaction(&mut self) -> EngineResult<usize> {
        let flushed = self.poll_flush()?;
        let results = self.compaction_scheduler.poll_results();
        let mut applied = 0usize;
        let mut worker_failed = false;
        for result in results {
            match result {
                Ok(r) => {
                    self.compaction_scheduler.clear_staged();
                    self.submit_compaction_apply(r)?;
                    applied += 1;
                }
                Err(e) => {
                    tracing::error!(error = %e, "background compaction failed");
                    self.compaction_scheduler.clear_staged();
                    self.compaction_scheduler.note_worker_failed(e);
                    worker_failed = true;
                }
            }
        }
        applied += self.drain_catalog_apply()?;
        self.finish_pending_applies()?;
        // Don't resubmit a compaction in the same pass that just recorded a
        // worker failure: `submit_work` clears the `worker_failed` flag, which
        // would erase the failure before `drain_compaction` (the only consumer)
        // can observe it. With a deterministic failure that never advances the
        // catalog -- e.g. an injected rename failpoint -- the same job is
        // immediately replannable, so the resubmit-then-clear loop spins
        // forever and `drain_compaction` never returns. Leaving the flag set
        // lets the failure surface; a later poll resumes normal background
        // retry once the flag is consumed or the catalog changes.
        if !worker_failed {
            self.maybe_submit_compaction();
        }
        self.try_submit_flush();
        self.update_compaction_gauges();
        self.refresh_topology_gauges();
        self.maybe_sync_data_dir()?;
        Ok(flushed + applied)
    }

    /// Block until pending compaction work is drained. Used by tests, shutdown,
    /// and `force_flush`.
    pub fn drain_compaction(&mut self) -> EngineResult<()> {
        self.request_compaction();
        loop {
            self.poll_compaction()?;
            if let Some(err) = self.compaction_scheduler.take_worker_failure() {
                if err.contains("injected failpoint") {
                    return Err(EngineError::Io(err));
                }
                // Worker panic (e.g. failpoint "panic" mode) — stop retrying in this
                // drain; the catalog is unchanged and a later drain can replan.
                tracing::warn!(error = %err, "compaction worker error; aborting drain");
                break;
            }
            if !self.compaction_scheduler.is_worker_busy() {
                if self.plan_compaction_job().is_none()
                    && !self.compaction_scheduler.compaction_needed()
                {
                    break;
                }
                if self.maybe_submit_compaction() {
                    continue;
                }
                if self.plan_compaction_job().is_none() {
                    break;
                }
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
        self.poll_compaction()?;
        self.finish_pending_applies()?;
        Ok(())
    }

    pub(crate) fn compaction_gc_watermark(&self) -> u64 {
        let ps = self.pin_state.lock().expect("pin state lock");
        ps.live_snapshot_seqs
            .keys()
            .next()
            .copied()
            .unwrap_or(self.wal_sync.synced_seq())
    }

    pub(crate) fn compaction_allow_tombstone_drop(&self) -> bool {
        let ps = self.pin_state.lock().expect("pin state lock");
        ps.live_snapshot_seqs.is_empty()
    }

    pub(crate) fn compaction_drop_tombstones(
        &self,
        job: &crate::compaction::CompactionJob,
    ) -> bool {
        job.output_level == self.cfg.compaction.max_level && self.compaction_allow_tombstone_drop()
    }

    pub(crate) fn maybe_submit_compaction(&mut self) -> bool {
        if self.compaction_scheduler.try_submit_staged() {
            return true;
        }
        let Some((job, input_metas)) = self.plan_compaction_job() else {
            if !self.compaction_scheduler.is_worker_busy() {
                self.compaction_scheduler.clear_compaction_needed();
            }
            return false;
        };
        let drop_tombstones = self.compaction_drop_tombstones(&job);
        self.compaction_scheduler.try_submit(
            job,
            input_metas,
            self.cfg.data_dir.clone(),
            self.cfg.compaction,
            self.block_cache.clone(),
            self.reader_cache.clone(),
            self.next_sstable_id_atomic.clone(),
            self.compaction_gc_watermark(),
            drop_tombstones,
        )
    }

    pub(crate) fn plan_compaction_job(
        &self,
    ) -> Option<(crate::compaction::CompactionJob, Vec<SstableMeta>)> {
        let metas: Vec<SstableMeta> = self.sstables.iter().map(|s| s.meta.clone()).collect();
        let planner = crate::compaction::CompactionPlanner::new(&metas, &self.cfg.compaction);
        let job = planner.plan()?;
        let input_id_set: std::collections::HashSet<u64> = job.inputs.iter().copied().collect();
        let input_metas: Vec<SstableMeta> = metas
            .into_iter()
            .filter(|m| input_id_set.contains(&m.id))
            .collect();
        if input_metas.len() != job.inputs.len() {
            return None;
        }
        let is_l2_gc =
            job.input_level == job.output_level && job.input_level == self.cfg.compaction.max_level;
        if job.input_level == job.output_level
            && !is_l2_gc
            && !crate::compaction::same_level_compaction_would_make_progress(
                &input_metas,
                self.cfg.compaction.target_file_bytes,
            )
        {
            tracing::warn!(
                input_level = job.input_level,
                inputs = job.inputs.len(),
                "rejecting same-level compaction that cannot reduce file count"
            );
            if let Some(m) = &self.metrics {
                m.compaction_rejected_no_progress.inc();
            }
            return None;
        }
        Some((job, input_metas))
    }

    pub(crate) fn submit_compaction_apply(
        &mut self,
        result: crate::compaction_worker::CompactionExecuteResult,
    ) -> EngineResult<()> {
        let crate::compaction_worker::CompactionExecuteResult {
            job,
            new_metas,
            input_metas,
            bytes_read,
            bytes_written,
            versions_dropped,
            tombstones_dropped,
            worker_elapsed,
        } = result;

        let mut manifest_records: Vec<ManifestRecord> =
            Vec::with_capacity(new_metas.len() + input_metas.len());
        for m in &new_metas {
            manifest_records.push(ManifestRecord::SstableAdd(m.clone()));
        }
        for m in &input_metas {
            manifest_records.push(ManifestRecord::SstableRemove { id: m.id });
        }
        self.apply_scheduler
            .submit(crate::apply_worker::ReadyCatalogApply {
                manifest_records,
                add_metas: new_metas,
                remove_sstable_ids: input_metas.iter().map(|m| m.id).collect(),
                unlink_wal_up_to: None,
                flush_max_seq: None,
                compaction: Some(crate::apply_worker::CompactionApplyMetrics {
                    job,
                    bytes_read,
                    bytes_written,
                    versions_dropped,
                    tombstones_dropped,
                    worker_elapsed,
                }),
            })
    }

    pub(crate) fn drain_catalog_apply(&mut self) -> EngineResult<usize> {
        let ready = self.apply_scheduler.drain_ready();
        let mut count = 0usize;
        for apply in ready {
            self.publish_catalog_apply(apply)?;
            count += 1;
        }
        Ok(count)
    }

    pub(crate) fn finish_pending_applies(&mut self) -> EngineResult<()> {
        let deadline = Instant::now() + std::time::Duration::from_secs(30);
        loop {
            if self.apply_scheduler.take_failed() {
                return Err(EngineError::Io("background apply failed".into()));
            }
            self.drain_catalog_apply()?;
            if self.apply_scheduler.pending_applies() == 0
                && self.apply_scheduler.ready_count() == 0
            {
                if self.apply_scheduler.take_failed() {
                    return Err(EngineError::Io("background apply failed".into()));
                }
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(EngineError::Io("apply worker drain timeout".into()));
            }
            std::thread::sleep(std::time::Duration::from_millis(1));
        }
    }

    pub(crate) fn publish_catalog_apply(
        &mut self,
        apply: crate::apply_worker::ReadyCatalogApply,
    ) -> EngineResult<()> {
        let apply_start = Instant::now();

        let remove_set: std::collections::HashSet<u64> =
            apply.remove_sstable_ids.iter().copied().collect();
        // Credit L0 tokens for compacted-away L0 inputs (Phase 5b).
        let removed_l0: Vec<_> = self
            .sstables
            .iter()
            .filter(|s| remove_set.contains(&s.meta.id) && s.meta.level == 0)
            .map(|s| s.meta.clone())
            .collect();
        for m in &removed_l0 {
            if let Some(t) = crate::tenant_fair::tenant_from_user_key(&m.min_key) {
                self.fair.note_l0_remove(t, 1, m.size_bytes);
            }
        }
        self.sstables.retain(|s| !remove_set.contains(&s.meta.id));
        for m in &apply.add_metas {
            let path = Self::sstable_path(&self.cfg.data_dir, m.id);
            let reader = self
                .reader_cache
                .get_or_open(&path, m.id, self.block_cache.clone())?;
            self.sstables.push(LoadedSstable {
                meta: m.clone(),
                reader,
            });
        }
        self.sstables.sort_by_key(|s| s.meta.id);

        self.next_sstable_id = self
            .next_sstable_id
            .max(self.next_sstable_id_atomic.load(Ordering::Acquire));
        for m in &apply.add_metas {
            self.next_sstable_id = self.next_sstable_id.max(m.id + 1);
        }
        self.next_sstable_id_atomic
            .store(self.next_sstable_id, Ordering::Release);

        crate::failpoints::failpoint_result(crate::failpoints::APPLY_AFTER_PUBLISH_BEFORE_UNLINK)?;
        if apply.compaction.is_some() {
            crate::failpoints::failpoint_result(
                crate::failpoints::COMPACTION_AFTER_MANIFEST_BEFORE_UNLINK,
            )?;
        }

        for id in &apply.remove_sstable_ids {
            self.try_unlink_sstable(*id);
        }
        if let Some(up_to) = apply.unlink_wal_up_to {
            self.unlink_wal_segments_up_to(up_to)?;
        }
        self.dir_fsync_pending = true;

        if let Some(max_seq) = apply.flush_max_seq {
            if let Some(m) = &self.metrics {
                m.sstable_flushes_total.inc();
                m.last_durable_seq.set(max_seq as i64);
            }
            self.in_flight_wal_bytes = 0;
            self.wal_sync.set_watermarks(max_seq);
            tracing::info!(
                sstable_id = apply.add_metas.first().map(|m| m.id),
                max_seq,
                "flush applied (background)"
            );
        }

        if let Some(c) = apply.compaction {
            if let Some(metrics) = &self.metrics {
                metrics.compaction_jobs_total.inc();
                metrics
                    .compaction_jobs_by_input_level
                    .with_label_values(&[&c.job.input_level.to_string()])
                    .inc();
                metrics.compaction_bytes_read_total.inc_by(c.bytes_read);
                metrics
                    .compaction_bytes_written_total
                    .inc_by(c.bytes_written);
                metrics
                    .compaction_versions_dropped_total
                    .inc_by(c.versions_dropped);
                metrics
                    .compaction_tombstones_dropped_total
                    .inc_by(c.tombstones_dropped);
                metrics
                    .compaction_duration_seconds
                    .observe(c.worker_elapsed.as_secs_f64());
                metrics
                    .compaction_apply_duration_seconds
                    .observe(apply_start.elapsed().as_secs_f64());
            }
            tracing::info!(
                inputs = apply.remove_sstable_ids.len(),
                outputs = apply.add_metas.len(),
                bytes_read = c.bytes_read,
                bytes_written = c.bytes_written,
                elapsed_ms = c.worker_elapsed.as_millis() as u64,
                "compaction completed (background apply)"
            );
        }

        self.record_apply_duration(apply_start.elapsed());
        self.process_deferred_unlinks()?;
        self.refresh_topology_gauges();
        if apply.flush_max_seq.is_some() {
            self.request_compaction();
            self.maybe_submit_compaction();
        }
        Ok(())
    }

    pub(crate) fn try_unlink_sstable(&mut self, id: u64) {
        let mut ps = self.pin_state.lock().expect("pin state lock");
        if ps.pin_counts.get(&id).copied().unwrap_or(0) > 0 {
            if !ps.deferred_unlinks.contains(&id) {
                ps.deferred_unlinks.push(id);
            }
            return;
        }
        drop(ps);
        let path = Self::sstable_path(&self.cfg.data_dir, id);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(error = %e, path = %path.display(), "compaction: unlink input failed");
        }
        self.block_cache.invalidate_sstable(id);
        self.reader_cache.remove(id);
    }

    pub(crate) fn process_deferred_unlinks(&mut self) -> EngineResult<()> {
        let pending: Vec<u64> = {
            let ps = self.pin_state.lock().expect("pin state lock");
            ps.deferred_unlinks.clone()
        };
        for id in pending {
            self.try_unlink_sstable(id);
        }
        Ok(())
    }

    pub(crate) fn update_compaction_gauges(&self) {
        if let Some(m) = &self.metrics {
            let depth = self.compaction_scheduler.queue_depth()
                + if self.compaction_scheduler.compaction_needed() {
                    1
                } else {
                    0
                };
            m.compaction_queue_depth.set(depth as i64);
            m.compaction_worker_busy
                .set(if self.compaction_scheduler.is_worker_busy() {
                    1
                } else {
                    0
                });
        }
    }

    /// Execute at most one compaction job synchronously (tests / explicit compact).
    /// Prefer background compaction via [`Engine::poll_compaction`].
    pub fn compact_once(&mut self) -> EngineResult<bool> {
        if self.compaction_scheduler.is_worker_busy() {
            self.poll_compaction()?;
            return Ok(false);
        }
        let Some((job, input_metas)) = self.plan_compaction_job() else {
            return Ok(false);
        };
        let drop_tombstones = self.compaction_drop_tombstones(&job);
        let result = crate::compaction_worker::execute_compaction(
            &job,
            &input_metas,
            &self.cfg.data_dir,
            &self.cfg.compaction,
            &self.block_cache,
            &self.reader_cache,
            &self.next_sstable_id_atomic,
            self.compaction_gc_watermark(),
            drop_tombstones,
        )?;
        self.submit_compaction_apply(result)?;
        self.finish_pending_applies()?;
        self.maybe_sync_data_dir()?;
        Ok(true)
    }

    /// Determine the highest WAL segment id fully covered by `max_seq` (all of
    /// its entries have seq <= max_seq). Sealed segments before the active one
    /// whose contents are entirely <= max_seq are eligible.
    ///
    /// Uses the in-memory `sealed_segment_max_seq` cache populated at seal time
    /// (and rebuilt on open from the replay scan). This is O(sealed segments)
    /// map lookups — no disk I/O — replacing what used to be a full read +
    /// decode of every sealed segment on every flush.
    pub(crate) fn wal_segments_covered(&self, max_seq: u64) -> EngineResult<u64> {
        let mut covered = 0u64;
        for (&id, &seg_max) in &self.sealed_segment_max_seq {
            if id >= self.active_wal_id {
                break; // BTreeMap iterates in id order; rest are >= active
            }
            if seg_max <= max_seq {
                covered = covered.max(id);
            }
        }
        Ok(covered)
    }

    pub(crate) fn unlink_wal_segments_up_to(&mut self, up_to_id: u64) -> EngineResult<()> {
        let segments = wal::list_segments(&self.cfg.wal_dir)?;
        for (id, path) in segments {
            if id <= up_to_id && id < self.active_wal_id {
                tracing::info!(segment = %path.display(), "unlinking covered WAL segment");
                let _ = std::fs::remove_file(&path);
                self.sealed_segment_max_seq.remove(&id);
            }
        }
        Ok(())
    }

    /// Fsync the manifest if dirty. Used by group-commit debouncing and forced
    /// durability points (shutdown, `force_flush`).
    pub(crate) fn sync_manifest(&mut self) -> EngineResult<()> {
        if !self.manifest_dirty {
            return Ok(());
        }
        let start = Instant::now();
        self.manifest_file
            .lock()
            .expect("manifest lock")
            .sync_all()?;
        crate::engine_fail_point!(crate::failpoints::MANIFEST_AFTER_FSYNC);
        self.manifest_dirty = false;
        self.last_manifest_sync = Instant::now();
        let elapsed = start.elapsed();
        if let Some(m) = &self.metrics {
            m.manifest_syncs_total.inc();
            m.manifest_sync_duration_seconds
                .observe(elapsed.as_secs_f64());
        }
        self.record_manifest_sync_duration(elapsed);
        Ok(())
    }

    pub(crate) fn maybe_sync_data_dir(&mut self) -> EngineResult<()> {
        if self.dir_fsync_pending {
            Self::fsync_dir(&self.cfg.data_dir)?;
            self.dir_fsync_pending = false;
        }
        Ok(())
    }

    pub(crate) fn fsync_dir(dir: &Path) -> EngineResult<()> {
        let f = File::open(dir)?;
        f.sync_all()?;
        Ok(())
    }

    /// Test-only: force the active WAL segment to seal as if it had hit the
    /// size threshold. Mirrors the roll path in `append_wal_buffered` so the
    /// `sealed_segment_max_seq` cache and ship-out semantics get exercised
    /// without needing to actually write `WAL_SEGMENT_SIZE` bytes.
    pub fn force_roll_wal_for_test(&mut self) -> EngineResult<()> {
        if self.active_wal_size == 0 {
            return Ok(()); // mirror should_roll: empty segments don't roll
        }
        self.sync_wal()?;
        let sealed_id = self.active_wal_id;
        self.sealed_segment_max_seq
            .insert(sealed_id, self.wal_sync.buffered_seq());
        self.ship_sealed_segment(sealed_id);
        self.active_wal_id += 1;
        self.open_new_wal_segment()
    }
}
