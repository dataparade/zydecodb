use super::*;

impl Engine {
    /// Sum of live SSTable bytes on disk (approximate disk usage for data files).
    pub fn estimate_disk_bytes(&self) -> u64 {
        self.sstables.iter().map(|s| s.meta.size_bytes).sum()
    }

    /// Sum of live user-key bytes (latest non-tombstone, non-expired values).
    pub fn estimate_logical_live_bytes(&self) -> EngineResult<u64> {
        use crate::keys::KS_USER;
        let lo = vec![KS_USER];
        let hi = vec![KS_USER + 1];
        let mut total = 0u64;
        let iter = self.scan(lo, hi)?;
        for row in iter {
            let (k, v) = row?;
            total = total.saturating_add(k.len() as u64 + v.len() as u64);
        }
        Ok(total.max(1))
    }

    /// Physical SSTable bytes divided by logical live bytes.
    pub fn space_amplification(&self) -> EngineResult<f64> {
        let physical = self.estimate_disk_bytes();
        let logical = self.estimate_logical_live_bytes()?;
        Ok(physical as f64 / logical.max(1) as f64)
    }

    /// Refresh disk / space-amplification gauges (full keyspace scan).
    pub fn refresh_space_metrics(&self) {
        if let Some(m) = &self.metrics {
            let disk = self.estimate_disk_bytes();
            m.disk_bytes_total.set(disk as i64);
            if let Ok(logical) = self.estimate_logical_live_bytes() {
                m.logical_live_bytes.set(logical as i64);
                m.space_amplification
                    .set(disk as f64 / logical.max(1) as f64);
            }
            if let Some(rc) = &self.result_cache {
                m.result_cache_resident_bytes
                    .set(rc.resident_bytes() as i64);
                m.result_cache_evictions_total.inc_by(
                    rc.evictions()
                        .saturating_sub(m.result_cache_evictions_total.get()),
                );
            }
        }
    }

    /// Per-level SSTable byte totals.
    pub fn disk_bytes_by_level(&self) -> Vec<(u8, u64)> {
        let mut by: BTreeMap<u8, u64> = BTreeMap::new();
        for s in &self.sstables {
            *by.entry(s.meta.level).or_insert(0) += s.meta.size_bytes;
        }
        by.into_iter().collect()
    }

    pub(crate) fn estimate_pending_compaction_bytes(&self) -> u64 {
        let metas: Vec<SstableMeta> = self.sstables.iter().map(|s| s.meta.clone()).collect();
        crate::compaction::CompactionPlanner::new(&metas, &self.cfg.compaction)
            .estimate_pending_bytes()
    }

    /// Manifest fsync timing accumulated since the last drain (count, sum_ns, max_ns).
    pub fn drain_manifest_sync_window_stats(&self) -> (u64, u64, u64) {
        self.apply_scheduler.manifest_sync_window().drain()
    }

    pub(crate) fn record_manifest_sync_duration(&self, elapsed: std::time::Duration) {
        self.apply_scheduler.manifest_sync_window().record(elapsed);
    }

    /// Apply timing accumulated since the last drain (count, sum_ns, max_ns).
    pub fn drain_apply_window_stats(&self) -> (u64, u64, u64) {
        (
            self.apply_window_count.swap(0, Ordering::Relaxed),
            self.apply_window_sum_ns.swap(0, Ordering::Relaxed),
            self.apply_window_max_ns.swap(0, Ordering::Relaxed),
        )
    }

    pub(crate) fn record_apply_duration(&self, elapsed: std::time::Duration) {
        let ns = elapsed.as_nanos() as u64;
        self.apply_window_count.fetch_add(1, Ordering::Relaxed);
        self.apply_window_sum_ns.fetch_add(ns, Ordering::Relaxed);
        let mut cur = self.apply_window_max_ns.load(Ordering::Relaxed);
        while ns > cur {
            match self.apply_window_max_ns.compare_exchange_weak(
                cur,
                ns,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    /// L2 SSTable size stats for soak / fragmentation checks.
    pub fn l2_file_size_stats(&self) -> (u64, u64, u64) {
        let mut sizes: Vec<u64> = self
            .sstables
            .iter()
            .filter(|s| s.meta.level == self.cfg.compaction.max_level)
            .map(|s| s.meta.size_bytes)
            .collect();
        if sizes.is_empty() {
            return (0, 0, 0);
        }
        sizes.sort_unstable();
        let min = sizes[0];
        let max = *sizes.last().unwrap();
        let median = sizes[sizes.len() / 2];
        (min, median, max)
    }

    pub fn stats(&self) -> Stats {
        let sstables = self
            .sstables
            .iter()
            .map(|s| SstableStat {
                id: s.meta.id,
                size_bytes: s.meta.size_bytes,
                min_seq: s.meta.min_seq,
                max_seq: s.meta.max_seq,
                entries: 0,
            })
            .collect();
        let wal_segments = wal::list_segments(&self.cfg.wal_dir)
            .unwrap_or_default()
            .into_iter()
            .map(|(id, path)| {
                let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
                WalSegmentStat {
                    id,
                    first_seq: 0,
                    max_seq: 0,
                    size_bytes: size,
                    active: id == self.active_wal_id,
                }
            })
            .collect();
        Stats {
            uptime_s: self.started.elapsed().as_secs(),
            last_durable_seq: self.seq.peek().saturating_sub(1),
            memtable_bytes: self.active.size_bytes() as u64,
            memtable_entries: self.active.len() as u64,
            immutable_memtables: self.immutable.len() as u64,
            sstables,
            wal_segments,
        }
    }

    pub(crate) fn update_gauges(&self) {
        if let Some(m) = &self.metrics {
            m.memtable_size_bytes.set(self.active.size_bytes() as i64);
            m.immutable_memtable_count.set(self.immutable.len() as i64);
            m.live_sstable_count.set(self.sstables.len() as i64);
            m.last_durable_seq
                .set(self.seq.peek().saturating_sub(1) as i64);

            // Per-level live SSTable counts. Reset all observed levels each
            // time so that a level transitioning to zero is reflected.
            let mut by_level: std::collections::HashMap<u8, i64> = std::collections::HashMap::new();
            for s in &self.sstables {
                *by_level.entry(s.meta.level).or_insert(0) += 1;
            }
            // Cover levels 0..=max_level so previously-populated levels
            // show 0 instead of stale values when they drain.
            for lvl in 0..=self.cfg.compaction.max_level {
                let v = by_level.get(&lvl).copied().unwrap_or(0);
                m.live_sstables_by_level
                    .with_label_values(&[&lvl.to_string()])
                    .set(v);
            }

            // Block-cache snapshot.
            let bc = self.block_cache.stats();
            m.block_cache_hits_total
                .inc_by(bc.hits.saturating_sub(m.block_cache_hits_total.get()));
            m.block_cache_misses_total
                .inc_by(bc.misses.saturating_sub(m.block_cache_misses_total.get()));
            m.block_cache_compaction_reads_total.inc_by(
                bc.compaction_reads
                    .saturating_sub(m.block_cache_compaction_reads_total.get()),
            );
            m.block_cache_evictions_total.inc_by(
                bc.evictions
                    .saturating_sub(m.block_cache_evictions_total.get()),
            );
            m.block_cache_resident_bytes.set(bc.resident_bytes as i64);
            m.block_cache_resident_entries
                .set(bc.resident_entries as i64);
            m.disk_bytes_total.set(self.estimate_disk_bytes() as i64);
            if let Some(rc) = &self.result_cache {
                m.result_cache_resident_bytes
                    .set(rc.resident_bytes() as i64);
                m.result_cache_evictions_total.inc_by(
                    rc.evictions()
                        .saturating_sub(m.result_cache_evictions_total.get()),
                );
            }
            let seg_count = wal::list_segments(&self.cfg.wal_dir)
                .map(|v| v.len())
                .unwrap_or(0);
            m.wal_segment_count.set(seg_count as i64);
            // RPO surface: bytes in the still-active segment not yet shipped.
            let unshipped = if self.ship_dir.is_some() {
                self.active_wal_size as i64
            } else {
                0
            };
            m.wal_unshipped_bytes.set(unshipped);
            m.pending_compaction_bytes
                .set(self.estimate_pending_compaction_bytes() as i64);
        }
    }

    /// The highest write sequence assigned so far (0 if none). Cheap (no
    /// snapshot build); used by the shipping heartbeat to report the primary's
    /// current position.
    pub fn current_seq(&self) -> u64 {
        self.seq.peek().saturating_sub(1)
    }

    #[cfg(test)]
    pub fn immutable_len(&self) -> usize {
        self.immutable.len()
    }

    #[cfg(test)]
    pub fn sstable_count(&self) -> usize {
        self.sstables.len()
    }

    #[cfg(test)]
    pub fn seq_peek(&self) -> u64 {
        self.seq.peek()
    }

    pub fn sealed_segment_max_seq_snapshot(&self) -> Vec<(u64, u64)> {
        self.sealed_segment_max_seq
            .iter()
            .map(|(&a, &b)| (a, b))
            .collect()
    }
}
