use super::*;

impl Engine {
    /// Fsync the active WAL segment, making all buffered appends durable. Returns
    /// the highest sequence number now guaranteed on disk. No-op when there is
    /// nothing new to sync. This is the single shared fsync of group commit.
    pub fn sync_wal(&mut self) -> EngineResult<u64> {
        // Delegates to the decoupled durability state. `WalSync::sync` releases
        // its fd lock before the actual `fsync(2)`, so even when the engine lock
        // is held by an internal caller the slow I/O is the same single shared
        // group-commit fsync -- the win is that the *commit coordinator* path no
        // longer needs the engine mutex at all.
        self.wal_sync.sync()
    }

    /// Highest seq buffered into the WAL (durable or not).
    pub fn last_buffered_seq(&self) -> u64 {
        self.wal_sync.buffered_seq()
    }

    pub(crate) fn now_ms() -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// PUT a key/value with optional expiry. Validates limits, consults the
    /// write policy, writes WAL, then memtable. Returns the assigned sequence
    /// number so the caller can await its durability via [`sync_wal`] (group
    /// commit). When group commit is disabled the WAL is already fsynced on
    /// return.
    ///
    /// The engine treats the key as opaque bytes. Any caller-side accounting or
    /// gating runs through the installed [`crate::policy::WritePolicy`], which
    /// receives both the bytes and a mutable handle to the engine's
    /// system keyspace.
    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>, expires_at: u64) -> EngineResult<u64> {
        keys::validate_user_key(&key)?;
        keys::validate_value(&value)?;
        self.check_backpressure_for_key(&key)?;

        // Policy gate: reject before any WAL/memtable mutation. The policy is
        // cloned out so it can borrow `self` mutably (read/write the system
        // keyspace) without aliasing the field. Skip the full LSM get when the
        // policy does not need existing lengths (noop / non-quota).
        let policy = Arc::clone(&self.policy);
        let existing_value_len = if policy.needs_existing_len() {
            self.get(&key)?.map(|v| v.len())
        } else {
            None
        };
        policy.pre_write(self, &key, value.len(), existing_value_len, false)?;

        // FairDB memtable admit (reserved + global pools). No WAL reservation.
        if let Some(t) = crate::tenant_fair::tenant_from_user_key(&key) {
            self.fair.admit_memtable(t, value.len() as u64)?;
        }

        let seq = self.seq.next();
        let value_len = value.len();
        // Encode WAL from borrowed slices; keep one key clone for post-write /
        // cache invalidate after the key moves into the memtable.
        let wal_bytes = wal::encode_put(seq, expires_at, &key, &value);
        self.append_bytes_buffered(&wal_bytes, seq)?;
        if !self.group_commit {
            self.sync_wal()?;
        }
        let policy_key = key.clone();

        let ik = InternalKey::new(key, seq, EntryKind::Value);
        let entry = Entry::value(
            value,
            if expires_at == 0 {
                None
            } else {
                Some(expires_at)
            },
        );
        crate::engine_fail_point!(crate::failpoints::ENGINE_BEFORE_MEMTABLE_INSERT);
        self.active_mut().insert(ik, entry);
        crate::engine_fail_point!(crate::failpoints::ENGINE_AFTER_MEMTABLE_INSERT);
        if let Some(rc) = &self.result_cache {
            rc.invalidate(&policy_key);
        }

        // Post-write bookkeeping (e.g. durable usage counters), on the same
        // commit path so it joins this write's group-commit fsync.
        policy.post_write(self, &policy_key, value_len, existing_value_len, false);

        if let Some(m) = &self.metrics {
            m.user_bytes_written_total.inc_by(value_len as u64);
        }

        // Per-caller operation counts (e.g. labeled by routing context) are
        // the caller's responsibility — see crate::metrics docs. The engine
        // deliberately stays out of label cardinality decisions.
        self.maybe_freeze();
        self.try_submit_flush();
        self.maybe_submit_compaction();
        self.update_gauges();
        Ok(seq)
    }

    /// DEL a key (writes a tombstone). Returns whether the key existed and the
    /// assigned sequence number (for group-commit durability waiting).
    pub fn del(&mut self, key: Vec<u8>) -> EngineResult<(bool, u64)> {
        keys::validate_user_key(&key)?;
        self.check_backpressure_for_key(&key)?;

        // Del always point-gets: the wire response reports whether the key
        // existed, and quota policies need the freed length. (Unlike put, there
        // is no "skip get" win without changing the Del response contract.)
        let existing_value_len = self.get(&key)?.map(|v| v.len());
        let existed = existing_value_len.is_some();

        // A delete cannot be rejected by a usage-style policy, but we still call
        // pre_write for symmetry and to let custom policies veto if they choose.
        let policy = Arc::clone(&self.policy);
        policy.pre_write(self, &key, 0, existing_value_len, true)?;

        let seq = self.seq.next();
        let wal_bytes = wal::encode_del(seq, &key);
        self.append_bytes_buffered(&wal_bytes, seq)?;
        if !self.group_commit {
            self.sync_wal()?;
        }
        let policy_key = key.clone();

        let ik = InternalKey::new(key, seq, EntryKind::Tombstone);
        self.active_mut().insert(ik, Entry::tombstone());
        if let Some(rc) = &self.result_cache {
            rc.invalidate(&policy_key);
        }

        // Post-write bookkeeping: a policy may release usage for the freed key.
        policy.post_write(self, &policy_key, 0, existing_value_len, true);

        // See `put`: per-caller operation counts are the caller's concern.
        self.maybe_freeze();
        self.try_submit_flush();
        self.maybe_submit_compaction();
        self.update_gauges();
        Ok((existed, seq))
    }

    /// Atomically apply a batch of puts and deletes. Either every op is
    /// persisted or none is: the whole batch is written as a SINGLE self-framed
    /// WAL record with one CRC, so a torn crash mid-batch replays no ops (the
    /// torn tail is truncated). All ops share one sequence number; duplicate
    /// user keys within a batch are rejected (they would collide at that seq).
    ///
    /// The policy is consulted for every op BEFORE any mutation — any rejection
    /// aborts the entire batch with nothing persisted. Returns the batch's
    /// assigned sequence number (await durability via [`sync_wal`] under group
    /// commit; when group commit is disabled the WAL is fsynced on return).
    pub fn write_batch(&mut self, ops: Vec<BatchOp>) -> EngineResult<u64> {
        if ops.is_empty() {
            return Err(EngineError::InvalidKey("empty batch".into()));
        }
        if ops.len() > keys::MAX_BATCH_KEYS {
            return Err(EngineError::InvalidKey(format!(
                "batch size {} exceeds MAX_BATCH_KEYS {}",
                ops.len(),
                keys::MAX_BATCH_KEYS
            )));
        }

        // Validate every key/value and reject duplicate user keys up front,
        // before any mutation. (Two ops on the same key would collide at the
        // shared batch seq, making newest-wins ambiguous.)
        {
            let mut seen: std::collections::HashSet<&[u8]> =
                std::collections::HashSet::with_capacity(ops.len());
            for op in &ops {
                keys::validate_user_key(op.key())?;
                if let BatchOp::Put { value, .. } = op {
                    keys::validate_value(value)?;
                }
                if !seen.insert(op.key()) {
                    return Err(EngineError::InvalidKey("duplicate key in batch".into()));
                }
            }
        }
        self.check_backpressure_for_key(ops[0].key())?;

        // Policy gate: consult the policy for every op BEFORE any mutation. Any
        // rejection aborts the whole batch with nothing persisted. Existing
        // value lengths are captured here for the matching post_write calls —
        // skipped entirely when the policy does not need them.
        let policy = Arc::clone(&self.policy);
        let need_len = policy.needs_existing_len();
        let mut existing_lens: Vec<Option<usize>> = Vec::with_capacity(ops.len());
        for op in &ops {
            let existing = if need_len {
                self.get(op.key())?.map(|v| v.len())
            } else {
                None
            };
            existing_lens.push(existing);
            policy.pre_write(self, op.key(), op.value_len(), existing, op.is_delete())?;
        }

        // FairDB memtable admit for all puts (all-or-nothing with the batch).
        for op in &ops {
            if let BatchOp::Put { key, value, .. } = op {
                if let Some(t) = crate::tenant_fair::tenant_from_user_key(key) {
                    self.fair.admit_memtable(t, value.len() as u64)?;
                }
            }
        }

        // One seq for the whole batch.
        let seq = self.seq.next();

        // Build one self-framed batch WAL record (one CRC = atomic on a torn
        // crash) and append it on the group-commit path.
        let wal_ops: Vec<wal::WalOp> = ops
            .iter()
            .map(|op| match op {
                BatchOp::Put {
                    key,
                    value,
                    expires_at,
                } => wal::WalOp {
                    command: wal::WAL_PUT,
                    expires_at: *expires_at,
                    key: key.clone(),
                    value: value.clone(),
                },
                BatchOp::Del { key } => wal::WalOp {
                    command: wal::WAL_DEL,
                    expires_at: 0,
                    key: key.clone(),
                    value: Vec::new(),
                },
            })
            .collect();
        let rec_bytes = wal::WalBatch { seq, ops: wal_ops }.encode();
        self.append_bytes_buffered(&rec_bytes, seq)?;
        if !self.group_commit {
            self.sync_wal()?;
        }

        // Insert every op into the memtable under the shared batch seq.
        // Consume ops by value so key/value move once (WAL already encoded).
        crate::engine_fail_point!(crate::failpoints::ENGINE_BEFORE_MEMTABLE_INSERT);
        let mut total_value_len = 0usize;
        for (op, existing) in ops.into_iter().zip(existing_lens.into_iter()) {
            let (key_for_hooks, value_len, is_delete) = match &op {
                BatchOp::Put { key, value, .. } => (key.clone(), value.len(), false),
                BatchOp::Del { key } => (key.clone(), 0, true),
            };
            total_value_len += value_len;
            let (ik, entry) = match op {
                BatchOp::Put {
                    key,
                    value,
                    expires_at,
                } => (
                    InternalKey::new(key, seq, EntryKind::Value),
                    Entry::value(
                        value,
                        if expires_at == 0 {
                            None
                        } else {
                            Some(expires_at)
                        },
                    ),
                ),
                BatchOp::Del { key } => (
                    InternalKey::new(key, seq, EntryKind::Tombstone),
                    Entry::tombstone(),
                ),
            };
            self.active_mut().insert(ik, entry);
            if let Some(rc) = &self.result_cache {
                rc.invalidate(&key_for_hooks);
            }
            policy.post_write(self, &key_for_hooks, value_len, existing, is_delete);
        }
        crate::engine_fail_point!(crate::failpoints::ENGINE_AFTER_MEMTABLE_INSERT);

        if let Some(m) = &self.metrics {
            m.user_bytes_written_total.inc_by(total_value_len as u64);
        }

        self.maybe_freeze();
        self.try_submit_flush();
        self.maybe_submit_compaction();
        self.update_gauges();
        Ok(seq)
    }

    #[inline]
    pub(crate) fn active_mut(&mut self) -> &mut Memtable {
        Arc::make_mut(&mut self.active)
    }

    /// Refuse new writes until [`Engine::unfreeze_writes`] is called.
    pub fn freeze_writes(&mut self) {
        self.freeze_writes = true;
    }

    pub fn unfreeze_writes(&mut self) {
        self.freeze_writes = false;
    }

    pub fn writes_frozen(&self) -> bool {
        self.freeze_writes
    }

    pub(crate) fn check_backpressure(&mut self) -> EngineResult<()> {
        self.check_backpressure_for_key(&[])
    }

    pub(crate) fn check_backpressure_for_key(&mut self, key: &[u8]) -> EngineResult<()> {
        self.pending_write_slowdown = std::time::Duration::ZERO;
        if self.freeze_writes {
            return Err(EngineError::EngineBusy("writes frozen".into()));
        }
        if self.in_flight_wal_bytes > keys::MAX_IN_FLIGHT_WAL_BYTES {
            return Err(EngineError::EngineBusy("WAL in-flight limit".into()));
        }

        let fair_on = self.fair.config().enabled;
        let tenant = crate::tenant_fair::tenant_from_user_key(key);
        let attribute_to_noisy = fair_on
            && tenant
                .map(|t| self.fair.should_attribute_stall(t))
                .unwrap_or(false);

        let imm_len = self.immutable.len();
        let imm_soft = self.cfg.max_immutable_memtables;
        // Absolute safety: always reject if the queue grows unboundedly even
        // when fair mode would otherwise spare a well-behaved tenant.
        let imm_hard = imm_soft.saturating_mul(2).max(imm_soft + 1);
        if imm_len >= imm_hard {
            return Err(EngineError::EngineBusy("flush queue full".into()));
        }
        if imm_len >= imm_soft {
            if fair_on && !attribute_to_noisy {
                // Well-behaved tenant: skip soft flush-queue reject.
            } else {
                if let Some(t) = tenant {
                    if attribute_to_noisy {
                        self.fair.note_stall(t);
                        self.fair.charge_l0_tokens(t, 1);
                    }
                }
                return Err(EngineError::EngineBusy("flush queue full".into()));
            }
        }

        let queue_depth = self.compaction_scheduler.queue_depth()
            + self.flush_scheduler.queue_depth()
            + if self.compaction_scheduler.compaction_needed() {
                1
            } else {
                0
            };
        let l0_count = self.sstables.iter().filter(|s| s.meta.level == 0).count();
        let l0_stall_threshold = self
            .cfg
            .l0_write_stall_threshold
            .unwrap_or_else(|| self.cfg.compaction.l0_trigger.saturating_mul(5).max(20));

        if queue_depth >= 4 {
            if fair_on && !attribute_to_noisy {
                // Well-behaved tenant under fair mode: skip hard reject.
            } else {
                if let Some(t) = tenant {
                    if attribute_to_noisy {
                        self.fair.note_stall(t);
                        self.fair.charge_l0_tokens(t, 1);
                    }
                }
                return Err(EngineError::EngineBusy(format!(
                    "compaction backlog (queue depth {})",
                    queue_depth
                )));
            }
        }
        if l0_count >= l0_stall_threshold {
            if fair_on && !attribute_to_noisy {
                // skip
            } else {
                if let Some(t) = tenant {
                    if attribute_to_noisy {
                        self.fair.note_stall(t);
                        self.fair.charge_l0_tokens(t, 1);
                    }
                }
                return Err(EngineError::EngineBusy(format!(
                    "L0 compaction backlog ({} files)",
                    l0_count
                )));
            }
        }

        let pending = self.estimate_pending_compaction_bytes();
        let hard = self.cfg.compaction.hard_pending_compaction_bytes;
        let soft = self.cfg.compaction.soft_pending_compaction_bytes;
        // Compute slowdown but do NOT sleep here — callers hold the engine mutex.
        if hard > 0 && pending >= hard {
            let ratio = (pending - hard) as f64 / hard as f64;
            let delay_ms = (1.0 + ratio * 4.0).min(5.0) as u64;
            self.pending_write_slowdown = std::time::Duration::from_millis(delay_ms);
        } else if soft > 0 && pending > soft {
            let ratio = (pending - soft) as f64 / soft as f64;
            let delay_us = (100.0 + ratio * 900.0).min(1000.0) as u64;
            self.pending_write_slowdown = std::time::Duration::from_micros(delay_us);
        }

        // Cockroach-style: pace only over-share / token-debt tenants so they
        // cannot deepen flush/L0 pressure at full rate. Well-behaved tenants
        // keep the fast path. Callers must take_write_slowdown after unlock.
        if fair_on && attribute_to_noisy {
            let pace = std::time::Duration::from_millis(2);
            if self.pending_write_slowdown < pace {
                self.pending_write_slowdown = pace;
            }
        }
        Ok(())
    }

    /// Install δ-fair configuration (Phase 5). Off by default.
    pub fn with_fair_share(
        mut self,
        fair: std::sync::Arc<crate::tenant_fair::FairShareState>,
    ) -> Self {
        self.fair = fair;
        self
    }

    pub fn fair_share(&self) -> std::sync::Arc<crate::tenant_fair::FairShareState> {
        Arc::clone(&self.fair)
    }

    /// Take the write slowdown suggested by the last successful backpressure
    /// check. Apply with [`apply_write_slowdown`] only after releasing any
    /// engine mutex.
    pub fn take_write_slowdown(&mut self) -> std::time::Duration {
        std::mem::take(&mut self.pending_write_slowdown)
    }

    /// Sleep for a write slowdown **outside** the engine mutex.
    pub fn apply_write_slowdown(d: std::time::Duration) {
        if !d.is_zero() {
            std::thread::sleep(d);
        }
    }

    /// Freeze the active memtable if it exceeds the flush threshold, then queue
    /// a background flush (Tidewalker / flush worker).
    pub(crate) fn maybe_freeze(&mut self) {
        if self.active.size_bytes() <= self.cfg.memtable_flush_threshold {
            return;
        }
        let frozen = std::mem::replace(&mut self.active, Arc::new(Memtable::new()));
        self.immutable.push_back(frozen);
        self.refresh_topology_gauges();
        self.try_submit_flush();
    }

    pub(crate) fn try_submit_flush(&mut self) -> bool {
        if self.flush_scheduler.is_worker_busy() {
            return false;
        }
        let Some(mt) = self.immutable.front() else {
            return false;
        };
        if mt.is_empty() {
            self.immutable.pop_front();
            self.refresh_topology_gauges();
            return self.try_submit_flush();
        }
        let pairs: Vec<(InternalKey, Entry)> =
            mt.iter().map(|(k, e)| (k.clone(), e.clone())).collect();
        // Tenant attribution for flush fairness + L0 token charge (Phase 5b).
        // Immutable memtables stay FIFO (seq correctness); fairness is via
        // admission + stall attribution, not flush reorder on a shared LSM.
        let mut tenant_bytes: std::collections::HashMap<crate::tenant_fair::TenantId, u64> =
            std::collections::HashMap::new();
        for (ik, e) in &pairs {
            if let Some(t) = crate::tenant_fair::tenant_from_user_key(&ik.user_key) {
                let sz = (e.value_len() as u64).max(1);
                *tenant_bytes.entry(t).or_default() += sz;
            }
        }
        let priority_tenant = self.fair.pick_flush_priority_tenant(&tenant_bytes);
        let submitted = self.flush_scheduler.try_submit(
            pairs,
            self.cfg.data_dir.clone(),
            self.next_sstable_id_atomic.clone(),
            tenant_bytes,
            priority_tenant,
        );
        if submitted {
            self.immutable.pop_front();
            self.refresh_topology_gauges();
        }
        submitted
    }

    pub(crate) fn submit_flush_apply(
        &mut self,
        result: crate::flush_worker::FlushExecuteResult,
    ) -> EngineResult<()> {
        let crate::flush_worker::FlushExecuteResult {
            meta,
            max_seq,
            tenant_bytes,
            ..
        } = result;
        // Release memtable pool credits and charge L0 byte tokens / Fork B domain.
        let file_size = meta.size_bytes;
        let total_attr: u64 = tenant_bytes.values().sum::<u64>().max(1);
        let dominant = tenant_bytes.iter().max_by_key(|(_, b)| *b).map(|(k, _)| *k);
        for (t, bytes) in &tenant_bytes {
            self.fair.release_memtable(*t, *bytes);
            let share = ((*bytes as u128) * file_size as u128 / total_attr as u128) as u64;
            let files = u64::from(Some(*t) == dominant);
            self.fair.note_l0_add(*t, files, share);
        }
        let mut manifest_records = vec![ManifestRecord::SstableAdd(meta.clone())];
        let covered_up_to = self.wal_segments_covered(max_seq)?;
        if covered_up_to > 0 {
            manifest_records.push(ManifestRecord::WalTruncate {
                up_to_segment_id: covered_up_to,
            });
        }
        self.apply_scheduler
            .submit(crate::apply_worker::ReadyCatalogApply {
                manifest_records,
                add_metas: vec![meta],
                remove_sstable_ids: vec![],
                unlink_wal_up_to: if covered_up_to > 0 {
                    Some(covered_up_to)
                } else {
                    None
                },
                flush_max_seq: Some(max_seq),
                compaction: None,
            })
    }
}
