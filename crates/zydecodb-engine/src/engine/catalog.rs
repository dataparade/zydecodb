use super::*;

impl Engine {
    /// Delete every key whose storage key starts with `prefix`, writing
    /// tombstones in atomic [`Engine::write_batch`] chunks. Returns the number of
    /// keys deleted. Intended for coarse operations such as tenant offboarding.
    ///
    /// Matching keys are collected from a single consistent snapshot first (so
    /// memory scales with the number of live keys under the prefix), then deleted
    /// in `MAX_BATCH_KEYS`-sized batches. Keys written concurrently after the scan
    /// are not included. The tombstoned space is reclaimed lazily by background
    /// compaction, or immediately via [`Engine::compact_all`].
    pub fn delete_prefix(&mut self, prefix: Vec<u8>) -> EngineResult<u64> {
        let keys_to_delete: Vec<Vec<u8>> = {
            let iter = self.prefix_scan(prefix)?;
            let mut out = Vec::new();
            for item in iter {
                let (k, _v) = item?;
                out.push(k);
            }
            out
        };
        let total = keys_to_delete.len() as u64;
        for chunk in keys_to_delete.chunks(keys::MAX_BATCH_KEYS) {
            let ops: Vec<BatchOp> = chunk
                .iter()
                .map(|k| BatchOp::Del { key: k.clone() })
                .collect();
            self.write_batch(ops)?;
        }
        Ok(total)
    }

    /// Flush the active memtable, then run compaction jobs until the planner
    /// finds no more work. Used by offline maintenance (e.g. reclaiming space
    /// after [`Engine::delete_prefix`]); flushing first ensures freshly written
    /// tombstones participate in the compaction that drops them.
    pub fn compact_all(&mut self) -> EngineResult<()> {
        self.force_flush()?;
        while self.compact_once()? {}
        Ok(())
    }

    /// Capture a consistent base snapshot of the current durable state into
    /// `dir`. Flushes the active memtable (so all data lives in SSTables +
    /// manifest), drains background work, then hardlinks the live SSTable set
    /// and copies the `MANIFEST` into `dir`, plus a small `SNAPMETA` recording
    /// the snapshot sequence. Hardlinks are instant and share storage on the
    /// same filesystem (a full copy is used across filesystems); SSTable
    /// immutability makes the result internally consistent. Returns the snapshot
    /// sequence (the max seq durable at capture time).
    ///
    /// Run offline against a stopped node, or against a replica's `data_dir` for
    /// zero primary impact. The snapshot directory is itself a valid `data_dir`
    /// (open it with an empty WAL to read state as of the snapshot); combine it
    /// with shipped WAL for point-in-time restore.
    pub fn snapshot_to(&mut self, dir: &Path) -> EngineResult<u64> {
        self.force_flush()?;
        self.drain_background_work()?;
        self.sync_manifest()?;
        std::fs::create_dir_all(dir)?;

        for s in &self.sstables {
            let src = Self::sstable_path(&self.cfg.data_dir, s.meta.id);
            let dst = Self::sstable_path(dir, s.meta.id);
            Self::link_or_copy(&src, &dst)?;
        }

        let manifest_src = self.cfg.data_dir.join("MANIFEST");
        let manifest_dst = dir.join("MANIFEST");
        std::fs::copy(&manifest_src, &manifest_dst)?;

        let snapshot_seq = self.current_seq();
        let snapmeta = dir.join("SNAPMETA");
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&snapmeta)?;
        f.write_all(format!("{{\"snapshot_seq\":{snapshot_seq}}}\n").as_bytes())?;
        f.sync_all()?;
        Self::fsync_dir(dir)?;
        tracing::info!(snapshot_seq, dir = %dir.display(), "base snapshot written");
        Ok(snapshot_seq)
    }

    /// Hardlink `src` to `dst`, falling back to a full copy when hardlinking is
    /// not possible (e.g. across filesystems).
    pub(crate) fn link_or_copy(src: &Path, dst: &Path) -> EngineResult<()> {
        match std::fs::hard_link(src, dst) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
            Err(_) => {
                std::fs::copy(src, dst)?;
                Ok(())
            }
        }
    }

    // ---- System keyspace (opaque caller-defined metadata sidecar) ----
    //
    // These mirror put/get/del but accept only `KS_SYSTEM` keys. They share the
    // same WAL/memtable/SSTable path, so system records are durable and recover
    // on restart for free. User writes can never reach this keyspace (the user
    // path enforces `KS_USER`); only the embedder calls these. The engine
    // treats their contents as opaque bytes.

    /// PUT a system record with its own durability point (inline fsync). `key`
    /// must be in the system keyspace (`0x00`). Used for standalone metadata
    /// writes that are not part of a surrounding user write's commit batch.
    pub fn sys_put(&mut self, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        keys::validate_system_key(&key)?;
        keys::validate_value(&value)?;
        self.check_backpressure()?;

        let seq = self.seq.next();
        let rec = WalRecord::put(seq, 0, key.clone(), value.clone());
        self.append_wal(&rec)?;

        let ik = InternalKey::new(key, seq, EntryKind::Value);
        self.active_mut().insert(ik, Entry::value(value, None));

        self.maybe_freeze();
        self.update_gauges();
        Ok(())
    }

    /// Write a system record on the same buffered WAL path as the surrounding
    /// user write, so it joins the same group-commit fsync. Intended for a
    /// [`crate::policy::WritePolicy`] to persist bookkeeping (e.g. usage
    /// counters) from within `post_write`. Does NOT fsync here — the caller's
    /// commit covers it. When group commit is off it still buffers; the user
    /// path fsyncs inline.
    pub fn sys_put_policy(&mut self, key: Vec<u8>, value: Vec<u8>) -> EngineResult<()> {
        keys::validate_system_key(&key)?;
        keys::validate_value(&value)?;
        let seq = self.seq.next();
        let rec = WalRecord::put(seq, 0, key.clone(), value.clone());
        if self.group_commit {
            self.append_wal_buffered(&rec)?;
        } else {
            self.append_wal(&rec)?;
        }
        let ik = InternalKey::new(key, seq, EntryKind::Value);
        self.active_mut().insert(ik, Entry::value(value, None));
        Ok(())
    }

    /// GET a system record. `key` must be in the system keyspace (`0x00`).
    pub fn sys_get(&self, key: &[u8]) -> EngineResult<Option<Vec<u8>>> {
        keys::validate_system_key(key)?;
        // System keys share the same LSM as user keys; reuse the snapshot
        // path so there's one read implementation.
        self.snapshot_get(u64::MAX, key)
    }

    /// DEL a system record (tombstone). Returns whether it existed.
    pub fn sys_del(&mut self, key: Vec<u8>) -> EngineResult<bool> {
        keys::validate_system_key(&key)?;
        self.check_backpressure()?;

        let existed = self.sys_get(&key)?.is_some();

        let seq = self.seq.next();
        let rec = WalRecord::del(seq, key.clone());
        self.append_wal(&rec)?;

        let ik = InternalKey::new(key, seq, EntryKind::Tombstone);
        self.active_mut().insert(ik, Entry::tombstone());

        self.maybe_freeze();
        self.update_gauges();
        Ok(existed)
    }

    /// Lazy-expiry sweep over the active memtable: tombstone expired entries.
    pub fn sweep_expired(&mut self) -> EngineResult<usize> {
        let now = Self::now_ms();
        let mut expired_keys: Vec<Vec<u8>> = Vec::new();
        for (ik, entry) in self.active.iter() {
            if entry.is_expired(now) && !entry.is_tombstone() {
                expired_keys.push(ik.user_key.clone());
            }
        }
        // Deduplicate (a key may appear with multiple seqs).
        expired_keys.sort();
        expired_keys.dedup();
        let count = expired_keys.len();
        for key in expired_keys {
            // Only tombstone if the latest version is still an expired value.
            if let Some((_, entry)) = self.active.get_latest(&key) {
                if entry.is_expired(now) && !entry.is_tombstone() {
                    let seq = self.seq.next();
                    let ik = InternalKey::new(key.clone(), seq, EntryKind::Tombstone);
                    self.active_mut().insert(ik, Entry::tombstone());
                }
            }
        }
        Ok(count)
    }
}
