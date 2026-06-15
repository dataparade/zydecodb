# ParadeKV L2 — Research Memo #2: The 59-Minute Repack Death Spiral

**To:** Engine dev
**Re:** Why every soak past ~59 min collapses, and why the fix is mostly deletion
**Verdict:** You are one mechanism away from a passing 90-minute soak. The first 58 minutes already pass every gate. A single feature, the emergency whole-level repack of the bottom level, turns on at L2=8 and spins forever because it is arithmetically incapable of doing what it was built to do. Delete it. Then remove the reason the planner ever asks for it: the max level should never schedule a compaction of itself.

---

## 1. What the data already proves

Read the JSONL before reading any theory. The story is in two halves.

**Minutes 0 to ~58 (elapsed 0–3480s): every gate green.**
- Throughput 2944–2990 ops/sec (98–99.7% of 3000)
- L2 median file size ~66.5 MB against a 64 MB target. Correctly sized. Not fragments.
- `compaction_write_amp` rising slowly 0.5 → 2.2 (normal cumulative leveled write amp)
- p99 80–266 µs, p999 ≤ 540 µs
- `max_us` peaks ~143 ms, under your 200 ms ceiling
- errors 0, `compaction_repack_total` 0–3

That's a pass. The overlap-merge from Memo #1 worked. Backpressure worked. Apply batching worked. The geometry is healthy.

**Minute 59 onward (elapsed 3540s+): one mechanism eats the engine.**
- 3480s: L2=7, `repack_total`=3, `l2_window`=0, write_amp=2.2, throughput 2990
- 3540s: L2=**8**, `repack_total`=**16**, `l2_window`=**13**, write_amp 3.85, throughput 2892
- 3600s: `repack_total`=48, `l2_window`=**32**, write_amp 7.5, throughput 2777
- 3720s: `repack_total`=111, write_amp 14, throughput 2784
- 3960s: write_amp 26, throughput 1576
- 4080s: write_amp 31, throughput 1236
- 5340s (kill): `repack_total`=837, write_amp **75**, throughput 1656

The instant L2 reaches 8, emergency repack starts firing ~32 times per minute and never stops. `compaction_jobs_l2_window` and `compaction_repack_total` move in lockstep, so these L2 jobs are all `emergency_repack == true`. Everything else (throughput, write amp, L0 backlog, p50) is downstream of that one loop.

---

## 2. Why repack at the bottom level is a do-nothing infinite loop

At the cliff your 8 L2 files are ~66.5 MB each, non-overlapping, totaling ~532 MB. `ceil(532 / 64) = 9`. Eight target-sized non-overlapping files is at or below the physical floor for that much data.

Emergency repack reads all 8, k-way merges, cuts output at 64 MB, and produces 8 target-sized non-overlapping files. Same count. It cannot reduce 8 by rewriting 8, because the bytes don't shrink and the cut size doesn't change. So the post-condition that's supposed to "fix fragmentation" is identical to the pre-condition, the trigger is still satisfied, and it fires again on the next poll. Every iteration reads 512 MB and writes 512 MB to accomplish nothing. That's your write_amp climbing to 75x and the worker pinned at 100% on useless I/O while L0 starves and backs up.

This is not a tuning problem in the repack trigger. A repack of the bottom level is the wrong operation. The bottommost level in leveled compaction is never compacted within itself for file-count reasons. Per RocksDB's own architecture docs, the last level holds fully compacted data and no further compaction occurs there. Its file count is just `ceil(level_bytes / target_file_bytes)`, and that value is correct by definition. There is nothing to fix.

---

## 3. The real root cause: the max level is scheduling itself

Memo #1 told you to delete steady-state repack and you kept it "for emergencies at count ≥ 8." The deeper bug is why the planner ever wants to touch L2 at all once it's full. Two design choices combine into a trap.

**Dynamic L2 target = actual L2 bytes makes the byte score permanently 1.0.** You size L2's target to its own current size (bottom-up sizing, correct for deriving L1's target). But then the planner computes L2's compaction score as `size / target`, which for the bottom level is always `actual / actual = 1.0`. So L2 looks perpetually eligible for compaction. This directly answers your open question 4: the planner is not idle at steady state. It thinks L2 always needs work.

That's a category error, and it's worth being precise about. In RocksDB with dynamic level bytes, the last level's "target = actual size" exists only to size the levels *above* it: target size of the last level is the actual size of the level, and each upper level's target is the next level's target divided by the multiplier. The last level's score being 1.0 never triggers a compaction, because a level's score governs compacting it *down into the next level*, and there is no level below the last one. The max level is a destination, not a source.

ParadeKV gave the max level a self-referential score and a same-level "repack" job to satisfy it. Once L2 is the highest-scoring level (which is always, at 1.0) and the only available L2 action is a same-level merge, the engine does a same-level merge. On non-overlapping target-sized files, that merge is the do-nothing loop from section 2.

So: the planner asks for L2 work because of the always-1.0 score, and the only L2 work it can express is a repack that can't make progress. Remove either half and the trap is gone. Remove both and it's gone permanently.

---

## 4. Why it passed unit tests but died in the soak, and why exactly at 8

Your `eight_target_sized_disjoint_l2_no_repack` test feeds the planner a clean, static catalog of exactly 8 well-formed files. In that frozen state `is_l2_fragmented()` returns false and the test passes. The live system never sits in that clean state. It's continuously mid-apply, and the planner samples the catalog every 50 ms.

The onset pinned to *exactly* count=8 is the tell. Your intended gate is `count > 2 × expected`, and with `expected = ceil(532/64) = 9` that wouldn't fire until count ~18. It fired at 8. That means a literal threshold of 8 still lives somewhere in the L2 planning path that your new math didn't replace. The prime suspect is the old `max_level_file_count` steady target (8, before you raised the disaster ceiling to 32) surviving as a constant, or `is_l2_fragmented()` / the score denominator still referencing `max_level_file_count / 2` or an `l2_file_target` of 8 rather than the `expected` you compute elsewhere.

You don't need to win the argument about which line. Instrument it once: every time the planner schedules an L2 job, log `{count, expected, median_size, byte_score, file_score, trigger_path}` plus the catalog file list. Run to the cliff. The first repack's log line names the exact predicate and value that tripped. My money is on a comparison against 8.

It's moot for the fix, because the fix deletes the mechanism the predicate guards. But it tells you the gate-hardening approach was never going to work. You can't gate a mechanism that has no correct invocation at this geometry.

---

## 5. Answers to your open questions

1. **Why ~32 repacks/min at median 66 MB, count 8?** A surviving count-8 threshold (or always-1.0 byte score) keeps selecting L2; the repack it schedules can't reduce 8 non-overlapping 64 MB files below 8, so it re-fires every poll. See sections 2–4.
2. **Is 8 files at ~512 MB healthy?** Yes. `ceil(512/64) = 8` is the floor. No whole-level repack should exist at this geometry, or any geometry where files are already target-sized and non-overlapping.
3. **Do hot keys cause repeated all-8 merges via the non-repack path?** The metric says no. `repack_total` tracks `l2_window` one-for-one, so these are repack jobs, not same-level overlap-peer merges. Hot-key overlap is handled correctly during L1→L2 (that's why pre-cliff write_amp is a healthy ~2x).
4. **Does dynamic target = actual keep the planner busy?** Yes, and that's the core bug. Byte score is permanently 1.0. The bottom level must not generate a self-score.
5. **Expected geometry at ~1.4 MB/s and ~512 MB L2?** L2 file count `ceil(bytes/64MB)`, growing slowly as the cold 20% of the keyspace accumulates unique data. Single worker is sufficient; pre-cliff proves it ran 58 minutes at 99% on one worker. Healthy compaction job rate is single-digit per minute (your pre-cliff `compaction_jobs_total` moved ~1 job per several minutes). 32/min is the pathology.
6. **Recommended fix class:** Remove emergency repack entirely and stop the max level from self-scheduling. Not a stronger guard. See section 6.

---

## 6. The fix (mostly deletion)

### #1 — Stop the max level from generating a compaction score. *(Load-bearing.)*
The bottom level is a destination only. Exclude it from the "which level needs compaction" scorer. Its `size / target = 1.0` must not mean "compact me." The only thing that writes to L2 is an L1→L2 overlap-merge, which is triggered by L1's score, not L2's. Keep using L2's actual size to derive L1's target. Don't use it to schedule L2.

### #2 — Delete the emergency whole-level repack. *(Load-bearing.)*
Remove the `is_l2_fragmented()` branch and the `emergency_repack` job entirely. There is no catalog state of target-sized non-overlapping files where repacking helps. Memo #1 said keep it behind a hard ceiling; that was wrong, because count ≥ 8 is normal here, not an emergency. With #1 in place the planner never reaches this code anyway. Delete it so no future bug can.

### #3 — Add a permanent no-progress guard. *(Cheap safety net.)*
Before submitting any compaction whose output level equals its input level, assert it will reduce a cost function: reject it if the inputs are already non-overlapping and each is ≥ target_file_bytes/2, because the output cannot have fewer files. One predicate. It makes the do-nothing loop structurally impossible regardless of what any future planner change does. This is the guard rail that should have existed instead of `is_l2_fragmented()`.

### #4 — Keep exactly one legitimate bottom-level compaction, optional, for the 24h run. *(Not needed for the 90m gate.)*
The one real reason to rewrite the bottom level is reclaiming space from tombstones and superseded versions, not reducing file count. DEL is 5% of your ops, so garbage accumulates slowly. If you want it, trigger on estimated garbage ratio per file (tombstone/obsolete-version bytes over file bytes), compact only files above a threshold, and only when the output is expected to be smaller than the input. This is the bottommost/TTL-style compaction mature engines run for deletions, gated on bytes-reclaimed, never on count. Skip it for now. Your live set isn't garbage-bound at 90 minutes.

---

## 7. What this leaves untouched, and why

**Backpressure is innocent. Don't touch it.** You flagged that pending-bytes micro-delays might be shaving throughput. The data clears it. Pre-cliff, `pending_compaction_bytes` oscillates 0 / 125M / 188M, all under your 256M soft limit, and throughput is 99%. It only crosses the soft and hard limits *during* the spiral (287M → 538M), because repack manufactures fake compaction debt. Kill repack, the fake debt disappears, backpressure never engages. Retuning it now would mask the real fix.

**Single worker is fine.** It ran 58 minutes at 99% on one thread. The worker only saturates when repack hands it 512 MB of useless work every 2 seconds.

**`compaction_pri` / coldest-first is not needed for the gate.** Pre-cliff write_amp ~2x says hot-key handling is already fine. File it under future optimization, not blocker.

---

## 8. The one residual, for the 24h gate not the 90m gate

Pre-cliff `max_us` spikes to ~143 ms (e.g. 1140s, 1500s) and `apply_max_us` runs ~25–160 ms whenever an L0→L1 or L1→L2 applies. That's the synchronous fsync in apply landing on the engine owner thread, Memo #1 item #3, still present. It passes the 200 ms ceiling today but it's the thing most likely to breach a tighter gate or the 24h run under the scan/snapshot mix. Decouple apply: have `poll_compaction()` flip a ready flag and swap the catalog under a short lock, do the manifest fsync on a dedicated thread. Do this after the repack deletion is verified green, not before. One change at a time so the soak attributes cleanly.

---

## 9. Order of operations and expected result

1. Exclude the max level from the compaction scorer (#1).
2. Delete `is_l2_fragmented()` and the `emergency_repack` job (#2).
3. Add the no-progress guard (#3).
4. `cargo build --release -p paradekv-engine` so the soak binary actually contains the change. Your last run reported "Finished in 0.06s," which means a cached binary. Confirm the build timestamp or a version string in the metrics header before trusting any result.
5. Run `HOURS=1.5 OPS=3000 POLL_COMPACTION_MS=50`. Expect the pre-cliff steady state to simply continue: 99% throughput, L2 growing slowly with `ceil(bytes/64MB)`, write_amp flat around 2x, `repack_total` 0, all gates green at 90 minutes.
6. Then decouple apply (#8) and run the 24h with scan/snapshot.

The prediction is specific and falsifiable: with #1–#3 in, `compaction_repack_total` stays 0 for the entire run and there is no cliff. If a cliff still appears, the no-progress guard will have logged the exact rejected job, which names whatever path I didn't anticipate. Either way you get a green run or a precise culprit, not another fishing trip.

---

### Source basis
RocksDB architecture docs (bottommost level holds fully compacted data, no further compaction there), RocksDB Leveled Compaction wiki and Dynamic Level Size blog (last-level target sizes the upper levels; level score = size/target governs down-compaction, max level is a destination), RocksDB Compaction wiki (`max_compaction_bytes`, bottommost/TTL compaction for deletion cleanup). Citations in the accompanying chat message.