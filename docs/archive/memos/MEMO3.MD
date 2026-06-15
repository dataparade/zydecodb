# ParadeKV L2 — Research Memo #3: The Spiral Is Dead, Now Fix the Gate and the Tail

**To:** Engine dev
**Re:** Reading the post-fix 90-minute soak, and what's actually left
**Verdict:** The repack death spiral is gone. `compaction_repack_total` is 0 in every one of the 90 samples, the run cleared the 59-minute cliff that killed every prior attempt, and the engine held 98.3% throughput with zero errors for the full hour and a half on a single worker. This run does not pass the analyzer, but for two reasons that have nothing to do with compaction: a gate that's measuring the wrong thing, and the apply-fsync latency tail that was always the last item on the list. Neither is the spiral. Fix the gate first so you stop chasing a non-bug.

---

## 1. What the run proves

The fix worked exactly as predicted in Memo #2. Remove the max level's self-scheduling and the emergency repack, and the healthy pre-cliff steady state simply continues.

- `compaction_repack_total`: **0** across all 90 samples. The do-nothing loop is structurally gone.
- Duration: 5403s (90 min), `avg_ops_per_sec` 2948.75 (**98.3%** of target), `errors` 0, clean shutdown.
- Past the cliff: every prior run died at ~3540s. This one ran 3540s at 2989 ops/sec like nothing happened and kept going another 30 minutes.
- `compaction_write_amp` settled around **2.2x**, which is normal leveled write amp. Compare 75x at the same point in the failing run.
- RSS flat at 646 MB. Single worker never saturated.

That's the whole thesis of Memos #1 and #2 confirmed in one run. The compaction problem is solved.

---

## 2. Why the run still fails the analyzer, and why one reason is the analyzer's fault

Gate-by-gate on this run:

| Gate | Ceiling | This run | Result | Real cause |
|---|---|---|---|---|
| L2 max files | ≤ 10 | **13** | FAIL | Mis-specified gate, see §3 |
| Throughput ratio | ≥ 0.95 | 0.983 | PASS | — |
| p99 | ≤ 500 µs | **1122 µs** | FAIL | Apply fsync on poll loop, see §4 |
| p999 | ≤ 2000 µs | **2549 µs** | FAIL | Same |
| max single-op | ≤ 200 ms | 166 ms | PASS | (Same cause, just under ceiling) |
| repack_total | 0 | 0 | PASS | The point of the whole exercise |
| errors | 0 | 0 | PASS | — |

Two failing gates. One is wrong about what health means. The other is a real fix you already have queued.

---

## 3. The L2 file-count gate is measuring the wrong thing

L2 grew over the run: 1 file at 14 min, 10 by minute 46, flat at 10 for about 33 minutes, then 11 → 12 → 13 over the final 10 minutes, still climbing at shutdown. That is not fragmentation and not a leak. It's your cold 20% of the keyspace introducing genuinely new data, and in a 3-level tree all real data lives in L2, so its file count is `ceil(L2_bytes / target_file_bytes)`. At ~865 MB that's 13. It will keep rising as long as unique keys keep arriving.

A flat ceiling of 10 on your only data level was guaranteed to fail the moment the database held more than ~640 MB of live data. That gate is a leftover from the era when L2 hitting 8 meant the fragmentation bug. It doesn't mean that anymore.

Bottom-level file count is not a health signal in leveled compaction. A point lookup touches at most one file per non-L0 level, because those levels have non-overlapping key ranges and the reader binary-searches the file boundaries to find the single candidate. Thirteen files or 130 costs the same per GET. For scale reference, Scylla's leveled compaction treats L1 as ~10 sstables, L2 as ~100, L3 as ~1000 in normal operation. File count growing with data is the design, not a defect.

**Fix:** stop gating on raw L2 file count. Gate on the things that actually indicate a sick compactor:

```python
# analyze-soak.py — replace the flat L2 ceiling
PASS = (
    repack_total == 0                          # the spiral signature
    and write_amp_max < 5.0                     # redundant-rewrite detector
    and throughput_ratio >= 0.95
    and p99_max <= 500_000                      # ns or us, match your unit
    and p999_max <= 2_000_000
    and max_us <= 200_000
    and errors == 0
)

# If you still want a file-count sanity check, make it bytes-derived,
# not a constant. This catches real fragmentation (tiny files) without
# punishing legitimate data growth.
expected = math.ceil(l2_bytes / TARGET_FILE_BYTES)
assert l2_file_count <= expected + 2, "fragmentation: more files than data justifies"
assert l2_median_size_bytes >= TARGET_FILE_BYTES // 2, "fragmentation: files too small"
```

With that change, this run passes everything except the apply tail. The `write_amp < 5` and `median >= target/2` checks are the real fragmentation guards. They'd have screamed at 75x and they're quiet at 2.2x.

---

## 4. The apply-fsync tail, now the gating latency issue

Three windows breach. At 1260s, p99 hit 961 µs and p999 hit 2549 µs. At 1680s, p99 hit 1122 µs. `max_us` peaked at 166 ms, under your 200 ms ceiling but ugly. Every one of those lines up with an `apply_max_us` of 130 to 161 ms and a burst of applies in the window (`apply_count_window` 3 to 6). That's the synchronous manifest fsync executing on the poll loop, the residual flagged in Memo #1 (#3) and Memo #2 (#8). With the spiral gone, it's the last thing standing between you and a clean latency profile.

The harness polling before each op is fine. The fix is making apply not block. `poll_compaction()` should do near-zero work: check whether the worker published a result, swap the catalog under a short lock, and return. The durable manifest write and its fsync move to a dedicated thread.

Sketch (adapt to your types):

```rust
// Worker thread, after building output SSTables:
//   1. write the new SSTables to disk (already done off the poll loop)
//   2. hand the manifest edit + new readers to an apply queue
apply_tx.send(PendingApply { edit, new_readers, inputs_to_unlink }).unwrap();

// Dedicated apply thread (NOT the poll/owner hot loop):
while let Ok(p) = apply_rx.recv() {
    manifest.append(&p.edit);          // serialize the version edit
    manifest.fsync();                   // the 130–160 ms cost, now off the hot path
    // publish atomically for the owner thread to pick up
    ready_slot.store(Some(AppliedResult {
        catalog_delta: p.edit,
        new_readers: p.new_readers,
        inputs_to_unlink: p.inputs_to_unlink,
    }));
}

// poll_compaction() on the owner/poll loop becomes cheap:
fn poll_compaction(&mut self) {
    if let Some(applied) = self.ready_slot.take() {
        // short critical section: pointer swaps only, no I/O
        self.catalog.apply_delta(applied.catalog_delta, applied.new_readers);
    }
    // defer unlink + fsync_dir to a periodic cadence, not per-apply
    self.maybe_unlink_obsolete();
}
```

The ordering rule that keeps recovery correct: the manifest fsync must complete before the catalog swap makes the new files reachable, and obsolete inputs must not be unlinked until after the swap is durable. The sketch preserves that by fsyncing on the apply thread before publishing to `ready_slot`, and unlinking lazily after the swap. Keep `fsync_dir` on a timer (you already deferred it once), not per apply.

Expected result: p99/p999 stop spiking on compaction applies, `max_us` drops from 166 ms to single-digit ms, all latency gates clear.

---

## 5. The no-progress guard (owed from Memo #2)

Even though §1–§4 fix the observed problems, ship this. It's the guard rail that makes the entire class of "do-nothing compaction loop" impossible regardless of any future planner change. The bug in the spiral was that a job could be scheduled that provably couldn't reduce file count. Forbid that at the point of submission:

```rust
/// Reject any same-level (output_level == input_level) compaction that
/// cannot reduce file count. A merge of already-target-sized,
/// non-overlapping inputs reproduces the same number of outputs, so it
/// is pure wasted I/O. This is the structural fix for the repack spiral:
/// no gate hole can resurrect it.
fn can_make_progress(job: &CompactionJob, catalog: &Catalog, cfg: &CompactionConfig) -> bool {
    if job.output_level != job.input_level {
        return true; // cross-level compaction always does real work
    }
    let inputs = catalog.files(&job.inputs);
    let all_target_sized = inputs.iter().all(|f| f.size_bytes >= cfg.target_file_bytes / 2);
    let non_overlapping = !catalog.any_overlap(&job.inputs);
    // If inputs are already big and disjoint, output count >= input count.
    // Nothing to gain. Refuse.
    !(all_target_sized && non_overlapping)
}

// At submission:
if !can_make_progress(&job, &catalog, &cfg) {
    metrics.compaction_rejected_no_progress.inc(); // watch this; should stay 0
    return; // do not submit
}
```

If `compaction_rejected_no_progress` ever increments in a soak, a planner change tried to reintroduce the spiral and the guard caught it. That's the metric that should have existed instead of `is_l2_fragmented()`.

---

## 6. The real 24h risk is memory, not compaction

The compaction story is closed. The next wall is the block cache, and this run shows the first crack. In the back third, as L2 grew 10 → 13, p99 crept from ~200 µs to ~440 µs and throughput eased from 2990 to ~2880. `block_cache_resident_bytes` is pinned at the 268 MB ceiling the whole time. Your live set (~865 MB and rising) is outrunning a 256 MB cache, so a growing fraction of GETs miss cache and hit disk. Point-lookup read amplification rises as the cache shrinks relative to the data, and no compaction tuning touches that.

Before the 24h run, do three things:

1. **Make block-cache hit rate a first-class metric.** You're emitting cumulative hits/misses; export the per-window rate so you can see it fall in real time instead of inferring it from p99.
2. **Confirm bloom filters exist on the L2 readers.** 25% of ops are GETs, some on absent keys. Without blooms, every absent-key GET pays a disk read. Bloom filters indicate when a key is definitely not present and skip the I/O. If you don't have them, this is the highest-leverage read-path change you can make.
3. **Size the cache against projected 24h L2 bytes**, or accept a known p99 curve as data grows. This is a capacity decision, not a bug.

---

## 7. The open question this run can't answer

L2's growth shape is strange and worth understanding before the 24h. It sat flat at 10 files for 33 minutes, then climbed 10 → 13 in the final 10. That's either bytes crossing three 64 MB file boundaries in quick succession after hovering just under one, or the hot/cold key split shifting late in the run. Over 24 hours that slope decides whether L2 lands near 30 files or near 300, and whether the cache pressure in §6 is a gentle creep or a wall you hit at hour 6. The 24h run measures it. Don't extrapolate hard from a 10-minute climb. If the long-run slope is steep and read latency degrades with it, that's the moment to ask whether a 4th level (L3) earns its place, since more levels keep more of the data in a deeper, larger-target-file level and change the cache math. Not before. You don't add levels to fix a number that isn't hurting anything yet.

---

## 8. Order of operations

1. **Fix the gate (§3).** Stop failing on L2 file count. Gate on repack, write_amp, latency, throughput. This turns the current run into a pass-minus-the-tail.
2. **Decouple apply (§4).** Clears the p99/p999/max_us tail. Re-run 90m. Expect all gates green.
3. **Ship the no-progress guard (§5).** Permanent insurance.
4. **Instrument cache hit rate, confirm blooms (§6).** Prep for 24h.
5. **Run 24h with scan/snapshot mix.** Watch L2 slope, cache hit rate, p99-vs-L2-bytes. That run's failure mode, if any, will be memory, not compaction.

Steps 1 and 2 get you a clean 90m. Steps 4 and 5 are the real graduation to 24/7.

---

### Source basis
RocksDB Leveled Compaction wiki (point lookup binary-searches one file per non-L0 level; non-overlapping level invariant; bottommost/TTL compaction for deletion cleanup). ScyllaDB leveled compaction blog (levels as runs of exponentially growing sstable counts, L1≈10 / L2≈100 / L3≈1000). "Constructing and Analyzing the LSM Compaction Design Space" (read amplification influenced by block cache size and file structure). LSM fundamentals on bloom filters skipping disk I/O for absent keys. Citations in the accompanying chat message.