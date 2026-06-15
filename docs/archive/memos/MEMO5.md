# ParadeKV — Research Memo #5: The Fixes Worked, the Result Cache Was the Wrong Lever, and RSS Is the Bill

**To:** Engine dev
**Re:** Reading the post-cache soak; one red gate; correcting my own Memo #4 call
**Verdict:** Bet #1 (block cache) and bet #2 (GC + space-amp + disk-full) both worked, cleanly. Throughput recovered to 95.3% and stopped eroding, space amplification is bounded, the GC drops garbage every compaction, and every stability gate passes except RSS. The RSS breach is the bill for the caches you added, not a leak. And the specific cache I told you was the highest-leverage read fix, the result cache, is the one underperforming and eating the memory that broke the ceiling. I got that call backwards. Here's the evidence and the correction.

---

## 1. What worked, with numbers

**Block cache (bet #1): the real read-path unlock.**
- Mean throughput 2858 ops/sec (95.3%), up from 92.6% in the MEMO3 run, and it *holds* ~2850–2900 across the full 90 minutes instead of eroding to a ~2600 floor. The decline-to-a-plateau pattern is gone.
- `block_cache_hit_rate_window` floor lifted from ~0.60 to ~0.75, with most windows 0.88–0.96. Resident bytes now run to ~536 MB (was pinned at ~268 MB), so you roughly doubled the cache and it paid.
- p99 mostly 90–260 µs with rare 400–500 µs windows, versus the MEMO3 run that hit 2035 µs. p999 mostly under 600 µs.

This is exactly the prediction from Memo #4: the working set had outgrown a 256 MB cache, and the cache was the binding constraint. Confirmed.

**GC + space amplification (bet #2): bounded and healthy.**
- `space_amplification` oscillates 1.07–1.79 and trends toward ~1.2 as live data grows to dominate. Bounded, sawtoothing around GC events, never running away.
- `versions_dropped_window` and `tombstones_dropped_window` fire on every compaction wave (e.g. ~420k versions and ~30k tombstones per window). The reclamation works.
- `disk_bytes_total` peaks ~703 MB and GC pulls it back toward ~623 MB. Disk is bounded, not monotonic.

**Stability gates: all green except RSS.** Errors 0, repack 0, no-progress 0, write_amp 2.32, space_amp 1.79, L2 bytes-derived count and median size all pass. The split into stability versus performance SLOs (Memo #4 §5) is working exactly as intended: a healthy engine reads as healthy.

---

## 2. The result cache was the wrong lever. That's on me.

In Memo #4 I called a key/value result cache "the single highest-leverage read-path change you can make" for this workload. The data says I had it backwards, and I want to be precise about why so you don't keep paying for the mistake.

`result_cache_hit_rate_window` warms up and plateaus at ~0.22–0.23. That's low, and it's not a bug in your implementation. It's structural. Result caching wins on scan-heavy workloads and on workloads with few updates, because a cached result survives compaction (it isn't addressed by file and offset like a block is). What it does *not* survive is a logical write to the same key. Your workload is the worst case for it on both axes: zero scans, and 70% writes concentrated on the 80% hot keys. Every hot write invalidates exactly the result-cache entry a subsequent hot read would want. So the cache spends its memory holding entries that get knocked out before they're reused.

What I underweighted in Memo #4: I cited the research that result caches resist *compaction* invalidation, which is true, and I didn't weight that your no-scan, write-heavy-on-hot-keys profile is precisely where that resilience doesn't help, because *logical* invalidation from writes dominates. The literature is explicit that result caching suits scan-heavy and low-update workloads. Yours is neither.

The cost side makes it worse. The result cache is consuming memory that pushed RSS past the ceiling, and it's returning a 22% hit rate for it. Per megabyte, that memory buys far more in the block cache (88–96% hit rate). So:

**Cut the result cache, or shrink it hard, and give that memory to the block cache.** You lose ~5% of GET disk reads avoided and you get back the memory plus a higher block-cache hit rate. One caveat before you delete: confirm the hit-rate denominator. `result_cache_hits + misses` per window (~143k) is much larger than your GET count per window (~43k), which means the counter is measuring against something broader than GETs. Re-measure result-cache value as "GET disk reads avoided per MB" against "block-cache GET disk reads avoided per MB" and let that decide. My strong lean from the shape of the data: the block cache wins that comparison decisively, and the result cache should go.

---

## 3. RSS: the bill for the caches, in two parts

RSS climbs from 149 MB to ~1068 MB and plateaus ~1020–1068 MB in the back half, breaching the 768 MB ceiling. This is not a leak. Account for it: the MEMO3 run held ~640 MB flat with a 256 MB block cache. You roughly doubled the block cache (+~256 MB) and added a result cache (new allocation). 640 + 256 + result cache ≈ what you see. The memory went exactly where you told it to go.

There are two distinct issues here, and only one is a stale gate.

**(a) The immediate breach is a stale ceiling plus a bad allocation.** The 768 MB ceiling predates the caches. Cut the result cache (§2), set the block-cache budget explicitly, and set the RSS ceiling to the sum of configured budgets plus headroom. That turns RSS from an emergent mystery into a configured number the gate checks against, the same way the L2 gate became bytes-derived in Memo #3.

**(b) The structural issue is the one that matters for GA: your memory is not bounded independent of dataset size.** Look at the slow drift in the back half, RSS creeping ~31 MB over the final 46 minutes while the caches are already full. That's not the caches. That's SSTable reader memory: index and bloom-filter blocks held in memory, which scale with the number of open files, which scales with total data. Over 24 hours and a larger dataset, that drift keeps going. For a 24/7 engine you need an enforced memory cap that holds regardless of how much data is stored, which means either accounting index and filter blocks inside the bounded block-cache budget (RocksDB's `cache_index_and_filter_blocks`) or moving to partitioned filters so you don't pin the whole filter set per open file. Without that, "bounded RSS" is only true for a given dataset size, which isn't good enough for a production claim.

So RSS isn't a "raise the ceiling and move on" like the L2 count was. Memory is a real budget. The fix is to make total memory an enforced, configured cap that's independent of data size, then set the gate to that cap.

---

## 4. The two residuals, both now perf-SLO not stability

**Read-path: compaction-wave dips remain.** The block-cache hit rate still sags to ~0.75 in windows where an L1→L2 compaction completes, because the overlap-merge rewrites the hot file and invalidates its cached blocks. The result cache was supposed to cover this and doesn't, for the reasons in §2. If you want the last few points, the targeted fix is block prefetch after compaction (warm the new file's hot blocks as part of the apply), which the research calls out specifically for this. But you're at 95.3% and holding. I would not chase this until something downstream actually needs it.

**Tail: `max_us` still spikes 100–335 ms in compaction windows.** `poll_max_us` hits 335k and `apply_max_us` hits 320k in the same windows. This is the apply/poll stall during compaction completion, the thing I told you in Memo #4 to instrument rather than chase. It's now correctly a performance SLO, not a stability gate. When you decide to attribute it, the candidates are unchanged: reader-open I/O during the catalog swap, the obsolete-file unlink on the owner thread, or lock handoff with the apply thread. One focused attribution pass tells you which.

---

## 5. Where you actually are

Stability is essentially met. Every gate passes except RSS, and RSS is failing for a good reason (the fixes that worked) against a stale ceiling, plus one genuine structural gap (unbounded reader memory). That gap is the last real stability item between you and engine GA, and it's a bounded piece of work, not a rewrite.

The corrected scoreboard against Memo #4's roadmap:
- Bet #1 (block cache): done, worked.
- Bet #2 (GC + space-amp + disk-full): done, worked.
- Bet #3 (result cache): done, but it's the wrong cache for this workload. Cut it.
- Bet #4 (stability-vs-SLO split, deployment harness): split is done and working; the deployment-shaped harness is still worth building before you publish a perf SLA.
- Bet #5 (24h soak, GA definition): not yet, and now you're close enough that it's the right next move *after* the memory cap.

---

## 6. The next moves, ordered

1. **Cut or shrink the result cache; reallocate its memory to the block cache (§2).** This simultaneously lifts read performance and reduces RSS. Highest-leverage single change, and it reverses the one thing I steered wrong.
2. **Enforce a data-size-independent memory cap (§3b).** Account index and filter blocks inside the block-cache budget, or adopt partitioned filters. This is the last stability gap. Then set the RSS ceiling to the configured cap plus headroom.
3. **Re-run the 90-minute stability soak.** With the result cache gone and memory capped, expect every stability gate green, including RSS, and throughput at or above 95%.
4. **Then the 24h soak (§5) and write the engine-GA definition.** This is the graduation run, and you're now positioned for it to be a confirmation rather than a discovery.

The one-sentence version: the engine is healthy, the block cache and GC did their jobs, the result cache was my miscall and should come out, and the only real work left before GA is making memory a hard cap that doesn't grow with the dataset.

---

### Source basis
EDBT 2026 (AdCache) and related LSM caching research: result/range caching achieves lower hit rates than block cache in low-update workloads and suits scan-heavy access; block prefetch after compaction mitigates compaction-driven cache invalidation. RocksDB FAQ and tuning guide: `cache_index_and_filter_blocks` bounds index/filter memory inside the block-cache budget; without it, index and bloom blocks are pinned per open SST file up to `max_open_files`, so reader memory scales with file count. Citations in the accompanying chat message.