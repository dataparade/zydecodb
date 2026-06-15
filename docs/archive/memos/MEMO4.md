# ParadeKV — Research Memo #4: The Engine Is Done Being Sick. Now Decide What You're Shipping.

**To:** Engine dev
**Re:** Reading the MEMO3 soak, why apply-decouple didn't pay, and the shortest honest path to production
**Verdict:** The compaction chapter is closed. Repack 0, no-progress 0, write_amp 2.2, zero errors, bounded RSS, L2 as clean leveled data. The soak SLO miss is not a compaction problem and not an apply problem. It's a read-path cache problem: your working set outgrew a 256 MB block cache, and compaction keeps invalidating the hot blocks. Apply-decouple cost you ~6 points of throughput because it optimized a cost that was never dominant. The biggest decision in front of you isn't latency. It's whether you're shipping an embeddable engine or a hosted product, because that answer, not p99, determines what "production ready" means.

---

## 1. What the MEMO3 soak actually says

The structural work from Memos #1–#3 held perfectly across 90 minutes:

- `compaction_repack_total`: 0 every sample.
- `compaction_rejected_no_progress`: 0 every sample. The guard is in and quiet.
- `compaction_write_amp`: settled ~2.2. Textbook leveled.
- L2: ~12 files, median ~66 MB, bytes-derived gate passes.
- RSS flat ~640 MB, 0 errors, clean shutdown.

That's a healthy compaction engine. Stop looking there. Nothing in the L2 lifecycle is wrong.

What degraded is throughput (98.3% in MEMO2 → 92.6% here) and tail latency (p99 up to 2035 µs, p999 to 4427 µs, max single-op to 243 ms). The shape: excellent for the first ~35 minutes (~2985 ops/sec, p99 <160 µs), then a trough, then a lower plateau around 2550–2650 ops/sec for the last half hour. Not a cliff. A slow erosion to a new floor.

---

## 2. Why apply-decouple didn't pay, with the evidence

You moved the manifest fsync off the owner/poll path onto a dedicated thread. Throughput went *down* and the tails didn't improve. That's because the apply fsync was never the dominant cost. The dominant cost is read-path cache pressure, and the proof is in your new metric.

`block_cache_hit_rate_window` falls from ~0.92 early to ~0.60 late, and throughput and p99 track it. Look at the windows where things go bad: they're the ones where an L1→L2 compaction completes. In those windows `block_cache_resident_bytes` collapses from the 268 MB ceiling to 66–141 MB, hit rate craters to ~0.60–0.65, and that's exactly where throughput dips and p99/max spike. The mechanism is well documented: when compaction rewrites an SSTable, the cached blocks for that file (keyed by file and offset) are invalidated even though the data didn't change. With 80% hot keys, your hot L2 file is rewritten on nearly every promotion, so compaction repeatedly throws away the cache entries for your hottest data. Then the next wave of GETs reloads them from disk, p99 jumps, throughput sags, and the cache slowly refills until the next compaction wave does it again.

So apply-decouple added a thread, a queue, and a cross-thread catalog handoff, all real overhead, to fix a stall that wasn't the binding constraint. The architecture is more *correct* (fsync genuinely should not sit on the owner path), so don't revert it. But stop expecting latency wins from more async-drain tinkering. That well is dry.

One loose end worth instrumenting, not chasing: `poll_max_us` still spikes to 150–262 ms in compaction-completion windows even with apply decoupled. That means a stall the user sees still lands on the poll path during compaction. Candidates are opening new SSTable readers during the catalog swap (index/filter block I/O), the obsolete-file unlink you kept on the owner thread, or lock contention with the apply thread. Attribute `poll_max_us` by phase once and you'll know which. It's a secondary effect. The cache is the headline.

---

## 3. The real fix for the read path

The latency and throughput erosion is one problem: a 256 MB block cache against an ~800 MB-and-growing working set, made worse by compaction invalidation. Four levers, by impact:

1. **Add a key/value (result) cache, not just a block cache.** This is the workload-specific unlock. A KV cache keyed by user key survives compaction invalidation, because it isn't addressed by file and offset. For an 80%-hot-key point-read workload getting hammered by hot-file rewrites, this is the single highest-leverage read-path change you can make. It directly kills the sawtooth. RocksDB ships exactly this as `row_cache` for the same reason.
2. **Make the block cache bigger.** 256 MB is small for this data size. This is a one-line capacity change with immediate effect on hit rate. Cheap, do it first to confirm the diagnosis even before the KV cache lands.
3. **Verify compaction reads actually bypass the cache.** Your change log says `fill_cache=false` shipped, but resident bytes still churn hard during compaction. Confirm the flag is honored on the compaction read path, because if compaction is still filling the cache it's accelerating the eviction of hot user blocks.
4. **Confirm bloom filters on L2 readers.** 25% of ops are GETs, some on absent or deleted keys. Bloom filters skip the disk read for definitely-absent keys. Without them, every miss on a cold or deleted key pays full read amplification.

Levers 2 and 3 are hours of work and will move the number now. Lever 1 is the structural answer for this workload and is worth a real design pass.

---

## 4. The thing nobody's gating that will take you down at 3 AM

Disk space. You noted it in passing: "disk doesn't shrink on delete." That's not a nice-to-have, it's an availability bug for anything running 24/7. You have 5% DELs and constant hot-key overwrites. Overwritten and deleted versions accumulate, and in a 3-level tree there's no level below L2 to drain them into, so without an explicit reclamation path that garbage may never get dropped. RSS is flat, which is reassuring for memory, but RSS is not disk. L2 bytes grow, and you currently can't tell how much of that growth is live cold-key data versus uncollected garbage, because you don't measure space amplification.

Before any 24/7 claim you need two things: a space-amplification metric (logical live bytes vs physical bytes on disk) and a reclamation mechanism (bottommost/periodic compaction that drops tombstones and superseded versions, gated on bytes reclaimed, never on file count). This is the one item I'd call a hard correctness blocker that isn't on your perf radar. Unbounded disk growth ends in a disk-full outage, and you have no disk-full injection test either.

---

## 5. Graduation criteria: split stability from performance

Your gates are conflating two different things, which is why a healthy engine keeps "failing." Separate them.

**Stability gates (hard, pass/fail, non-negotiable for any release):**
- `errors == 0`
- `compaction_repack_total == 0`
- `compaction_rejected_no_progress == 0`
- `compaction_write_amp < 5`
- L2 count `<= ceil(l2_bytes / target) + 2`, median `>= target/2`
- RSS bounded over the run
- **space_amplification bounded** (new, per §4)
- crash/failpoint matrix green, **including disk-full** (new)

**Performance SLOs (tunable targets, not GA gates):**
- throughput ratio, p99, p999, max single-op

The performance numbers are a function of hardware, cache budget, and workload. 3000 ops/sec with sub-ms p99 is a benchmark setting, not a law of nature. Publishing "p99 < X at Y ops/sec on Z hardware with W cache" as a documented SLA is real. Failing your own arbitrary synthetic ceiling and calling the engine not-ready is benchmark theater. Gate releases on stability. Track performance as a curve you publish, and regression-test it against itself (this build vs last build), not against a number someone picked.

And replace the synthetic harness as the oracle. A per-target-rate loop in a debug-ish build measuring apply timing is not your deployment. The honest benchmark is: release build, the actual server process, a fixed memory budget, a realistic read/scan/snapshot mix, run against a held workload. Build that and the SLO conversation becomes real instead of theatrical.

---

## 6. The decision that actually drives the roadmap

Your question D is the whole game: **engine for embedders, or hosted NoSQL product?** These are different products with different definitions of done, and you can't prioritize until you pick.

**If the deliverable is an embeddable LSM engine** (the RocksDB/Pebble shape), then "production ready" means: durable, crash-safe, bounded resources, space reclamation, a documented performance envelope, and a stable API. That's a *small, reachable surface* and it's where your maturity already is. MVCC, transactions, indexes, and backup tooling are things your *embedders* build on top. They are not your GA blockers.

**If the deliverable is a hosted NoSQL product**, then the engine is one component and the bar is much higher: MVCC/transactions, secondary indexes, owned snapshots and point-in-time backup, multi-tenant isolation under load, operational tooling, the works. That's a different company-scale bet and a year-shaped roadmap.

My read from the evidence: ship the **engine** to beta/GA first. It's the artifact that's nearly done, the surface is small, and "production-grade embeddable KV" is a coherent thing to release. Let the server be a separate track that consumes a GA'd engine. Do not let "we might build a hosted product someday" hold the engine hostage, and do not start MVCC/indexes/transactions until the engine is sealed, because those are product bets, not engine bets.

Here's your table, filled with my calls for the **engine-GA** definition:

| Area | Call |
|------|------|
| 24h soak, bounded RSS/SST count | **Hard blocker**, but run it *last*, as confirmation of the fixes below |
| Sub-ms p99 at 3k (this harness) | **Not a blocker.** Publish an envelope; gate on stability |
| Tombstone/version GC + space-amp metric | **Hard blocker.** Unbounded disk is an outage |
| Disk-full injection test | **Hard blocker.** Untested real-world failure mode |
| Read path (KV cache, bigger block cache, blooms) | **Blocker for a credible perf SLA**; the SLA value itself is configurable |
| Owned snapshots / backup | **Nice-to-have** for embedded, **blocker** for hosted product |
| Fuzz + Miri + CI gates | **Nice-to-have hardening.** Cheap insurance, not GA-gating |
| Second compaction worker | **Skip.** write_amp 2.2, worker not saturated. Closed chapter |
| MVCC / transactions / indexes | **Product features.** Only blockers if you chose "hosted product" |

---

## 7. Stop / Start / Continue

**Stop:**
- Grinding apply/poll async strategies for latency. The bottleneck is cache invalidation, not apply timing (§2).
- Treating 3000 ops/sec and sub-ms p99 as the graduation bar. It's a setting, not an SLA (§5).
- Adding compaction machinery (workers, scheduling, repack variants). That chapter is closed and the data proves it.
- Running the synthetic per-rate soak as the pass/fail oracle.

**Start:**
- Measuring space amplification and reclaiming space (§4). Highest-priority correctness work.
- A KV/result cache resilient to compaction invalidation (§3, lever 1). Highest-priority perf work for this workload.
- A deployment-shaped benchmark: release build, server process, fixed memory, realistic mix (§5).
- Writing the explicit **engine-GA definition** so "done" stops moving (§6).

**Continue:**
- The apply-thread architecture. It's more correct. Keep it, just don't expect perf from it.
- The honest stability gates (repack, no-progress, write_amp, bytes-derived L2). They work.
- Crash/failpoint discipline. Extend it to disk-full.

---

## 8. The ordered next five bets

1. **Bigger block cache + verify compaction `fill_cache=false` + bloom filters on L2.** Hours of work, moves throughput and p99 now, and confirms the §2 diagnosis. Do this first because it's cheap and it validates the theory before you invest in lever 1.
2. **Space reclamation + space-amp metric + disk-full injection test.** The correctness/availability blocker. Without it there is no 24/7 story.
3. **KV/result cache.** The structural read-path fix for an 80%-hot-key workload under compaction. Bigger design effort, biggest durable win.
4. **Redefine graduation (stability vs SLO) and build the deployment-shaped harness.** Turns "are we ready" from a feeling into a checklist.
5. **24h soak as confirmation, then write the engine-GA definition and ship beta.** Run the long soak only after 1–3, because today you already know what it would show: throughput erodes as the working set outgrows the cache, and disk grows from deletes. Buy the long soak as proof the fixes worked, not as a discovery run.

---

## 9. Honest rating

As an embeddable LSM engine, this is a solid **6.5/10, late beta.** The durability and compaction core is genuinely strong and the zero-error 90-minute run at ~2.8k mixed ops/sec on a single worker is real. The gaps that keep it from GA are concrete and short: space reclamation, a read path that doesn't erode under growth, a disk-full test, and a long-soak confirmation. That's a handful of focused work items, not a rewrite. You are closer than the red gates make it feel, and the path is the five bets above, in that order.

The one-sentence version: the engine is done being sick, the remaining work is finishing and proving, and the highest-value thing you can do this week is stop optimizing the part that already works and go measure the disk.

---

### Source basis
AdCache and AC-Key research (EDBT 2026 / USENIX ATC 2020): block caches are invalidated by compaction rewriting SSTables, causing hit-rate collapse; result/KV/key-pointer caches are resilient to it. RocksDB Block Cache wiki and `row_cache` (separate point-lookup cache for this exact problem). RocksDB Leveled Compaction wiki (bottommost/periodic compaction for tombstone and version reclamation). Citations in the accompanying chat message.