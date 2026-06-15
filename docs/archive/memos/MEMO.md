# ParadeKV L2 Compaction — Research Memo

**To:** Engine dev
**Re:** Layer 4 (compaction churn / throughput collapse) and why it exists
**Verdict:** The repack ↔ promotion oscillation is real, but it's a symptom. The root cause is that L1→L2 promotion fragments the bottom level instead of merging into it. Fix the promotion geometry and the oscillation, the repack storms, and most of the throughput loss go away together. Whole-level repack should not exist as a steady-state mechanism.

---

## 1. Root cause: confirmed, but one layer deeper than the brief

The brief ranks "promotion geometry" as the quaternary hypothesis. It's primary. Everything else is downstream of it.

In leveled compaction as implemented by RocksDB, LevelDB, and Pebble, a level-N to level-N+1 compaction does **not** add a file to the output level. It picks one or more files from Ln, gathers the **overlapping** files in Ln+1, k-way merges all of them, and writes the result back into Ln+1 with outputs cut at the target file size. RocksDB's own wiki describes it plainly: pick at least one L1 file, merge it with the overlapping range of L2, place the results in L2.

The structural consequence is the thing you've been fighting to recreate by hand:

> The bottom level is **always packed**. Its file count is bounded by `ceil(live_bottom_level_bytes / target_file_bytes)` by construction. No "repack" mechanism exists in these engines because none is needed.

ParadeKV's L1→L2 path instead emits **one disjoint fragment per promotion**. Each promoted L1 file lands in L2 as its own small SSTable covering its own key range. Nothing merges it into the existing L2 files. So:

- File count is governed by **number of promotions**, not by data size.
- 200 MB of live data became **26 files** (~6 MB each, ~10x under your 64 MB target). That ratio is the fingerprint of fragmentation, not of a sizing bug.
- The only file-count reduction tool you have is whole-level repack, which reads and rewrites the entire level every time it fires.

Whole-level repack "fixed" the count because it's the one operation that re-packs disjoint fragments. But it fires at `count >= 4`, promotions immediately re-fragment back to 4, and you get a limit cycle. That's the oscillation. It isn't a tuning problem in the repack trigger. It's that repack is doing a job that overlap-merge is supposed to do incrementally and cheaply on every promotion.

### Why this also explains the throughput and cache numbers

Repack reads **all** of L2 and rewrites it on every cycle. At 80–200 MB that's 80–200 MB of read+write per repack, fired repeatedly. Two follow-on effects, both in your metrics:

- **Block cache thrash.** A full L2 scan streams ~200 MB through a 256 MB cache and evicts the hot user blocks. That's your +766k misses in one minute.
- **Apply contention.** The soak harness runs `poll_compaction()` before every op, and apply does manifest append + fsync + open new readers + unlink + `fsync_dir` synchronously on that hot loop. Frequent repacks mean frequent applies landing inside per-op latency. That's the 143 ms `max_us` spike, and a big share of the ops/sec drop even while p99 stays flat.

So the single worker isn't the bottleneck. The worker is **doing redundant work**. Stop the redundant work and one worker is plenty at this scale (1.4 MB/s of user writes, 200 MB live).

---

## 2. The fix, ranked

Do #1. Most of the list below becomes optional once #1 lands. I've marked what's load-bearing versus what's polish.

### #1 — Make L1→L2 an overlap-merge, delete repack from the steady state. *(Load-bearing. Do this first.)*

Change the L1→L2 plan so the input set is: the chosen L1 file(s) **plus every L2 file whose key range overlaps them**. K-way merge the union (you already have `MergeMode::Raw` and target-size cutting), emit outputs cut at `target_file_bytes`, write them back to L2. Same as RocksDB/LevelDB/Pebble.

Result: L2 stays packed at `ceil(live_bytes / 64 MB)` ≈ 3–4 files permanently. No fragments accumulate, so there's nothing to repack. Remove `plan_max_level_file_pressure`'s whole-level repack branch from the normal path. Keep at most a manual/cold-start repack behind the **hard** ceiling (count ≥ 8 or byte overflow), never at 4.

Expected impact: oscillation gone, repack storms gone, `compaction_jobs_total` drops by ~30–50x, write amplification drops to leveled-normal, cache thrash subsides. Cost: moderate. The merge machinery exists; the work is in the planner gathering overlapping output-level files into the input set and in apply handling the multi-input-multi-output edit.

One thing to expect and not panic about: with 80% hot keys, most L1 files overlap the **same** hot L2 file, so that hot file gets rewritten on nearly every promotion. That's correct and bounded. It's the normal write-amplification cost of leveled compaction (roughly the fanout). The cold 20% of the keyspace barely moves. This is the behavior `compaction_pri = kOldestLargestSeqFirst` exists to optimize in RocksDB, by preferring the coldest range first and leaving hot ranges resident. Worth adopting later, not required for the gate.

### #2 — Add ingest backpressure via pending-compaction-bytes. *(Load-bearing for throughput stability.)*

An LSM has no natural backpressure, so you have to manufacture it. RocksDB estimates "bytes pending compaction" and, above a soft limit, slows writes; above a hard limit, stops them. You don't need the full machinery. You need a debt estimate and a tiny per-op delay.

- Compute estimated pending bytes (sum over levels of `max(0, level_bytes - target_bytes)`, weighted by fanout for upper levels).
- Above a soft limit, sleep each PUT ~0.1–1 ms so ingest rate tracks sustainable compaction throughput instead of overrunning it and forcing the worker into thrash.

This turns the failure mode from "throughput collapses and jobs explode" into "throughput settles a hair below peak and stays there," which is what the 0.95 gate actually wants. It also makes the system stable under the eventual 24h scan/snapshot mix.

### #3 — Decouple apply from the poll loop. *(Directly targets the `max_us` gate.)*

The 143 ms spike is an fsync in apply executing inside a `poll_compaction()` call on the op hot loop. Options, cheapest first:

- **Batch/group-commit the manifest:** coalesce manifest edits and fsync once per batch rather than per job.
- **Move fsync off the critical path:** apply on a dedicated thread; `poll_compaction()` only flips a ready flag and swaps the catalog under a short lock.
- **Defer `fsync_dir`** to a periodic cadence rather than per-apply.

The harness calling `poll` before every op is intentional and fine. The fix is making apply cheap, not making poll rare.

### #4 — Stop compaction reads from filling the block cache. *(Cheap, kills the miss spike.)*

RocksDB reads compaction inputs with `fill_cache = false`. Mark your compaction-iterator block reads as non-cache-filling (or low priority). Hot user blocks stop getting evicted by a sweep through cold L2. This alone should clear most of the 766k-miss spike. Near-zero implementation cost.

### #5 — Replace drop-on-busy with single-slot coalescing. *(Polish, not a queue.)*

You don't need a multi-job pipeline. You need to not **lose** the highest-priority plan when the worker is busy. Keep one pending slot holding the best plan seen since last submit, overwrite it when a higher-score plan appears, submit it when the worker frees up. Combined with #2's backpressure, that's enough. A real queue is overkill at 200 MB.

### #6 — Second worker / subcompactions. *(Do NOT do this first. Probably never, at this scale.)*

RocksDB parallelizes via `max_background_jobs` and `max_subcompactions`; Pebble adds L0 sublevels for concurrent L0→Lbase drain. Both exist to relieve bottlenecks at hundreds-of-GB scale and during ingest floods, not for a 200 MB bottom level fed at 1.4 MB/s. Adding a second worker now masks the redundant-work problem instead of fixing it. Revisit only if, after #1–#5, a profiled soak still shows the single worker saturated. It won't.

### #7 — Levels and compaction style. *(No change.)*

Three levels is correct here. You have exactly one level of real data. Adding L3 only matters once L2 exceeds its 2.5 GB target, which you never approach. Switching to tiered/universal would trade read amplification for write amplification and make your 25% GET path worse for zero benefit. Stay leveled.

---

## 3. Config recommendations, with the math

**File-count floor.** `floor = ceil(live_L2_bytes / target_file_bytes)`. At ~200 MB live and 64 MB files that's **4**, which is exactly your steady target and well under the 10 gate. Keep `target_file_bytes = 64 MB`. The number was never wrong; promotion just ignored it.

**Ingest vs compaction rate.** 3000 ops/s × 0.70 PUT = 2100 PUT/s. Values 64–1024 B (call it ~550 B mean) plus key and framing ≈ 650 B/record → **~1.4 MB/s** of user writes. With 80% hot keys, live set churns far below raw ingest, which is why L2 sits at 80–200 MB rather than growing. One L2-bound compaction's worth of fresh data (64 MB) accumulates roughly every ~45 s of ingest. Even multiplying by fanout rewrites, you should see **single-digit to low-double-digit compactions per minute**, not 159/min.

**Pending-compaction-bytes limits (for #2).** Suggested starting point: soft = 4 × target ≈ **256 MB**, hard = **512 MB**, with the per-op delay scaled so steady-state ingest matches measured compaction throughput. Tune from the soak, but disable nothing to zero, that just reintroduces the runaway.

**Repack trigger (if you keep any).** Move it from `count >= 4` to the hard ceiling only (`count >= 8` or byte overflow). With #1 in place it should fire approximately never.

---

## 4. Metrics to export before the next soak

You ran a 30-minute soak to discover thrash. Instrument so you see it in 60 seconds.

| Metric | What it catches | Healthy range |
|---|---|---|
| `compaction_write_amp` = compaction bytes written / user bytes written | Redundant rewriting (repack thrash) | leveled-normal, low single digits; alarm if >10x |
| `compaction_read_amp` = compaction bytes read / user bytes written | Repack reading whole levels | bounded; spikes flag full-level reads |
| `pending_compaction_bytes` (estimated) | Backlog before it becomes a stall | below soft limit in steady state |
| `compaction_jobs_per_min` + `mean_bytes_per_job` | Flood of tiny jobs = thrash | low-double-digit jobs/min, jobs near target_file_bytes |
| `l2_file_size_histogram` | Fragmentation regression | median near 64 MB; alarm if median ≪ target |
| `repack_count` | Repack still firing | ~0 after the fix |
| `apply_latency` / `fsync_us_in_poll` | The `max_us` regressor | sub-ms; isolates the 143 ms class of spike |
| `block_cache_miss_rate` split by compaction vs user reads | Cache thrash from compaction reads | compaction-attributed misses near zero with #4 |

The two leading indicators are `compaction_write_amp` and `l2_file_size_histogram`. If write amp climbs or median file size collapses, fragmentation is back before file count or throughput ever moves.

---

## 5. Hypothesis scorecard

- **Primary (repack ↔ promotion oscillation):** Confirmed as the observed dynamic, but it's a symptom of fragmentation, not a root cause. Fixed indirectly by #1.
- **Secondary (repack threshold too aggressive):** True but irrelevant once repack leaves the steady state. Don't bother retuning the 4.
- **Tertiary (single worker + apply-on-poll):** Half right. Apply-on-poll is a real `max_us` contributor (#3). Single worker is **not** the bottleneck at this scale; it's just doing wasted work.
- **Quaternary (promotion geometry):** This is the actual root cause. Promote into the overlapping output files, not as a new fragment.

---

## 6. Order of operations

1. Implement overlap-merge for L1→L2; remove steady-state repack (#1).
2. Mark compaction reads non-cache-filling (#4). One-liner, immediate cache win.
3. Decouple/batch manifest apply and fsync (#3). Targets `max_us`.
4. Add pending-bytes backpressure with per-op micro-delay (#2). Targets throughput-ratio stability.
5. Single-slot coalescing submit (#5).
6. Re-run the 30-minute soak with the new metrics. Expect all five gates green: L2 ≈ 4, throughput ≥ 0.95, `max_us` under 10 ms.

Items 1–4 are where the gate lives. Everything after is hardening for the 24h scan/snapshot run.

---

### Source basis

RocksDB Leveled Compaction wiki (overlap-merge into output level, dynamic level targets, trivial move, `compaction_pri`), RocksDB Write Stalls wiki and pending-compaction-bytes internals (backpressure model), RocksDB output-file-boundary alignment blog (target-size cutting), Pebble compaction picker and L0 sublevels (concurrency rationale, scoring). Citations in the accompanying chat message.