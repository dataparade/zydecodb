# ParadeKV — Research Memo #6: The Metadata-in-LRU Call Was Wrong, and Your Minute-6 Numbers Say the Regression Isn't What You Think

**To:** Engine dev
**Re:** phase1-memo5-90m-v2 stopped at 6 minutes; correcting Memo #5 §3b
**Verdict:** You were right to stop, and my §3b recommendation (charge index and bloom into the data block-cache LRU) was the wrong path. It's RocksDB's non-default setting specifically because it hurts reads, and I gave it to you without the pinning that makes it tolerable. Own it. But your own data contradicts the mechanism you diagnosed: at minute 6 with 5 files, there isn't enough metadata in the cache to evict hot data, and your block-cache hit rate is flat. The regression is per-op path cost from the refactor, not metadata evicting data blocks. That distinction matters, because a separate metadata budget fixes the thing that isn't broken yet. Profile the op path first, then bound memory by capping open readers, not by charging metadata into the data cache. That gets you MEMO4's speed and GA's memory bound at the same time.

---

## 1. Owning the §3b miscall

Memo #5 §3b said to account index and filter blocks inside the block-cache budget to bound memory. That maps to RocksDB's `cache_index_and_filter_blocks = true`, which is **not** the default. The default is false, and the reason is exactly your result: with it on, index and filter blocks compete with data blocks for cache space, and reads get slower. RocksDB's own guidance is that turning it on hurts performance in most cases unless you also pin metadata (`pin_l0_filter_and_index_blocks_in_cache`, or `unpartitioned_pinning = kAll`) to stop it from thrashing. I recommended the memory-bounding half without the pinning half. Your instinct in option 1 (pin on the reader, don't LRU the metadata) is the RocksDB default and the correct call. I'm reversing my §3b.

Two cache recommendations from me in a row have now missed: the result cache (#4) and metadata-in-LRU (#5). Both were aimed at real goals (read perf, bounded memory) and both picked the wrong instrument. The model below is the one that actually serves both goals, and it's grounded in what your data shows rather than what I assumed.

---

## 2. Your data says the minute-6 regression is per-op, not capacity

You diagnosed it as: metadata fills the LRU and evicts hot data blocks. That's the right worry for hour two. It is not what's happening at minute 6, and three numbers in your own run say so.

**The metadata volume is too small to evict anything yet.** At minute 6 you have 5 files. Index plus bloom for a 64 MB SSTable is roughly 0.5 to 1.5 MB (bloom at 10 bits/key is ~125 KB; index depends on block size). Five files is a few megabytes of metadata against a 640 MB cache. That cannot displace 85 MB of hot data. The eviction mechanism you described is real, but it doesn't engage until you have far more files than this.

**Your hit rate is flat.** `block_cache_hit_rate_window` holds 0.91 to 0.97 the whole time. If metadata were evicting hot data blocks, the data hit rate would fall. It doesn't. You noted this yourself and waved it off with "hit rate can look fine while hot data is displaced," but at 5 files there's no displacement to hide. The hit rate is flat because the data blocks are fine.

**The one event you pointed at happens in both runs.** You cited the t=240 resident drop (166 to 81 MB, compaction reads, 88 ms apply). MEMO4 has the same event at the same point and held 2841 ops/sec at 45 µs. So that event is not the differentiator. It's the normal compaction-invalidation churn both runs share. The divergence is elsewhere.

Put together: same engine state, same files, same hit rate, same compaction event, and 7x the p50. When the cache contents are the same and the speed is different, the cost is in the **code path**, not the cache. And the p50 cliff lands exactly at t≈180s when you cross from single-file into multi-file reads, which means the cost scales with files probed per op. Per-file metadata access got more expensive in this changeset, and it compounds as files accumulate.

So the fix you proposed (separate metadata budget) addresses capacity, which isn't the binding constraint at minute 6. You could implement it and still be slow.

---

## 3. Profile the op path before changing the memory model

Don't run another 90-minute soak, and don't rebuild the memory model on a guess. Take a 60-second CPU profile (flamegraph) of the op path at minute 6, MEMO5 v2 versus MEMO4, same seed. The cliff is reproducible and isolated, so this is a five-minute experiment that tells you exactly what got slower. Candidates, in order of likelihood:

- **Arc refcount contention.** If `get_latest` and `might_contain` clone an `Arc` to the reader's metadata on every probe, the atomic refcount lives on a shared cacheline that ping-pongs between cores. Cost scales with probes per op times op rate, which is exactly the file-count-dependent shape you see. Fix: borrow with a lifetime instead of cloning, or take the reader-table read lock once per op and hold it across all probes.
- **A recency touch on pinned metadata.** If accessing the pinned metadata still pokes the LRU to update recency, that's a sharded-cache mutex on the hottest path. Even pinned, routing access through the cache costs a lock. Fix: access pinned metadata directly off the reader, never through the cache.
- **The result-cache removal moving load.** Every PUT's policy `get()` now runs the full per-file probe instead of short-circuiting on the result cache. That widens the per-file path's importance, so any per-probe regression hits PUTs (70% of ops, hence the p50 move) harder.
- **Metadata not actually staying pinned.** Confirm the `Arc` is held for the reader's lifetime and nothing is re-decoding. You fixed decode-on-every-call in v1; verify v2 didn't reintroduce a partial version.

The flamegraph will name one of these. The fix depends on which, and none of them is "give metadata its own budget."

---

## 4. The memory model that gives you both

The goal from Memo #5 stands: total memory bounded independent of dataset size. The way to get it without the regression is RocksDB's default model, not the one I sent you.

Keep MEMO4's fast path: metadata decoded once and pinned in table-reader memory, accessed lock-free. Do not put metadata bytes in the data LRU. Bound total metadata by bounding the **number of open readers** with a table/reader cache capped on file-handle count, which is exactly what RocksDB's `max_open_files` does. RocksDB keeps file descriptors in a table cache, and when the count exceeds the limit, files are evicted and their descriptors closed, which frees their pinned metadata. Then:

```
RSS = data_block_cache_cap
    + (max_open_readers × per_file_metadata)
    + memtables + WAL buffers + misc
```

Every term is configured. Nothing scales with dataset size except through `max_open_readers`, which you cap. At your current scale (14 files) set the cap high enough that nothing ever evicts, so there's zero perf cost today and the bound only engages at large data. When a GET touches a file whose reader was evicted, you reopen and re-decode its metadata: a cold, bounded, rare cost if the cap covers the working set. This is the RocksDB default (`cache_index_and_filter_blocks = false`), and their docs note metadata held this way is preloaded, non-evictable, and not counted against the block cache, so thrashing is structurally prevented.

This is your option 1, confirmed and made concrete. Don't charge metadata to the data LRU.

---

## 5. Two workload-specific levers that shrink the footprint you're bounding

Both reduce metadata memory directly, which makes the cap in §4 cheaper:

- **`optimize_filters_for_hits`: skip building bloom filters on the bottom level.** The bottom level holds ~90% of your data, and your 80%-hot-key GETs mostly hit keys that exist, so the L2 bloom rarely earns its keep. RocksDB skips the last-level filter for exactly this reason. This can cut bloom memory by most of its total.
- **Larger `block_size` (16-32 KB vs the 4 KB default).** Index size is one entry per data block, so fewer, larger blocks shrink the index linearly. Most production RocksDB runs 16-32 KB. This is the single biggest lever on index memory.

Neither changes correctness. Both make "bounded metadata" a smaller number to bound.

---

## 6. The result cache stays cut

Keep it out. It was right for RSS and 22% is a poor return on the memory. You're right that cutting it costs on GETs, but the answer to GET cost is not re-adding a result cache. It's bloom filters skipping disk on absent keys plus keeping hot data blocks resident, both of which you get from §4 and §5. If, after the per-op fix, GETs still lag, measure GET disk-reads-avoided per MB before adding any cache back. Don't re-add on feel.

---

## 7. We are not accepting a lower perf envelope

Your option 3 (document ~85-90% and don't gate GA on 95%) is the fallback, and I don't think you need it. MEMO4 proved this workload runs 95% at 50 µs p50 with stable compaction. The reader-cache-count model in §4 gives bounded memory without touching that fast path, so you should get MEMO4's speed and GA's memory bound together. Take option 3 only if profiling shows the per-op cost is intrinsic to a correct bounded design, which I doubt it will.

---

## 8. Order of operations

1. **Profile the minute-6 op path (§3).** Five-minute experiment. Find the per-op regression. Do this before anything else.
2. **Revert metadata out of the data LRU.** Restore MEMO4's pinned, lock-free metadata access. This alone should recover most of the speed.
3. **Bound metadata by capping open readers (§4),** a table cache on file-handle count, not bytes in the data LRU. This restores the GA memory bound the right way.
4. **Apply `optimize_filters_for_hits` and a larger `block_size` (§5)** to shrink the metadata footprint.
5. **Re-run, expecting MEMO4 speed (95% / ~50 µs p50) and a flat, bounded RSS.** Only then the 90-minute and 24-hour soaks.

The one-sentence version: my metadata-in-LRU call was wrong and it's reverted; your minute-6 data says the regression is per-op path cost, not metadata evicting data, so profile the op path, restore MEMO4's lock-free pinned metadata, and bound memory by capping the number of open readers instead of charging metadata into the data cache.

---

### Source basis
RocksDB Memory-Usage and Block-Cache wikis: `cache_index_and_filter_blocks` defaults to false; when true, index/filter blocks compete with data blocks and hurt read performance unless pinned (`pin_l0_filter_and_index_blocks_in_cache`, `unpartitioned_pinning = kAll`); when false, metadata is held in table-reader memory bounded by `max_open_files`, preloaded and non-evictable, not charged to block cache. RocksDB tuning guide: `max_open_files` governs the table cache that evicts file descriptors and their metadata. `optimize_filters_for_hits` skips bottom-level bloom filters (~90% of data). A documented ~33% read regression from enabling `cache_index_and_filter_blocks`. Citations in the accompanying chat message.