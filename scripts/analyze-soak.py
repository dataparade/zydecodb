#!/usr/bin/env python3
"""Analyze a zydecodb-engine soak run.

Reads a JSONL metrics file produced by `engine-soak --metrics-out` and prints
a human-readable summary. After a baseline 24h run produces stable numbers,
flip the `TODO_*` ceiling constants below from `None` to real values and
this script becomes a CI gate.

Usage:
    scripts/analyze-soak.py soak-runs/<timestamp>/metrics.jsonl

Exit codes:
    0  all observed numbers within ceilings (or no ceilings set yet)
    1  one or more ceilings breached
    2  malformed input

Steady-state computation:
    "Steady state" excludes the first 10% of samples (warm-up: empty caches,
    cold memtable). The remaining samples are summarized as min / mean / max /
    p99 per metric. The ceilings (when set) apply to the max or p99 of the
    steady-state window, never to the warm-up.
"""

from __future__ import annotations

import json
import math
import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

# ---------------------------------------------------------------------------
# CEILINGS — DO NOT INVENT VALUES.
#
# Each ceiling stays `None` until a real baseline run has been performed and
# its steady-state numbers are known. Flipping a ceiling from None to a real
# number is a deliberate PR with the baseline JSONL attached as evidence.
#
# The rule of thumb (per the plan): set the ceiling at 20-30% above the
# observed steady-state value. Tighter than that produces flakes; looser than
# that lets real regressions slip.

TODO_RSS_BYTES_CEILING: int | None = None
# Baseline: ga24-fixed-20260801 (24h @ 3k ops, 255M ops). Observed max 33 FDs;
# ceiling ~45% above (FDs arrive in lumps: sstable readers + WAL + manifest).
TODO_OPEN_FDS_CEILING: int | None = 48
# Baseline: same run — paced 3k ops never sustains flush backlog (mean 0.00).
TODO_IMMUTABLE_MEMTABLE_MEAN_CEILING: float | None = 1.0
# Baseline: same run — observed max 0; allow freeze-while-flushing transient
# (one immutable plus one more mid-freeze), catch real backlog above that.
TODO_IMMUTABLE_MEMTABLE_MAX_CEILING: int | None = 2
TODO_P99_US_CEILING: int | None = 500
TODO_P999_US_CEILING: int | None = 2000
# Rare tail on timed ops (scan drain, cache cold miss). 50ms poll cadence means
# apply spikes do not land on every op; 200ms catches real regressions only.
TODO_MAX_US_CEILING: int | None = 200_000
TODO_OPS_PER_SEC_MIN_RATIO: float | None = 0.95  # observed/target
TODO_ERRORS_TOTAL_CEILING: int | None = 0
# MEMO3: compaction health gates (repack spiral signature + redundant rewrite).
TODO_REPACK_TOTAL_MAX_CEILING: int | None = 0
TODO_WRITE_AMP_MAX_CEILING: float | None = 5.0
TODO_REJECTED_NO_PROGRESS_MAX_CEILING: int | None = 0
TODO_SPACE_AMP_MAX_CEILING: float | None = 3.0
# Bytes-derived L2 file count slack (not a flat constant — see MEMO3 §3).
TARGET_FILE_BYTES: int = 64 * 1024 * 1024
L2_FILE_COUNT_SLACK: int = 2
TODO_RSS_BYTES_STABILITY_CEILING: int | None = None
# Headroom beyond configured caches when deriving RSS from soak header (MB).
# Baseline ga24-fixed-20260801 (24h @ 3k ops): RSS plateaus at ~1076 MB by
# hour 3 and stays flat for 21h (no leak), with transient spikes to 1142 MB
# (~66 MB of allocator/compaction scratch above the plateau). 512 MB puts the
# derived ceiling ~17% above that observed max, per the 20-30% rule applied to
# a workload whose metadata term already scales with live sstables.
DEFAULT_RSS_HEADROOM_MB = 512
# Estimated pinned index+bloom per open reader (MB), for derived RSS ceiling
# when the run's live sstable count is unknown.
DEFAULT_PER_READER_METADATA_MB = 1.5
# Pinned index+bloom+footer per live sstable (MB). The reader-cap estimate
# undercounts badly at 20+ live 64MB files (~11MB observed per file in the
# memo5 24h), so when samples are available the metadata term scales with
# observed live sstables instead of the open-reader cap.
DEFAULT_PER_SSTABLE_METADATA_MB = 12.0

# ---------------------------------------------------------------------------


def derived_rss_ceiling(header: dict, max_live_sstables: int | None = None) -> int | None:
    """RSS stability ceiling from soak header cache budgets + headroom (MEMO6).

    Reader metadata scales with observed live sstable count when provided —
    leaks still trip the gate, legitimate topology growth does not.
    """
    block_mb = header.get("block_cache_mb")
    if block_mb is None:
        return None
    result_mb = header.get("result_cache_mb", 0)
    headroom_mb = int(os.environ.get("SOAK_RSS_HEADROOM_MB", DEFAULT_RSS_HEADROOM_MB))
    if max_live_sstables is not None:
        per_file_mb = float(
            os.environ.get(
                "SOAK_PER_SSTABLE_METADATA_MB", DEFAULT_PER_SSTABLE_METADATA_MB
            )
        )
        metadata_mb = max_live_sstables * per_file_mb
    else:
        max_readers = int(header.get("max_open_readers", 128))
        per_reader_mb = float(
            os.environ.get("SOAK_PER_READER_METADATA_MB", DEFAULT_PER_READER_METADATA_MB)
        )
        metadata_mb = max_readers * per_reader_mb
    return int((int(block_mb) + int(result_mb) + metadata_mb + headroom_mb) * 1024 * 1024)


# ---------------------------------------------------------------------------


@dataclass
class Sample:
    elapsed_secs: int
    ops_done: int
    ops_per_sec_observed: float
    rss_bytes: int
    open_fds: int
    immutable_memtable_count: int
    live_sstable_count: int
    wal_segment_count: int
    last_durable_seq: int
    p50_us: int
    p99_us: int
    p999_us: int
    max_us: int
    sstables_l2: int = 0
    l2_median_size_bytes: int = 0
    compaction_write_amp: float = 0.0
    compaction_repack_total: int = 0
    compaction_rejected_no_progress: int = 0
    block_cache_hits_window: int = 0
    block_cache_misses_window: int = 0
    block_cache_hit_rate_window: float = 0.0
    poll_max_us: int = 0
    poll_mean_us: int = 0
    apply_max_us: int = 0
    compaction_jobs_l0_window: int = 0
    compaction_jobs_l1_window: int = 0
    compaction_jobs_l2_window: int = 0
    manifest_sync_max_us: int = 0
    errors_window: int = 0
    disk_bytes_total: int = 0
    logical_live_bytes: int = 0
    space_amplification: float = 0.0
    tombstones_dropped_window: int = 0
    result_cache_hit_rate_window: float = 0.0


def parse_jsonl(path: Path) -> tuple[dict, list[Sample], dict | None]:
    header: dict | None = None
    samples: list[Sample] = []
    summary: dict | None = None
    with path.open() as f:
        for lineno, raw in enumerate(f, start=1):
            raw = raw.strip()
            if not raw:
                continue
            try:
                obj = json.loads(raw)
            except json.JSONDecodeError as e:
                sys.stderr.write(f"line {lineno}: bad JSON: {e}\n")
                sys.exit(2)
            kind = obj.get("kind")
            if kind == "header":
                header = obj
            elif kind == "sample":
                samples.append(
                    Sample(
                        elapsed_secs=obj["elapsed_secs"],
                        ops_done=obj["ops_done"],
                        ops_per_sec_observed=obj["ops_per_sec_observed"],
                        rss_bytes=obj["rss_bytes"],
                        open_fds=obj["open_fds"],
                        immutable_memtable_count=obj["immutable_memtable_count"],
                        live_sstable_count=obj["live_sstable_count"],
                        wal_segment_count=obj["wal_segment_count"],
                        last_durable_seq=obj["last_durable_seq"],
                        p50_us=obj["p50_us"],
                        p99_us=obj["p99_us"],
                        p999_us=obj["p999_us"],
                        max_us=obj["max_us"],
                        sstables_l2=obj.get("sstables_l2", 0),
                        l2_median_size_bytes=obj.get("l2_median_size_bytes", 0),
                        compaction_write_amp=obj.get("compaction_write_amp", 0.0),
                        compaction_repack_total=obj.get("compaction_repack_total", 0),
                        compaction_rejected_no_progress=obj.get(
                            "compaction_rejected_no_progress", 0
                        ),
                        block_cache_hits_window=obj.get("block_cache_hits_window", 0),
                        block_cache_misses_window=obj.get("block_cache_misses_window", 0),
                        block_cache_hit_rate_window=obj.get(
                            "block_cache_hit_rate_window", 0.0
                        ),
                        poll_max_us=obj.get("poll_max_us", 0),
                        poll_mean_us=obj.get("poll_mean_us", 0),
                        apply_max_us=obj.get("apply_max_us", 0),
                        compaction_jobs_l0_window=obj.get("compaction_jobs_l0_window", 0),
                        compaction_jobs_l1_window=obj.get("compaction_jobs_l1_window", 0),
                        compaction_jobs_l2_window=obj.get("compaction_jobs_l2_window", 0),
                        manifest_sync_max_us=obj.get("manifest_sync_max_us", 0),
                        errors_window=obj.get("errors_window", 0),
                        disk_bytes_total=obj.get("disk_bytes_total", 0),
                        logical_live_bytes=obj.get("logical_live_bytes", 0),
                        space_amplification=obj.get("space_amplification", 0.0),
                        tombstones_dropped_window=obj.get(
                            "tombstones_dropped_window", 0
                        ),
                        result_cache_hit_rate_window=obj.get(
                            "result_cache_hit_rate_window", 0.0
                        ),
                    )
                )
            elif kind == "summary":
                summary = obj
            else:
                sys.stderr.write(f"line {lineno}: unknown kind {kind!r}\n")
    if header is None:
        sys.stderr.write("no header line found\n")
        sys.exit(2)
    return header, samples, summary


def steady_state(samples: list[Sample]) -> list[Sample]:
    """Drop the first 10% of samples as warm-up.

    For short runs this might leave very few samples; the analyzer handles
    that case by falling back to the full set (with a note).
    """
    if len(samples) < 10:
        return samples
    drop = max(1, len(samples) // 10)
    return samples[drop:]


def percentile(values: Iterable[int | float], p: float) -> float:
    vals = sorted(values)
    if not vals:
        return 0.0
    k = (p / 100.0) * (len(vals) - 1)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return float(vals[int(k)])
    return vals[f] + (vals[c] - vals[f]) * (k - f)


def fmt_bytes(n: int | float) -> str:
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def summarize_int(name: str, values: list[int]) -> dict:
    if not values:
        return {"name": name, "min": 0, "mean": 0, "max": 0, "p99": 0}
    return {
        "name": name,
        "min": min(values),
        "mean": sum(values) / len(values),
        "max": max(values),
        "p99": percentile(values, 99),
    }


def report(
    header: dict,
    samples: list[Sample],
    summary: dict | None,
    mode: str = "stability",
    perf_fail: bool = False,
) -> int:
    print("=" * 72)
    print("zydecodb-engine soak analysis")
    print("=" * 72)
    print(f"target: {header['hours']}h @ {header['ops_per_sec_target']} ops/sec")
    print(
        f"mix: {header['put_pct']}% put / {header['get_pct']}% get / "
        f"{100 - header['put_pct'] - header['get_pct']}% del"
    )
    print(f"hot-pct: {header['hot_pct']}% | val: {header['val_min']}-{header['val_max']}B")
    print(f"seed: {header['seed']} | sample every {header['sample_every_secs']}s")
    if "block_cache_mb" in header:
        print(
            f"cache: block={header['block_cache_mb']} MB, "
            f"result={header.get('result_cache_mb', 0)} MB"
        )
    print(f"samples collected: {len(samples)}")
    print()

    steady = steady_state(samples)
    print(f"steady-state window: {len(steady)} samples (warm-up dropped)")
    if len(steady) < 10:
        print("  WARN: tiny steady-state window; numbers below are unreliable")
    print()

    if steady:
        rss = summarize_int("RSS", [s.rss_bytes for s in steady])
        fds = summarize_int("Open FDs", [s.open_fds for s in steady])
        immut = summarize_int(
            "Immutable memtables", [s.immutable_memtable_count for s in steady]
        )
        ssts = summarize_int("Live SSTables", [s.live_sstable_count for s in steady])
        l2 = summarize_int("SSTables L2", [s.sstables_l2 for s in steady])
        walsegs = summarize_int(
            "WAL segments", [s.wal_segment_count for s in steady]
        )
        ops = summarize_int(
            "Ops/sec (observed)",
            [int(s.ops_per_sec_observed) for s in steady],
        )
        p50 = summarize_int("p50 (µs)", [s.p50_us for s in steady])
        p99 = summarize_int("p99 (µs)", [s.p99_us for s in steady])
        p999 = summarize_int("p999 (µs)", [s.p999_us for s in steady])
        mx = summarize_int("max (µs)", [s.max_us for s in steady])

        # Pretty table.
        def row(stats: dict, fmt=str):
            print(
                f"  {stats['name']:<24} "
                f"min={fmt(stats['min'])} "
                f"mean={fmt(int(stats['mean']))} "
                f"p99={fmt(int(stats['p99']))} "
                f"max={fmt(stats['max'])}"
            )

        print("Resource usage:")
        row(rss, fmt_bytes)
        row(fds)
        print()
        print("Engine state:")
        row(immut)
        row(ssts)
        row(l2)
        row(walsegs)
        print()
        print("Throughput:")
        row(ops)
        print()
        print("Latency:")
        row(p50)
        row(p99)
        row(p999)
        row(mx)
        print()

    if summary:
        print("Final summary line:")
        print(
            f"  total: {summary['total_secs']:.1f}s, "
            f"{summary['total_ops']} ops, "
            f"avg {summary['avg_ops_per_sec']:.1f} ops/sec"
        )
        print(
            f"  errors: {summary['errors']} | shutdown: "
            f"{'OK' if summary['shutdown_ok'] else 'FAIL'} "
            f"in {summary['shutdown_secs']:.3f}s"
        )
        print(
            f"  final: rss={fmt_bytes(summary['final_rss_bytes'])}, "
            f"fds={summary['final_open_fds']}, "
            f"ssts={summary['final_live_sstable_count']}, "
            f"wal_segs={summary['final_wal_segment_count']}"
        )
        print()

    # --- Ceiling checks ---
    stability_breaches: list[str] = []
    perf_breaches: list[str] = []

    def check(
        label: str,
        observed,
        ceiling,
        op=lambda obs, c: obs > c,
        bucket: list[str] | None = None,
    ):
        target = bucket if bucket is not None else stability_breaches
        if ceiling is None:
            print(f"  [skip ] {label}: no ceiling set (run baseline first)")
            return
        if op(observed, ceiling):
            print(f"  [BREACH] {label}: observed {observed} vs ceiling {ceiling}")
            target.append(label)
        else:
            print(f"  [pass ] {label}: observed {observed} <= ceiling {ceiling}")

    run_stability = mode in ("stability", "all")
    run_perf = mode in ("perf", "all")

    print(f"Ceiling checks (mode={mode}):")
    if run_stability:
        print("  -- stability gates --")
    if summary is not None and run_stability:
        check(
            "Total op errors",
            summary.get("errors", 0),
            TODO_ERRORS_TOTAL_CEILING,
        )
    if steady and run_stability:
        max_live_ssts = max(s.live_sstable_count for s in steady)
        rss_ceiling = (
            derived_rss_ceiling(header, max_live_ssts)
            or TODO_RSS_BYTES_STABILITY_CEILING
            or TODO_RSS_BYTES_CEILING
        )
        if rss_ceiling is not None and derived_rss_ceiling(header, max_live_ssts) is not None:
            headroom_mb = int(
                os.environ.get("SOAK_RSS_HEADROOM_MB", DEFAULT_RSS_HEADROOM_MB)
            )
            per_file_mb = float(
                os.environ.get(
                    "SOAK_PER_SSTABLE_METADATA_MB", DEFAULT_PER_SSTABLE_METADATA_MB
                )
            )
            metadata_mb = int(max_live_ssts * per_file_mb)
            print(
                f"  RSS ceiling (derived): "
                f"{rss_ceiling // (1024 * 1024)} MB "
                f"(caches + {metadata_mb} MB sstable metadata "
                f"({max_live_ssts} live files) + {headroom_mb} MB headroom)"
            )
        check(
            "RSS max (bytes)",
            max(s.rss_bytes for s in steady),
            rss_ceiling,
        )
        check(
            "Open FDs max",
            max(s.open_fds for s in steady),
            TODO_OPEN_FDS_CEILING,
        )
        check(
            "Immutable memtable mean",
            sum(s.immutable_memtable_count for s in steady) / max(1, len(steady)),
            TODO_IMMUTABLE_MEMTABLE_MEAN_CEILING,
        )
        check(
            "Immutable memtable max",
            max(s.immutable_memtable_count for s in steady),
            TODO_IMMUTABLE_MEMTABLE_MAX_CEILING,
        )
        check(
            "compaction_repack_total max",
            max(s.compaction_repack_total for s in steady),
            TODO_REPACK_TOTAL_MAX_CEILING,
        )
        amps = [s.compaction_write_amp for s in steady if s.compaction_write_amp > 0]
        if amps:
            check(
                "compaction_write_amp max",
                round(max(amps), 4),
                TODO_WRITE_AMP_MAX_CEILING,
            )
        else:
            print("  [skip ] compaction_write_amp max: no non-zero samples")
        check(
            "compaction_rejected_no_progress max",
            max(s.compaction_rejected_no_progress for s in steady),
            TODO_REJECTED_NO_PROGRESS_MAX_CEILING,
        )
        space_amps = [
            s.space_amplification
            for s in steady
            if s.space_amplification > 0 and s.logical_live_bytes > 0
        ]
        if space_amps:
            check(
                "space_amplification max",
                round(max(space_amps), 4),
                TODO_SPACE_AMP_MAX_CEILING,
            )
        else:
            print("  [skip ] space_amplification max: no measured samples")
        # Bytes-derived L2: file count should track ceil(l2_bytes / target_file).
        l2_overshoot = 0
        for s in steady:
            if s.sstables_l2 == 0 or s.l2_median_size_bytes == 0:
                continue
            est_l2_bytes = s.sstables_l2 * s.l2_median_size_bytes
            expected = math.ceil(est_l2_bytes / TARGET_FILE_BYTES)
            if s.sstables_l2 > expected + L2_FILE_COUNT_SLACK:
                l2_overshoot = max(l2_overshoot, s.sstables_l2 - expected)
        if any(s.sstables_l2 > 0 for s in steady):
            label = (
                f"L2 file count vs bytes-derived expected+{L2_FILE_COUNT_SLACK}"
            )
            if l2_overshoot > 0:
                print(f"  [BREACH] {label}: overshoot by {l2_overshoot} files")
                stability_breaches.append(label)
            else:
                print(f"  [pass ] {label}")
        medians = [s.l2_median_size_bytes for s in steady if s.l2_median_size_bytes > 0]
        if medians:
            l2_median_min = min(medians)
            floor = TARGET_FILE_BYTES // 2
            if l2_median_min < floor:
                print(
                    f"  [BREACH] L2 median file size min (bytes): "
                    f"observed {l2_median_min} vs floor {floor}"
                )
                stability_breaches.append("L2 median file size min (bytes)")
            else:
                print(
                    f"  [pass ] L2 median file size min (bytes): "
                    f"observed {l2_median_min} >= floor {floor}"
                )

    if run_perf and steady:
        print("  -- performance SLOs (informational unless --perf-fail) --")
        check(
            "p99 max (µs)",
            max(s.p99_us for s in steady),
            TODO_P99_US_CEILING,
            bucket=perf_breaches,
        )
        check(
            "p999 max (µs)",
            max(s.p999_us for s in steady),
            TODO_P999_US_CEILING,
            bucket=perf_breaches,
        )
        check(
            "max single-op (µs)",
            max(s.max_us for s in steady),
            TODO_MAX_US_CEILING,
            bucket=perf_breaches,
        )
        target = header["ops_per_sec_target"]
        observed_mean = sum(s.ops_per_sec_observed for s in steady) / max(1, len(steady))
        ratio = observed_mean / target if target else 0
        check(
            f"Ops/sec ratio (observed mean / {target})",
            round(ratio, 3),
            TODO_OPS_PER_SEC_MIN_RATIO,
            op=lambda obs, c: obs < c,
            bucket=perf_breaches,
        )
    print()

    # Honesty warnings (operational regressions).
    if steady:
        target_file = TARGET_FILE_BYTES
        hit_rates = [
            s.block_cache_hit_rate_window
            for s in steady
            if s.block_cache_hits_window + s.block_cache_misses_window > 0
        ]
        if hit_rates and min(hit_rates) < 0.85:
            print(
                f"  [WARN ] block_cache hit rate min {min(hit_rates):.3f} "
                f"(< 0.85 — live set outrunning cache?)"
            )
        poll_maxes = [s.poll_max_us for s in steady if s.poll_max_us > 0]
        if poll_maxes and max(poll_maxes) > 5_000:
            print(
                f"  [WARN ] poll_compaction max {max(poll_maxes)} µs "
                f"(apply/fsync on hot path?)"
            )
        apply_maxes = [s.apply_max_us for s in steady if s.apply_max_us > 0]
        if apply_maxes and max(apply_maxes) > 5_000:
            print(
                f"  [WARN ] compaction apply max {max(apply_maxes)} µs "
                f"(catalog apply stall — manifest fsync should be separate)"
            )
        manifest_sync_maxes = [s.manifest_sync_max_us for s in steady if s.manifest_sync_max_us > 0]
        if manifest_sync_maxes and max(manifest_sync_maxes) > 5_000:
            print(
                f"  [WARN ] manifest group-commit fsync max {max(manifest_sync_maxes)} µs"
            )
        err_windows = [s.errors_window for s in steady if s.errors_window > 0]
        if err_windows:
            print(
                f"  [WARN ] errors_window max {max(err_windows)} "
                f"(EngineBusy / backpressure rejects in sample)"
            )
        l0_burst = max((s.compaction_jobs_l0_window for s in steady), default=0)
        l1_burst = max((s.compaction_jobs_l1_window for s in steady), default=0)
        if l0_burst + l1_burst > 30:
            print(
                f"  [WARN ] upper-level compaction burst "
                f"(L0={l0_burst}/min L1={l1_burst}/min max in window)"
            )
        print()

    if stability_breaches:
        print(f"FAIL: {len(stability_breaches)} stability ceiling(s) breached")
        for b in stability_breaches:
            print(f"  - {b}")
    elif run_stability:
        print("OK: no stability ceiling breaches")

    if perf_breaches:
        print(f"PERF: {len(perf_breaches)} performance SLO(s) missed")
        for b in perf_breaches:
            print(f"  - {b}")
    elif run_perf:
        print("PERF OK: no performance SLO misses")

    if stability_breaches:
        return 1
    if perf_breaches and (mode == "perf" or perf_fail or mode == "all"):
        return 1
    return 0


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Analyze zydecodb-engine soak JSONL")
    parser.add_argument("metrics_jsonl", type=Path)
    parser.add_argument(
        "--mode",
        choices=("stability", "perf", "all"),
        default="stability",
        help="stability=GA gates (default); perf=SLO tracking; all=both",
    )
    parser.add_argument(
        "--perf-fail",
        action="store_true",
        help="Exit 1 on perf SLO misses when mode=stability",
    )
    args = parser.parse_args()
    if not args.metrics_jsonl.is_file():
        sys.stderr.write(f"not a file: {args.metrics_jsonl}\n")
        sys.exit(2)
    header, samples, summary = parse_jsonl(args.metrics_jsonl)
    sys.exit(report(header, samples, summary, args.mode, args.perf_fail))


if __name__ == "__main__":
    main()
