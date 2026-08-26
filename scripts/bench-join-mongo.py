#!/usr/bin/env python3
"""Mongo 8.x $lookup bench — same dataset and consume as bench-join.

Materializes every joined document (list(cursor)). Stream-and-drop is
disallowed: that would not match the Rust bench's retained result set.

Scenarios (explain-gated; NestedLoopJoin aborts):
  hash      — no index on dims.k, foreignField k   → HashJoin
  _id-probe — foreignField _id                     → IndexedLoopJoin
  k-probe   — index on dims.k, foreignField k      → IndexedLoopJoin
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from typing import Any

from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.database import Database

JOIN_STRATEGIES = ("IndexedLoopJoin", "HashJoin", "NestedLoopJoin")
BATCH = 5_000


def rss_bytes() -> int:
    try:
        with open("/proc/self/status", encoding="utf-8") as f:
            for line in f:
                if line.startswith("VmRSS:"):
                    return int(line.split()[1]) * 1024
    except OSError:
        return 0
    return 0


def walk_strategy(node: Any) -> str | None:
    if isinstance(node, dict):
        strat = node.get("strategy")
        if strat in JOIN_STRATEGIES:
            return strat
        stage = node.get("stage")
        if stage == "EQ_LOOKUP" and node.get("strategy") in JOIN_STRATEGIES:
            return node["strategy"]
        for v in node.values():
            found = walk_strategy(v)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = walk_strategy(item)
            if found:
                return found
    return None


def explain_strategy(db: Database, pipeline: list[dict], allow_disk: bool) -> str:
    raw = db.command(
        {
            "explain": {
                "aggregate": "facts",
                "pipeline": pipeline,
                "cursor": {},
                "allowDiskUse": allow_disk,
            },
            "verbosity": "queryPlanner",
        }
    )
    found = walk_strategy(raw)
    if found is None:
        raise SystemExit(f"no EQ_LOOKUP strategy in explain: {json.dumps(raw)[:2000]}")
    return found


def pipeline(foreign_field: str) -> list[dict]:
    return [
        {
            "$lookup": {
                "from": "dims",
                "localField": "k",
                "foreignField": foreign_field,
                "as": "dim",
            }
        }
    ]


def seed(db: Database, fact_docs: int, dim_docs: int) -> int:
    db.facts.drop()
    db.dims.drop()
    t0 = time.perf_counter()
    db.dims.insert_many(
        [{"_id": f"d{i}", "k": f"d{i}", "attrs": f"dim-{i}"} for i in range(dim_docs)],
        ordered=False,
    )
    pad = "x" * 100
    batch: list[dict] = []
    for i in range(fact_docs):
        batch.append(
            {
                "_id": f"f{i:07d}",
                "k": f"d{i % dim_docs}",
                "amount": i,
                "pad": pad,
            }
        )
        if len(batch) >= BATCH:
            db.facts.insert_many(batch, ordered=False)
            batch = []
    if batch:
        db.facts.insert_many(batch, ordered=False)
    return int((time.perf_counter() - t0) * 1000)


def run_one(
    facts: Collection,
    db: Database,
    *,
    scenario: str,
    foreign_field: str,
    expect: str,
    fact_docs: int,
    dim_docs: int,
    runs: int,
    load_ms: int,
    backfill_ms: int,
    mongo_version: str,
    wt_cache_gb: float,
) -> dict:
    pipe = pipeline(foreign_field)
    allow_disk = expect == "HashJoin"
    strategy = explain_strategy(db, pipe, allow_disk)
    if strategy == "NestedLoopJoin":
        raise SystemExit(
            f"{scenario}: explain reported NestedLoopJoin — Mongo's per-outer "
            "scan fallback, not a competitor. aborting."
        )
    if strategy != expect:
        raise SystemExit(
            f"{scenario}: expected {expect}, explain reported {strategy}"
        )

    def consume() -> int:
        rows = list(facts.aggregate(pipe, allowDiskUse=allow_disk))
        n = len(rows)
        if n != fact_docs:
            raise SystemExit(f"{scenario}: expected {fact_docs} rows, got {n}")
        # RSS while the result set is still retained — same consume
        # model as the Rust bench. Measuring after `rows` drops would
        # report an empty-client RSS and lie in the comparison.
        live = rss_bytes()
        consume.peak_rss = max(getattr(consume, "peak_rss", 0), live)  # type: ignore[attr-defined]
        return n

    consume.peak_rss = rss_bytes()  # type: ignore[attr-defined]
    consume()  # warmup
    elapsed: list[int] = []
    for _ in range(runs):
        t0 = time.perf_counter()
        consume()
        elapsed.append(int((time.perf_counter() - t0) * 1000))
    elapsed.sort()
    median_ms = elapsed[len(elapsed) // 2]
    secs = max(median_ms, 1) / 1000.0
    return {
        "scenario": scenario,
        "fact_docs": fact_docs,
        "dim_docs": dim_docs,
        "runs": runs,
        "elapsed_ms": median_ms,
        "docs_sec": fact_docs / secs,
        "probe_p50_us": 0,
        "probe_p99_us": 0,
        "rss_bytes": consume.peak_rss,  # type: ignore[attr-defined]
        "load_ms": load_ms,
        "backfill_ms": backfill_ms,
        "mongo_version": mongo_version,
        "explain_strategy": strategy,
        "wt_cache_gb": wt_cache_gb,
    }


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--uri", default="mongodb://127.0.0.1:27017")
    p.add_argument("--fact-docs", type=int, default=1_000_000)
    p.add_argument("--dim-docs", type=int, default=1_000)
    p.add_argument("--runs", type=int, default=3)
    p.add_argument("--wt-cache-gb", type=float, default=0.25)
    p.add_argument(
        "--scenario",
        default="both",
        choices=("id", "index", "hash", "both"),
    )
    args = p.parse_args()

    client = MongoClient(args.uri)
    db = client.zydeco_bench_join
    mongo_version = client.server_info()["version"]
    load_ms = seed(db, args.fact_docs, args.dim_docs)

    want_hash = args.scenario in ("hash", "both")
    want_inlj = args.scenario in ("id", "index", "both")
    results: list[dict] = []
    facts = db.facts

    if want_hash:
        # No secondary index: SBE should pick HashJoin (1K dims << 10K gate).
        db.dims.drop_indexes()
        results.append(
            run_one(
                facts,
                db,
                scenario="hash",
                foreign_field="k",
                expect="HashJoin",
                fact_docs=args.fact_docs,
                dim_docs=args.dim_docs,
                runs=args.runs,
                load_ms=load_ms,
                backfill_ms=0,
                mongo_version=mongo_version,
                wt_cache_gb=args.wt_cache_gb,
            )
        )

    backfill_ms = 0
    if want_inlj:
        t0 = time.perf_counter()
        db.dims.create_index("k")
        backfill_ms = int((time.perf_counter() - t0) * 1000)
        fields: list[tuple[str, str]] = []
        if args.scenario in ("id", "both"):
            fields.append(("_id-probe", "_id"))
        if args.scenario in ("index", "both"):
            fields.append(("k-probe", "k"))
        for scenario, foreign_field in fields:
            results.append(
                run_one(
                    facts,
                    db,
                    scenario=scenario,
                    foreign_field=foreign_field,
                    expect="IndexedLoopJoin",
                    fact_docs=args.fact_docs,
                    dim_docs=args.dim_docs,
                    runs=args.runs,
                    load_ms=load_ms,
                    backfill_ms=backfill_ms,
                    mongo_version=mongo_version,
                    wt_cache_gb=args.wt_cache_gb,
                )
            )

    json.dump(results, sys.stdout, indent=2)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
