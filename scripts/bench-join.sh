#!/usr/bin/env bash
# $lookup benchmark (INLJ + bounded hash) with optional baseline compare.
#
# Usage:
#   scripts/bench-join.sh
#   COMPARE=1 scripts/bench-join.sh          # fail if docs_sec drops >20% vs baseline
#   BASELINE=path THRESHOLD_PCT=20 FACT_DOCS=1000000 scripts/bench-join.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
OUT="${OUT:-$REPO_ROOT/soak-runs/bench-join-$(date -u +%Y%m%dT%H%M%SZ).json}"
BASELINE="${BASELINE:-$REPO_ROOT/docs/soak-baselines/bench-join-baseline.json}"
COMPARE="${COMPARE:-0}"
THRESHOLD_PCT="${THRESHOLD_PCT:-20}"
FACT_DOCS="${FACT_DOCS:-100000}"
DIM_DOCS="${DIM_DOCS:-1000}"
RUNS="${RUNS:-3}"

mkdir -p "$(dirname "$OUT")"
echo "building bench-join (release)..." >&2
( cd "$REPO_ROOT" && cargo build --release -p zydecodb-document --bin bench-join ) >&2

BIN="$CARGO_TARGET_DIR/release/bench-join"
DATA_DIR="${DATA_DIR:-${TMPDIR:-/tmp}/zydeco-bench-join-$$}"
"$BIN" --data-dir "$DATA_DIR" --fact-docs "$FACT_DOCS" --dim-docs "$DIM_DOCS" \
  --scenario both --runs "$RUNS" | tee "$OUT"
rm -rf "$DATA_DIR"

if [[ "$COMPARE" != "1" ]]; then
  echo "results: $OUT (COMPARE=1 to gate vs baseline)" >&2
  exit 0
fi

if [[ ! -f "$BASELINE" ]]; then
  echo "missing baseline: $BASELINE" >&2
  exit 1
fi

# docs_sec is higher-is-better (inverted vs p99-style metrics): fail when the
# current run is MORE than THRESHOLD_PCT below baseline. probe_p99_us and
# rss_bytes keep the usual lower-is-better direction.
python3 - "$OUT" "$BASELINE" "$THRESHOLD_PCT" <<'PY'
import json, sys
cur_path, base_path, thr_s = sys.argv[1], sys.argv[2], sys.argv[3]
thr = float(thr_s) / 100.0
ABS_FLOOR = {"probe_p99_us": 100.0, "rss_bytes": 64 * 1024 * 1024}
cur = {s["scenario"]: s for s in json.load(open(cur_path))}
base = {s["scenario"]: s for s in json.load(open(base_path))}
failed = False
for scenario, b in base.items():
    c = cur.get(scenario)
    if c is None:
        print(f"scenario {scenario} missing from current run", file=sys.stderr)
        failed = True
        continue
    # Throughput: fail on regression (current < baseline * (1 - thr)).
    bb, cc = float(b["docs_sec"]), float(c["docs_sec"])
    floor = bb * (1.0 - thr)
    ratio = (cc - bb) / bb if bb > 0 else 0.0
    status = "OK" if cc >= floor else "FAIL"
    print(f"{scenario} docs_sec: current={cc:.0f} baseline={bb:.0f} "
          f"delta={ratio*100:+.1f}% floor={floor:.0f} (-{thr*100:.0f}%) [{status}]")
    if cc < floor:
        failed = True
    # Latency/RSS: fail on growth beyond threshold (with absolute slack).
    for key in ("probe_p99_us", "rss_bytes"):
        bb, cc = float(b[key]), float(c[key])
        limit = max(bb * (1.0 + thr), bb + ABS_FLOOR[key])
        ratio = (cc - bb) / bb if bb > 0 else 0.0
        status = "OK" if cc <= limit else "FAIL"
        print(f"{scenario} {key}: current={cc:.0f} baseline={bb:.0f} "
              f"delta={ratio*100:+.1f}% limit={limit:.0f} [{status}]")
        if cc > limit:
            failed = True
sys.exit(2 if failed else 0)
PY
