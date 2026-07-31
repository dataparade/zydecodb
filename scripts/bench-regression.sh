#!/usr/bin/env bash
# Short paced put/get microbench with optional baseline compare.
#
# Usage:
#   scripts/bench-regression.sh
#   COMPARE=1 scripts/bench-regression.sh          # fail if p99 or RSS >20% vs baseline
#   BASELINE=path THRESHOLD_PCT=20 scripts/bench-regression.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
OUT="${OUT:-$REPO_ROOT/soak-runs/bench-regression-$(date -u +%Y%m%dT%H%M%SZ).json}"
BASELINE="${BASELINE:-$REPO_ROOT/docs/soak-baselines/bench-baseline.json}"
COMPARE="${COMPARE:-0}"
THRESHOLD_PCT="${THRESHOLD_PCT:-20}"
OPS="${OPS:-50000}"
WARMUP="${WARMUP:-5000}"

mkdir -p "$(dirname "$OUT")"
echo "building bench-regression (release)..." >&2
( cd "$REPO_ROOT" && cargo build --release -p zydecodb-engine --bin bench-regression ) >&2

BIN="$CARGO_TARGET_DIR/release/bench-regression"
DATA_DIR="${DATA_DIR:-${TMPDIR:-/tmp}/zydeco-bench-regression-$$}"
"$BIN" --data-dir "$DATA_DIR" --ops "$OPS" --warmup "$WARMUP" | tee "$OUT"
rm -rf "$DATA_DIR"

if [[ "$COMPARE" != "1" ]]; then
  echo "results: $OUT (COMPARE=1 to gate vs baseline)" >&2
  exit 0
fi

if [[ ! -f "$BASELINE" ]]; then
  echo "missing baseline: $BASELINE" >&2
  exit 1
fi

python3 - "$OUT" "$BASELINE" "$THRESHOLD_PCT" <<'PY'
import json, sys
cur_path, base_path, thr_s = sys.argv[1], sys.argv[2], sys.argv[3]
thr = float(thr_s) / 100.0
# Absolute slack so sub-10µs engine microbenches don't flap on GHA clocks.
ABS_FLOOR = {"p99_us": 100.0, "rss_bytes": 8 * 1024 * 1024}
cur = json.load(open(cur_path))
base = json.load(open(base_path))
failed = False
for key in ("p99_us", "rss_bytes"):
    b = float(base[key])
    c = float(cur[key])
    if b <= 0:
        print(f"baseline {key} is zero; cannot compare", file=sys.stderr)
        failed = True
        continue
    limit = max(b * (1.0 + thr), b + ABS_FLOOR[key])
    ratio = (c - b) / b
    status = "OK" if c <= limit else "FAIL"
    print(
        f"{key}: current={c:.0f} baseline={b:.0f} delta={ratio*100:+.1f}% "
        f"limit={limit:.0f} (max(+{thr*100:.0f}%, +{ABS_FLOOR[key]:.0f})) [{status}]"
    )
    if c > limit:
        failed = True
sys.exit(2 if failed else 0)
PY
