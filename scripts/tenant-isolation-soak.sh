#!/usr/bin/env bash
# Simulated pods noisy-neighbor soak. Measures:
#   steady:  delta = victim_put_p99(V|N) - victim_put_p99(V solo)   (ship ≤50ms)
#   ramp-up: idle while N floods, then V reclaim burst δ            (≤350ms)
#
# Usage:
#   ./scripts/tenant-isolation-soak.sh                 # both modes, 30s steady
#   ./scripts/tenant-isolation-soak.sh 20 50           # 20s steady, ship 50ms
#   MODE=rampup ./scripts/tenant-isolation-soak.sh     # reclaim only
#   MODE=steady ./scripts/tenant-isolation-soak.sh     # steady only
#   RELEASE=0 ./scripts/tenant-isolation-soak.sh       # debug build
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
SECONDS_PER="${1:-30}"
FAIL_DELTA_MS="${2:-50}"
SHIP_DELTA_MS="${3:-50}"
RAMPUP_FAIL_MS="${RAMPUP_FAIL_MS:-350}"
RAMPUP_IDLE="${RAMPUP_IDLE:-10}"
MODE="${MODE:-both}"
DATA_ROOT="${DATA_ROOT:-/tmp/zydeco-tenant-soak}"
JSON_OUT="${JSON_OUT:-$DATA_ROOT/summary.json}"
PROFILE_FLAG="--release"
if [[ "${RELEASE:-1}" == "0" ]]; then
  PROFILE_FLAG=""
fi

mkdir -p "$DATA_ROOT"
cd "$ROOT"
echo "building tenant-isolation-soak..."
# shellcheck disable=SC2086
cargo build -p zydecodb-engine --bin tenant-isolation-soak $PROFILE_FLAG

BIN="$ROOT/target/debug/tenant-isolation-soak"
if [[ -n "$PROFILE_FLAG" ]]; then
  BIN="$ROOT/target/release/tenant-isolation-soak"
fi

echo "running soak: mode=${MODE} ${SECONDS_PER}s/steady fail_delta=${FAIL_DELTA_MS}ms rampup_δ=${RAMPUP_FAIL_MS}ms"
"$BIN" \
  --data-root "$DATA_ROOT" \
  --mode "$MODE" \
  --seconds "$SECONDS_PER" \
  --victim-ops-per-sec 50 \
  --noisy-writers 1 \
  --noisy-readers 1 \
  --memtable-mb 8 \
  --block-cache-mb 16 \
  --retry-budget-ms 500 \
  --fail-delta-ms "$FAIL_DELTA_MS" \
  --ship-delta-ms "$SHIP_DELTA_MS" \
  --rampup-idle-secs "$RAMPUP_IDLE" \
  --rampup-fail-delta-ms "$RAMPUP_FAIL_MS" \
  --min-success-ratio 0.85 \
  --json-out "$JSON_OUT"

echo "summary written to $JSON_OUT"
