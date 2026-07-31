#!/usr/bin/env bash
# crash-soak.sh — kill-loop crash recovery soak for zydecodb-engine.
#
# Starts engine-soak, SIGKILLs it at random intervals, reopens the same data
# dir, runs a CRC/integrity pass (open + force_flush + sample get/put), and
# loops. Gate: zero failed reopens; integrity must pass every cycle.
#
# Usage:
#   scripts/crash-soak.sh                     # ~35 min CI default
#   MINUTES=180 scripts/crash-soak.sh         # multi-hour VPS run
#   CYCLES=20 KILL_MIN_MS=200 KILL_MAX_MS=2000 scripts/crash-soak.sh
#
# Env:
#   MINUTES       Wall-clock budget (default 35). Ignored if CYCLES is set.
#   CYCLES        Exact cycle count (overrides MINUTES when set).
#   KILL_MIN_MS   Min soak runtime before SIGKILL (default 500).
#   KILL_MAX_MS   Max soak runtime before SIGKILL (default 5000).
#   OPS           engine-soak ops/sec target (default 800).
#   OUT_DIR       Output root (default ./soak-runs/crash-<timestamp>/).
#   RELEASE       1 = release build (default 1).
#
# Exit: 0 all cycles ok; 1 reopen/integrity failure; 3 bad invocation.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

MINUTES="${MINUTES:-35}"
CYCLES="${CYCLES:-}"
KILL_MIN_MS="${KILL_MIN_MS:-500}"
KILL_MAX_MS="${KILL_MAX_MS:-5000}"
OPS="${OPS:-800}"
RELEASE="${RELEASE:-1}"
SEED="${SEED:-42}"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/soak-runs/crash-$TIMESTAMP}"
DATA_DIR="$OUT_DIR/data"
WAL_DIR="$OUT_DIR/wal"
LOG="$OUT_DIR/crash-soak.log"
SUMMARY="$OUT_DIR/summary.json"

mkdir -p "$DATA_DIR" "$WAL_DIR"
: >"$LOG"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
PROFILE_FLAG=()
BIN_DIR="$CARGO_TARGET_DIR/debug"
if [[ "$RELEASE" == "1" ]]; then
  PROFILE_FLAG=(--release)
  BIN_DIR="$CARGO_TARGET_DIR/release"
fi

echo "building engine-soak + crash-soak-integrity..." | tee -a "$LOG"
(
  cd "$REPO_ROOT" && cargo build "${PROFILE_FLAG[@]}" \
    -p zydecodb-engine --bin engine-soak --bin crash-soak-integrity
) >>"$LOG" 2>&1

SOAK_BIN="$BIN_DIR/engine-soak"
INTEGRITY_BIN="$BIN_DIR/crash-soak-integrity"

rand_ms() {
  local lo=$1 hi=$2
  echo $(( lo + RANDOM % (hi - lo + 1) ))
}

deadline_epoch=$(( $(date +%s) + MINUTES * 60 ))
cycle=0
failed=0

echo "crash-soak starting out=$OUT_DIR minutes=$MINUTES cycles=${CYCLES:-until-deadline}" | tee -a "$LOG"

while true; do
  if [[ -n "$CYCLES" ]]; then
    [[ "$cycle" -ge "$CYCLES" ]] && break
  else
    [[ "$(date +%s)" -ge "$deadline_epoch" ]] && break
  fi
  cycle=$((cycle + 1))
  kill_ms="$(rand_ms "$KILL_MIN_MS" "$KILL_MAX_MS")"
  echo "cycle=$cycle kill_after_ms=$kill_ms" | tee -a "$LOG"

  "$SOAK_BIN" \
    --data-dir "$DATA_DIR" \
    --wal-dir "$WAL_DIR" \
    --hours 1 \
    --ops-per-sec "$OPS" \
    --seed "$((SEED + cycle))" \
    --metrics-out "$OUT_DIR/metrics-$cycle.jsonl" \
    >>"$OUT_DIR/soak-$cycle.log" 2>&1 &
  pid=$!

  sleep "$(awk -v ms="$kill_ms" 'BEGIN { printf "%.3f", ms/1000 }')"
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true

  if ! "$INTEGRITY_BIN" --data-dir "$DATA_DIR" --wal-dir "$WAL_DIR" >>"$LOG" 2>&1; then
    echo "FAIL cycle=$cycle integrity/reopen" | tee -a "$LOG"
    failed=$((failed + 1))
    break
  fi
  echo "ok cycle=$cycle" | tee -a "$LOG"
done

cat >"$SUMMARY" <<EOF
{"cycles": $cycle, "failed": $failed, "out_dir": "$OUT_DIR"}
EOF

if [[ "$failed" -ne 0 ]]; then
  echo "crash-soak FAILED after $cycle cycles" | tee -a "$LOG"
  exit 1
fi
echo "crash-soak passed cycles=$cycle" | tee -a "$LOG"
exit 0
