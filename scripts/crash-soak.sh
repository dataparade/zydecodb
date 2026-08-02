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
#   STOP_PCT      Per-cycle chance (0-100, default 0) of a SIGSTOP/SIGCONT
#                 pause before the kill — exercises resume mid-flush/compaction.
#   ENOSPC        1 = run the data dir on a small tmpfs (default 0; needs
#                 sudo). The engine hits ENOSPC mid-run; the gate is that
#                 reopen + integrity still pass once space is freed.
#   ENOSPC_MB     tmpfs size for the ENOSPC axis (default 256).
#   ENOSPC_FREE_MB  space left for the DB after the filler (default 24);
#                 raise KILL_MAX_MS so cycles live long enough to hit it.
#   IOTHROTTLE    1 = run the soak under ionice idle class (default 0).
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
STOP_PCT="${STOP_PCT:-0}"
ENOSPC="${ENOSPC:-0}"
ENOSPC_MB="${ENOSPC_MB:-256}"
ENOSPC_FREE_MB="${ENOSPC_FREE_MB:-24}"
IOTHROTTLE="${IOTHROTTLE:-0}"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/soak-runs/crash-$TIMESTAMP}"
DATA_DIR="$OUT_DIR/data"
WAL_DIR="$OUT_DIR/wal"
LOG="$OUT_DIR/crash-soak.log"
SUMMARY="$OUT_DIR/summary.json"

mkdir -p "$OUT_DIR"
: >"$LOG"

ENOSPC_MOUNT=""
ENOSPC_FILLER=""
if [[ "$ENOSPC" == "1" ]]; then
  ENOSPC_MOUNT="$OUT_DIR/enospc-fs"
  mkdir -p "$ENOSPC_MOUNT"
  if sudo mount -t tmpfs -o "size=${ENOSPC_MB}M" tmpfs "$ENOSPC_MOUNT" 2>/dev/null; then
    # Fill most of the tmpfs with a filler file so the database itself hits
    # ENOSPC once it outgrows the remainder. Deleting the FILLER (never a
    # database file) frees space for the reopen + integrity pass.
    ENOSPC_FILLER="$ENOSPC_MOUNT/filler"
    fallocate -l "$(( ENOSPC_MB - ENOSPC_FREE_MB ))M" "$ENOSPC_FILLER" 2>/dev/null \
      || dd if=/dev/zero of="$ENOSPC_FILLER" bs=1M count="$(( ENOSPC_MB - ENOSPC_FREE_MB ))" status=none
    DATA_DIR="$ENOSPC_MOUNT/data"
    echo "ENOSPC axis: data dir on ${ENOSPC_MB}M tmpfs, ${ENOSPC_FREE_MB}M free for the DB" | tee -a "$LOG"
  else
    echo "WARN: ENOSPC=1 but tmpfs mount failed (no sudo?); axis disabled" | tee -a "$LOG"
    ENOSPC="0"
    ENOSPC_MOUNT=""
  fi
fi

cleanup() {
  if [[ -n "$ENOSPC_MOUNT" ]]; then
    sudo umount "$ENOSPC_MOUNT" 2>/dev/null || true
  fi
}
trap cleanup EXIT

mkdir -p "$DATA_DIR" "$WAL_DIR"

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

  IONICE=()
  if [[ "$IOTHROTTLE" == "1" ]]; then
    IONICE=(ionice -c3)
  fi

  "${IONICE[@]}" "$SOAK_BIN" \
    --data-dir "$DATA_DIR" \
    --wal-dir "$WAL_DIR" \
    --hours 1 \
    --ops-per-sec "$OPS" \
    --seed "$((SEED + cycle))" \
    --metrics-out "$OUT_DIR/metrics-$cycle.jsonl" \
    >>"$OUT_DIR/soak-$cycle.log" 2>&1 &
  pid=$!

  # SIGSTOP/SIGCONT axis: freeze the process mid-flight (possibly mid-flush
  # or mid-compaction), hold, resume, then let the kill land on schedule.
  if [[ "$STOP_PCT" -gt 0 ]] && (( RANDOM % 100 < STOP_PCT )); then
    stop_at_ms="$(rand_ms 100 "$kill_ms")"
    hold_ms="$(rand_ms 50 500)"
    (
      sleep "$(awk -v ms="$stop_at_ms" 'BEGIN { printf "%.3f", ms/1000 }')"
      kill -STOP "$pid" 2>/dev/null || exit 0
      sleep "$(awk -v ms="$hold_ms" 'BEGIN { printf "%.3f", ms/1000 }')"
      kill -CONT "$pid" 2>/dev/null || true
    ) &
    stopper=$!
  else
    stopper=""
  fi

  sleep "$(awk -v ms="$kill_ms" 'BEGIN { printf "%.3f", ms/1000 }')"
  kill -9 "$pid" 2>/dev/null || true
  wait "$pid" 2>/dev/null || true
  [[ -n "$stopper" ]] && wait "$stopper" 2>/dev/null || true

  # ENOSPC axis: once the DB outgrew the free remainder and errored, free
  # the filler (never a database file) so reopen + integrity can proceed.
  if [[ "$ENOSPC" == "1" && -n "$ENOSPC_FILLER" && -f "$ENOSPC_FILLER" ]]; then
    rm -f "$ENOSPC_FILLER"
  fi

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
