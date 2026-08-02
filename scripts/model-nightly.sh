#!/usr/bin/env bash
# model-nightly.sh — run many engine-model seeds in parallel and collect any
# divergence lines. The seed IS the regression artifact: a failing run
# replays exactly via `scripts/model-repro.sh <seed>`.
#
# Usage:
#   scripts/model-nightly.sh [SEEDS] [STEPS] [JOBS]
# Defaults: 256 seeds x 10000 steps on all cores.
#
# Exit 0 = every seed clean. Exit 1 = at least one divergence; the failing
# lines (seed + step + detail) are printed on stdout and saved to
# $OUT_FILE (default /tmp/model-nightly-failures.jsonl).
set -euo pipefail

SEEDS="${1:-256}"
STEPS="${2:-10000}"
JOBS="${3:-$(nproc 2>/dev/null || echo 4)}"
OUT_FILE="${OUT_FILE:-/tmp/model-nightly-failures.jsonl}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
echo "== building engine-model (release) ==" >&2
( cd "$REPO_ROOT" && cargo build --release -p zydecodb-engine --bin engine-model ) >&2
BIN="$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | grep -o '"target_directory":"[^"]*"' | head -1 \
    | cut -d'"' -f4)/release/engine-model"
[[ -x "$BIN" ]] || { echo "ERROR: engine-model not found at $BIN" >&2; exit 1; }

BASE="$(mktemp -d /tmp/model-nightly-XXXXXX)"
trap 'rm -rf "$BASE"' EXIT
: > "$OUT_FILE"

echo "== $SEEDS seeds x $STEPS steps on $JOBS jobs ==" >&2
export BIN BASE STEPS
seq 1 "$SEEDS" | xargs -P "$JOBS" -I{} bash -c '
    seed={}
    dir="$BASE/seed-$seed"
    mkdir -p "$dir"
    out="$("$BIN" --data-dir "$dir/data" --wal-dir "$dir/wal" \
        --seed "$seed" --steps "$STEPS" 2>&1)" && exit 0
    echo "$out" | grep "\"kind\":\"divergence\"" >> "$BASE/fail-{}" || true
    exit 1
' || true

cat "$BASE"/fail-* > "$OUT_FILE" 2>/dev/null || true
FAILS="$(wc -l < "$OUT_FILE")"
if [[ "$FAILS" -gt 0 ]]; then
    echo "== $FAILS divergence(s) ==" >&2
    cat "$OUT_FILE"
    exit 1
fi
echo "== all $SEEDS seeds clean ($STEPS steps each) ==" >&2
