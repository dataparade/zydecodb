#!/usr/bin/env bash
# model-repro.sh — replay an engine-model seed and, on divergence, shrink the
# replay to the exact failing step.
#
# Usage:
#   scripts/model-repro.sh <seed> [steps]     # steps default 100000
#
# The harness is deterministic: seed + step count reproduces the exact op
# sequence. On divergence this prints a minimal replay command (steps =
# failing step + 1) suitable for pasting into a committed regression test's
# comment header. The committed test itself should re-drive the failing op
# sequence through the engine API directly (see tests/ for the pattern).
set -euo pipefail

SEED="${1:?usage: model-repro.sh <seed> [steps]}"
STEPS="${2:-100000}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
( cd "$REPO_ROOT" && cargo build --release -p zydecodb-engine --bin engine-model ) >&2
BIN="$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | grep -o '"target_directory":"[^"]*"' | head -1 \
    | cut -d'"' -f4)/release/engine-model"
if [[ ! -x "$BIN" ]]; then
    echo "ERROR: engine-model not found at $BIN" >&2
    exit 1
fi

RUN_DIR="$(mktemp -d /tmp/model-repro-XXXXXX)"
trap 'rm -rf "$RUN_DIR"' EXIT

echo "== replaying seed $SEED ($STEPS steps) ==" >&2
set +e
OUT="$("$BIN" --data-dir "$RUN_DIR/data" --wal-dir "$RUN_DIR/wal" \
    --seed "$SEED" --steps "$STEPS" 2>&1)"
rc=$?
set -e
echo "$OUT"

if [[ $rc -eq 0 ]]; then
    echo "== no divergence: seed $SEED is clean over $STEPS steps ==" >&2
    exit 0
fi

FAIL_STEP="$(echo "$OUT" | grep -o '"step":[0-9]*' | tail -1 | cut -d: -f2)"
if [[ -z "$FAIL_STEP" ]]; then
    echo "== failed without a divergence line (harness error?) ==" >&2
    exit 1
fi

MIN_STEPS=$((FAIL_STEP + 1))
echo "== verifying minimal replay: --seed $SEED --steps $MIN_STEPS ==" >&2
rm -rf "$RUN_DIR/data" "$RUN_DIR/wal"
set +e
"$BIN" --data-dir "$RUN_DIR/data" --wal-dir "$RUN_DIR/wal" \
    --seed "$SEED" --steps "$MIN_STEPS" 2>&1 | tail -2
rc2=$?
set -e

if [[ $rc2 -ne 0 ]]; then
    cat >&2 <<EOF

Minimal replay confirmed:
  scripts/model-repro.sh $SEED $MIN_STEPS

Next: encode the failing sequence as a committed test under
crates/zydecodb-engine/tests/ BEFORE fixing (SQLite law).
EOF
else
    echo "== WARNING: minimal replay did not diverge (nondeterminism?) ==" >&2
    exit 1
fi
