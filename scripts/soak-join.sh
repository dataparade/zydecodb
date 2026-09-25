#!/usr/bin/env bash
# soak-join.sh — drive the document-layer join soak harness for a configurable
# duration and collect JSONL metrics. Unlike scripts/soak.sh (KV PUT/GET/DEL on
# the engine), this exercises $lookup pipelines against a live Engine while a
# writer mutates the inner collection.
#
# Usage:
#   scripts/soak-join.sh                 # default: 24h, writer paced at 200 upserts/s
#   HOURS=0.33 scripts/soak-join.sh      # CI-length run
#   HOURS=2 OPS=50 scripts/soak-join.sh  # slower writer churn
#
# Output files (under $OUT_DIR, default ./soak-runs/join-<timestamp>/):
#   metrics.jsonl     One JSON line per sample window + summary
#   stderr.log        doc-join-soak stderr (op errors, panics)
#   data/             Engine data directory (kept after the run for forensics)
#   wal/              WAL directory
#
# Exit codes:
#   0  run completed with zero op errors and clean shutdown
#   1  doc-join-soak exited non-zero
#   3  invalid invocation
#
# Gating (errors == 0, shutdown_ok) is done by the caller from the
# "kind":"summary" line, same as soak-variants.yml does for engine soaks.

set -euo pipefail

HOURS="${HOURS:-24}"
OPS="${OPS:-200}"

while [[ "${1:-}" != "" ]]; do
    case "$1" in
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 3 ;;
    esac
    shift
done

# Repo root (script lives at $REPO_ROOT/scripts/soak-join.sh).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/soak-runs/join-$TIMESTAMP}"
mkdir -p "$OUT_DIR"
METRICS_FILE="$OUT_DIR/metrics.jsonl"
STDERR_FILE="$OUT_DIR/stderr.log"

echo "join soak run starting" >&2
echo "  hours:           $HOURS" >&2
echo "  writer ops/sec:  $OPS" >&2
echo "  out dir:         $OUT_DIR" >&2
echo "  metrics:         $METRICS_FILE" >&2

# Build release first so the build time isn't counted against the run.
# Pin target dir so soak always runs the binary we just built (not a stale copy).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
echo "building doc-join-soak (release)..." >&2
( cd "$REPO_ROOT" && cargo build --release -p zydecodb-document --bin doc-join-soak ) >&2

# Resolve the binary from the target dir cargo ACTUALLY used (an inherited
# CARGO_TARGET_DIR — e.g. an IDE sandbox cache — wins over the pin above, and
# the hardcoded path can silently be a stale binary from a previous build).
BIN="$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
    | grep -o '"target_directory":"[^"]*"' | head -1 \
    | cut -d'"' -f4)/release/doc-join-soak"
if [[ ! -x "$BIN" ]]; then
    echo "ERROR: freshly built doc-join-soak not found at $BIN" >&2
    exit 1
fi
# Refuse to run a binary older than the newest document-layer source file.
NEWEST_SRC="$(find "$REPO_ROOT/crates/zydecodb-document/src" -name '*.rs' -newer "$BIN" | head -1)"
if [[ -n "$NEWEST_SRC" ]]; then
    echo "ERROR: $BIN is older than $NEWEST_SRC — refusing to soak a stale binary" >&2
    exit 1
fi

set +e
"$BIN" \
    --hours "$HOURS" \
    --ops "$OPS" \
    --out-dir "$OUT_DIR" \
    > "$METRICS_FILE" \
    2> "$STDERR_FILE"
HARNESS_EXIT=$?
set -e

echo "join soak harness exited: $HARNESS_EXIT" >&2

if [[ $HARNESS_EXIT -ne 0 ]]; then
    echo "ERROR: doc-join-soak exited non-zero. Last lines of stderr:" >&2
    tail -20 "$STDERR_FILE" >&2
    exit 1
fi

echo "join soak run complete: $OUT_DIR" >&2
