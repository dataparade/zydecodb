#!/usr/bin/env bash
# soak.sh — drive the zydecodb-engine soak harness for a configurable duration,
# collect JSONL metrics, and hand off to the analyzer.
#
# Usage:
#   scripts/soak.sh                    # default: 24h at 1000 ops/sec
#   HOURS=2 OPS=500 scripts/soak.sh    # short run for harness validation
#   scripts/soak.sh --no-analyze       # skip the post-run analysis step
#
# Output files (under $OUT_DIR, default ./soak-runs/<timestamp>/):
#   metrics.jsonl     One JSON line per sample window + header + summary
#   stderr.log        engine-soak stderr (op errors, panics)
#   data/             Engine data directory (kept after the run for forensics)
#   wal/              WAL directory
#
# Exit codes:
#   0  run completed and analysis (if run) was within ceilings
#   1  engine-soak exited non-zero (e.g. shutdown failed)
#   2  analyze step found a stability ceiling breach
#   3  invalid invocation
#
# Stability gates are enforced by scripts/analyze-soak.py --mode stability
# (errors, write amp, RSS, space amp, L2 shape for paced runs). Use
# --no-analyze to collect metrics only; release checklist requires a green
# stability pass before RC tags.

set -euo pipefail

HOURS="${HOURS:-24}"
OPS="${OPS:-1000}"
HOT_PCT="${HOT_PCT:-80}"
PUT_PCT="${PUT_PCT:-70}"
GET_PCT="${GET_PCT:-25}"
VAL_MIN="${VAL_MIN:-64}"
VAL_MAX="${VAL_MAX:-1024}"
SEED="${SEED:-16045690984503098046}"
SCAN_PCT="${SCAN_PCT:-0}"
SNAPSHOT_EVERY="${SNAPSHOT_EVERY:-0}"
SAMPLE_EVERY="${SAMPLE_EVERY:-60}"
POLL_COMPACTION_MS="${POLL_COMPACTION_MS:-50}"
BLOCK_CACHE_MB="${BLOCK_CACHE_MB:-640}"
RESULT_CACHE_MB="${RESULT_CACHE_MB:-0}"

ANALYZE=1
while [[ "${1:-}" != "" ]]; do
    case "$1" in
        --no-analyze) ANALYZE=0 ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *) echo "unknown arg: $1" >&2; exit 3 ;;
    esac
    shift
done

# Repo root (script lives at $REPO_ROOT/scripts/soak.sh).
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

TIMESTAMP="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="${OUT_DIR:-$REPO_ROOT/soak-runs/$TIMESTAMP}"
mkdir -p "$OUT_DIR/data"
METRICS_FILE="$OUT_DIR/metrics.jsonl"
STDERR_FILE="$OUT_DIR/stderr.log"

echo "soak run starting" >&2
echo "  hours:           $HOURS" >&2
echo "  ops/sec target:  $OPS" >&2
echo "  out dir:         $OUT_DIR" >&2
echo "  metrics:         $METRICS_FILE" >&2

# Build release first so the build time isn't counted against the run.
# Pin target dir so soak always runs the binary we just built (not a stale copy).
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
echo "building engine-soak (release)..." >&2
( cd "$REPO_ROOT" && cargo build --release -p zydecodb-engine --bin engine-soak ) >&2

BIN="$REPO_ROOT/target/release/engine-soak"
if [[ ! -x "$BIN" ]]; then
    # Fall back to the workspace target dir cargo actually used.
    BIN="$(cargo metadata --format-version 1 --no-deps 2>/dev/null \
        | grep -o '"target_directory":"[^"]*"' | head -1 \
        | cut -d'"' -f4)/release/engine-soak"
fi

set +e
"$BIN" \
    --data-dir "$OUT_DIR/data" \
    --wal-dir "$OUT_DIR/wal" \
    --hours "$HOURS" \
    --ops-per-sec "$OPS" \
    --put-pct "$PUT_PCT" \
    --get-pct "$GET_PCT" \
    --hot-pct "$HOT_PCT" \
    --val-min "$VAL_MIN" \
    --val-max "$VAL_MAX" \
    --seed "$SEED" \
    --sample-every-secs "$SAMPLE_EVERY" \
    --poll-compaction-ms "$POLL_COMPACTION_MS" \
    --block-cache-mb "$BLOCK_CACHE_MB" \
    --result-cache-mb "$RESULT_CACHE_MB" \
    ${SCAN_PCT:+--scan-pct "$SCAN_PCT"} \
    ${SNAPSHOT_EVERY:+--snapshot-every-secs "$SNAPSHOT_EVERY"} \
    --metrics-out "$METRICS_FILE" \
    2> "$STDERR_FILE"
HARNESS_EXIT=$?
set -e

echo "soak harness exited: $HARNESS_EXIT" >&2

if [[ $HARNESS_EXIT -ne 0 ]]; then
    echo "ERROR: engine-soak exited non-zero. Last lines of stderr:" >&2
    tail -20 "$STDERR_FILE" >&2
    exit 1
fi

if [[ $ANALYZE -eq 1 ]]; then
    if [[ -x "$REPO_ROOT/scripts/analyze-soak.py" ]]; then
        echo "running analyzer..." >&2
        if ! python3 "$REPO_ROOT/scripts/analyze-soak.py" --mode stability "$METRICS_FILE"; then
            echo "ERROR: analyzer reported a ceiling breach." >&2
            exit 2
        fi
    else
        echo "WARN: scripts/analyze-soak.py not found or not executable; skipping" >&2
    fi
fi

echo "soak run complete: $OUT_DIR" >&2
