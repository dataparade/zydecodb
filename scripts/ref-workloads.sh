#!/usr/bin/env bash
# Drive the reference-workload harness and write JSON results.
#
# Usage:
#   scripts/ref-workloads.sh
#   OPS=500 OUT=docs/soak-baselines/ref-workloads-local.json scripts/ref-workloads.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
export CARGO_TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
OPS="${OPS:-2000}"
SEED="${SEED:-42}"
OUT="${OUT:-$REPO_ROOT/soak-runs/ref-workloads-$(date -u +%Y%m%dT%H%M%SZ).json}"

mkdir -p "$(dirname "$OUT")"
echo "building ref-workloads (release)..." >&2
( cd "$REPO_ROOT" && cargo build --release -p zydecodb --bin ref-workloads ) >&2

BIN="$CARGO_TARGET_DIR/release/ref-workloads"
"$BIN" --ops "$OPS" --seed "$SEED" --out "$OUT"
echo "results: $OUT" >&2
