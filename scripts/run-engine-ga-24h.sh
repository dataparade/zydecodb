#!/usr/bin/env bash
# 24h *paced* confirmation soak (OPS=3000). Not the RC uncapped bar.
# For release-candidate uncapped capacity, use scripts/vps-soak.sh with OPS=0
# and archive under docs/soak-baselines/rc/<version>/24h-uncapped.jsonl.
# Run only after a green 90m stability soak.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export HOURS=24
export OPS=3000
export PUT_PCT=55
export GET_PCT=25
export HOT_PCT=80
export BLOCK_CACHE_MB=640
export RESULT_CACHE_MB=0
export OUT_DIR="${OUT_DIR:-$ROOT/soak-runs/phase1-memo5-engine-ga-24h}"

exec "$ROOT/scripts/soak.sh"
