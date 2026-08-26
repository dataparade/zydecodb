#!/usr/bin/env bash
# Mongo 8.x $lookup comparison harness (local claim-check, not CI).
#
# Usage:
#   scripts/bench-join-mongo.sh
#   FACT_DOCS=1000000 DIM_DOCS=1000 ./scripts/bench-join-mongo.sh
#   MONGO_URI=mongodb://127.0.0.1:27017 ./scripts/bench-join-mongo.sh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
OUT="${OUT:-$REPO_ROOT/soak-runs/bench-join-mongo-$(date -u +%Y%m%dT%H%M%SZ).json}"
FACT_DOCS="${FACT_DOCS:-100000}"
DIM_DOCS="${DIM_DOCS:-1000}"
RUNS="${RUNS:-3}"
SCENARIO="${SCENARIO:-both}"
WT_CACHE_GB="${WT_CACHE_GB:-0.25}"
MONGO_IMAGE="${MONGO_IMAGE:-mongo:8}"
CONTAINER="zydeco-bench-mongo-$$"
STARTED_CONTAINER=0

cleanup() {
  if [[ "$STARTED_CONTAINER" == "1" ]]; then
    docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

mkdir -p "$(dirname "$OUT")"

if [[ -z "${MONGO_URI:-}" ]]; then
  if ! command -v docker >/dev/null 2>&1; then
    echo "docker not found and MONGO_URI unset" >&2
    exit 1
  fi
  echo "starting $MONGO_IMAGE (wiredTigerCacheSizeGB=$WT_CACHE_GB)..." >&2
  docker run -d --rm --name "$CONTAINER" -p 27017:27017 "$MONGO_IMAGE" \
    --wiredTigerCacheSizeGB "$WT_CACHE_GB" >/dev/null
  STARTED_CONTAINER=1
  MONGO_URI="mongodb://127.0.0.1:27017"
  for _ in $(seq 1 60); do
    if docker exec "$CONTAINER" mongosh --quiet --eval "db.runCommand({ping:1}).ok" >/dev/null 2>&1; then
      break
    fi
    sleep 1
  done
fi

if ! python3 -c "import pymongo" >/dev/null 2>&1; then
  echo "installing pymongo..." >&2
  python3 -m pip install --user --quiet pymongo
fi

python3 "$SCRIPT_DIR/bench-join-mongo.py" \
  --uri "$MONGO_URI" \
  --fact-docs "$FACT_DOCS" \
  --dim-docs "$DIM_DOCS" \
  --runs "$RUNS" \
  --scenario "$SCENARIO" \
  --wt-cache-gb "$WT_CACHE_GB" | tee "$OUT"

echo "results: $OUT" >&2
