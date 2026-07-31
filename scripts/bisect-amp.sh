#!/usr/bin/env bash
# bisect-amp.sh — build engine-soak at a commit in a throwaway worktree, run a
# short paced soak (memo5 GA config), and report the cumulative compaction
# write amp with a CLEAN/DIRTY verdict against the 5.0 ceiling.
#
# Usage:
#   scripts/bisect-amp.sh <commit-ish> [HOURS]     # HOURS default 1.5
#
# Artifacts (build log, metrics, stderr) land in soak-runs/bisect-<sha>-<ts>/.
# The worktree is removed afterwards; sequential runs only — parallel soaks
# contaminate each other's I/O and poison the measurement.
set -euo pipefail

COMMIT="${1:?usage: bisect-amp.sh <commit-ish> [HOURS]}"
HOURS="${2:-1.5}"
THRESHOLD="${THRESHOLD:-5.0}"

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SHORT="$(git -C "$REPO_ROOT" rev-parse --short "$COMMIT")"
TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT_DIR="$REPO_ROOT/soak-runs/bisect-$SHORT-$TS"
WT="$OUT_DIR/wt"
mkdir -p "$OUT_DIR"

echo "== bisect-amp: commit=$SHORT hours=$HOURS out=$OUT_DIR"

git -C "$REPO_ROOT" worktree add --detach "$WT" "$COMMIT" >/dev/null
cleanup() { git -C "$REPO_ROOT" worktree remove --force "$WT" >/dev/null 2>&1 || true; }
trap cleanup EXIT

# Early commits predate some workspace members (e.g. 6006bf6 lists
# zydecodb-document before it existed). Trim missing members so cargo can
# resolve the workspace; build-only tweak, engine sources untouched.
python3 - "$WT" <<'PY'
import re, sys, pathlib
wt = pathlib.Path(sys.argv[1])
root = wt / "Cargo.toml"
text = root.read_text()
m = re.search(r'members = \[(.*?)\]', text, re.S)
if m:
    members = re.findall(r'"([^"]+)"', m.group(1))
    keep = [p for p in members if (wt / p / "Cargo.toml").exists()]
    if len(keep) != len(members):
        new = "members = [" + ", ".join(f'"{p}"' for p in keep) + "]"
        root.write_text(text[:m.start()] + new + text[m.end():])
        print(f"trimmed missing workspace members: {sorted(set(members) - set(keep))}")
PY

export CARGO_TARGET_DIR="$WT/target"
echo "== building engine-soak (release) at $SHORT ..."
( cd "$WT" && cargo build --release -p zydecodb-engine --bin engine-soak ) >"$OUT_DIR/build.log" 2>&1 || {
    echo "== build failed; tail of build.log:"
    tail -20 "$OUT_DIR/build.log"
    exit 1
}

echo "== soaking ${HOURS}h at 3000 ops/s ..."
set +e
"$WT/target/release/engine-soak" \
    --data-dir "$OUT_DIR/data" \
    --wal-dir "$OUT_DIR/wal" \
    --hours "$HOURS" \
    --ops-per-sec 3000 \
    --put-pct 55 \
    --get-pct 25 \
    --hot-pct 80 \
    --val-min 64 \
    --val-max 1024 \
    --seed 16045690984503098046 \
    --sample-every-secs 60 \
    --poll-compaction-ms 50 \
    --block-cache-mb 640 \
    --result-cache-mb 0 \
    --metrics-out "$OUT_DIR/metrics.jsonl" \
    2>"$OUT_DIR/stderr.log"
rc=$?
set -e
if [[ $rc -ne 0 ]]; then
    echo "== engine-soak exited $rc; tail of stderr:"
    tail -10 "$OUT_DIR/stderr.log"
    exit 1
fi

python3 - "$OUT_DIR/metrics.jsonl" "$THRESHOLD" <<'PY'
import json, sys

path, threshold = sys.argv[1], float(sys.argv[2])
amp = None
samples = 0
with open(path) as f:
    for line in f:
        d = json.loads(line)
        if d.get("kind") == "sample":
            samples += 1
            if d.get("compaction_write_amp", 0) > 0:
                amp = d["compaction_write_amp"]
ok = amp is not None and amp <= threshold
print(f"samples: {samples}")
print(f"final cumulative compaction_write_amp: {amp:.2f}" if amp is not None else "no amp samples")
print(f"VERDICT: {'CLEAN' if ok else 'DIRTY'} (threshold {threshold})")
sys.exit(0 if ok else 1)
PY
