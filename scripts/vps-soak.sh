#!/usr/bin/env bash
# vps-soak.sh — deploy zydecodb to a VPS, run a long soak in tmux, pull results.
#
# Requires: rsync, ssh, scp (local). Remote: bash, curl, build-essential, python3, tmux.
# setup also installs Rust 1.91 via rustup if missing.
#
# Usage:
#   export VPS_HOST=203.0.113.10
#   export VPS_USER=root                    # optional
#   export VPS_KEY=~/.ssh/id_ed25519        # optional
#
#   scripts/vps-soak.sh setup               # once: deps + rust on VPS
#   scripts/vps-soak.sh deploy              # rsync repo, release build on VPS
#   scripts/vps-soak.sh start               # 24h uncapped soak in tmux
#   scripts/vps-soak.sh status              # tmux, last sample, disk, memory
#   scripts/vps-soak.sh pull                # copy metrics + logs to local machine
#   scripts/vps-soak.sh analyze             # pull + stability analyzer
#
# Soak env (override before start):
#   HOURS=24 OPS=0 SAMPLE_EVERY=60 RUN_NAME=uncapped-24h-clean

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VPS_HOST="${VPS_HOST:-}"
VPS_USER="${VPS_USER:-root}"
VPS_KEY="${VPS_KEY:-}"
REMOTE_DIR="${REMOTE_DIR:-zydecodb}"
RUN_NAME="${RUN_NAME:-uncapped-24h-clean}"
TMUX_SESSION="${TMUX_SESSION:-zydecodb-soak}"

HOURS="${HOURS:-24}"
OPS="${OPS:-0}"
SAMPLE_EVERY="${SAMPLE_EVERY:-60}"
SEED="${SEED:-16045690984503098046}"

LOCAL_PULL_DIR="${LOCAL_PULL_DIR:-$REPO_ROOT/soak-runs/$RUN_NAME}"

SSH_OPTS=(-o BatchMode=yes -o StrictHostKeyChecking=accept-new)
[[ -n "$VPS_KEY" ]] && SSH_OPTS+=(-i "$VPS_KEY")
RSYNC_SSH=(ssh "${SSH_OPTS[@]}")

usage() {
    sed -n '2,22p' "$0"
    exit "${1:-0}"
}

need_host() {
    if [[ -z "$VPS_HOST" ]]; then
        echo "ERROR: set VPS_HOST (server IP or hostname)" >&2
        exit 3
    fi
}

remote() {
    need_host
    ssh "${SSH_OPTS[@]}" "${VPS_USER}@${VPS_HOST}" "$@"
}

# rsync/scp destination (relative to remote $HOME unless REMOTE_DIR is absolute).
remote_rsync_dest() {
    if [[ "$REMOTE_DIR" == /* ]]; then
        echo "$REMOTE_DIR"
    else
        echo "$REMOTE_DIR"
    fi
}

scp_run_dir() {
    local dest
    dest="$(remote_rsync_dest)"
    if [[ "$dest" == /* ]]; then
        echo "${VPS_USER}@${VPS_HOST}:${dest}/soak-runs/${RUN_NAME}"
    else
        echo "${VPS_USER}@${VPS_HOST}:${dest}/soak-runs/${RUN_NAME}"
    fi
}

cmd_setup() {
    need_host
    echo "==> Installing packages and Rust 1.91 on ${VPS_USER}@${VPS_HOST}" >&2
    remote bash -s <<'REMOTE_SETUP'
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq build-essential pkg-config python3 tmux curl ca-certificates
elif command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y gcc gcc-c++ make python3 tmux curl ca-certificates
else
    echo "WARN: unknown package manager; ensure build-essential, python3, tmux, curl exist" >&2
fi
if ! command -v cargo >/dev/null 2>&1; then
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain 1.91
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env"
rustup toolchain install 1.91 2>/dev/null || true
rustup default 1.91
rustc --version
cargo --version
python3 --version
tmux -V
REMOTE_SETUP
}

cmd_deploy() {
    need_host
    local dest
    dest="$(remote_rsync_dest)"
    echo "==> Rsync repo to ${VPS_USER}@${VPS_HOST}:${dest}" >&2
    if [[ "$dest" == /* ]]; then
        remote "mkdir -p '$dest'"
    else
        remote "mkdir -p \"\$HOME/$dest\""
    fi
    rsync -avz --delete \
        --exclude '/target/' \
        --exclude '/soak-runs/' \
        --exclude '/.git/' \
        -e "${RSYNC_SSH[*]}" \
        "$REPO_ROOT/" "${VPS_USER}@${VPS_HOST}:${dest}/"
    echo "==> Release build on VPS" >&2
    remote bash -s "$REMOTE_DIR" <<'REMOTE_BUILD'
set -euo pipefail
REMOTE_DIR=$1
if [[ "$REMOTE_DIR" == /* ]]; then
    cd "$REMOTE_DIR"
else
    cd "$HOME/$REMOTE_DIR"
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env" 2>/dev/null || true
cargo build --release -p zydecodb-engine --bin engine-soak
REMOTE_BUILD
}

cmd_build() {
    need_host
    remote bash -s "$REMOTE_DIR" <<'REMOTE_BUILD'
set -euo pipefail
REMOTE_DIR=$1
if [[ "$REMOTE_DIR" == /* ]]; then
    cd "$REMOTE_DIR"
else
    cd "$HOME/$REMOTE_DIR"
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env" 2>/dev/null || true
cargo build --release -p zydecodb-engine --bin engine-soak
REMOTE_BUILD
}

cmd_start() {
    need_host
    echo "==> Starting soak in tmux session '${TMUX_SESSION}'" >&2
    echo "    HOURS=$HOURS OPS=$OPS SAMPLE_EVERY=$SAMPLE_EVERY RUN_NAME=$RUN_NAME" >&2
    remote bash -s \
        "$REMOTE_DIR" "$RUN_NAME" "$TMUX_SESSION" \
        "$HOURS" "$OPS" "$SAMPLE_EVERY" "$SEED" <<'REMOTE_START'
set -euo pipefail
REMOTE_DIR=$1 RUN_NAME=$2 TMUX_SESSION=$3
HOURS=$4 OPS=$5 SAMPLE_EVERY=$6 SEED=$7
if [[ "$REMOTE_DIR" == /* ]]; then
    cd "$REMOTE_DIR"
else
    cd "$HOME/$REMOTE_DIR"
fi
# shellcheck disable=SC1091
source "$HOME/.cargo/env" 2>/dev/null || true
mkdir -p "soak-runs/$RUN_NAME"
if tmux has-session -t "$TMUX_SESSION" 2>/dev/null; then
    echo "ERROR: tmux session $TMUX_SESSION already exists" >&2
    exit 1
fi
tmux new-session -d -s "$TMUX_SESSION" "env \
    HOURS=$HOURS OPS=$OPS SAMPLE_EVERY=$SAMPLE_EVERY SEED=$SEED \
    OUT_DIR=soak-runs/$RUN_NAME \
    scripts/soak.sh --no-analyze \
    2>&1 | tee soak-runs/$RUN_NAME/run.log"
REMOTE_START
    echo "==> Soak running. Check: scripts/vps-soak.sh status" >&2
}

cmd_stop() {
    need_host
    remote "tmux kill-session -t ${TMUX_SESSION} 2>/dev/null || echo 'no session ${TMUX_SESSION}'" >&2
}

cmd_status() {
    need_host
    remote bash -s "$REMOTE_DIR" "$RUN_NAME" <<'REMOTE_STATUS'
set -euo pipefail
REMOTE_DIR=$1 RUN_NAME=$2
if [[ "$REMOTE_DIR" == /* ]]; then
    REPO="$REMOTE_DIR"
else
    REPO="$HOME/$REMOTE_DIR"
fi
cd "$REPO"
echo "--- tmux ---"
tmux ls 2>/dev/null || echo "(no tmux sessions)"
echo "--- disk / memory ---"
df -h "$REPO" 2>/dev/null || df -h /
free -h
METRICS="$REPO/soak-runs/$RUN_NAME/metrics.jsonl"
if [[ -f "$METRICS" ]]; then
    echo "--- metrics (last 2 lines) ---"
    tail -2 "$METRICS"
    echo "--- sample count ---"
    wc -l "$METRICS"
else
    echo "(no metrics yet: $METRICS)"
fi
LOG="$REPO/soak-runs/$RUN_NAME/run.log"
if [[ -f "$LOG" ]]; then
    echo "--- run.log (last 5 lines) ---"
    tail -5 "$LOG"
fi
REMOTE_STATUS
}

cmd_pull() {
    need_host
    mkdir -p "$LOCAL_PULL_DIR"
    local base
    base="$(scp_run_dir)"
    echo "==> Pulling from $base to $LOCAL_PULL_DIR" >&2
    scp "${SSH_OPTS[@]}" \
        "${base}/metrics.jsonl" \
        "${LOCAL_PULL_DIR}/" 2>/dev/null || echo "WARN: metrics.jsonl not found yet" >&2
    scp "${SSH_OPTS[@]}" \
        "${base}/stderr.log" \
        "${base}/run.log" \
        "${LOCAL_PULL_DIR}/" 2>/dev/null || true
    echo "==> Local files:" >&2
    ls -la "$LOCAL_PULL_DIR"
}

cmd_analyze() {
    cmd_pull
    local metrics="$LOCAL_PULL_DIR/metrics.jsonl"
    if [[ ! -f "$metrics" ]]; then
        echo "ERROR: $metrics missing" >&2
        exit 1
    fi
    echo "==> stability" >&2
    python3 "$REPO_ROOT/scripts/analyze-soak.py" --mode stability "$metrics" || true
    echo "==> perf (informational)" >&2
    python3 "$REPO_ROOT/scripts/analyze-soak.py" --mode perf "$metrics" || true
    if tail -1 "$metrics" | grep -q '"kind":"summary"'; then
        echo "==> summary" >&2
        tail -1 "$metrics" | python3 -m json.tool
    fi
}

main() {
    local cmd="${1:-}"
    shift || true
    case "$cmd" in
        setup)   cmd_setup ;;
        deploy)  cmd_deploy ;;
        build)   cmd_build ;;
        start)   cmd_start ;;
        stop)    cmd_stop ;;
        status)  cmd_status ;;
        pull)    cmd_pull ;;
        analyze) cmd_analyze ;;
        -h|--help|help|"") usage 0 ;;
        *)
            echo "unknown command: $cmd" >&2
            usage 3
            ;;
    esac
}

main "$@"
