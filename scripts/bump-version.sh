#!/usr/bin/env bash
# bump-version.sh X.Y.Z — set the release version in every place version
# identity lives, then refresh Cargo.lock. Release commits must use this
# script; hand-edits are how 1.3.0 shipped agent pages pinned at 1.2.0.
#
# Covers:
#   workspace Cargo.toml            [workspace.package] version
#   Cargo.lock                      via cargo check
#   clients/python/pyproject.toml   version =
#   clients/python/zydecodb/__init__.py  __version__
#   clients/typescript/package.json "version"
#   clients/typescript/package-lock.json "version" (root + packages[""])
#   clients/go/README.md            go get .../clients/go@vX.Y.Z
#   docs/agent/*.md                 zydecodb: X.Y.Z front-matter header
#
# Usage:
#   scripts/bump-version.sh 1.3.2
#
# Exit codes:
#   0  all files updated, cargo check refreshed the lock
#   1  bad argument, a target file missing, or cargo check failed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"

VER="${1:-}"
if [[ ! "$VER" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$ ]]; then
    echo "usage: scripts/bump-version.sh X.Y.Z (or X.Y.Z-rc.N)" >&2
    exit 1
fi

cd "$REPO_ROOT"

# Every edit goes through replace_in_file: it fails loudly if the expected
# pattern is absent, so a renamed manifest key cannot be silently skipped.
replace_in_file() {
    local file="$1" old_re="$2" new="$3"
    if [[ ! -f "$file" ]]; then
        echo "ERROR: $file not found" >&2
        exit 1
    fi
    if ! grep -qE "$old_re" "$file"; then
        echo "ERROR: $file: pattern not found: $old_re" >&2
        exit 1
    fi
    sed -i -E "s|$old_re|$new|g" "$file"
}

# Workspace crate version (first `version = "..."` under [workspace.package]).
replace_in_file Cargo.toml \
    '^version = "[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?"$' \
    "version = \"$VER\""

replace_in_file clients/python/pyproject.toml \
    '^version = "[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?"$' \
    "version = \"$VER\""

replace_in_file clients/python/zydecodb/__init__.py \
    '^__version__ = "[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?"$' \
    "__version__ = \"$VER\""

replace_in_file clients/typescript/package.json \
    '^  "version": "[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?",$' \
    "  \"version\": \"$VER\","

# Lockfile carries the version twice: top-level and packages[""]. The
# top-level line is the only 2-space "version" in the file. The packages[""]
# entry is 6-space indented — but so is every node_modules/* entry, so a
# bare indented match rewrites dependency versions (this broke the 1.4.0
# npm publish). Scope the second edit to the `    "": {` block only.
replace_in_file clients/typescript/package-lock.json \
    '^  "version": "[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?",$' \
    "  \"version\": \"$VER\","
LOCK=clients/typescript/package-lock.json
if ! grep -qE '^    "": \{$' "$LOCK"; then
    echo "ERROR: $LOCK: packages[\"\"] block not found" >&2
    exit 1
fi
sed -i -E '/^    "": \{$/,/^    \},?$/ s|^      "version": "[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?",$|      "version": "'"$VER"'",|' "$LOCK"
# Guard: no node_modules/* dependency entry may carry the release version.
if grep -A1 '^    "node_modules/' "$LOCK" | grep -qE '^      "version": "'"$VER"'",$'; then
    echo "ERROR: $LOCK: a dependency entry was rewritten to $VER — refusing" >&2
    exit 1
fi

replace_in_file clients/go/README.md \
    'go get github\.com/dataparade/zydecodb/clients/go@v[0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?' \
    "go get github.com/dataparade/zydecodb/clients/go@v$VER"

# Agent pages are embedded via include_str! and checked against
# CARGO_PKG_VERSION by agent::tests::every_topic_renders_under_cap.
shopt -s nullglob
AGENT_PAGES=(docs/agent/*.md)
if [[ ${#AGENT_PAGES[@]} -eq 0 ]]; then
    echo "ERROR: no docs/agent/*.md found" >&2
    exit 1
fi
for f in "${AGENT_PAGES[@]}"; do
    replace_in_file "$f" \
        '^zydecodb: [0-9]+\.[0-9]+\.[0-9]+(-rc\.[0-9]+)?$' \
        "zydecodb: $VER"
done

# Refresh Cargo.lock with the new workspace version.
cargo check --workspace --quiet >&2

echo "bumped to $VER in:" >&2
printf '  %s\n' \
    "Cargo.toml (+ Cargo.lock)" \
    "clients/python/pyproject.toml" \
    "clients/python/zydecodb/__init__.py" \
    "clients/typescript/package.json" \
    "clients/typescript/package-lock.json" \
    "clients/go/README.md" \
    "${AGENT_PAGES[@]}" >&2

cat >&2 <<'EOF'

Next:
  cargo test -p zydecodb --lib agent::tests::every_topic_renders_under_cap
  git commit -am "release: X.Y.Z" && git tag -a vX.Y.Z
EOF
