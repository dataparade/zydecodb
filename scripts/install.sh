#!/bin/sh
# ==============================================================================
# ZydecoDB installer — https://zydecodb.com/install.sh
# ==============================================================================
# Downloads the prebuilt zydecodb binary for this OS/arch from GitHub Releases,
# verifies its sha256, and installs it onto your PATH.
#
#   curl -sSL https://zydecodb.com/install.sh | sh
#
# Options (environment variables):
#   ZYDECODB_VERSION      — install a specific tag (e.g. v0.9.0-beta.1).
#                           Default: the newest published release.
#   ZYDECODB_INSTALL_DIR  — target directory. Default: /usr/local/bin if
#                           writable, otherwise ~/.local/bin.
#
# ZydecoDB is Unix-only (Linux and macOS, x86_64 and arm64).
# ==============================================================================

set -eu

REPO="dataparade/zydecodb"
API="https://api.github.com/repos/${REPO}/releases"
DOWNLOAD_BASE="https://github.com/${REPO}/releases/download"

say()  { printf '%s\n' "$*"; }
fail() { printf 'error: %s\n' "$*" >&2; exit 1; }

command -v curl >/dev/null 2>&1 || fail "curl is required"
command -v tar  >/dev/null 2>&1 || fail "tar is required"

# --- 1. Detect OS/arch and map to a release target ---------------------------
os=$(uname -s)
arch=$(uname -m)

case "$arch" in
    x86_64|amd64)  arch="x86_64" ;;
    aarch64|arm64) arch="aarch64" ;;
    *) fail "unsupported architecture: $arch (supported: x86_64, aarch64/arm64)" ;;
esac

case "$os" in
    Linux)  target="${arch}-unknown-linux-musl" ;;
    Darwin) target="${arch}-apple-darwin" ;;
    *) fail "unsupported OS: $os (ZydecoDB runs on Linux and macOS; Windows is not supported)" ;;
esac

# --- 2. Resolve the version tag ----------------------------------------------
tag="${ZYDECODB_VERSION:-}"
if [ -z "$tag" ]; then
    # /releases/latest excludes pre-releases; during the beta fall back to the
    # newest release of any kind.
    tag=$(curl -fsSL "${API}/latest" 2>/dev/null \
        | grep -m1 '"tag_name"' | cut -d'"' -f4 || true)
    if [ -z "$tag" ]; then
        tag=$(curl -fsSL "${API}?per_page=1" \
            | grep -m1 '"tag_name"' | cut -d'"' -f4 || true)
    fi
    [ -n "$tag" ] || fail "could not determine the latest release of ${REPO}"
fi

archive="zydecodb-${tag}-${target}.tar.gz"
url="${DOWNLOAD_BASE}/${tag}/${archive}"

say "Installing ZydecoDB ${tag} (${target})"

# --- 3. Download and verify ---------------------------------------------------
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

say "Downloading ${url}"
curl -fSL --progress-bar -o "${tmp}/${archive}" "$url" \
    || fail "download failed — no ${target} build for ${tag}?"
curl -fsSL -o "${tmp}/${archive}.sha256" "${url}.sha256" \
    || fail "checksum sidecar missing for ${archive}"

(
    cd "$tmp"
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum -c "${archive}.sha256" >/dev/null
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 -c "${archive}.sha256" >/dev/null
    else
        fail "neither sha256sum nor shasum found — cannot verify download"
    fi
) || fail "sha256 verification FAILED for ${archive} — aborting"
say "Checksum verified."

tar -xzf "${tmp}/${archive}" -C "$tmp"
[ -f "${tmp}/zydecodb" ] || fail "archive did not contain the zydecodb binary"

# --- 4. Install onto PATH ------------------------------------------------------
install_dir="${ZYDECODB_INSTALL_DIR:-}"
if [ -z "$install_dir" ]; then
    if [ -w /usr/local/bin ]; then
        install_dir="/usr/local/bin"
    else
        install_dir="${HOME}/.local/bin"
    fi
fi
mkdir -p "$install_dir"
install -m 755 "${tmp}/zydecodb" "${install_dir}/zydecodb" 2>/dev/null \
    || { cp "${tmp}/zydecodb" "${install_dir}/zydecodb" && chmod 755 "${install_dir}/zydecodb"; }

say ""
say "Installed: ${install_dir}/zydecodb"

case ":${PATH}:" in
    *":${install_dir}:"*) ;;
    *)
        say ""
        say "NOTE: ${install_dir} is not on your PATH. Add it with:"
        say "  export PATH=\"${install_dir}:\$PATH\""
        ;;
esac

say ""
say "Get started:"
say "  zydecodb serve                # starts on 127.0.0.1:9470, data in ~/.zydecodb"
say ""
say "Then, in another terminal, grab a driver:"
say "  pip install zydecodb          # Python"
say "  npm install zydecodb          # TypeScript/Node"
say "  go get github.com/${REPO}/clients/go   # Go"
say ""
say "Docs: https://github.com/${REPO}#readme"
