#!/bin/bash
# ==============================================================================
# ZydecoDB Website Deployment Script
# ==============================================================================
# Deploys the marketing website, configures Caddy, and reloads the web server.
#
# Required:
#   DEPLOY_HOST   — hostname or IP of the target server
#
# Optional:
#   DEPLOY_USER          — SSH user (default: root).
#   KNOWN_HOSTS_FILE     — path to known_hosts (default: ~/.ssh/known_hosts)
#   REMOTE_WWW_DIR       — default /var/www/zydecodb.com
#   REMOTE_CADDY_DIR     — default /etc/caddy
#
# Usage:
#   DEPLOY_HOST=web.example.com ./scripts/deploy-website.sh
# ==============================================================================

set -euo pipefail

if [[ -z "${DEPLOY_HOST:-}" ]]; then
    DEPLOY_HOST="157.230.215.234"
fi

SERVER_IP="${DEPLOY_HOST}"
SERVER_USER="${DEPLOY_USER:-root}"
KNOWN_HOSTS_FILE="${KNOWN_HOSTS_FILE:-${HOME}/.ssh/known_hosts}"
REMOTE_WWW_DIR="${REMOTE_WWW_DIR:-/var/www/zydecodb.com}"
REMOTE_CADDY_DIR="${REMOTE_CADDY_DIR:-/etc/caddy}"
LOCAL_WEBSITE_DIR="website"

SSH_OPTS=(
    -o StrictHostKeyChecking=yes
    -o UserKnownHostsFile="${KNOWN_HOSTS_FILE}"
    -o BatchMode=yes
)

ssh_cmd() {
    ssh "${SSH_OPTS[@]}" "${SERVER_USER}@${SERVER_IP}" "$@"
}

# ANSI Color Codes
GREEN='\033[0;32m'
ORANGE='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${ORANGE}======================================================================${NC}"
echo -e "${ORANGE}             ZYDECODB MARKETING WEBSITE DEPLOYMENT PIPELINE            ${NC}"
echo -e "${ORANGE}======================================================================${NC}"
echo -e "Target: ${SERVER_USER}@${SERVER_IP}"

# 1. Verification of Local Assets
echo -e "\n[1/5] Verifying local website directory..."
if [ ! -d "$LOCAL_WEBSITE_DIR" ]; then
    echo -e "${RED}Error: Local directory '$LOCAL_WEBSITE_DIR' does not exist.${NC}"
    exit 1
fi

if [ ! -f "$LOCAL_WEBSITE_DIR/index.html" ]; then
    echo -e "${RED}Error: '$LOCAL_WEBSITE_DIR/index.html' is missing.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Local assets verified.${NC}"

# 2. Establish Remote Target Directory
echo -e "\n[2/5] Preparing remote directory structure on $SERVER_IP..."
ssh_cmd "mkdir -p $REMOTE_WWW_DIR && sudo chown -R www-data:www-data $REMOTE_WWW_DIR"
echo -e "${GREEN}✓ Remote directory structure initialized.${NC}"

# 3. Synchronize Web Assets
echo -e "\n[3/5] Syncing static assets to remote web server..."
# Using rsync if available, fallback to scp
if command -v rsync >/dev/null 2>&1; then
    rsync -avz --delete \
        --exclude 'design*.html' \
        --exclude 'hud.html' \
        --exclude 'copy.md' \
        -e "ssh ${SSH_OPTS[*]}" \
        "$LOCAL_WEBSITE_DIR/" "${SERVER_USER}@${SERVER_IP}:${REMOTE_WWW_DIR}/"
else
    echo -e "${ORANGE}rsync not found locally. Falling back to scp...${NC}"
    scp "${SSH_OPTS[@]}" -r "$LOCAL_WEBSITE_DIR"/* "${SERVER_USER}@${SERVER_IP}:${REMOTE_WWW_DIR}/"
fi
# Publish the binary installer alongside the site (https://zydecodb.com/install.sh)
if [ -f "scripts/install.sh" ]; then
    scp "${SSH_OPTS[@]}" "scripts/install.sh" "${SERVER_USER}@${SERVER_IP}:${REMOTE_WWW_DIR}/install.sh"
else
    echo -e "${RED}Error: scripts/install.sh is missing — the site advertises it.${NC}"
    exit 1
fi
# Fix permissions on the remote server
ssh_cmd "sudo chown -R www-data:www-data $REMOTE_WWW_DIR && sudo chmod -R 755 $REMOTE_WWW_DIR"
echo -e "${GREEN}✓ Web assets synchronized successfully.${NC}"

# 4. Configure Caddy Server
echo -e "\n[4/5] Deploying Caddyfile configuration..."
# Generate the site-specific Caddyfile locally
LOCAL_CADDYFILE_TMP=$(mktemp)
cat <<EOF > "$LOCAL_CADDYFILE_TMP"
zydecodb.com, www.zydecodb.com {
    # Serve frontend marketing website
    root * $REMOTE_WWW_DIR
    file_server

    # Enable Gzip and Zstandard compression
    encode gzip zstd

    # Security headers
    header {
        # Protect against clickjacking
        X-Frame-Options "DENY"
        # Prevent MIME-type sniffing
        X-Content-Type-Options "nosniff"
        # Enable XSS protection in older browsers
        X-XSS-Protection "1; mode=block"
        # Strict Transport Security (HSTS)
        Strict-Transport-Security "max-age=31536000; includeSubDomains; preload"
        # Referrer Policy
        Referrer-Policy "strict-origin-when-cross-origin"
        # Content Security Policy — Tailwind CDN, Google Fonts, Three.js (unpkg), inline JS/CSS
        Content-Security-Policy "default-src 'self'; script-src 'self' 'unsafe-inline' https://cdn.tailwindcss.com https://unpkg.com; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://cdn.tailwindcss.com; font-src 'self' https://fonts.gstatic.com; img-src 'self' data:; connect-src 'self' https://cdn.tailwindcss.com https://unpkg.com; worker-src 'self' blob:; base-uri 'self'; form-action 'self'; frame-ancestors 'none'"
    }

    # Custom 404 handling
    handle_errors {
        @404 {
            expression {err.status} == 404
        }
        rewrite @404 /index.html
        file_server
    }
}
EOF

# Upload Caddyfile to /etc/caddy/zydecodb.caddyfile
scp "${SSH_OPTS[@]}" "$LOCAL_CADDYFILE_TMP" "${SERVER_USER}@${SERVER_IP}:/tmp/zydecodb.caddyfile"
rm -f "$LOCAL_CADDYFILE_TMP"
ssh_cmd "sudo mv /tmp/zydecodb.caddyfile ${REMOTE_CADDY_DIR}/zydecodb.caddyfile && sudo chmod 644 ${REMOTE_CADDY_DIR}/zydecodb.caddyfile"

# Ensure Caddy main configuration imports our new file
echo -e "Updating global Caddyfile imports..."
ssh_cmd "
if ! grep -q 'import /etc/caddy/zydecodb.caddyfile' /etc/caddy/Caddyfile; then
    echo 'import /etc/caddy/zydecodb.caddyfile' | sudo tee -a /etc/caddy/Caddyfile >/dev/null
    echo 'Added import to Caddyfile'
else
    echo 'Import already exists in Caddyfile'
fi
"
echo -e "${GREEN}✓ Caddy configuration deployed.${NC}"

# 5. Validate and Reload Caddy
echo -e "\n[5/5] Validating configuration and reloading Caddy..."
ssh_cmd "
sudo caddy validate --config /etc/caddy/Caddyfile
sudo systemctl reload caddy
"
echo -e "${GREEN}✓ Caddy validated and reloaded successfully.${NC}"

echo -e "\n${GREEN}======================================================================${NC}"
echo -e "${GREEN} DEPLOYMENT COMPLETE!                                                 ${NC}"
echo -e "${GREEN} ZydecoDB.com is now live and serving traffic from $SERVER_IP         ${NC}"
echo -e "${GREEN}======================================================================${NC}"
