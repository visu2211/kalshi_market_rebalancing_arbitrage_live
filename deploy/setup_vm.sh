#!/usr/bin/env bash
# Run this ON the Oracle Cloud VM (Ubuntu or Oracle Linux — auto-detected
# below) to deploy/update the scanner as a systemd service. Idempotent —
# re-running after a `git push` updates the code and restarts the service.
#
# Usage:
#   bash setup_vm.sh <git-repo-url> [deploy-dir]
#
# deploy-dir defaults to ~/kalshi_market_rebalancing_arbitrage_live.
#
# What this does NOT do (deliberately manual, since these are secrets):
#   - Create .env — see .env.example. scp it to <deploy-dir>/.env yourself.
#   - Create secrets/kalshi_private_key.pem — scp your Kalshi private key
#     to <deploy-dir>/secrets/ yourself, and point KALSHI_PRIVATE_KEY_PATH
#     at it in .env. Without these two, the service still runs (Stage 1/2
#     only — see stream/monitor.py's has_signing_credentials check) rather than
#     failing to start.

set -euo pipefail

REPO_URL="${1:?Usage: bash setup_vm.sh <git-repo-url> [deploy-dir]}"
DEPLOY_DIR="${2:-$HOME/kalshi_market_rebalancing_arbitrage_live}"
SERVICE_NAME="kalshi-scanner-live"

echo "==> Installing system packages (python3, venv, git)..."
if command -v apt-get >/dev/null 2>&1; then
    sudo apt-get update -qq
    sudo apt-get install -y -qq python3 python3-venv python3-pip git
elif command -v dnf >/dev/null 2>&1; then
    # Oracle Linux / RHEL-family — python3's venv module is part of the
    # base python3 package here (no separate *-venv package like Debian).
    sudo dnf install -y -q python3 python3-pip git
else
    echo "Unsupported distro: need apt-get (Debian/Ubuntu) or dnf (Oracle Linux/RHEL)." >&2
    exit 1
fi

if [ -d "$DEPLOY_DIR/.git" ]; then
    echo "==> $DEPLOY_DIR already exists, pulling latest..."
    git -C "$DEPLOY_DIR" pull --ff-only
else
    echo "==> Cloning $REPO_URL into $DEPLOY_DIR..."
    git clone "$REPO_URL" "$DEPLOY_DIR"
fi

echo "==> Creating/updating virtualenv..."
cd "$DEPLOY_DIR"
python3 -m venv .venv
.venv/bin/pip install --quiet --upgrade pip
.venv/bin/pip install --quiet -r requirements.txt

if [ ! -f "$DEPLOY_DIR/.env" ]; then
    echo "==> No .env found — copying .env.example as a placeholder."
    echo "    scp your real .env and secrets/kalshi_private_key.pem here before Stage 3 can stream."
    cp .env.example .env
fi
mkdir -p "$DEPLOY_DIR/data" "$DEPLOY_DIR/secrets"
chmod 600 "$DEPLOY_DIR/.env"
chmod 700 "$DEPLOY_DIR/secrets"

echo "==> Installing systemd service..."
sed -e "s#__DEPLOY_USER__#$(whoami)#g" -e "s#__DEPLOY_DIR__#$DEPLOY_DIR#g" \
    "$DEPLOY_DIR/deploy/kalshi-scanner.service.template" | sudo tee "/etc/systemd/system/${SERVICE_NAME}.service" > /dev/null

sudo systemctl daemon-reload
sudo systemctl enable "$SERVICE_NAME"
sudo systemctl restart "$SERVICE_NAME"

echo "==> Done. Status:"
sudo systemctl --no-pager status "$SERVICE_NAME" || true
echo ""
echo "Logs: sudo journalctl -u $SERVICE_NAME -f"
echo "Opportunities: tail -f $DEPLOY_DIR/data/opportunities.jsonl"
