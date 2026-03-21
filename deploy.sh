#!/usr/bin/env bash
# Deploy peplink-monitor to Mac Mini (user@YOUR_REMOTE_HOST).
# SSH agent forwarding is used; ensure your key is loaded with ssh-add.

set -euo pipefail

REMOTE="user@YOUR_REMOTE_HOST"
REMOTE_PATH="Documents/Code/peplink-monitor"
LOCAL_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "==> Rsyncing project to ${REMOTE}:~/${REMOTE_PATH}/ ..."
ssh -A "${REMOTE}" mkdir -p "~/${REMOTE_PATH}"
rsync -av \
    --exclude='.git' \
    --exclude='__pycache__' \
    --exclude='*.pyc' \
    --exclude='*.py[cod]' \
    --exclude='.python-version' \
    --exclude='data/' \
    --exclude='logs/' \
    "${LOCAL_DIR}/" \
    "${REMOTE}:~/${REMOTE_PATH}/"

echo ""
echo "==> Installing dependencies on Mac Mini into pyenv 3.14.0 ..."
ssh -A "${REMOTE}" bash <<'ENDSSH'
set -euo pipefail
cd ~/Documents/Code/peplink-monitor
~/.pyenv/versions/3.14.0/bin/pip install -q -r requirements.txt
echo "Dependencies installed."
mkdir -p ~/Documents/Code/peplink-monitor/logs
mkdir -p ~/Documents/Code/peplink-monitor/data
echo "Log and data directories ready."
ENDSSH

echo ""
echo "==> Deployment complete!"
echo ""
echo "----------------------------------------------------------------"
echo "Add this crontab entry on the Mac Mini (ssh in, run: crontab -e)"
echo "----------------------------------------------------------------"
echo ""
echo "*/5 * * * * YOUR_REPO_PATH/collector.py >> YOUR_REPO_PATH/logs/collector.log 2>&1"
echo ""
echo "----------------------------------------------------------------"
echo "If config.yaml is not yet on the Mini, copy it manually:"
echo "  scp -A config.yaml ${REMOTE}:~/${REMOTE_PATH}/config.yaml"
echo "----------------------------------------------------------------"
