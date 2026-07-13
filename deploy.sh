#!/usr/bin/env bash
# Deploy peplink-monitor to the remote host configured in config.yaml.
# Pulls the current branch from origin on the remote host.
# SSH agent forwarding is used; ensure your key is loaded with ssh-add.

set -euo pipefail

# Read remote_host, remote_user, remote_python from config.yaml
REMOTE_HOST=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c['remote_host'])")
REMOTE_USER=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c.get('remote_user', 'user'))")
REMOTE_PYTHON=$(python3 -c "import yaml; c=yaml.safe_load(open('config.yaml')); print(c.get('remote_python', 'python3'))")
REMOTE_PATH="Documents/Code/peplink-monitor"
REMOTE="${REMOTE_USER}@${REMOTE_HOST}"

echo "==> Pulling latest code on ${REMOTE}:~/${REMOTE_PATH}/ ..."
ssh -A "${REMOTE}" "cd ~/${REMOTE_PATH} && git pull"

echo ""
echo "==> Installing dependencies on remote host ..."
ssh -A "${REMOTE}" "${REMOTE_PYTHON} -m pip install -q -r ~/${REMOTE_PATH}/requirements.txt"
echo "Dependencies installed."

ssh -A "${REMOTE}" "mkdir -p ~/${REMOTE_PATH}/logs ~/${REMOTE_PATH}/data"
echo "Log and data directories ready."

echo ""
echo "==> Deployment complete!"
echo ""
echo "----------------------------------------------------------------"
echo "Add these crontab entries on the remote host (ssh in, run: crontab -e)"
echo "----------------------------------------------------------------"
echo ""
echo "*/5 * * * * ${REMOTE_PYTHON} ~/${REMOTE_PATH}/collector.py >> ~/${REMOTE_PATH}/logs/collector.log 2>&1"
echo "5 21 * * * ${REMOTE_PYTHON} ~/${REMOTE_PATH}/rollup.py >> ~/${REMOTE_PATH}/logs/rollup.log 2>&1"
echo ""
echo "The rollup job runs at 21:05 LOCAL time on purpose (not UTC): day"
echo "boundaries in the DB are UTC, and 21:05 local is safely after UTC"
echo "midnight year-round under US Eastern DST/EST, so 'yesterday' (UTC) is"
echo "always complete by the time it runs. If the remote host is in a"
echo "different timezone, adjust the hour so it still lands after UTC"
echo "midnight."
echo ""
echo "----------------------------------------------------------------"
echo "If config.yaml is not yet on the remote host, copy it manually:"
echo "  scp -A config.yaml ${REMOTE}:~/${REMOTE_PATH}/config.yaml"
echo "----------------------------------------------------------------"
