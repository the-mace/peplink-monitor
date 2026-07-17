#!/usr/bin/env bash
# Deploy peplink-monitor to the remote host configured in config.yaml.
# Pulls the current branch from origin on the remote host.
# SSH agent forwarding is used; ensure your key is loaded with ssh-add.

set -euo pipefail

# Read remote settings from config.yaml
REMOTE_HOST=$(python3 -c "from config import load_config; c=load_config(); print(c['remote_host'])")
REMOTE_USER=$(python3 -c "from config import load_config; c=load_config(); print(c.get('remote_user', 'user'))")
REMOTE_PYTHON=$(python3 -c "from config import load_config; c=load_config(); print(c.get('remote_python', 'python3'))")
REMOTE_PATH=$(python3 -c "from config import load_config; c=load_config(); print(c.get('remote_path', '~/Documents/Code/peplink-monitor'))")
REMOTE="${REMOTE_USER}@${REMOTE_HOST}"

echo "==> Pulling latest code on ${REMOTE}:${REMOTE_PATH}/ ..."
ssh -A "${REMOTE}" "cd ${REMOTE_PATH} && git pull"

# Resolve absolute path on remote (cron does not expand ~)
REMOTE_ABS=$(ssh -A "${REMOTE}" "cd ${REMOTE_PATH} && pwd")
echo "Remote project dir: ${REMOTE_ABS}"

echo ""
echo "==> Installing dependencies on remote host ..."
ssh -A "${REMOTE}" "cd ${REMOTE_PATH} && ${REMOTE_PYTHON} -m pip install -q -r requirements.txt"
echo "Dependencies installed."

ssh -A "${REMOTE}" "cd ${REMOTE_PATH} && mkdir -p logs data reports"
echo "Log and data directories ready."

# Soft-rotate oversized collector log (keep last 5 MB of content).
echo ""
echo "==> Checking collector log size on remote ..."
ssh -A "${REMOTE}" "cd ${REMOTE_PATH} && \
  if [ -f logs/collector.log ]; then \
    size=\$(wc -c < logs/collector.log); \
    if [ \"\$size\" -gt 10485760 ]; then \
      echo \"collector.log is \$((size/1024/1024))MB — rotating\"; \
      tail -c 5242880 logs/collector.log > logs/collector.log.tmp && \
        mv logs/collector.log.tmp logs/collector.log; \
      echo \"Trimmed to last 5MB\"; \
    else \
      echo \"collector.log is \$((size/1024))KB — ok\"; \
    fi; \
  fi"

echo ""
echo "==> Deployment complete!"
echo ""
echo "----------------------------------------------------------------"
echo "Add these crontab entries on the remote host (ssh in, run: crontab -e)"
echo "----------------------------------------------------------------"
echo ""
echo "*/5 * * * * ${REMOTE_PYTHON} ${REMOTE_ABS}/collector.py >> ${REMOTE_ABS}/logs/collector.log 2>&1"
echo "5 21 * * * ${REMOTE_PYTHON} ${REMOTE_ABS}/rollup.py >> ${REMOTE_ABS}/logs/rollup.log 2>&1"
echo "15 3 * * 0 [ -f ${REMOTE_ABS}/logs/collector.log ] && tail -c 5242880 ${REMOTE_ABS}/logs/collector.log > ${REMOTE_ABS}/logs/collector.log.tmp && mv ${REMOTE_ABS}/logs/collector.log.tmp ${REMOTE_ABS}/logs/collector.log"
echo ""
echo "The last line is a weekly soft-rotate: keeps the last 5MB of collector.log"
echo "so the log does not grow without bound (deploy.sh also trims if >10MB)."
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
echo "  scp -A config.yaml ${REMOTE}:${REMOTE_ABS}/config.yaml"
echo "----------------------------------------------------------------"
