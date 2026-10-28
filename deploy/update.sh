#!/usr/bin/env bash
# Pull the deployed branch, reinstall and restart. Run as root on the server.
# Database migrations are applied from a dev machine (scripts/apply_migrations.py).
set -euo pipefail
cd /opt/stockmonitor
sudo -u stockmonitor git pull --ff-only
sudo -u stockmonitor .venv/bin/pip install --quiet .
systemctl restart stockmonitor-web stockmonitor-worker stockmonitor-bot
systemctl --no-pager --lines=0 status stockmonitor-web stockmonitor-worker stockmonitor-bot | grep -E "●|Active:"
