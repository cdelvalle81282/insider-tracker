#!/bin/bash
# Run this from your local machine to push and set up on the server.
# Usage: bash deploy.sh YOURDOMAIN.duckdns.org
# First run: bash deploy.sh YOURDOMAIN.duckdns.org --setup

set -e
SERVER="deploy@167.99.167.244"
APP_DIR="/home/deploy/insider-tracker"
DOMAIN="${1:-opi-insider.duckdns.org}"
SETUP="${2:-}"

echo "==> Syncing code to $SERVER:$APP_DIR"
rsync -av --exclude='.git' --exclude='data/' --exclude='.venv/' \
  --exclude='config_overrides.json' --exclude='.env' \
  "$(dirname "$0")/" "$SERVER:$APP_DIR/"

if [ "$SETUP" = "--setup" ]; then
  echo "==> First-time setup on server"
  ssh "$SERVER" bash <<EOF
set -e
cd $APP_DIR

# Python venv + deps
python3 -m venv .venv
.venv/bin/pip install -q -r requirements.txt

# Create data dir
mkdir -p data

# Copy .env if it doesn't exist
if [ ! -f .env ]; then
  cp .env.example .env
  echo "  !! Edit $APP_DIR/.env on the server and add your SLACK_WEBHOOK_URL"
fi

# Install systemd services
sudo cp schedule/insider-tracker.service /etc/systemd/system/
sudo cp schedule/insider-ingest.service  /etc/systemd/system/
sudo cp schedule/insider-ingest.timer    /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now insider-tracker.service
sudo systemctl enable --now insider-ingest.timer

# Nginx config — replace YOURDOMAIN placeholder
sudo cp schedule/nginx-insider-tracker.conf /etc/nginx/sites-available/insider-tracker
sudo sed -i "s/YOURDOMAIN.duckdns.org/$DOMAIN/g" /etc/nginx/sites-available/insider-tracker
sudo ln -sf /etc/nginx/sites-available/insider-tracker /etc/nginx/sites-enabled/insider-tracker
sudo nginx -t && sudo systemctl reload nginx

echo "==> Running certbot for HTTPS"
sudo certbot --nginx -d $DOMAIN --non-interactive --agree-tos -m charlie@optionpit.com

echo "==> Running initial backfill (last 30 days)..."
.venv/bin/python ingest.py --backfill-days 30

echo ""
echo "Done! Dashboard live at https://$DOMAIN"
EOF

else
  echo "==> Restarting web service"
  ssh "$SERVER" "cd $APP_DIR && .venv/bin/pip install -q -r requirements.txt && sudo systemctl restart insider-tracker.service"
  echo "==> Deployed. https://$DOMAIN"
fi
