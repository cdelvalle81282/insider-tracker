#!/bin/bash
# Run this from your local machine to push and set up on the server.
# Usage: bash deploy.sh YOURDOMAIN.duckdns.org
# First run: bash deploy.sh YOURDOMAIN.duckdns.org --setup
#
# Both paths run `alembic upgrade head` before anything touches the database.
# The normal path used to skip migrations entirely, so a commit that added one
# would deploy code against an old schema.

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

# Python venv + deps. Safe to re-run.
if [ ! -d .venv ]; then
  python3 -m venv .venv
fi
.venv/bin/pip install -q -r requirements.txt

mkdir -p data

# Stop here on the very first run: the required values in .env.example cannot be
# guessed, and continuing without them used to fail confusingly several steps
# later (the backfill died because DATABASE_URL was never set).
if [ ! -f .env ]; then
  cp .env.example .env
  echo ""
  echo "  !! Created $APP_DIR/.env from the template."
  echo "  !! Fill in DATABASE_URL, BASIC_AUTH_USER, BASIC_AUTH_PASSWORD and"
  echo "  !! SECRET_KEY (openssl rand -hex 32), then re-run this command."
  exit 1
fi

# Gate on a real DATABASE_URL rather than discovering it is missing later.
if ! grep -Eq '^DATABASE_URL=.+' .env; then
  echo "  !! DATABASE_URL is empty in $APP_DIR/.env. Set it and re-run." >&2
  exit 1
fi
if ! grep -Eq '^BASIC_AUTH_USER=.+' .env || ! grep -Eq '^BASIC_AUTH_PASSWORD=.+' .env; then
  echo "  !! BASIC_AUTH_USER / BASIC_AUTH_PASSWORD are empty in $APP_DIR/.env." >&2
  echo "  !! The app fails closed without them and nginx needs them for the" >&2
  echo "  !! htpasswd file. Set both and re-run." >&2
  exit 1
fi

# Export .env so alembic and the ingester see it. ingest.py does not call
# load_dotenv, and neither does migrations/env.py.
set -a
. ./.env
set +a

# Schema BEFORE anything reads or writes the database.
echo "==> Running migrations"
.venv/bin/alembic upgrade head

# Install every unit in schedule/, not just the web service and the ingester.
# Installing only a subset meant a rebuilt server silently had no backups, no
# price updates, no backtest, and no perf-profile refresh.
echo "==> Installing systemd units"
sudo cp schedule/*.service schedule/*.timer /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now insider-tracker.service
for timer in schedule/*.timer; do
  sudo systemctl enable --now "\$(basename "\$timer")"
done

# nginx: Basic Auth lives in the committed config, so the front door is closed
# by default. The htpasswd file is generated from the same credentials the app
# uses, and nginx forwards Authorization upstream, so one browser prompt
# satisfies both layers.
echo "==> Generating nginx htpasswd"
printf '%s:%s\n' "\$BASIC_AUTH_USER" "\$(openssl passwd -apr1 "\$BASIC_AUTH_PASSWORD")" \
  | sudo tee /etc/nginx/.htpasswd-insider > /dev/null
sudo chmod 640 /etc/nginx/.htpasswd-insider
sudo chown root:www-data /etc/nginx/.htpasswd-insider

sudo cp schedule/nginx-insider-tracker.conf /etc/nginx/sites-available/insider-tracker
sudo sed -i "s/YOURDOMAIN.duckdns.org/$DOMAIN/g" /etc/nginx/sites-available/insider-tracker
sudo ln -sf /etc/nginx/sites-available/insider-tracker /etc/nginx/sites-enabled/insider-tracker
sudo nginx -t && sudo systemctl reload nginx

echo ""
echo "  NOTE: until certbot finishes below, the site is served over plain HTTP."
echo "  Both auth layers are already active, but avoid typing credentials until"
echo "  the certificate is issued."
echo ""

echo "==> Running certbot for HTTPS"
sudo certbot --nginx -d $DOMAIN --non-interactive --agree-tos -m charlie@optionpit.com

echo "==> Running initial backfill (last 30 days)..."
.venv/bin/python ingest.py --backfill-days 30

echo ""
echo "Done! Dashboard live at https://$DOMAIN"
EOF

else
  echo "==> Installing deps and running migrations"
  ssh "$SERVER" bash <<EOF
set -e
cd $APP_DIR
.venv/bin/pip install -q -r requirements.txt
set -a
. ./.env
set +a
.venv/bin/alembic upgrade head
sudo systemctl restart insider-tracker.service
EOF
  echo "==> Deployed. https://$DOMAIN"
fi
