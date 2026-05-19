#!/usr/bin/env bash
set -euo pipefail

DB_PATH="/home/deploy/insider-tracker/data/insider_tracker.db"
BACKUP_DIR="/tmp"
DATE=$(date -u +%F)
BACKUP_FILE="$BACKUP_DIR/insider_tracker_$DATE.db"
GZ_FILE="$BACKUP_FILE.gz"
S3_BUCKET="${BACKUP_S3_BUCKET:-opi-insider-backups}"
S3_ENDPOINT="${BACKUP_S3_ENDPOINT:-https://nyc3.digitaloceanspaces.com}"
HEARTBEAT_URL="${BACKUP_HEARTBEAT_URL:-}"

sqlite3 "$DB_PATH" ".backup '$BACKUP_FILE'"
gzip -f "$BACKUP_FILE"
aws s3 cp "$GZ_FILE" "s3://$S3_BUCKET/sqlite/$DATE.db.gz" \
    --endpoint-url "$S3_ENDPOINT" \
    --quiet

rm -f "$GZ_FILE"
echo "Backup complete: s3://$S3_BUCKET/sqlite/$DATE.db.gz"

# Ping UptimeRobot heartbeat if configured
if [ -n "$HEARTBEAT_URL" ]; then
    curl --max-time 10 --silent --output /dev/null "$HEARTBEAT_URL" || true
fi
