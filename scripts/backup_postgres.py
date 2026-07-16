#!/usr/bin/env python3
"""
Nightly PostgreSQL backup -> DigitalOcean Spaces (S3-compatible).

Runs via insider-backup.service (systemd), which loads DATABASE_URL and the
BACKUP_S3_* / BACKUP_HEARTBEAT_URL vars from .env into the process environment.
"""
from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import urllib.request
from datetime import datetime, timezone

import boto3

DATABASE_URL = os.environ["DATABASE_URL"]
S3_BUCKET    = os.environ.get("BACKUP_S3_BUCKET", "opi-insider-backups")
S3_ENDPOINT  = os.environ.get("BACKUP_S3_ENDPOINT", "https://nyc3.digitaloceanspaces.com")
S3_KEY       = os.environ.get("BACKUP_S3_ACCESS_KEY_ID", "")
S3_SECRET    = os.environ.get("BACKUP_S3_SECRET_ACCESS_KEY", "")
HEARTBEAT    = os.environ.get("BACKUP_HEARTBEAT_URL", "")


def main() -> None:
    if not S3_KEY or not S3_SECRET:
        print("BACKUP_S3_ACCESS_KEY_ID / BACKUP_S3_SECRET_ACCESS_KEY not set", file=sys.stderr)
        sys.exit(1)

    today = datetime.now(timezone.utc).date().isoformat()

    with tempfile.NamedTemporaryFile(suffix=".dump", delete=False) as tmp:
        dump_path = tmp.name

    try:
        # Custom format (-Fc): compressed, supports pg_restore -j for parallel restore.
        subprocess.run(["pg_dump", DATABASE_URL, "-Fc", "-f", dump_path], check=True)

        s3 = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT,
            aws_access_key_id=S3_KEY,
            aws_secret_access_key=S3_SECRET,
        )
        key = f"postgres/{today}.dump"
        s3.upload_file(dump_path, S3_BUCKET, key)
        print(f"Backup complete: s3://{S3_BUCKET}/{key}")
    finally:
        os.remove(dump_path)

    if HEARTBEAT:
        try:
            urllib.request.urlopen(HEARTBEAT, timeout=10)
        except Exception:
            pass


if __name__ == "__main__":
    main()
