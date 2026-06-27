# Ops Reference

## Environment variables

Set in `/home/deploy/insider-tracker/.env`, loaded by systemd `EnvironmentFile`:

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | Direct PostgreSQL DSN (e.g. `postgresql://insider_app:pass@localhost:5432/insider_tracker`); required by all scripts |
| `PGBOUNCER_URL` | PgBouncer DSN (port 6432, transaction mode); used by the web app pool when set |
| `SLACK_WEBHOOK_URL` | Slack incoming webhook for alerts |
| `POLYGON_API_KEY` | Polygon.io API key for chart price data and `--backfill-metadata` |
| `AINVEST_API_KEY` | AInvest API key for congressional trades (`congress_ingest.py`) |
| `ANTHROPIC_API_KEY` | Claude API key for `auto_diagnose.py` (autonomous alert diagnosis) |
| `WEBHOOK_SECRET` | HMAC secret for `/webhook/alert` endpoint (validates Healthchecks.io/BetterStack payloads) |
| `INGEST_HEARTBEAT_URL` | Uptime heartbeat pinged after each successful nightly ingest (optional) |
| `PRICES_HEARTBEAT_URL` | Uptime heartbeat pinged after each successful `--update-prices` run (optional) |

## Systemd services

| Timer/Service | Schedule | What it does |
|---------------|----------|--------------|
| `insider-tracker.service` | always-on | uvicorn web app |
| `insider-ingest.timer` | 10:30, 14:00, 19:00 UTC Mon–Fri | `--date today` (6:30 AM / 10 AM / 3 PM ET) |
| `insider-ingest-nightly.timer` | 03:00 UTC Mon–Sat | `--since-last-run` (11 PM ET) — catches EDGAR's end-of-day index |
| `insider-prices.timer` | 01:00 UTC Mon–Fri | `--update-prices` (9 PM ET) — refreshes close prices in `ticker_metadata` |
| `insider-congress.timer` | 13:00 UTC Mon–Fri | `congress_ingest.py` (9 AM ET) — refreshes congressional trades |
| `insider-exec.timer` | 14:00 UTC every Monday | `exec_ingest.py` — weekly executive branch trades from Open Cabinet |
| `insider-backup.timer` | 05:30 UTC daily | PG backup to S3 (staggered 30 min after `sync_job` cron) |

```bash
sudo systemctl status insider-tracker.service
sudo journalctl -u insider-tracker.service -f
sudo systemctl status insider-ingest.timer
sudo systemctl status insider-ingest-nightly.timer
```

## Server

- **Host:** deploy@167.99.167.244
- **App dir:** /home/deploy/insider-tracker
- **Port:** 8002 (behind nginx)
- **URL:** https://opi-insider.duckdns.org
- **GitHub:** https://github.com/cdelvalle81282/insider-tracker

## DuckDNS

`opi-insider.duckdns.org` is kept alive by a cron job on the server. DuckDNS free domains expire after 90 days of inactivity without regular pings.

- **Script:** `/usr/local/bin/duckdns_update.sh` — calls the DuckDNS update API with the current server IP
- **Schedule:** `*/5 * * * *` (every 5 minutes, deploy user's crontab)
- **Log:** `/home/deploy/duckdns.log` — should contain `OK` after each run
- **Token:** stored in the script itself (deploy@167.99.167.244)

If DNS breaks again: SSH in, run `/usr/local/bin/duckdns_update.sh`, and check the log. If the response is `KO`, the token or domain name is wrong. Re-add the cron entry with `crontab -e` if it's missing.

## nginx notes

- `sites-enabled` is NOT a symlink — always edit `/etc/nginx/sites-enabled/insider-tracker` directly, then `nginx -t && systemctl reload nginx`
- Block AI crawlers (`GPTBot`, etc.) with `if ($http_user_agent ~* ...)` INSIDE each `location` block
- `/robots.txt` must be served from its own `location` block (without the `if` guard)
- Robots.txt lives at `/var/www/insider/robots.txt`
