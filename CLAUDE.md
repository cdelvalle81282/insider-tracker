# Insider Tracker

SEC Form 4 insider trading dashboard for Option Pit Research editorial use.

## What this is

Pulls Form 4 filings (insider buys/sells) from SEC EDGAR, stores them in SQLite, and serves a web dashboard at https://opi-insider.duckdns.org. Built for daily editorial research — "what happened today, ranked by dollar value."

## Server

- **Host:** deploy@167.99.167.244
- **App dir:** /home/deploy/insider-tracker
- **Port:** 8002 (behind nginx)
- **URL:** https://opi-insider.duckdns.org
- **GitHub:** https://github.com/cdelvalle81282/insider-tracker

## Stack

- Python 3.12, FastAPI, Jinja2, HTMX, Tailwind CSS (CDN)
- SQLite at `data/insider_tracker.db`
- No ORM — raw sqlite3, all queries in `queries.py`

## Key files

| File | Purpose |
|------|---------|
| `config.py` | All rules, thresholds, glossaries — single source of truth |
| `ingest.py` | CLI ingester: pulls EDGAR, parses XML, writes to SQLite |
| `parser.py` | Form 4 XML → transaction row dicts |
| `tickers.py` | CIK → ticker cache (EDGAR company_tickers.json, refreshes weekly) |
| `queries.py` | All SQL queries — no SQL in app.py |
| `app.py` | FastAPI routes |
| `templates/logic.html` | Logic & Config tab — editable thresholds, code glossary, Phase 2 placeholders |

## Running locally

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt   # Windows
.venv/bin/pip install -r requirements.txt        # Linux

# Ingest today's filings
python ingest.py --date today

# Start dashboard
uvicorn app:app --reload
# → http://localhost:8000
```

## Ingester CLI

```bash
python ingest.py --date today
python ingest.py --date 2026-04-22
python ingest.py --backfill 2024-01-01 2026-04-22
python ingest.py --backfill-days 30
python ingest.py --since-last-run   # used by systemd timer
```

## Deploy

```bash
# Push code update to server (from project root)
git push
ssh deploy@167.99.167.244 "cd /home/deploy/insider-tracker && git pull && sudo systemctl restart insider-tracker.service"
```

## Systemd services (on server)

- `insider-tracker.service` — uvicorn web app, always-on
- `insider-ingest.timer` — runs ingest at 10:30 AM, 2:00 PM, 7:00 PM ET Mon–Fri
- `insider-ingest.service` — oneshot triggered by timer

```bash
sudo systemctl status insider-tracker.service
sudo journalctl -u insider-tracker.service -f   # live logs
sudo systemctl status insider-ingest.timer
```

## Schema

Primary table: `filings` — one row per transaction (not per filing).
`transaction_id` = `{accession_no}-{ND|D}-{row_index}` is the true PK.
A single Form 4 can produce multiple rows (nonDerivativeTable + derivativeTable).

Key indexes: `filed_at`, `issuer_ticker`, `insider_cik`, `transaction_code`, `issuer_cik`

Other tables: `run_log` (ingest history), `alerts_sent` (dedup for Slack — Ticket 4)

## Config / Logic tab

All tunable parameters live in `config.py`. The `/logic` page in the dashboard renders and edits them. Edits save to `config_overrides.json` (gitignored) which overrides defaults without touching source files.

## Known gotchas

- **EDGAR XML structure:** All numeric values are wrapped in a `<value>` child element — `parser.py` handles this
- **form.idx columns:** Parse by anchoring on the `edgar/data` string, not fixed column offsets
- **XML filename:** Fetched from the `-index.htm` filing page — it's NOT `{accession_no}.xml`
- **Starlette API:** Use `TemplateResponse(request, "name.html", context)` — not the old `(name, context)` form
- **static/ dir:** Must exist on server; it's empty so git doesn't track it — `mkdir -p static` after fresh clone
- **10b5-1 detection:** Checks `<rule10b5-1Indicator>` element first, falls back to footnote text scan
- **Amendments (4/A):** Stored as separate rows — not merged with originals yet (Phase 2)

## SEC compliance

- User-Agent: `"Option Pit Research charlie@optionpit.com"` (required — SEC blocks missing/generic UAs)
- Rate limit: 8 req/sec (SEC cap is 10)

## Phase 2 (not built yet)

- Slack push alerts (Ticket 4) — rules defined in config.py, `alerts_sent` table ready
- Systemd scheduler (Ticket 3) — service/timer files in `schedule/`, not yet deployed
- Historical baseline comparison (is this buy unusual vs. insider's own history?)
- Amendment (4/A) resolution
- Scoring model
