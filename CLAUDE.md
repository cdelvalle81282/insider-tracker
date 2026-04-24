# Insider Tracker

SEC Form 4 insider trading dashboard for Option Pit Research editorial use.

## What this is

Pulls Form 4 filings (insider buys/sells) from SEC EDGAR, stores them in SQLite, and serves a web dashboard at https://opi-insider.duckdns.org. Built for daily editorial research — "what happened today, ranked by dollar value and conviction score."

## Server

- **Host:** deploy@167.99.167.244
- **App dir:** /home/deploy/insider-tracker
- **Port:** 8002 (behind nginx)
- **URL:** https://opi-insider.duckdns.org
- **GitHub:** https://github.com/cdelvalle81282/insider-tracker

## Stack

- Python 3.12, FastAPI, Jinja2, HTMX 1.9.12, Tailwind CSS (CDN)
- SQLite at `data/insider_tracker.db`
- No ORM — raw sqlite3, all queries in `queries.py`
- Linting: `ruff` (`pip install ruff`, run `ruff check .`)

## Key files

| File | Purpose |
|------|---------|
| `config.py` | All rules, thresholds, conviction weights — single source of truth |
| `ingest.py` | CLI ingester: pulls EDGAR, parses XML, writes to SQLite |
| `parser.py` | Form 4 XML → transaction row dicts |
| `tickers.py` | CIK → ticker cache (EDGAR company_tickers.json, refreshes weekly) |
| `sector.py` | SIC code → sector enrichment, EDGAR fetch + 90-day DB cache |
| `alerts.py` | Slack push alerts — big buy, C-suite buy, cluster detection |
| `queries.py` | All SQL queries + EnrichContext dataclass — no SQL in app.py |
| `app.py` | FastAPI routes + `render_sparkline()` + `_resolve_date_range()` helpers |
| `templates/logic.html` | Logic & Config tab — editable thresholds, conviction weights, research basis |
| `templates/watchlist.html` | Watchlist management page |

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
python ingest.py --since-last-run        # used by systemd timer (alerts fire)
python ingest.py --resolve-amendments    # backfill 4/A supersession
python ingest.py --backfill-sectors      # fetch missing SIC/sector for all issuers
```

## Deploy

```bash
git push
ssh deploy@167.99.167.244 "cd /home/deploy/insider-tracker && git pull && sudo systemctl restart insider-tracker.service"
```

## Systemd services (on server)

- `insider-tracker.service` — uvicorn web app, always-on
- `insider-ingest.timer` — runs ingest at 10:30 AM, 2:00 PM, 7:00 PM ET Mon–Fri
- `insider-ingest.service` — oneshot triggered by timer

```bash
sudo systemctl status insider-tracker.service
sudo journalctl -u insider-tracker.service -f
sudo systemctl status insider-ingest.timer
```

## Schema

Primary table: `filings` — one row per transaction (not per filing).
`transaction_id` = `{accession_no}-{ND|D}-{row_index}` is the true PK.

Additional columns added via `_migrate()` (idempotent, runs at startup):
- `superseded_by TEXT` — set when a 4/A amendment supersedes this row
- `sector TEXT` — enriched from EDGAR SIC codes via `sector.py`

Other tables: `run_log`, `alerts_sent`, `sectors`, `watchlist`

## Config / Logic tab

All tunable parameters live in `config.py` (alert thresholds, conviction weights, filter defaults). The `/logic` page renders and edits them. Edits save to `config_overrides.json` (gitignored) without touching source files.

`config_overrides.json` keys: `alert_rules`, `filter_defaults`, `conviction_flags`

## EnrichContext (queries.py)

`_enrich(rows, ctx)` attaches computed fields to every row. Pass an `EnrichContext` to enable conviction scoring, watchlist flags, etc. Add new enrichment fields to the dataclass — never add positional params to `_enrich`.

```python
ctx = EnrichContext(
    conn=db,
    conviction_flags=..., conviction_tiers=...,
    cluster_window_days=14,
    ceo_cfo_keywords=[...],
    watched_tickers=set(), watched_insiders=set(),
    compute_conviction=True,
)
```

## Dashboard features

- **Conviction score** 1–10 per buy trade, research-backed weights (editable in /logic)
- **Cluster detection** — 2+ insiders same issuer same day, expandable cards
- **Amendment resolution** — 4/A rows automatically supersede originals on ingest
- **Slack alerts** — big buy ($1M+), C-suite buy ($250K+), cluster (3+ insiders/10 days). Set `SLACK_WEBHOOK_URL` in `.env`
- **Sector enrichment** — SIC codes from EDGAR, 11-bucket mapping, filter by sector
- **Watchlist** — pin tickers/insiders, gold ★ in dashboard, "Watched only" filter
- **Date range** — Today/7d/30d presets + custom range; daily summary for ranges >7 days
- **CSV export** — downloads current filtered view (SQL-capped at 10k rows before enrichment)
- **Sparkline** — 6-month buy/sell trend on issuer pages, Monday-date week keys

## Known gotchas

- **EDGAR XML structure:** Numeric values wrapped in `<value>` child element — `parser.py` handles this
- **form.idx columns:** Anchor on the `edgar/data` string, not fixed column offsets
- **XML filename:** Fetched from `-index.htm` filing index — it's NOT `{accession_no}.xml`
- **Starlette API:** Use `TemplateResponse(request, "name.html", context)` — not the old `(name, context)` form
- **static/ dir:** Empty → not tracked by git. Run `mkdir -p static` after fresh clone on server
- **10b5-1 detection:** Checks `<rule10b5-1Indicator>` XML element first, falls back to footnote scan
- **Sparkline week keys:** Use Monday-date strings (not `%Y-%W`) — `%G/%V` requires SQLite 3.38+; server ships 3.37
- **`_batch_cluster_counts`:** One SQL query for all (issuer_cik, transaction_date) pairs — not N+1
- **Alert dedup:** `INSERT OR IGNORE` + `cursor.rowcount` before commit (not `changes()` after)
- **Buy alert keys:** Unified `buy:` prefix — prevents double-firing when a trade matches both big_buy and insider_buy thresholds
- **`_resolve_date_range(d, start_date, end_date)`:** Use this helper in any route that accepts date params — don't inline the parsing block again

## SEC compliance

- User-Agent: `"Option Pit Research charlie@optionpit.com"` (required — SEC blocks missing/generic UAs)
- Rate limit: 8 req/sec (SEC cap is 10)

## Phase 3 candidates (after ~1 month of real use)

- **Earnings proximity flag** — mark trades within 10 days of earnings (needs earnings calendar source)
- **Historical baseline signal** — flag when a buy is an outlier vs. this insider's own history
- **Conviction weight tuning** — calibrate against actual forward returns
- **Market cap tier filter** — small-cap signal is 2–3× stronger per research; needs price data
- **AI trade analysis** — Claude API "why is this notable" blurb on high-conviction trades
- **Notes/tags on filings** — internal editorial commentary
- **Email digest** — daily summary as alternative to Slack
