# Insider Scanner

SEC Form 4 insider trading dashboard for Option Pit Research editorial use.

Pulls Form 4 filings (insider buys/sells) from SEC EDGAR, stores them in PostgreSQL, and serves a web dashboard at https://opi-insider.duckdns.org. Built for daily editorial research — "what happened today, ranked by dollar value and conviction score."

## Self-Improvement Protocol

**Every time you fix a bug, hit an unexpected edge case, or discover a non-obvious behavior — add a bullet to `private/gotchas.md` before committing.**

Format: `- **Short label:** What breaks / what to do instead. Why it matters.`

This file + the docs/ folder + the gitignored `private/` folder is the institutional memory for this codebase.

## Reference docs

- **`private/gotchas.md`** — Known gotchas (EDGAR quirks, psycopg3 patterns, PG migration notes, caching, alert dedup). Read this before touching any existing logic. Gitignored (not in the public repo) — contains operational detail.
- **`private/ops.md`** — Environment variables, systemd timers/services, server info, nginx notes. Gitignored (not in the public repo) — contains server IP and infra detail.
- **`docs/architecture.md`** — DB schema, concurrency model, dashboard features list.

## Stack

- Python 3.12, FastAPI, Jinja2, HTMX 1.9.12, Tailwind CSS (CDN)
- PostgreSQL 16 via psycopg3 (connection pool via psycopg_pool + PgBouncer on port 6432)
- Redis (cache layer — query/stats/cluster HTML results; db=3)
- Schema managed by Alembic; no ORM — raw psycopg3, all queries in `queries.py`
- Linting: `ruff` (`pip install ruff`, run `ruff check .`)

## Key files

| File | Purpose |
|------|---------|
| `config.py` | All rules, thresholds, conviction weights — single source of truth |
| `db.py` | PostgreSQL connection pool (`get_db`, `get_request_db`, `get_cli_db`) and PgBouncer wiring |
| `cache.py` | Redis cache layer (`cache_get`/`cache_set`), sentinel mtime, `invalidate_query_cache()` |
| `ingest.py` | CLI ingester: pulls EDGAR, parses XML, writes to PostgreSQL via `get_cli_db()` |
| `parser.py` | Form 4 XML → transaction row dicts |
| `tickers.py` | CIK → ticker cache (EDGAR company_tickers.json, refreshes weekly) |
| `sector.py` | SIC code → sector enrichment, EDGAR fetch + 90-day DB cache |
| `alerts.py` | Slack push alerts — big buy, C-suite buy, cluster detection |
| `queries.py` | All SQL queries + EnrichContext dataclass — no SQL in app.py. Also `MARKET_CAP_TIERS` constant. |
| `app.py` | FastAPI routes. Main routes use acquire-late DB pattern; secondary routes use `Depends(get_request_db)`. |
| `auto_diagnose.py` | Autonomous Claude API diagnostic agent — triggered by `/webhook/alert` on uptime alerts |
| `health_check.py` | Nightly health check — queries `run_log` and posts Slack alert if nightly ingest missed |
| `polygon_client.py` | Polygon.io: daily OHLCV bars, earnings, and `fetch_ticker_metadata()` (market cap + options) |
| `congress_ingest.py` | Congressional trades ingester — AInvest API, ticker-by-ticker, run manually or on schedule |
| `backtest_insiders.py` | Per-insider forward excess return vs. SPY, writes `data/insider_backtest.csv`. Runs weekly via `insider-perf-profile.timer` |
| `load_insider_profiles.py` | Loads `data/insider_backtest.csv` into `insider_perf_profile`, auto-adds high-win-rate insiders to the watchlist. Runs weekly via `insider-perf-profile.timer`, right after `backtest_insiders.py` |
| `exec_ingest.py` | Executive branch trades ingester — Open Cabinet JSON download, weekly refresh, no API key needed |
| `templates/chart.html` | Candlestick chart page with insider markers (TradingView Lightweight Charts) |
| `templates/logic.html` | Logic & Config tab — editable thresholds, conviction weights, research basis |
| `templates/watchlist.html` | Watchlist management page |
| `templates/insider.html` | Insider detail page — all trades by one person across all companies |
| `templates/leaderboard.html` | Leaderboard tab — insider track-record ranking, sentiment index chart, cross-company buying |
| `templates/congress.html` | Congressional trades tab — AInvest data, chamber/party/type filters |
| `templates/base.html` | Shared nav; add new tabs here |

## Running locally

```bash
python -m venv .venv
.venv/Scripts/pip install -r requirements.txt   # Windows
.venv/bin/pip install -r requirements.txt        # Linux

# Minimum: DATABASE_URL=postgresql://user:pass@localhost:5432/insider_tracker
alembic upgrade head

# Ingest today's filings (DATABASE_URL must be exported — ingest.py doesn't call load_dotenv)
export DATABASE_URL=postgresql://...
python ingest.py --date today

# Start dashboard (.env is loaded at startup via python-dotenv)
uvicorn app:app --reload
# → http://localhost:8000
```

## Ingester CLI

```bash
python ingest.py --date today
python ingest.py --date 2026-04-22
python ingest.py --backfill 2024-01-01 2026-04-22
python ingest.py --backfill-days 30
python ingest.py --since-last-run          # used by systemd timer (alerts fire)
python ingest.py --resolve-amendments      # backfill 4/A supersession
python ingest.py --backfill-sectors        # fetch missing SIC/sector for all issuers
python ingest.py --backfill-metadata       # fetch Polygon market_cap + has_options for all tickers
python ingest.py --update-prices           # fetch latest close prices for all tickers in ticker_metadata
python ingest.py --mark-joint-filers       # detect and deduplicate joint-filer Form 4 pairs
python ingest.py --normalize-tickers       # clean malformed issuer_ticker values (NONE→NULL, NYSE:X→X, etc.)
```

## Congressional trades ingester

```bash
# Requires AINVEST_API_KEY in .env
python congress_ingest.py                   # all tickers (skips fresh < 7 days)
python congress_ingest.py --ticker AAPL    # single ticker
python congress_ingest.py --limit 100      # cap for testing
python congress_ingest.py --stale-days 30  # change freshness threshold
```

## Deploy

```bash
git push
ssh deploy@167.99.167.244 "cd /home/deploy/insider-tracker && git pull && sudo systemctl restart insider-tracker.service"
```

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

## Adding new filters — checklist

Every new filter param must appear in ALL of these or it will be silently dropped:
1. `get_filings_for_date()` signature in `queries.py` (with a safe default)
2. `_build_filings_where()` in `queries.py` (the WHERE-builder — source of truth)
3. `get_filings_count()` signature in `queries.py` (must stay in sync with data query)
4. The `GET /` route in `app.py`
5. The `GET /htmx/filings` route in `app.py`
6. The `GET /export.csv` route in `app.py`
7. The `filters` dict returned to the template in the index route
8. `cache_key_dict` built in `_filters_dict()` in `app.py`
9. The checkbox/input in `templates/index.html`
10. Empty-state colspan increments in `templates/_tables_partial.html` if adding a column

## SEC compliance

- User-Agent: `"Option Pit Research charlie@optionpit.com"` (required — SEC blocks missing/generic UAs)
- Rate limit: 8 req/sec (SEC cap is 10)

## Future candidates

- **Auth / CSRF on mutating endpoints** — `/logic/save`, `/watchlist/add`, `/watchlist/remove` have no *application-level* auth. In production this is currently mitigated at the network layer: nginx's `location /` block on the live server already applies whole-site `auth_basic` (excluding only `/healthz`, `/webhook/alert`, `/robots.txt` — see `private/ops.md`), so these routes aren't actually reachable unauthenticated today. Still worth real app-level auth/CSRF before sharing more broadly or changing the nginx front door — don't rely on the network-layer gate as a permanent substitute.
- **Earnings proximity flag** — mark trades within 10 days of earnings (needs earnings calendar source)
- **Historical baseline signal** — flag when a buy is an outlier vs. this insider's own history
- **Conviction weight tuning** — calibrate against actual forward returns
- **AI trade analysis** — Claude API "why is this notable" blurb on high-conviction trades
- **Notes/tags on filings** — internal editorial commentary
- **Email digest** — daily summary as alternative to Slack
