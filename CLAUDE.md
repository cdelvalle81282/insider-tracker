# Insider Tracker

SEC Form 4 insider trading dashboard for Option Pit Research editorial use.

## What this is

Pulls Form 4 filings (insider buys/sells) from SEC EDGAR, stores them in SQLite, and serves a web dashboard at https://opi-insider.duckdns.org. Built for daily editorial research — "what happened today, ranked by dollar value and conviction score."

## Self-Improvement Protocol

**Every time you fix a bug, hit an unexpected edge case, or discover a non-obvious behavior — add a bullet to "Known gotchas" before committing.** Use the format:
- **Short label:** What breaks / what to do instead. Why it matters.

This file is the institutional memory for this codebase. If you had to investigate it, the next session shouldn't have to.

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
| `ingest.py` | CLI ingester: pulls EDGAR, parses XML, writes to SQLite. Also houses `get_db()` and `_migrate()`. |
| `parser.py` | Form 4 XML → transaction row dicts |
| `tickers.py` | CIK → ticker cache (EDGAR company_tickers.json, refreshes weekly) |
| `sector.py` | SIC code → sector enrichment, EDGAR fetch + 90-day DB cache |
| `alerts.py` | Slack push alerts — big buy, C-suite buy, cluster detection |
| `queries.py` | All SQL queries + EnrichContext dataclass — no SQL in app.py. Also `MARKET_CAP_TIERS` constant. |
| `app.py` | FastAPI routes. Uses `Depends(get_request_db)` for per-request DB connections. |
| `polygon_client.py` | Polygon.io: daily OHLCV bars, earnings, and `fetch_ticker_metadata()` (market cap + options) |
| `congress_ingest.py` | Congressional trades ingester — AInvest API, ticker-by-ticker, run manually or on schedule |
| `templates/chart.html` | Candlestick chart page with insider markers (TradingView Lightweight Charts) |
| `templates/logic.html` | Logic & Config tab — editable thresholds, conviction weights, research basis |
| `templates/watchlist.html` | Watchlist management page |
| `templates/insider.html` | Insider detail page — all trades by one person across all companies |
| `templates/congress.html` | Congressional trades tab — AInvest data, chamber/party/type filters |
| `templates/base.html` | Shared nav; add new tabs here |

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
python ingest.py --since-last-run          # used by systemd timer (alerts fire)
python ingest.py --resolve-amendments      # backfill 4/A supersession
python ingest.py --backfill-sectors        # fetch missing SIC/sector for all issuers
python ingest.py --backfill-metadata       # fetch Polygon market_cap + has_options for all tickers
                                           # (free tier: ~5 req/min → hours for full DB)
python ingest.py --update-prices           # fetch latest close prices for all tickers in ticker_metadata
```

## Congressional trades ingester

```bash
# Populate congressional trades from AInvest API (requires AINVEST_API_KEY in .env)
python congress_ingest.py                   # all tickers in filings DB (skips tickers fresh < 7 days)
python congress_ingest.py --ticker AAPL    # single ticker
python congress_ingest.py --limit 100      # cap for testing
python congress_ingest.py --stale-days 30  # change freshness threshold
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

Additional columns added via `_migrate()` (idempotent, runs at every `get_db()` call):
- `superseded_by TEXT` — set when a 4/A amendment supersedes this row
- `sector TEXT` — enriched from EDGAR SIC codes via `sector.py`
- `joint_filer_of TEXT` — for deduplicated joint-filer filings

Other tables:

| Table | Purpose |
|-------|---------|
| `run_log` | One row per ingest run — date, filings found, rows inserted, errors |
| `alerts_sent` | Dedup table for Slack alerts; keyed on `alert_key` |
| `sectors` | `issuer_cik → sic_code, sector` — 90-day cache from EDGAR |
| `watchlist` | Pinned tickers and insider CIKs |
| `ticker_metadata` | `ticker → has_options (0/1), market_cap (float)` — populated by `--backfill-metadata` |
| `congress_trades` | Congressional trades from AInvest API — `source, transaction_id (UNIQUE), politician_name, chamber, party, state, ticker, transaction_type, transaction_date, disclosure_date, amount_*` |

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
- **Buy/Sell/Both toggle** — 3-way button group at top of filter bar; sets transaction codes via JS
- **CSV export** — downloads current filtered view (SQL-capped at 10k rows before enrichment)
- **Sparkline** — 6-month buy/sell trend on issuer pages, Monday-date week keys
- **Chart page** — `/chart/{ticker}` candlestick chart (Polygon.io) with insider buy/sell markers; 1m/3m/6m/1y timeframes; Buys/Sells/Both toggle. Requires `POLYGON_API_KEY` in `.env`
- **Held After column** — shows `shares_owned_after` in buys/sells tables
- **Insider detail page** — `/insider/{cik}` — full trade history across all companies for one person; links from insider names in main table
- **Hide Funds/ETFs/REITs** — checkbox filter; uses SIC codes 6726 (funds/ETFs/BDCs) and 6798 (REITs) from `sectors` table. Conservative: unenriched issuers remain visible.
- **Has Options Only** — checkbox filter; uses `ticker_metadata.has_options`. Unenriched tickers are excluded when filter is active (user opted in).
- **Market Cap tiers** — multi-checkbox: Micro/Small/Mid/Large/Mega; defined in `queries.MARKET_CAP_TIERS`. Unenriched tickers remain visible.
- **Congressional trades tab** — `/congress`; sourced from AInvest API (live 2024+ data); filterable by chamber, party, type, ticker, politician

## Environment variables (on server)

Set in `/home/deploy/insider-tracker/.env`, loaded by systemd `EnvironmentFile`:
- `SLACK_WEBHOOK_URL` — Slack incoming webhook for alerts
- `POLYGON_API_KEY` — Polygon.io API key for chart price data and `--backfill-metadata`
- `AINVEST_API_KEY` — AInvest API key for congressional trades (`congress_ingest.py`)

## Concurrency model

- **Per-request DB connections** via `Depends(get_request_db)` in `app.py`. Each request opens its own `sqlite3.Connection` and closes it on response.
- **WAL mode** — multiple simultaneous readers, one writer, no reader-writer blocking.
- **`busy_timeout=5000`** — writes wait up to 5 seconds before raising `OperationalError: database is locked`.
- **`check_same_thread=False`** — required because FastAPI's dependency injection can create the connection in a different thread than the route handler runs in.
- **External ingesters** (`ingest.py`, `congress_ingest.py`) open their own connection — WAL keeps them from blocking web reads, `busy_timeout` handles write contention gracefully.
- **Rate limits** — `@limiter.limit("60/minute")` on `/`, `/htmx/filings`, `/congress`, `/export.csv`, `/chart/{ticker}`, `/logic/test-alert`.

## Adding new filters — checklist

Every new filter param must appear in ALL of these or it will be silently dropped:
1. `get_filings_for_date()` signature in `queries.py` (with a safe default)
2. The SQL WHERE clause builder in `get_filings_for_date()`
3. The `GET /` route in `app.py`
4. The `GET /htmx/filings` route in `app.py`
5. The `GET /export.csv` route in `app.py`
6. The `filters` dict returned to the template in the index route
7. The checkbox/input in `templates/index.html`
8. Empty-state colspan increments in `templates/_tables_partial.html` if adding a column
9. Add to `_build_filings_where()` in `queries.py` (the WHERE-builder)
10. Add to `get_filings_count()` signature in `queries.py` (must stay in sync with data query)
11. Add to `cache_key_dict` built in `_filters_dict()` in `app.py`

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
- **Lightweight Charts:** Always use `autoSize: true` — never `width: element.clientWidth` at init time (clientWidth can be 0 before CSS applies). Pin CDN version: `@4.2.0`
- **Duplicate form inputs:** Never have two `<input>` elements with the same `name` in the HTMX filter form — FastAPI receives them as a list and may 422 or silently mishandle.
- **EDGAR daily-index vs quarterly:** `full-index/YYYY/QTRn/form.idx` is updated with a multi-day lag. For recent dates use `daily-index/YYYY/QTRn/form.YYYYMMDD.idx` (same-day). Fall back to quarterly on any non-200 (not just 404 — SEC also returns 403 for missing dates).
- **Daily-index date format:** Daily index embeds dates as `YYYYMMDD` (no dashes); quarterly uses `YYYY-MM-DD`. Normalize to ISO before storing as `filed_at` or `DATE(filed_at)` queries return 0 results.
- **SQLite `check_same_thread`:** Must pass `check_same_thread=False` to `sqlite3.connect()`. FastAPI's `Depends()` dependency runs in a different thread than the route, so without this flag every request raises `ProgrammingError: SQLite objects created in a thread can only be used in that same thread`.
- **`urlencode` with multi-value params:** Always use `urlencode(..., doseq=True)` when the dict may contain lists (e.g. `market_cap_tiers`, `roles`, `codes`). Without `doseq=True`, a list value is stringified as `"['micro', 'small']"` instead of repeated `?market_cap_tiers=micro&market_cap_tiers=small`.
- **Jinja2 custom filters:** Register via `templates.env.filters["filter_name"] = fn` immediately after `Jinja2Templates(...)` init. Cannot be defined inside templates.
- **AInvest congress API:** Ticker-based only — no bulk endpoint. Paginate with `size=100` until `len(data) < 100`. The `data.data` field is `null` (not `[]`) when the ticker has no records — always use `outer.get("data") or []`, not just `.get("data", [])`.
- **Congress data sources (2026):** Senate Stock Watcher GitHub archive ends 2020. House Stock Watcher S3 bucket (`house-stock-watcher-data.s3-us-west-2.amazonaws.com`) returns 403 — effectively dead. `senatestockwatcher.com/api` and `housestockwatcher.com/api` are unreliable. Use AInvest API for current data.
- **`ticker_metadata` filter semantics:** `hide_funds` is conservative (unenriched = visible). `has_options_only` is restrictive (unenriched = excluded). `market_cap_tiers` is conservative (unenriched = visible). These semantics differ intentionally — document when adding new metadata-backed filters.
- **`_replace_filter` Jinja2 filter:** Used in congress.html sort links to build query strings. Registered in `app.py` after `Jinja2Templates` init. Requires `doseq=True` for multi-value params.
- **`_build_filings_where()` is the WHERE-clause source of truth:** Both `get_filings_for_date()` and `get_filings_count()` call this helper. New filters MUST go here first, then appear in both callers. Checklist items 9–10 in "Adding new filters" now reference this.
- **Pagination conviction sort:** SQL `LIMIT/OFFSET` cannot be applied when `sort_by="conviction"` because Python must sort ALL results first. The refactored `get_filings_for_date()` skips SQL pagination for conviction sort and Python-slices after enrichment. Do not add `LIMIT/OFFSET` to the conviction SQL path.
- **Response cache (TTLCache, 30s) is per-worker:** With `--workers 2`, each process has its own `_query_cache`. Cache key uses `json.dumps(normalized, sort_keys=True, default=str)` with lists pre-sorted — `sorted(v) if isinstance(v, list)`. Do not use `str(sorted(d.items()))` (unstable for list values).
- **Pager buttons must use `hx-include="false"`:** Pager buttons build a complete query string via `replace_filter` on the full `filters` dict. Adding `hx-include="#filter-form"` would double-send all filter params (422 errors for typed Query params). Always use `hx-include="false"` on any button that carries a full URL via `hx-get`.
- **`_filters_dict()` canonical contract:** Both `/` and `/htmx/filings` routes must call `_filters_dict()` to build the `filters` context. Boolean checkbox values are stored as `'1'`/`'0'` strings (not Python booleans) so they round-trip correctly through URLs and Jinja `== '1'` checks. Never pass raw Python booleans in the filters dict.
- **`asyncio.to_thread` sequential — one DB connection:** Multiple `asyncio.to_thread` calls in one route are sequential (`await` waits for each). Same `db` connection used for all. Do NOT use `asyncio.gather` with the same connection (concurrent thread access to one sqlite3 connection is unsafe even with `check_same_thread=False`).
- **`price_perf_pct` requires `--update-prices` to be meaningful:** The field is silently `None` until `insider-prices.timer` runs (weekdays 21:00 ET). Monitor staleness with `SELECT MAX(last_close_at) FROM ticker_metadata WHERE last_close IS NOT NULL`.

## SEC compliance

- User-Agent: `"Option Pit Research charlie@optionpit.com"` (required — SEC blocks missing/generic UAs)
- Rate limit: 8 req/sec (SEC cap is 10)

## Future candidates

- **Auth / CSRF on mutating endpoints** — `/logic/save`, `/watchlist/add`, `/watchlist/remove` have no auth. Low risk as internal tool, required before sharing more broadly.
- **Earnings proximity flag** — mark trades within 10 days of earnings (needs earnings calendar source)
- **Historical baseline signal** — flag when a buy is an outlier vs. this insider's own history
- **Conviction weight tuning** — calibrate against actual forward returns
- **AI trade analysis** — Claude API "why is this notable" blurb on high-conviction trades
- **Notes/tags on filings** — internal editorial commentary
- **Email digest** — daily summary as alternative to Slack
- **Congress ingest on timer** — wire `congress_ingest.py` into a systemd timer for automatic daily refresh
