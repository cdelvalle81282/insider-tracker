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
- `insider-ingest.timer` — runs `--date today` at 10:30, 14:00, 19:00 UTC Mon–Fri (6:30 AM / 10 AM / 3 PM ET)
- `insider-ingest.service` — oneshot triggered by timer
- `insider-ingest-nightly.timer` — runs `--since-last-run` at 03:00 UTC Mon–Sat (11 PM ET) — catches EDGAR's end-of-day index update
- `insider-ingest-nightly.service` — oneshot triggered by nightly timer

```bash
sudo systemctl status insider-tracker.service
sudo journalctl -u insider-tracker.service -f
sudo systemctl status insider-ingest.timer
sudo systemctl status insider-ingest-nightly.timer
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
- **Rate limits** — `@limiter.limit("60/minute")` on `/`, `/htmx/filings`, `/htmx/stats`, `/htmx/clusters`, `/congress`, `/export.csv`, `/chart/{ticker}`, `/logic/test-alert`.

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
- **Sentinel-aware cache (24h TTL) is per-worker:** With `--workers 2`, each process has its own in-process caches (`_query_cache`, `_stats_cache`, `_clusters_cache`, `_sectors_cache`, `_config_cache`). Invalidated by `data/.last_ingest` sentinel mtime — `ingest.py` touches this file after each run. Cache entries store `(pre_mtime, value)`; on read, if `pre_mtime < _sentinel_mtime()`, entry is stale. Capture `pre_mtime` BEFORE the DB query — not after — or a concurrent ingest during the query will be silently missed.
- **Pager buttons must use `hx-include="false"`:** Pager buttons build a complete query string via `replace_filter` on the full `filters` dict. Adding `hx-include="#filter-form"` would double-send all filter params (422 errors for typed Query params). Always use `hx-include="false"` on any button that carries a full URL via `hx-get`.
- **`_filters_dict()` canonical contract:** Both `/` and `/htmx/filings` routes must call `_filters_dict()` to build the `filters` context. Boolean checkbox values are stored as `'1'`/`'0'` strings (not Python booleans) so they round-trip correctly through URLs and Jinja `== '1'` checks. Never pass raw Python booleans in the filters dict.
- **`asyncio.to_thread` sequential — one DB connection:** Multiple `asyncio.to_thread` calls in one route are sequential (`await` waits for each). Same `db` connection used for all. Do NOT use `asyncio.gather` with the same connection (concurrent thread access to one sqlite3 connection is unsafe even with `check_same_thread=False`).
- **`price_perf_pct` requires `--update-prices` to be meaningful:** The field is silently `None` until `insider-prices.timer` runs (weekdays 21:00 ET). Monitor staleness with `SELECT MAX(last_close_at) FROM ticker_metadata WHERE last_close IS NOT NULL`.
- **Alert matchers must filter `joint_filer_of IS NULL`:** All three matchers in `alerts.py` (`_match_big_buy`, `_match_insider_buy`, `_match_cluster`) must include `AND joint_filer_of IS NULL` alongside `superseded_by IS NULL`. Without it, joint-filer secondary rows inflate `COUNT(DISTINCT insider_cik)` causing false-positive cluster alerts and duplicate buy alerts.
- **`get_summary_stats` must filter both superseded and joint-filer rows:** The KPI bar query (`queries.py`) is a raw `SELECT FROM filings` that bypasses `_build_filings_where()`. It needs explicit `AND superseded_by IS NULL AND joint_filer_of IS NULL` or KPI counts will diverge from the table rows below them.
- **Use `cur.rowcount` after INSERT OR IGNORE, not `SELECT changes()`:** Assign the cursor (`cur = conn.execute(...)`) and check `cur.rowcount` immediately. `changes()` can reflect a subsequent write if any intervening statement runs between the INSERT and the SELECT, causing under-counting of `inserted`.
- **`polygon_api_key` must not enter template context:** `cfg.load_config()` includes `polygon_api_key`. Strip it before passing to any template: `view_config = {k: v for k, v in active_config.items() if k != "polygon_api_key"}`. The `/logic` page is unauthenticated — leaking the key via a Jinja typo is a one-line mistake away.
- **Raw SQL outside `_build_filings_where()` drifts:** Every `SELECT FROM filings` that isn't routed through `_build_filings_where()` must manually add `superseded_by IS NULL` and `joint_filer_of IS NULL`. Audit any new direct query against this checklist.
- **`_resolve_amendment` uses two-pass share matching:** Pass 1 matches on shares (handles unchanged rows in a multi-row 4/A). Pass 2 drops shares (handles share-count corrections). If 2+ candidates in either pass, skips to avoid mis-attribution. Do not collapse back to a single pass.
- **`mark_joint_filers` must GROUP BY `issuer_cik` not `issuer_ticker`:** `issuer_ticker` is nullable — SQLite treats `NULL = NULL` in GROUP BY, which can merge unrelated issuers with missing tickers into false joint-filer groups. `issuer_cik` is `NOT NULL`. After changing this, re-run `--mark-joint-filers` on the server.
- **`get_summary_stats` and `get_cluster_activity` accept `codes`:** Both functions take `codes: list[str] | None` so the KPI bar and cluster section respect the Buy/Sell toggle. Both call sites in `/` and `/htmx/filings` must pass `codes=effective_codes`. Both SQL queries inside `get_cluster_activity` (main cluster aggregation AND secondary transaction fetch) must use the codes filter.
- **`%G-W%V` for ISO week in Python, not `%Y-W%W`:** `%W` produces week `00` in early January, causing duplicate cross-year keys in `alerts_sent`. Use `%G` (ISO week-based year) and `%V` (ISO week 01–53, never 00) in Python `datetime.strftime`. This is Python only — SQLite `strftime` on server 3.37 does not support `%G`/`%V`.
- **KPI stats and cluster activity are deferred HTMX loads:** `GET /` and `/htmx/filings` no longer return stats or clusters. They're fetched async by `#stats-container` and `#clusters-container` in `index.html` via `/htmx/stats` and `/htmx/clusters`. These endpoints accept only the narrow filter subset their queries use (date, hide_10b5_1, hide_equity_swap, codes) — extra params from `hx-include="#filter-form"` are silently ignored by FastAPI.
- **Watchlist changes must clear `_query_cache`:** Cached `(buys, sells)` have watchlist flags baked in by `_enrich()`. `/watchlist/add` and `/watchlist/remove` call `_query_cache.clear()` so the next load reflects the new star immediately. Without this, cached results show stale watchlist stars for up to 24h.
- **SEC rate-limit returns 200 + HTML:** When the server IP exceeds SEC's request rate, EDGAR returns `200 OK` with an HTML "Request Rate Threshold Exceeded" page instead of the plain-text index. `resp.raise_for_status()` does not catch this. Detection: check `resp.text.lstrip().startswith("<")` and raise. Without this guard, the ingester parses the HTML silently, finds no entries, records `filings_found=0, errors=0` — a silent false-negative that's hard to diagnose from run_log.
- **`_load_config_cached()` wraps `cfg.load_config()`:** 60s TTL in-process cache. Call `_config_cache.clear()` before `save_overrides()` in `/logic/save` so config changes are visible immediately. The internal call inside `_load_config_cached()` itself must stay as `cfg.load_config()` — replacing it causes infinite recursion.
- **Congress `transaction_type` is mixed-case from AInvest:** The API returns `"Purchase"` / `"Sale"` (capitalized). The summary query must use `LOWER(transaction_type) IN ('purchase', 'buy')` — a simple `= 'purchase'` match silently returns 0. Same applies to any future filter on this column.
- **Ticker badges use `text-white` — invisible in light mode:** Any element using `class="... text-white"` in a template renders white text on the light-mode `rgb(243,246,251)` background (contrast ~1.07:1). Always use `style="color:var(--text-1);"` for text that must be readable in both modes. `text-white` is only safe inside elements that have an explicit dark background.
- **Ticker case from SEC XML is not normalized:** `issuerTradingSymbol` can arrive lowercase (e.g. "vicr"). Always `.upper().strip()` the value in `parser.py` when extracting it. Existing rows with lowercase tickers in the DB won't be fixed retroactively by a re-ingest without a targeted UPDATE.
- **`NONE` / `N/A` appear as real tickers from SEC XML:** Some filers (funds, BDCs) have no exchange symbol. The XML emits the literal string `"NONE"` or leaves the field blank. Template ticker checks must guard `row.issuer_ticker not in ('NONE', 'N/A')` or users see a chart link to `/chart/NONE`.
- **Derivative table `price_per_share` is exercise/conversion price, not market price:** `table_type = 'D'` rows store the derivative's exercise/strike/conversion price in `transactionPricePerShare`, not what was paid. This makes `total_value` (shares × that price) meaningless for derivatives (e.g. VELO showed $31T). Always add `AND table_type = 'ND'` to any query that relies on `total_value` for value-based filtering — alerts, signal scanners, and any rank/sort by dollar value.
- **`alerts_sent.alert_type` is NOT NULL:** The `alerts_sent` table has `alert_type TEXT NOT NULL`. Any `INSERT OR IGNORE INTO alerts_sent` must supply both `alert_key` AND `alert_type` or it will raise `IntegrityError: NOT NULL constraint failed`. Pattern: `conn.execute("INSERT OR IGNORE INTO alerts_sent (alert_key, alert_type) VALUES (?, ?)", (key, "my_type"))`. The SCHEMA constant defines this — check it before writing new alert dedup code.
- **`run_log.run_kind` pre-migration rows get `'unknown'`:** The `run_kind` column was added via `_migrate()` with `DEFAULT 'unknown'`. Existing rows from before the migration are stamped `'unknown'`. The health checker filters `WHERE run_kind = 'nightly'`, so it correctly ignores pre-migration rows and daytime runs. The earliest possible health alert is after the first 2 nightly (`--since-last-run`) runs post-deploy.
- **`ingest.py` did not import `os` originally:** `os.getenv()` was not used in the original file. The module-level `import os` was added when heartbeat and health-check wire-up were added. If adding new code that reads env vars directly in `ingest.py`, verify `import os` is present — it was missing for the entire early lifespan of the file.
- **`/healthz` is intentionally exempt from rate limiting:** The endpoint reads only the sentinel file mtime — no DB query. Do not add `@limiter.limit()` to it; uptime monitors poll it every 30–60 seconds and must not be throttled.
- **EDGAR daily index not available until ~03:00 UTC (11 PM ET):** `form.YYYYMMDD.idx` is published ~22:00 ET each business day. The three scheduled daytime runs (10:30/14:00/19:00 UTC = 6:30 AM/10 AM/3 PM ET) consistently return 0 filings for that day's date because the index file either doesn't exist yet or is empty. The `insider-ingest-nightly.timer` at 03:00 UTC uses `--since-last-run` to catch the previous business day's filings after EDGAR publishes them. Do NOT rely on the daytime runs to capture same-day filings reliably.
- **Concurrent ingest processes multiply the EDGAR request rate:** `ingest.py` makes 2 HTTP requests per filing (index.htm + XML) at 0.12 sec/request (~8.3 req/sec per process). Running 2+ ingest processes simultaneously exceeds SEC's 10 req/sec cap, causing 429 errors and potentially triggering a temporary IP ban on the quarterly index (403). Never run a manual backfill while a scheduled ingest or `--update-prices` is running against EDGAR. `--update-prices` uses Polygon only — it is safe to run concurrently.

### PostgreSQL migration (Phase 3) gotchas

- **`DATABASE_URL` is required:** `db.py` raises `RuntimeError` if `DATABASE_URL` is missing. Set in `.env` (e.g. `postgresql://user:pass@localhost:5432/insider_tracker`). Alembic also reads it from the same env var via `migrations/env.py`.
- **Schema is now in Alembic — `ingest._migrate()` no longer exists:** `migrations/versions/0001_initial_schema.py` is the authoritative DDL. To create the schema on a fresh DB: `alembic upgrade head`. Adding columns at runtime via `ALTER TABLE` from Python (the old `_migrate()` pattern) is gone. Add a new Alembic revision instead.
- **psycopg3 named parameters use `%(name)s`, not SQLite's `:name`:** `_upsert_rows()` in `ingest.py` passes a dict with `%(transaction_id)s` placeholders. Searching for `:transaction_id` style in any new code is a regression sign.
- **Positional placeholders are `%s` (always), even for single args:** `conn.execute("... WHERE x = %s", [val])`. SQLite's `?` will raise a `psycopg.errors.SyntaxError: syntax error at or near "?"`.
- **`LIKE` is case-sensitive in PG — use `ILIKE` for user search:** SQLite's `LIKE` was case-insensitive by default. All user-facing search fields (`issuer_ticker`, `insider_name`, `insider_title`, `politician_name`, footnote scan) now use `ILIKE`. Forgetting this silently drops legitimate matches like searching `aapl` and missing `AAPL`.
- **`GROUP_CONCAT` → `STRING_AGG`:** PG syntax is `STRING_AGG(expr, sep)` (not the comma-separated GROUP_CONCAT). The separator argument is required — no default like SQLite. `STRING_AGG` ordering is not deterministic without an explicit `ORDER BY` clause inside the call; current code re-sorts in Python so it doesn't matter, but if you rely on aggregation order, add `STRING_AGG(x, ',' ORDER BY y)`.
- **PG aggregate subqueries require an alias:** `SELECT COUNT(*) FROM (SELECT ... GROUP BY ...) AS sub` — the `AS sub` is mandatory in PG, optional in SQLite. The cluster-count query in `get_summary_stats` is the only current example.
- **PG aborts the whole tx on any error — use per-row `conn.transaction()` in batch inserts:** Unlike SQLite (which keeps going), a failed `INSERT` in PG puts the connection in an aborted state — every subsequent statement until `ROLLBACK` raises "current transaction is aborted". `_upsert_rows()` wraps each row in `with conn.transaction():` so a bad row rolls back only itself. When this block runs at the top level it issues a real BEGIN/COMMIT per row; nested inside an existing tx it becomes a SAVEPOINT. Either way, prior batch rows are preserved. A bare `conn.rollback()` in the except clause would discard all prior inserts in the batch.
- **`PoolConnection.close()` returns to pool — it does NOT tear down the connection:** This means the legacy `try: ... finally: conn.close()` pattern in `app.htmx_stats`/`htmx_clusters` continues to work correctly with the pool. `getconn()` + `close()` and `getconn()` + `putconn()` are both valid; we use `close()` for parity with the old code.
- **`DATE(filed_at)` → `filed_at::date`:** PG `::date` casts are faster than calling a function and match the existing `idx_filed_date` functional index. Every WHERE clause comparing the date-part now uses `filed_at::date`.
- **`datetime('now', ?)` / `date('now', ?)` patterns translated to Python-computed cutoffs:** Rather than fight PG INTERVAL string syntax, compute the cutoff in Python (`(datetime.now(timezone.utc) - timedelta(days=N)).isoformat()`) and pass as a `%s` string parameter. Cleaner and identical semantics.
- **`strftime('%w', filed_at)` Monday-of-week → `date_trunc('week', filed_at)::date`:** PG's `date_trunc('week', ...)` returns Monday by definition. Much simpler than translating the SQLite printf expression and behaves identically.
- **`dict_row` row factory means rows ARE dicts, not Row objects:** `row[0]` index access no longer works — must use `row["column_name"]`. Every `COUNT(*)` must be aliased: `SELECT COUNT(*) AS n FROM ... fetchone()["n"]`. Every aggregate must have an explicit alias (`AS buy_count`, `AS total_value`) or it'll be keyed as the literal SQL expression string like `"COUNT(*)"` — fragile.
- **`transaction_date` is a `date` object in PG, was a string in SQLite:** Any code that compares `transaction_date` lexicographically to a string (e.g. `td >= "2025-01-01"`) needs explicit `.isoformat()` first. `_batch_cluster_counts` and `alerts.check_and_send_signals` both have explicit normalization to ISO strings. Routes using `SELECT *` (e.g. `get_issuer_filings`) don't cast `::text` so callers must normalize: `if hasattr(td, "isoformat"): td = td.isoformat()`. The `/chart/{ticker}` route 500'd on this when comparing PG `date` objects against Polygon's `"YYYY-MM-DD"` strings.
- **All files now use psycopg — no sqlite3 remains:** `health_check.py`, `backtest.py`, `generate_lc_chart.py`, `generate_signals_chart.py` are fully ported. `transaction_date::text` cast in SELECT keeps downstream string comparisons working without touching callers.
- **PostgreSQL cutover is LIVE (2026-05-19):** PG 16 runs on the same droplet (`localhost:5432`, DB `insider_tracker`, user `insider_app`). 411,546 filings migrated. `DATABASE_URL` is in `.env`. SQLite file at `data/insider_tracker.db` is now historical — do not delete until a full backup is confirmed.
- **Alembic uses SQLAlchemy 2 + psycopg3 dialect:** `migrations/env.py` swaps `postgresql://` → `postgresql+psycopg://` for SQLAlchemy but the app itself uses raw psycopg3. Run `alembic upgrade head` for new schema changes.
- **TIMESTAMP columns from PG are datetime objects — cast to text in queries:** `run_log.started_at`, `run_log.finished_at`, `alerts_sent.sent_at` are `datetime` in PG (text in SQLite). Queries use `::text` cast so existing `[:19].replace('T',' ')` template patterns work unchanged. Any new `TIMESTAMP` column exposed to templates must do the same or the template will 500.
- **SQLite `transaction_date != ''` guard removed:** `filings.transaction_date` is `DATE NOT NULL` in PG — the empty-string guard is invalid SQL and unnecessary. Removed from `generate_lc_chart.py` and `generate_signals_chart.py` during PG port.
- **Migration script date-cleaning:** Some SQLite `transaction_date` values had timezone offsets appended (e.g. `2026-03-23-05:00`). The migration script stripped to `[:10]` before inserting into PG's `DATE` column. No data was lost — all 411,546 rows inserted with 0 errors.
- **CLI scripts must use `get_cli_db()`, not `get_db()`:** `get_db()` creates a psycopg_pool `ConnectionPool` (min_size=2, max_size=8). Running any CLI script (ingest.py, congress_ingest.py, etc.) concurrently with the web app creates a second pool. Long-running operations like `mark_joint_filers` then compete for PG resources, causing the web app's pool to exhaust its 30-second `getconn()` timeout and return 500s. `get_cli_db()` is a plain `psycopg.connect()` — no pool, no background threads. Use it in all CLI/offline scripts.
- **Manual ingest produces no output if `DATABASE_URL` is not set:** `ingest.py` does not call `load_dotenv()`. It relies on the environment having `DATABASE_URL` pre-exported. When running manually over SSH, extract it first: `export DATABASE_URL=$(grep "^DATABASE_URL=" .env | head -1 | cut -d= -f2-)` — then run `PYTHONUNBUFFERED=1 .venv/bin/python ingest.py --date YYYY-MM-DD`.
- **`date.weekday()` — May 19, 2026 is Tuesday (weekday=1):** Double-check calendar math before diagnosing "missing" ingest dates. May 16 is Saturday (weekday=5) and is correctly skipped by the ingest. The actual missing business days after May 15 were May 18 (Monday) and May 19 (Tuesday).
- **GPTBot HTTP/2 multiplexing exhausts the PG connection pool:** GPTBot uses HTTP/2 and multiplexes 8+ concurrent `/filing/{id}` requests over one TCP connection, draining the entire pool (`max_size=8` per worker). Every subsequent request then fails with `PoolTimeout` for as long as the slow requests are in-flight. Fix: block AI crawlers in nginx with `if ($http_user_agent ~* "GPTBot|...")` INSIDE each `location` block — unreliable at the `server` level. Serve `/robots.txt` from a location block WITHOUT the `if` guard so crawlers can read the disallow rules. Robots.txt file lives at `/var/www/insider/robots.txt` (www-data can't traverse `/home/deploy/`).
- **nginx `sites-enabled` is NOT a symlink on this server:** Editing `/etc/nginx/sites-available/insider-tracker` does NOT update the live config — the `sites-enabled` copy is a separate file (`cp`, not `ln -s`). Always write changes directly to `/etc/nginx/sites-enabled/insider-tracker` then run `nginx -t && systemctl reload nginx`.
- **`rolling back returned connection` in pool logs is normal:** psycopg3 starts an implicit transaction on every statement (including SELECT). If the handler doesn't call `conn.commit()`, the pool sees an open transaction on return and rolls it back. Safe — not a sign of a bug.
- **Co-located cron jobs can OOM-kill uvicorn workers:** This server runs 8+ Python apps. `samcart-analytics/sync_job.py` (cron 05:00 UTC) consumed 3.6 GB RAM due to a top-level `import streamlit as st` in `cache.py` loading streamlit's full dependency tree even in headless mode. The OOM killer executed it, leaving the server memory-starved long enough for all uvicorn workers to fail to fork. Fix applied: lazy streamlit import in `cache.py` + `systemd-run --scope -p MemoryMax=1536M` wrapper on the cron. "No headers received" incident at 05:44–05:49 UTC 2026-05-29.
- **`cache_module._sentinel_mtime()` is the single source of truth for sentinel mtime:** `app.py` no longer defines its own `_sentinel_mtime()`. All callers (`_sentinel_get`, `_get_all_sectors_cached`, `healthz`) use `cache_module._sentinel_mtime()`. Do not add a local copy.
- **Redis cache lives in `cache.py` — `cache_module.cache_get/cache_set` replace `_sentinel_get/_sentinel_set` for the 3 main caches:** Keys `it:query:<hash>`, `it:stats:<hash>`, `it:clusters:<hash>` in Redis db=3. `_sectors_cache` and `_config_cache` stay in-process (too small/fast to warrant Redis RTT). Serialization is `pickle((pre_mtime, value))`; TTL 24h. Any `RedisError` silently falls through to a cache miss — Redis is a perf dep, not availability. `invalidate_query_cache()` scans `it:query:*` and deletes — used on watchlist add/remove to flush stale watchlist star flags across both workers.
- **`index()` and `htmx_filings()` use acquire-late pattern — no `Depends(get_request_db)`:** Both routes call `get_db()` manually, guarded by `need_db = (cached_result is None) or summary_mode or (cached_sectors is None)`. Hot path (cache hit, no date range, sectors warm) holds zero DB connections. Do NOT add `Depends(get_request_db)` back — it holds a connection for the full request duration including template render.
- **`htmx_stats()` and `htmx_clusters()` cache rendered HTML strings:** `_stats_cache` and `_clusters_cache` store HTML fragments rendered via `templates.env.get_template(name).render(context)`, not raw data dicts. On cache hit, return `HTMLResponse(html)` directly — no Jinja render, no DB access. Only safe because `_stats_partial.html` and `_clusters_partial.html` have no `request.*` calls.
- **`PoolTimeout` on data-heavy dates = rapid navigation exhausts pool:** `asyncio.to_thread()` SQL threads are non-cancellable. When the browser navigates away (RST_STREAM), the HTTP request is cancelled but the SQL thread keeps running and holds its pool connection. On dates with many filings (e.g. busy Fridays), queries are slow enough that 8+ cancelled threads can pile up, exhausting `max_size`. Fix applied: `max_size=16` and `statement_timeout=25000ms` in `db.py`'s `_get_pool()`. If this recurs, check for slow queries with `SELECT pid, now()-query_start, query FROM pg_stat_activity WHERE datname='insider_tracker'`.
- **`insider-tracker.service` has `MemoryMax=512M` / `MemoryHigh=400M`:** Workers currently use ~65 MB each (130 MB total). The hard limit is 512 MB — if the process exceeds this systemd kills it. Raise the limit in `/etc/systemd/system/insider-tracker.service` if adding workers or the cache grows significantly (then `sudo systemctl daemon-reload`).
- **`insider-backup.timer` runs at 05:30 UTC, not 05:00:** Staggered 30 minutes after the `sync_job` cron to avoid simultaneous AWS CLI + Python sync competing for memory at the same minute.

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
