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

`alembic upgrade head` is not optional: this recipe used to omit it, so a commit
that added a migration deployed code against the old schema.

```bash
git push
ssh deploy@167.99.167.244 "cd /home/deploy/insider-tracker && git pull \
  && .venv/bin/pip install -q -r requirements.txt \
  && set -a && . ./.env && set +a \
  && .venv/bin/alembic upgrade head \
  && sudo systemctl restart insider-tracker.service"
```

`bash deploy.sh <domain>` does the same thing, and `--setup` bootstraps a fresh
box (venv, .env gate, migrations, all 12 systemd units, nginx htpasswd, certbot).

## Wisepub SSO (added 2026-07-25)

The dashboard can be embedded on the paid Wisepub site (`vip.optionpit.com`) and a
logged-in Wisepub user gets in without the Basic Auth prompt. SSO Site record **5**
on Wisepub points here; outbound URL `https://vip.optionpit.com/sso-redirect/5`.

- `wisepub_sso.py` — HS256 JWT verification, replay guard, `wp_sso` session cookie.
  Copied verbatim from portfolio-tracker/income-value-tracker; keep the three in sync.
- `app.py` gains `GET /sso` (verify token, 303 to `/` with the cookie) and
  `GET /internal/sso-authz` (204/401 — nginx's `auth_request` target).
- **nginx does the gating**: `location /` and `/static/` use `satisfy any;` with both
  `auth_basic` and `auth_request /internal/sso-authz`, so Basic Auth (staff) or a
  Wisepub session (subscriber) both work. `/sso` is its own location with no
  `auth_basic`. The site also switched from `frame-deny` to `frame-vip`.
  Live config is `sites-enabled/insider-tracker`, which is a **real file, not a
  symlink** — edit that one.
- `WISEPUB_SSO_SECRET` in `.env` must match the record's "JWT Secret" field.
- The replay guard uses the Redis client from `cache.py` (db=3) on purpose: the
  service runs `--workers 2`, and an in-process guard would let a token be spent
  once per worker. The JWT has no `exp` claim, so `iat` freshness (120s) plus
  one-time use is the whole replay defense.
- SSO users get in, but they are **not** staff. `security.verify_mutation` is
  registered as an app-level dependency, so every POST requires a valid CSRF token
  *and* `is_staff()`, which an SSO session only satisfies when the token carried
  `allow_staff`. Shipped 2026-07-27; this was the stated precondition for turning
  `Show in Menu` on for that record.
- **Read authorization is still all-or-nothing.** Any authenticated user, staff or
  subscriber, can GET every page including `/logic`, `/run-log`, `/backtest` and
  `/export.csv`. See "Subscriber launch" below before exposing this to subscribers.

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

## Subscriber launch (audit 2026-07-27, not yet built)

State of play before opening this app to paying subscribers via Wisepub SSO.

**Writes are already safe.** `security.verify_mutation` is an app-level dependency,
so every POST requires CSRF plus `is_staff()`. A subscriber cannot save config,
touch the watchlist, or fire a test alert, at the app layer *and* at nginx.

**Reads are not gated at all.** Any authenticated user can GET `/logic` (every
threshold and conviction weight), `/run-log` (ingest operations), `/backtest` and
`/backtest-logic` (unpublished research), `/performance`, and `/export.csv` (bulk
extract of the whole filing set, capped only at 3/min per IP). These need a staff
read gate before launch.

**The watchlist is global, and that is the real blocker.** One table, `UNIQUE (type,
value)`, no owner column. It also drives Slack alerts and the auto-add in
`load_insider_profiles.py`. Giving subscribers write access to it as-is would let
any subscriber edit and delete every other subscriber's entries and spam editorial
Slack. Per-subscriber lists need an `owner` column (NULL meaning the house list),
with the house list exposed read-only so subscribers can see staff picks.

- **`wp_sso` sessions carry `email`**, so a stable per-subscriber identity already
  exists to key rows and analytics on. No new login system is needed.
- **Cache keys must include the owner once watchlists are per-user.**
  `it:query:watchlist-activity:{date}` is global today, and `_enrich` bakes
  `watched` flags into cached HTML. Left alone, subscriber A would be served
  subscriber B's watchlist out of Redis.
- **Rate limits are per worker, not global.** `Limiter(key_func=get_remote_address)`
  defaults to in-memory storage, so with `--workers 2` every stated limit is really
  double, and limits reset on restart. Point slowapi at the Redis on db=3.
- **No usage analytics exist.** Nothing records who viewed what.

Capacity is not a concern: the droplet is 4 vCPU / 8 GB (the `1vcpu-1gb` hostname
is stale and misleading), load ~0.3, DB 656 MB, service at 128 MB of a 512 MB cap,
9 of 100 Postgres connections used, and the expensive queries are already Redis
cached. Raise `--workers` and `MemoryMax` together if concurrency climbs.

## Future candidates

- **Filter out private-fund / tickerless filers (editorial scope)** — Insider activity on private, non-tradeable vehicles (in-house general-account trusts, private credit funds) is noise for this tool: you can't act on it. Real examples: Manulife's insurance subs (Manufacturers Life Insurance Co / Manulife (International)/(Singapore) / Manulife Reinsurance) filing Form 4 as 10%+ owners of "John Hancock GA Mortgage Trust" / "John Hancock GA Senior Loan Trust"; "Diameter Dynamic Credit Fund". These have `issuer_ticker = NULL` and are *already suppressed from Slack alerts* by the `issuer_ticker IS NOT NULL` guard in the matchers (see `private/gotchas.md`), but they still show up in the dashboard tables/KPIs. Decide the desired scope: (a) an explicit "hide tickerless / private-fund filers" filter on the dashboard, and/or (b) a curated issuer-name denylist, and/or (c) exclude from KPI aggregates. Low priority — the alert layer already keeps them out of Slack. Revisit later.
- **Per-subscriber watchlists (blocks the subscriber launch).** The `watchlist` table is global: `UNIQUE (type, value)` with no owner column, so there is exactly one list shared by everyone. Subscribers currently cannot write to it (all POSTs require staff), which is why nothing is broken today, but "let subscribers build a watchlist" needs an `owner` column plus a house/curated list they can read. See "Subscriber launch".
- **Read-side staff gating.** `security.verify_mutation` covers writes only. `/logic`, `/run-log`, `/backtest`, `/backtest-logic`, `/performance` and `/export.csv` are readable by any authenticated user, including subscribers. See "Subscriber launch".
- **Earnings proximity flag** — mark trades within 10 days of earnings (needs earnings calendar source)
- **Historical baseline signal** — flag when a buy is an outlier vs. this insider's own history
- **Conviction weight tuning** — calibrate against actual forward returns
- **AI trade analysis** — Claude API "why is this notable" blurb on high-conviction trades
- **Notes/tags on filings** — internal editorial commentary
- **Email digest** — daily summary as alternative to Slack
