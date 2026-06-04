# Architecture Reference

## Schema

Primary table: `filings` — one row per transaction (not per filing).
`transaction_id` = `{accession_no}-{ND|D}-{row_index}` is the true PK.

Schema is managed by Alembic (`alembic upgrade head`). Notable columns:
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

## Concurrency model

- **Connection pool** via `psycopg_pool.ConnectionPool` (min_size=2, max_size=16). Connects through PgBouncer (`PGBOUNCER_URL`, port 6432) when set, else direct PG (`DATABASE_URL`).
- **`autocommit=True`** on pool connections — each `execute()` auto-commits. Do NOT call `conn.commit()` from web routes; it raises `ProgrammingError`.
- **`prepare_threshold=None`** — prepared statements disabled; required for PgBouncer transaction pool mode.
- **`statement_timeout=8000ms`** set per connection via `_configure_connection()` in `db.py`.
- **Acquire-late pattern** in `index()` and `htmx_filings()` — these routes call `get_db()` manually only when a cache miss requires a DB query. Hot path (cache hit) holds zero connections.
- **`get_request_db()`** as FastAPI `Depends()` in secondary routes (`/filing/{id}`, `/issuer/…`, `/congress`, etc.) — connection held for request duration.
- **CLI scripts** (`ingest.py`, `congress_ingest.py`, etc.) use `get_cli_db()` — a plain `psycopg.connect()` directly to PG (not pooled, not PgBouncer). Supports explicit `conn.commit()`/`rollback()` and `with conn.transaction():`.
- **Redis** (`cache.py`) stores rendered HTML for query/stats/cluster results (TTL 24h, db=3). Cache miss falls through to DB silently — Redis is a perf dep, not availability.
- **Rate limits** — `@limiter.limit("60/minute")` on `/`, `/htmx/filings`, `/htmx/stats`, `/htmx/clusters`, `/congress`, `/export.csv`, `/chart/{ticker}`, `/logic/test-alert`.

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
- **Executive branch trades** — `/congress` Source filter `open_cabinet`; OGE 278-T filings via Open Cabinet; `exec_ingest.py` refreshes weekly
