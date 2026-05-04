from __future__ import annotations

import asyncio
import csv
import hashlib
import io
import json
import os
import re
import sqlite3
import statistics
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path

from cachetools import TTLCache
from fastapi import Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

import alerts as alert_module
import config as cfg
import polygon_client
import queries
from config import PAGE_SIZE, save_overrides
from ingest import get_db
from queries import EnrichContext

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _replace_filter(filters: dict, key: str, value) -> str:
    """Jinja2 filter: return query string with one key replaced."""
    from urllib.parse import urlencode
    updated = {**filters, key: value}
    return urlencode({k: v for k, v in updated.items() if v not in (None, "", [])}, doseq=True)


templates.env.filters["replace_filter"] = _replace_filter

_TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,10}$")
_CIK_RE = re.compile(r"^\d{1,10}$")
limiter = Limiter(key_func=get_remote_address)

_query_cache: TTLCache = TTLCache(maxsize=256, ttl=30)


def _cache_key(params: dict) -> str:
    """Stable cache key — normalizes list values so order doesn't affect result."""
    normalized = {k: sorted(v) if isinstance(v, list) else v for k, v in params.items()}
    return hashlib.md5(json.dumps(normalized, sort_keys=True, default=str).encode()).hexdigest()


def _filters_dict(
    *,
    d, start_date, end_date, min_value, codes, hide_10b5_1, hide_equity_swap,
    roles, search, ceo_cfo, sort_by, sort_order, sector, watched_only,
    hide_funds, has_options_only, market_cap_tiers, buys_page, sells_page,
) -> dict:
    """Canonical filters dict for templates. Stores checkboxes as '1'/'0' strings
    so pager URLs serialize correctly. All list values included as lists."""
    return {
        "d": d, "start_date": start_date, "end_date": end_date,
        "min_value": min_value,
        "codes": codes or [],
        "hide_10b5_1": "1" if hide_10b5_1 else "0",
        "hide_equity_swap": "1" if hide_equity_swap else "0",
        "roles": roles or [],
        "search": search or "",
        "ceo_cfo": ceo_cfo,
        "sort_by": sort_by, "sort_order": sort_order,
        "sector": sector or "",
        "watched_only": watched_only,
        "hide_funds": hide_funds,
        "has_options_only": has_options_only,
        "market_cap_tiers": market_cap_tiers or [],
        "buys_page": buys_page,
        "sells_page": sells_page,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield  # nothing to set up/tear down at app level


app = FastAPI(title="Insider Tracker", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def get_request_db() -> sqlite3.Connection:
    conn = get_db()
    try:
        yield conn
    finally:
        conn.close()


def _parse_date(d: str | None) -> date:
    if not d:
        return date.today()
    try:
        return date.fromisoformat(d)
    except ValueError:
        return date.today()


def _resolve_date_range(
    d: str | None,
    start_date: str | None,
    end_date: str | None,
) -> tuple[date, date, bool]:
    """Parse start/end date params; fall back to single date d. Returns (start, end, is_range)."""
    if start_date and end_date:
        s, e = _parse_date(start_date), _parse_date(end_date)
    else:
        s = e = _parse_date(d)
    return s, e, s != e


def render_sparkline(series: list[dict]) -> str:
    """Render buy/sell trend as an inline SVG string. Returns '' when all values are zero."""
    if not series or all(p['buy_total'] == 0 and p['sell_total'] == 0 for p in series):
        return ""

    W, H, PAD = 240, 40, 4
    x_step = (W - 2 * PAD) / max(len(series) - 1, 1)
    buys = [p['buy_total'] for p in series]
    sells = [p['sell_total'] for p in series]
    all_vals = buys + sells
    mn, mx = min(all_vals), max(all_vals)
    span = mx - mn

    def to_y(v: float) -> float:
        return H / 2 if span == 0 else H - PAD - ((v - mn) / span) * (H - 2 * PAD)

    def points(vals: list) -> str:
        return " ".join(f"{PAD + i * x_step:.1f},{to_y(v):.1f}" for i, v in enumerate(vals))

    zero_y = to_y(0)
    return (
        f'<svg width="{W}" height="{H}" class="w-full">'
        f'<line x1="{PAD}" y1="{zero_y:.1f}" x2="{W-PAD}" y2="{zero_y:.1f}" stroke="#374151" stroke-width="0.5"/>'
        f'<polyline points="{points(sells)}" fill="none" stroke="#ef4444" stroke-width="1.5" stroke-linejoin="round"/>'
        f'<polyline points="{points(buys)}" fill="none" stroke="#22c55e" stroke-width="1.5" stroke-linejoin="round"/>'
        f'</svg>'
    )


def _make_ctx(db: sqlite3.Connection, active_config: dict) -> EnrichContext:
    """Build an EnrichContext with conviction config and watchlist sets."""
    return EnrichContext(
        conn=db,
        conviction_flags=active_config.get("conviction_flags"),
        conviction_tiers=active_config.get("conviction_tiers"),
        conviction_max=active_config.get("conviction_max", 10),
        conviction_thresholds=active_config.get("conviction_thresholds"),
        cluster_window_days=active_config.get("alert_rules", {}).get("cluster_window_days", 14),
        ceo_cfo_keywords=active_config.get("alert_rules", {}).get("insider_title_keywords", []),
        watched_tickers=queries.watched_tickers(db),
        watched_insiders=queries.watched_insiders(db),
        compute_conviction=True,
    )


# ---------------------------------------------------------------------------
# Main dashboard
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def index(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
    d: str | None = Query(default=None),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
    min_value: float = Query(default=None),
    hide_10b5_1: str = Query(default=None),
    hide_equity_swap: str = Query(default=None),
    codes: list[str] = Query(default=None),
    roles: list[str] = Query(default=None),
    search: str | None = Query(default=None),
    ceo_cfo: str = Query(default="0"),
    sort_by: str = Query(default="value"),
    sort_order: str = Query(default="desc"),
    sector: str | None = Query(default=None),
    watched_only: str = Query(default="0"),
    hide_funds: str = Query(default="0"),
    has_options_only: str = Query(default="0"),
    market_cap_tiers: list[str] = Query(default=[]),
    buys_page: int = Query(default=1, ge=1),
    sells_page: int = Query(default=1, ge=1),
):
    active_config = cfg.load_config()
    fd = active_config["filter_defaults"]

    range_start, range_end, is_range = _resolve_date_range(d, start_date, end_date)
    summary_mode = is_range and (range_end - range_start).days > 7
    target_date = range_end

    effective_min = min_value if min_value is not None else fd["min_value"]
    effective_hide = (hide_10b5_1 != "0") if hide_10b5_1 is not None else fd["hide_10b5_1"]
    effective_hide_swap = (hide_equity_swap != "0") if hide_equity_swap is not None else fd.get("hide_equity_swap", True)
    effective_codes = codes if codes else fd["transaction_codes"]
    ceo_cfo_only = ceo_cfo == "1"
    only_watched = watched_only == "1"

    ctx = _make_ctx(db, active_config)

    effective_hide_funds = hide_funds == "1"
    effective_has_options_only = has_options_only == "1"
    effective_mktcap_tiers = [t for t in market_cap_tiers if t in queries.MARKET_CAP_TIERS]

    date_range_arg = (range_start, range_end) if is_range else None

    # Build canonical filters dict (used for template).
    filters = _filters_dict(
        d=d, start_date=start_date, end_date=end_date,
        min_value=effective_min, codes=effective_codes,
        hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
        roles=roles, search=search, ceo_cfo=ceo_cfo,
        sort_by=sort_by, sort_order=sort_order,
        sector=sector, watched_only=watched_only,
        hide_funds=hide_funds, has_options_only=has_options_only,
        market_cap_tiers=effective_mktcap_tiers,
        buys_page=buys_page, sells_page=sells_page,
    )

    # Index route is NOT cached — too many other context dependencies.
    buys, sells = await asyncio.to_thread(
        queries.get_filings_for_date,
        db, target_date,
        min_value=effective_min,
        transaction_codes=effective_codes,
        hide_10b5_1=effective_hide,
        hide_equity_swap=effective_hide_swap,
        roles=roles,
        search=search,
        ceo_cfo_only=ceo_cfo_only,
        ceo_cfo_keywords=active_config["alert_rules"]["insider_title_keywords"],
        sort_by=sort_by,
        sort_order=sort_order,
        sector=sector or None,
        watched_only=only_watched,
        date_range=date_range_arg,
        ctx=ctx,
        hide_funds=effective_hide_funds,
        has_options_only=effective_has_options_only,
        market_cap_tiers=effective_mktcap_tiers or None,
        buys_page=buys_page,
        sells_page=sells_page,
        page_size=PAGE_SIZE,
    )
    buy_count, sell_count = await asyncio.to_thread(
        queries.get_filings_count,
        db, target_date,
        min_value=effective_min,
        transaction_codes=effective_codes,
        hide_10b5_1=effective_hide,
        hide_equity_swap=effective_hide_swap,
        roles=roles,
        search=search,
        ceo_cfo_only=ceo_cfo_only,
        ceo_cfo_keywords=active_config["alert_rules"]["insider_title_keywords"],
        sector=sector or None,
        watched_only=only_watched,
        date_range=date_range_arg,
        hide_funds=effective_hide_funds,
        has_options_only=effective_has_options_only,
        market_cap_tiers=effective_mktcap_tiers or None,
    )
    stats = await asyncio.to_thread(
        queries.get_summary_stats, db, target_date,
        hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
    )
    clusters = await asyncio.to_thread(
        queries.get_cluster_activity, db, target_date,
        hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
        date_range=date_range_arg,
    )
    daily_summary = (
        queries.get_daily_summary(
            db, range_start, range_end, effective_hide, effective_min,
            transaction_codes=effective_codes,
            hide_equity_swap=effective_hide_swap,
        )
        if summary_mode else []
    )
    all_sectors = queries.get_all_sectors(db)

    return templates.TemplateResponse(request, "index.html", {
        "buys": buys,
        "sells": sells,
        "stats": stats,
        "clusters": clusters,
        "daily_summary": daily_summary,
        "summary_mode": summary_mode,
        "target_date": target_date.isoformat(),
        "range_start": range_start.isoformat(),
        "range_end": range_end.isoformat(),
        "is_range": is_range,
        "prev_date": (target_date - timedelta(days=1)).isoformat(),
        "next_date": (target_date + timedelta(days=1)).isoformat(),
        "is_today": target_date == date.today(),
        "buy_count": buy_count,
        "sell_count": sell_count,
        "buys_page": buys_page,
        "sells_page": sells_page,
        "buys_total_pages": max(1, -(-buy_count // PAGE_SIZE)),
        "sells_total_pages": max(1, -(-sell_count // PAGE_SIZE)),
        "filters": filters,
        "config": active_config,
        "all_sectors": all_sectors,
    })


# ---------------------------------------------------------------------------
# HTMX partial — filter updates
# ---------------------------------------------------------------------------

@app.get("/htmx/filings", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def htmx_filings(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
    d: str | None = Query(default=None),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
    min_value: float = Query(default=0),
    hide_10b5_1: str = Query(default="1"),
    hide_equity_swap: str = Query(default="1"),
    codes: list[str] = Query(default=["P", "S"]),
    roles: list[str] = Query(default=None),
    search: str | None = Query(default=None),
    ceo_cfo: str = Query(default="0"),
    sort_by: str = Query(default="value"),
    sort_order: str = Query(default="desc"),
    sector: str | None = Query(default=None),
    watched_only: str = Query(default="0"),
    hide_funds: str = Query(default="0"),
    has_options_only: str = Query(default="0"),
    market_cap_tiers: list[str] = Query(default=[]),
    buys_page: int = Query(default=1, ge=1),
    sells_page: int = Query(default=1, ge=1),
):
    active_config = cfg.load_config()
    effective_hide = hide_10b5_1 != "0"
    effective_hide_swap = hide_equity_swap != "0"
    effective_hide_funds = hide_funds == "1"
    effective_has_options_only = has_options_only == "1"
    effective_mktcap_tiers = [t for t in market_cap_tiers if t in queries.MARKET_CAP_TIERS]
    effective_min = min_value
    effective_codes = codes
    ceo_cfo_only = ceo_cfo == "1"
    only_watched = watched_only == "1"

    range_start, range_end, is_range = _resolve_date_range(d, start_date, end_date)
    summary_mode = is_range and (range_end - range_start).days > 7
    target_date = range_end

    date_range_arg = (range_start, range_end) if is_range else None

    # Build canonical filters dict (used for cache key and template).
    filters = _filters_dict(
        d=d, start_date=start_date, end_date=end_date,
        min_value=effective_min, codes=effective_codes,
        hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
        roles=roles, search=search, ceo_cfo=ceo_cfo,
        sort_by=sort_by, sort_order=sort_order,
        sector=sector, watched_only=watched_only,
        hide_funds=hide_funds, has_options_only=has_options_only,
        market_cap_tiers=effective_mktcap_tiers,
        buys_page=buys_page, sells_page=sells_page,
    )

    # Cache check
    ckey = _cache_key(filters)
    cached = _query_cache.get(ckey)
    if cached is not None:
        buys, sells, buy_count, sell_count = cached
    else:
        ctx = _make_ctx(db, active_config)
        buys, sells = await asyncio.to_thread(
            queries.get_filings_for_date,
            db, target_date,
            min_value=effective_min,
            transaction_codes=effective_codes,
            hide_10b5_1=effective_hide,
            hide_equity_swap=effective_hide_swap,
            roles=roles,
            search=search,
            ceo_cfo_only=ceo_cfo_only,
            ceo_cfo_keywords=active_config["alert_rules"]["insider_title_keywords"],
            sort_by=sort_by,
            sort_order=sort_order,
            sector=sector or None,
            watched_only=only_watched,
            date_range=date_range_arg,
            ctx=ctx,
            hide_funds=effective_hide_funds,
            has_options_only=effective_has_options_only,
            market_cap_tiers=effective_mktcap_tiers or None,
            buys_page=buys_page,
            sells_page=sells_page,
            page_size=PAGE_SIZE,
        )
        buy_count, sell_count = await asyncio.to_thread(
            queries.get_filings_count,
            db, target_date,
            min_value=effective_min,
            transaction_codes=effective_codes,
            hide_10b5_1=effective_hide,
            hide_equity_swap=effective_hide_swap,
            roles=roles,
            search=search,
            ceo_cfo_only=ceo_cfo_only,
            ceo_cfo_keywords=active_config["alert_rules"]["insider_title_keywords"],
            sector=sector or None,
            watched_only=only_watched,
            date_range=date_range_arg,
            hide_funds=effective_hide_funds,
            has_options_only=effective_has_options_only,
            market_cap_tiers=effective_mktcap_tiers or None,
        )
        _query_cache[ckey] = (buys, sells, buy_count, sell_count)

    stats = await asyncio.to_thread(
        queries.get_summary_stats, db, target_date,
        hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
    )
    clusters = await asyncio.to_thread(
        queries.get_cluster_activity, db, target_date,
        hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
        date_range=date_range_arg,
    )
    daily_summary = (
        queries.get_daily_summary(
            db, range_start, range_end, effective_hide, effective_min,
            transaction_codes=effective_codes,
            hide_equity_swap=effective_hide_swap,
        )
        if summary_mode else []
    )
    return templates.TemplateResponse(request, "_tables_partial.html", {
        "buys": buys,
        "sells": sells,
        "stats": stats,
        "clusters": clusters,
        "daily_summary": daily_summary,
        "summary_mode": summary_mode,
        "range_start": range_start.isoformat(),
        "range_end": range_end.isoformat(),
        "target_date": target_date.isoformat(),
        "buy_count": buy_count,
        "sell_count": sell_count,
        "buys_page": buys_page,
        "sells_page": sells_page,
        "buys_total_pages": max(1, -(-buy_count // PAGE_SIZE)),
        "sells_total_pages": max(1, -(-sell_count // PAGE_SIZE)),
        "filters": filters,
    })


# ---------------------------------------------------------------------------
# CSV export
# ---------------------------------------------------------------------------

_CSV_COLUMNS = [
    "filed_at", "issuer_ticker", "issuer_name", "sector",
    "insider_name", "insider_title", "transaction_code",
    "shares", "price_per_share", "total_value", "pct_holdings",
    "conviction", "is_10b5_1", "transaction_date", "transaction_id",
]

_CSV_MAX_ROWS = 10000


@app.get("/export.csv")
@limiter.limit("10/minute")
async def export_csv(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
    d: str | None = Query(default=None),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
    min_value: float = Query(default=0),
    hide_10b5_1: str = Query(default="1"),
    hide_equity_swap: str = Query(default="1"),
    codes: list[str] = Query(default=["P", "S"]),
    roles: list[str] = Query(default=None),
    search: str | None = Query(default=None),
    ceo_cfo: str = Query(default="0"),
    sort_by: str = Query(default="value"),
    sort_order: str = Query(default="desc"),
    sector: str | None = Query(default=None),
    watched_only: str = Query(default="0"),
    hide_funds: str = Query(default="0"),
    has_options_only: str = Query(default="0"),
    market_cap_tiers: list[str] = Query(default=[]),
):
    active_config = cfg.load_config()
    effective_hide = hide_10b5_1 != "0"
    effective_hide_swap = hide_equity_swap != "0"
    effective_hide_funds = hide_funds == "1"
    effective_has_options_only = has_options_only == "1"
    effective_mktcap_tiers = [t for t in market_cap_tiers if t in queries.MARKET_CAP_TIERS]
    ceo_cfo_only = ceo_cfo == "1"
    only_watched = watched_only == "1"

    range_start, range_end, is_range = _resolve_date_range(d, start_date, end_date)
    target_date = range_end

    ctx = _make_ctx(db, active_config)

    date_range_arg = (range_start, range_end) if is_range else None
    # CSV bypasses pagination — page_size left unset (default None) so legacy `limit` applies.
    buys, sells = queries.get_filings_for_date(
        db, target_date,
        min_value=min_value,
        transaction_codes=codes,
        hide_10b5_1=effective_hide,
        hide_equity_swap=effective_hide_swap,
        roles=roles,
        search=search,
        ceo_cfo_only=ceo_cfo_only,
        ceo_cfo_keywords=active_config["alert_rules"]["insider_title_keywords"],
        sort_by=sort_by,
        sort_order=sort_order,
        sector=sector or None,
        watched_only=only_watched,
        date_range=date_range_arg,
        limit=_CSV_MAX_ROWS,
        ctx=ctx,
        hide_funds=effective_hide_funds,
        has_options_only=effective_has_options_only,
        market_cap_tiers=effective_mktcap_tiers or None,
    )
    rows = buys + sells  # already capped by SQL LIMIT

    def generate():
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(_CSV_COLUMNS)
        yield buf.getvalue()
        buf.seek(0)
        buf.truncate()
        for r in rows:
            writer.writerow(["" if (v := r.get(col)) is None else v for col in _CSV_COLUMNS])
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate()

    filename = f"insiders_{range_start.isoformat()}_{range_end.isoformat()}.csv"
    return StreamingResponse(
        generate(),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ---------------------------------------------------------------------------
# Filing detail
# ---------------------------------------------------------------------------

@app.get("/filing/{transaction_id:path}", response_class=HTMLResponse)
async def filing_detail(
    request: Request,
    transaction_id: str,
    db: sqlite3.Connection = Depends(get_request_db),
):
    active_config = cfg.load_config()
    ctx = _make_ctx(db, active_config)
    filing = queries.get_filing_detail(db, transaction_id, ctx=ctx)
    if filing is None:
        raise HTTPException(status_code=404, detail="Filing not found")

    insider_history = queries.get_insider_history(db, filing["insider_cik"])
    issuer_insiders = queries.get_issuer_recent_insiders(
        db,
        filing["issuer_cik"],
        days=90,
        exclude_transaction_id=transaction_id,
    )

    return templates.TemplateResponse(request, "filing.html", {
        "filing": filing,
        "config": active_config,
        "insider_history": insider_history,
        "issuer_insiders": issuer_insiders,
    })


# ---------------------------------------------------------------------------
# Issuer view
# ---------------------------------------------------------------------------

@app.get("/issuer/{ticker}", response_class=HTMLResponse)
async def issuer_view(
    request: Request,
    ticker: str,
    db: sqlite3.Connection = Depends(get_request_db),
    days: int = Query(default=90),
):
    if not _TICKER_RE.match(ticker.upper()):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    filings = queries.get_issuer_filings(db, ticker, days=days)
    trend_svg = render_sparkline(queries.get_issuer_trend(db, ticker))
    return templates.TemplateResponse(request, "issuer.html", {
        "ticker": ticker.upper(),
        "filings": filings,
        "days": days,
        "trend_svg": trend_svg,
    })


@app.get("/insider/{cik}", response_class=HTMLResponse)
async def insider_view(
    request: Request,
    cik: str,
    db: sqlite3.Connection = Depends(get_request_db),
):
    if not _CIK_RE.match(cik):
        raise HTTPException(status_code=400, detail="Invalid CIK")
    config = cfg.load_config()
    ctx = _make_ctx(db, config)
    history = queries.get_insider_full_history(db, cik, ctx=ctx)
    summary = queries.get_insider_summary(db, cik)
    name = history[0]["insider_name"] if history else cik
    return templates.TemplateResponse(request, "insider.html", {
        "history": history,
        "summary": summary,
        "name": name,
        "cik": cik,
    })


_RANGE_DAYS = {"1m": 30, "3m": 90, "6m": 180, "1y": 365}


@app.get("/chart/{ticker}", response_class=HTMLResponse)
@limiter.limit("30/minute")
async def chart_view(
    request: Request,
    ticker: str,
    db: sqlite3.Connection = Depends(get_request_db),
    range: str = Query(default="6m"),
    mode: str = Query(default="both"),   # buys | sells | both
):
    ticker = ticker.upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    active_config = cfg.load_config()
    api_key = active_config.get("polygon_api_key", "")
    days = _RANGE_DAYS.get(range, 180)
    from_date = date.today() - timedelta(days=days)
    to_date = date.today()

    bars = polygon_client.get_daily_bars(ticker, from_date, to_date, api_key)
    earnings = polygon_client.get_earnings_estimate(ticker, api_key)
    filings = queries.get_issuer_filings(db, ticker, days=days)

    # Build marker list for Lightweight Charts
    code_filter = {"buys": ["P"], "sells": ["S"], "both": ["P", "S"]}.get(mode, ["P", "S"])
    markers = []
    for f in filings:
        if f.get("transaction_code") not in code_filter:
            continue
        tx_date = f.get("transaction_date") or ""
        if not tx_date:
            continue
        is_buy = f.get("transaction_code") == "P"
        label_parts = [f.get("insider_name", "")]
        if f.get("total_value_fmt"):
            label_parts.append(f["total_value_fmt"])
        markers.append({
            "time":     tx_date,
            "position": "belowBar" if is_buy else "aboveBar",
            "color":    "#22c55e" if is_buy else "#ef4444",
            "shape":    "arrowUp" if is_buy else "arrowDown",
            "text":     " ".join(label_parts),
        })

    return templates.TemplateResponse(request, "chart.html", {
        "ticker": ticker,
        "range": range,
        "mode": mode,
        "bars": bars,
        "markers": markers,
        "filings": filings,
        "earnings": earnings,
        "code_filter": code_filter,
        "has_api_key": bool(api_key),
        "ranges": list(_RANGE_DAYS.keys()),
    })


# ---------------------------------------------------------------------------
# Run log
# ---------------------------------------------------------------------------

@app.get("/watchlist", response_class=HTMLResponse)
async def watchlist_page(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
):
    wl = queries.list_watchlist(db)
    return templates.TemplateResponse(request, "watchlist.html", {"watchlist": wl})


@app.post("/watchlist/add")
async def watchlist_add(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
    watch_type: str = Form(...),
    value: str = Form(...),
    label: str = Form(default=""),
):
    if watch_type not in ("ticker", "insider"):
        raise HTTPException(status_code=400, detail="Invalid watch_type")
    value = value.strip()
    label = label.strip()
    if not value or len(value) > 64 or len(label) > 128:
        raise HTTPException(status_code=400, detail="Invalid value or label")
    queries.add_watch(db, watch_type, value, label or value)
    return RedirectResponse(url="/watchlist", status_code=303)


@app.post("/watchlist/remove")
async def watchlist_remove(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
    watch_id: int = Form(...),
):
    queries.remove_watch(db, watch_id)
    return RedirectResponse(url="/watchlist", status_code=303)


@app.get("/congress", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def congress_view(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
    ticker: str = Query(default=""),
    politician: str = Query(default=""),
    chamber: str = Query(default=""),
    tx_type: str = Query(default=""),
    days: int = Query(default=0),
    sort_by: str = Query(default="transaction_date"),
    sort_order: str = Query(default="desc"),
):
    effective_days = days if days > 0 else None
    trades = queries.get_congress_trades(
        db,
        ticker=ticker or None,
        politician=politician or None,
        chamber=chamber or None,
        tx_type=tx_type or None,
        days=effective_days,
        sort_by=sort_by,
        sort_order=sort_order,
    )
    summary = queries.get_congress_summary(db, days=effective_days)
    filters = {
        "ticker": ticker,
        "politician": politician,
        "chamber": chamber,
        "tx_type": tx_type,
        "days": days,
        "sort_by": sort_by,
        "sort_order": sort_order,
    }
    return templates.TemplateResponse(request, "congress.html", {
        "request": request,
        "trades": trades,
        "summary": summary,
        "filters": filters,
    })


@app.get("/run-log", response_class=HTMLResponse)
async def run_log(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
):
    log = queries.get_run_log(db)
    return templates.TemplateResponse(request, "run_log.html", {
        "log": log,
    })


@app.get("/backtest", response_class=HTMLResponse)
async def backtest(request: Request):
    csv_path = BASE_DIR / "data" / "backtest_results.csv"

    if not csv_path.exists():
        return templates.TemplateResponse(request, "backtest.html", {
            "rows": [],
            "summary": {},
            "running": True,
            "csv_path": "data/backtest_results.csv",
        })

    def _parse_bool(s: str) -> bool | None:
        if s == "True":
            return True
        if s == "False":
            return False
        return None

    def _parse_float(s: str) -> float | None:
        if s == "" or s is None:
            return None
        try:
            return float(s)
        except ValueError:
            return None

    bool_fields = {
        "is_10b5_1", "is_director", "is_officer",
        "gc_computable", "gc_30d", "gc_60d", "gc_90d",
        "rb_computable", "rb_30d", "rb_60d", "rb_90d",
        "hhl_computable", "hhl_30d", "hhl_60d", "hhl_90d",
        "cb_computable", "cb_30d", "cb_60d", "cb_90d",
    }
    float_fields = {
        "value", "trade_price",
        "gc_days", "rb_days", "hhl_days", "cb_days",
        "stacked_30d", "stacked_60d", "stacked_90d",
        "return_30d", "return_60d", "return_90d",
    }

    rows: list[dict] = []
    with csv_path.open(newline="", encoding="utf-8") as fh:
        reader = csv.DictReader(fh)
        for raw in reader:
            row: dict = {}
            for k, v in raw.items():
                k = k.strip()
                if k in bool_fields:
                    row[k] = _parse_bool(v.strip() if v else "")
                elif k in float_fields:
                    row[k] = _parse_float(v.strip() if v else "")
                else:
                    row[k] = v.strip() if v else v
            rows.append(row)

    SIGNALS = (
        ("gc",  "Golden Cross"),
        ("rb",  "Resistance Break"),
        ("hhl", "HH + HL"),
        ("cb",  "Channel Break"),
    )
    WINDOWS = (30, 60, 90)

    signals_summary: dict = {}
    for sig, label in SIGNALS:
        windows_data: dict = {}
        for w in WINDOWS:
            comp_key = f"{sig}_computable"
            fired_key = f"{sig}_{w}d"
            ret_key   = f"return_{w}d"

            computable_rows = [r for r in rows if r.get(comp_key) is True]
            fired_rows      = [r for r in computable_rows if r.get(fired_key) is True]
            returns         = [r[ret_key] for r in fired_rows if r.get(ret_key) is not None]

            n_computable = len(computable_rows)
            n_fired      = len(fired_rows)
            hit_rate     = (n_fired / n_computable * 100.0) if n_computable else 0.0
            avg_ret      = statistics.mean(returns) if returns else None
            med_ret      = statistics.median(returns) if returns else None

            windows_data[w] = {
                "computable":     n_computable,
                "fired":          n_fired,
                "hit_rate":       hit_rate,
                "avg_return":     avg_ret,
                "median_return":  med_ret,
            }
        signals_summary[sig] = {"label": label, "windows": windows_data}

    stacked_summary: dict = {}
    for w in WINDOWS:
        stacked_key = f"stacked_{w}d"
        ret_key     = f"return_{w}d"
        levels: dict = {}
        for n in (1, 2, 3, 4):
            qualified = [r for r in rows if (r.get(stacked_key) or 0) >= n]
            returns   = [r[ret_key] for r in qualified if r.get(ret_key) is not None]
            levels[n] = {
                "count":      len(qualified),
                "avg_return": statistics.mean(returns) if returns else None,
            }
        stacked_summary[w] = levels

    summary = {
        "total":   len(rows),
        "signals": signals_summary,
        "stacked": stacked_summary,
    }

    return templates.TemplateResponse(request, "backtest.html", {
        "rows":     rows,
        "summary":  summary,
        "running":  False,
        "csv_path": "data/backtest_results.csv",
    })


# ---------------------------------------------------------------------------
# Logic & Config
# ---------------------------------------------------------------------------

@app.get("/logic", response_class=HTMLResponse)
async def logic_page(
    request: Request,
    db: sqlite3.Connection = Depends(get_request_db),
):
    active_config = cfg.load_config()
    view_config = {k: v for k, v in active_config.items() if k != "polygon_api_key"}
    stats = queries.get_10b5_1_stats(db)
    recent_alerts = queries.get_recent_alerts(db)
    slack_configured = bool(os.getenv("SLACK_WEBHOOK_URL", ""))
    return templates.TemplateResponse(request, "logic.html", {
        "config": view_config,
        "stats": stats,
        "transaction_codes": cfg.TRANSACTION_CODES,
        "conviction_tiers": cfg.CONVICTION_TIERS,
        "recent_alerts": recent_alerts,
        "slack_configured": slack_configured,
    })


@app.post("/logic/save")
async def logic_save(
    request: Request,
    # Alert rules
    big_buy_threshold: float = Form(...),
    insider_buy_threshold: float = Form(...),
    insider_title_keywords: str = Form(...),
    cluster_window_days: int = Form(...),
    cluster_min_insiders: int = Form(...),
    # Filter defaults
    min_value: float = Form(...),
    hide_10b5_1: str = Form(default="off"),
    # Conviction flags (all optional — default None means "keep existing")
    conviction_base_open_market_buy: int | None = Form(default=None),
    conviction_ceo_cfo_bonus: int | None = Form(default=None),
    conviction_director_bonus: int | None = Form(default=None),
    conviction_ten_percent_owner_bonus: int | None = Form(default=None),
    conviction_cluster_bonus: int | None = Form(default=None),
    conviction_non_10b5_1_buy: int | None = Form(default=None),
    # Conviction tier point values
    conviction_value_250k_pts: int | None = Form(default=None),
    conviction_value_1m_pts: int | None = Form(default=None),
    conviction_value_5m_pts: int | None = Form(default=None),
    conviction_pct_20_pts: int | None = Form(default=None),
    conviction_pct_50_pts: int | None = Form(default=None),
):
    alert_rules = {
        "big_buy_threshold": big_buy_threshold,
        "insider_buy_threshold": insider_buy_threshold,
        "insider_title_keywords": [k.strip() for k in insider_title_keywords.split(",") if k.strip()],
        "cluster_window_days": cluster_window_days,
        "cluster_min_insiders": cluster_min_insiders,
    }
    filter_defaults = {
        "min_value": min_value,
        "hide_10b5_1": hide_10b5_1 == "on",
    }

    # Build conviction_flags only from non-None submitted values
    conviction_flags: dict | None = None
    flag_map = {
        "base_open_market_buy":    conviction_base_open_market_buy,
        "ceo_cfo_bonus":           conviction_ceo_cfo_bonus,
        "director_bonus":          conviction_director_bonus,
        "ten_percent_owner_bonus": conviction_ten_percent_owner_bonus,
        "cluster_bonus":           conviction_cluster_bonus,
        "non_10b5_1_buy":          conviction_non_10b5_1_buy,
    }
    tier_pts = {
        "value_250k":  conviction_value_250k_pts,
        "value_1m":    conviction_value_1m_pts,
        "value_5m":    conviction_value_5m_pts,
        "pct_20":      conviction_pct_20_pts,
        "pct_50":      conviction_pct_50_pts,
    }
    submitted_flags = {k: v for k, v in flag_map.items() if v is not None}
    submitted_tier_pts = {k: v for k, v in tier_pts.items() if v is not None}

    if submitted_flags or submitted_tier_pts:
        # Load existing flags to merge into
        existing = cfg.load_config()
        conviction_flags = dict(existing.get("conviction_flags") or cfg.CONVICTION_FLAGS)
        conviction_flags.update(submitted_flags)
        # Tier point overrides — update the points in CONVICTION_TIERS structure
        # These are stored separately in config_overrides under "conviction_tier_pts"
        # and applied at load_config time. For now store in conviction_flags with prefix.
        for key, val in submitted_tier_pts.items():
            conviction_flags[f"tier_pts_{key}"] = val

    save_overrides(alert_rules, filter_defaults, conviction_flags=conviction_flags or None)
    return RedirectResponse(url="/logic?saved=1", status_code=303)


@app.post("/logic/test-alert")
@limiter.limit("5/minute")
async def test_alert(request: Request):
    from fastapi.responses import JSONResponse
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return JSONResponse(
            {"ok": False, "error": "SLACK_WEBHOOK_URL is not set in the server environment"},
            status_code=400,
        )
    active_config = cfg.load_config()
    base_url = active_config.get("alert_base_url", "https://opi-insider.duckdns.org")
    ok = alert_module.send_test_alert(webhook_url, base_url)
    if ok:
        return JSONResponse({"ok": True, "message": "Test alert sent successfully"})
    return JSONResponse(
        {"ok": False, "error": "Slack returned an error — check the webhook URL"},
        status_code=502,
    )
