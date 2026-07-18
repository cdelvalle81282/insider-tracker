from __future__ import annotations

import asyncio
import csv
import hashlib
import hmac
import html
import io
import json
import logging
import os
import re
import statistics
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path

import psycopg
from cachetools import TTLCache
from fastapi import BackgroundTasks, Depends, FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from slowapi import Limiter, _rate_limit_exceeded_handler
from slowapi.errors import RateLimitExceeded
from slowapi.util import get_remote_address

import alerts as alert_module
import cache as cache_module
import config as cfg
import polygon_client
import queries
from backtest import (
    PRICE_WARMUP_DAYS,
    detect_channel_break,
    detect_golden_cross,
    detect_hhl,
    detect_resistance_break,
)
from config import PAGE_SIZE, save_overrides
from db import get_db, get_request_db, put_db
from queries import EnrichContext

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


def _replace_filter(filters: dict, key: str, value) -> str:
    """Jinja2 filter: return query string with one key replaced."""
    from urllib.parse import urlencode
    updated = {**filters, key: value}
    return urlencode({k: v for k, v in updated.items() if v not in (None, "", [])}, doseq=True)


templates.env.filters["replace_filter"] = _replace_filter
templates.env.filters["fmt_value"] = queries._fmt_value

_TICKER_RE = re.compile(r"^[A-Z0-9.\-]{1,10}$")
_CIK_RE = re.compile(r"^\d{1,10}$")
limiter = Limiter(key_func=get_remote_address)

INGEST_SENTINEL = Path(__file__).parent / "data" / ".last_ingest"

# Redis-backed caches (query results, stats, clusters HTML) live in cache_module.
# In-process caches for small, hot, non-shared lookups:
_sectors_cache:      TTLCache = TTLCache(maxsize=1, ttl=3600)
_config_cache:       TTLCache = TTLCache(maxsize=1, ttl=60)
_leaderboard_cache:  TTLCache = TTLCache(maxsize=1, ttl=3600)


def _sentinel_get(cache: TTLCache, key: str):
    entry = cache.get(key)
    if entry is None:
        return None
    stored_mtime, value = entry
    return value if stored_mtime >= cache_module._sentinel_mtime() else None


def _sentinel_set(cache: TTLCache, key: str, pre_mtime: float, value) -> None:
    cache[key] = (pre_mtime, value)


def _cache_key(params: dict) -> str:
    """Stable cache key — normalizes list values so order doesn't affect result."""
    normalized = {k: sorted(v) if isinstance(v, list) else v for k, v in params.items()}
    return hashlib.sha256(json.dumps(normalized, sort_keys=True, default=str).encode()).hexdigest()


def _load_config_cached() -> dict:
    cached = _config_cache.get("config")
    if cached is not None:
        return cached
    result = cfg.load_config()
    _config_cache["config"] = result
    return result


def _get_all_sectors_cached(db: psycopg.Connection) -> list[str]:
    cached = _sentinel_get(_sectors_cache, "sectors")
    if cached is not None:
        return cached
    pre_mtime = cache_module._sentinel_mtime()
    result = queries.get_all_sectors(db)
    _sentinel_set(_sectors_cache, "sectors", pre_mtime, result)
    return result


def _filters_dict(
    *,
    d, start_date, end_date, min_value, codes, hide_10b5_1, hide_equity_swap,
    roles, search, ceo_cfo, sort_by, sort_order, sector, watched_only,
    hide_funds, has_options_only, market_cap_tiers, hide_entity_filers, buys_page, sells_page,
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
        "hide_entity_filers": hide_entity_filers,
        "buys_page": buys_page,
        "sells_page": sells_page,
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    yield  # nothing to set up/tear down at app level


app = FastAPI(title="Insider Scanner", lifespan=lifespan)
app.state.limiter = limiter
app.add_exception_handler(RateLimitExceeded, _rate_limit_exceeded_handler)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


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
    """Parse start/end date params; fall back to single date d. Returns (start, end, is_range).
    When no params are given (fresh load), defaults to the last 7 days."""
    if start_date and end_date:
        s, e = _parse_date(start_date), _parse_date(end_date)
    elif d:
        s = e = _parse_date(d)
    else:
        e = date.today()
        s = e - timedelta(days=6)
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


def render_sentiment_chart(series: list[dict]) -> str:
    """Render market-wide weekly net insider buy/sell $ as a diverging bar chart --
    green above the zero baseline (net buying that week), red below (net selling).
    Native <title> tooltips give per-bar detail without a JS charting dependency.
    Returns '' when every week nets to zero."""
    if not series or all(p["net"] == 0 for p in series):
        return ""

    W, H, PAD_X, PAD_Y = 900, 200, 8, 20
    n = len(series)
    gap = 2
    bar_w = max((W - 2 * PAD_X) / n - gap, 1)
    mid_y = H / 2
    max_abs = max(abs(p["net"]) for p in series) or 1
    half_h = mid_y - PAD_Y

    bars = []
    for i, p in enumerate(series):
        net = p["net"]
        x = PAD_X + i * (bar_w + gap)
        bar_h = abs(net) / max_abs * half_h
        y = mid_y - bar_h if net >= 0 else mid_y
        color = "#0fcea0" if net >= 0 else "#f64b6e"
        label = "Net buying" if net >= 0 else "Net selling"
        bars.append(
            f'<rect x="{x:.1f}" y="{y:.1f}" width="{bar_w:.1f}" height="{max(bar_h, 0.5):.1f}" '
            f'fill="{color}" rx="1.5">'
            f'<title>Week of {p["week"]} — {label} {p["net_fmt"]} '
            f'(bought {queries._fmt_value(p["buy_total"])}, sold {queries._fmt_value(p["sell_total"])})</title>'
            f'</rect>'
        )

    first_label, last_label = series[0]["week"], series[-1]["week"]
    return (
        f'<svg viewBox="0 0 {W} {H}" class="w-full" style="height:180px;display:block;">'
        f'<line x1="{PAD_X}" y1="{mid_y:.1f}" x2="{W-PAD_X}" y2="{mid_y:.1f}" stroke="#374151" stroke-width="1"/>'
        + "".join(bars) +
        f'<text x="{PAD_X}" y="{H-4}" font-size="11" fill="#6b7280">{first_label}</text>'
        f'<text x="{W-PAD_X}" y="{H-4}" font-size="11" fill="#6b7280" text-anchor="end">{last_label}</text>'
        f'</svg>'
    )


def render_price_preview_svg(bars: list[dict], filings: list[dict]) -> str:
    """Compact price-line SVG for the ticker hover preview -- insider buy/sell
    dates marked as dots on the line, native <title> tooltips for detail.
    Returns '' when there are no bars (no Polygon key / no data for ticker)."""
    if not bars:
        return ""

    W, H, PAD_X, PAD_Y = 300, 84, 3, 8
    closes = [b["close"] for b in bars]
    mn, mx = min(closes), max(closes)
    span = mx - mn or 1
    n = len(bars)
    x_step = (W - 2 * PAD_X) / max(n - 1, 1)

    def x_at(i: int) -> float:
        return PAD_X + i * x_step

    def y_at(v: float) -> float:
        return H - PAD_Y - ((v - mn) / span) * (H - 2 * PAD_Y)

    pts = " ".join(f"{x_at(i):.1f},{y_at(c):.1f}" for i, c in enumerate(closes))
    line_color = "#0fcea0" if closes[-1] >= closes[0] else "#f64b6e"

    bar_dates = [b["time"] for b in bars]
    dots = []
    for f in filings:
        fd = f.get("transaction_date")
        if hasattr(fd, "isoformat"):
            fd = fd.isoformat()
        if not fd:
            continue
        idx = next((i for i, d in enumerate(bar_dates) if d >= fd), None)
        if idx is None:
            continue
        is_buy = f.get("transaction_code") == "P"
        color = "#0fcea0" if is_buy else "#f64b6e"
        label = html.escape(
            f'{"Buy" if is_buy else "Sell"} · {f.get("insider_name", "")} · '
            f'{f.get("total_value_fmt", "")} · {fd}'
        )
        dots.append(
            f'<circle cx="{x_at(idx):.1f}" cy="{y_at(closes[idx]):.1f}" r="2.6" '
            f'fill="{color}" stroke="#06090f" stroke-width="1"><title>{label}</title></circle>'
        )

    return (
        f'<svg viewBox="0 0 {W} {H}" width="{W}" height="{H}" style="display:block;">'
        f'<polyline points="{pts}" fill="none" stroke="{line_color}" stroke-width="1.6" stroke-linejoin="round"/>'
        + "".join(dots) +
        '</svg>'
    )


def _make_ctx(db: psycopg.Connection, active_config: dict) -> EnrichContext:
    """Build an EnrichContext with conviction config and watchlist sets."""
    return EnrichContext(
        conn=db,
        conviction_flags=active_config.get("conviction_flags"),
        conviction_tiers=active_config.get("conviction_tiers"),
        conviction_max=active_config.get("conviction_max", 10),
        conviction_thresholds=active_config.get("conviction_thresholds"),
        cluster_window_days=active_config.get("conviction_cluster_window_days", 14),
        ceo_cfo_keywords=active_config.get("alert_rules", {}).get("insider_title_keywords", []),
        watched_tickers=queries.watched_tickers(db),
        watched_insiders=queries.watched_insiders(db),
        compute_conviction=True,
        insider_baseline_cfg=active_config.get("insider_baseline"),
        compute_insider_baseline=True,
    )


# ---------------------------------------------------------------------------
# Health check (no rate limit, no DB round-trip)
# ---------------------------------------------------------------------------

@app.get("/healthz")
async def healthz():
    from datetime import datetime
    try:
        mtime = cache_module._sentinel_mtime()
        last_ingest = datetime.utcfromtimestamp(mtime).isoformat() + "Z" if mtime else None
    except Exception:
        last_ingest = None
    return {"ok": True}


# ---------------------------------------------------------------------------
# Main dashboard
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def index(
    request: Request,
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
    hide_entity_filers: str = Query(default="0"),
    buys_page: int = Query(default=1, ge=1),
    sells_page: int = Query(default=1, ge=1),
):
    active_config = _load_config_cached()
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
    effective_hide_funds = hide_funds == "1"
    effective_has_options_only = has_options_only == "1"
    effective_mktcap_tiers = [t for t in market_cap_tiers if t in queries.MARKET_CAP_TIERS]
    effective_hide_entity = hide_entity_filers == "1"
    date_range_arg = (range_start, range_end) if is_range else None

    filters = _filters_dict(
        d=d, start_date=start_date, end_date=end_date,
        min_value=effective_min, codes=effective_codes,
        hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
        roles=roles, search=search, ceo_cfo=ceo_cfo,
        sort_by=sort_by, sort_order=sort_order,
        sector=sector, watched_only=watched_only,
        hide_funds=hide_funds, has_options_only=has_options_only,
        market_cap_tiers=effective_mktcap_tiers,
        hide_entity_filers=hide_entity_filers,
        buys_page=buys_page, sells_page=sells_page,
    )

    ckey = _cache_key(filters)
    cached_result = cache_module.cache_get(f"it:query:{ckey}")
    cached_sectors = _sentinel_get(_sectors_cache, "sectors")

    # Acquire a DB connection only when something is missing from cache.
    # Hot path (cache hit, no summary_mode, sectors warm): zero connections held.
    need_db = (cached_result is None) or summary_mode or (cached_sectors is None)

    if need_db:
        pre_mtime = cache_module._sentinel_mtime()
        db = get_db()
        try:
            if cached_result is None:
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
                    hide_entity_filers=effective_hide_entity,
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
                    hide_entity_filers=effective_hide_entity,
                )
                cache_module.cache_set(f"it:query:{ckey}", pre_mtime, (buys, sells, buy_count, sell_count))
            else:
                buys, sells, buy_count, sell_count = cached_result

            daily_summary = (
                await asyncio.to_thread(
                    queries.get_daily_summary,
                    db, range_start, range_end, effective_hide, effective_min,
                    transaction_codes=effective_codes,
                    hide_equity_swap=effective_hide_swap,
                )
                if summary_mode else []
            )

            if cached_sectors is None:
                cached_sectors = _get_all_sectors_cached(db)
            all_sectors = cached_sectors
        finally:
            put_db(db)
    else:
        buys, sells, buy_count, sell_count = cached_result
        daily_summary = []
        all_sectors = cached_sectors

    return templates.TemplateResponse(request, "index.html", {
        "buys": buys,
        "sells": sells,
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
    hide_entity_filers: str = Query(default="0"),
    buys_page: int = Query(default=1, ge=1),
    sells_page: int = Query(default=1, ge=1),
):
    active_config = _load_config_cached()
    effective_hide = hide_10b5_1 != "0"
    effective_hide_swap = hide_equity_swap != "0"
    effective_hide_funds = hide_funds == "1"
    effective_has_options_only = has_options_only == "1"
    effective_mktcap_tiers = [t for t in market_cap_tiers if t in queries.MARKET_CAP_TIERS]
    effective_hide_entity = hide_entity_filers == "1"
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
        hide_entity_filers=hide_entity_filers,
        buys_page=buys_page, sells_page=sells_page,
    )

    ckey = _cache_key(filters)
    cached_result = cache_module.cache_get(f"it:query:{ckey}")

    need_db = (cached_result is None) or summary_mode

    if need_db:
        pre_mtime = cache_module._sentinel_mtime()
        db = get_db()
        try:
            if cached_result is None:
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
                    hide_entity_filers=effective_hide_entity,
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
                    hide_entity_filers=effective_hide_entity,
                )
                cache_module.cache_set(f"it:query:{ckey}", pre_mtime, (buys, sells, buy_count, sell_count))
            else:
                buys, sells, buy_count, sell_count = cached_result

            daily_summary = (
                await asyncio.to_thread(
                    queries.get_daily_summary,
                    db, range_start, range_end, effective_hide, effective_min,
                    transaction_codes=effective_codes,
                    hide_equity_swap=effective_hide_swap,
                )
                if summary_mode else []
            )
        finally:
            put_db(db)
    else:
        buys, sells, buy_count, sell_count = cached_result
        daily_summary = []

    return templates.TemplateResponse(request, "_tables_partial.html", {
        "buys": buys,
        "sells": sells,
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
# Async stats / cluster partials (loaded deferred by index.html)
# ---------------------------------------------------------------------------

@app.get("/htmx/stats", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def htmx_stats(
    request: Request,
    d: str | None = Query(default=None),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
    hide_10b5_1: str = Query(default="1"),
    hide_equity_swap: str = Query(default="1"),
    codes: list[str] = Query(default=["P", "S"]),
):
    effective_hide = hide_10b5_1 != "0"
    effective_hide_swap = hide_equity_swap != "0"
    effective_codes = codes or ["P", "S"]

    range_start, range_end, is_range = _resolve_date_range(d, start_date, end_date)
    target_date = range_end
    date_range_arg = (range_start, range_end) if is_range else None

    skey = _cache_key({
        "start_date": range_start.isoformat() if is_range else None,
        "target_date": target_date.isoformat(),
        "hide_10b5_1": "1" if effective_hide else "0",
        "hide_equity_swap": "1" if effective_hide_swap else "0",
        "codes": sorted(effective_codes),
    })
    html = cache_module.cache_get(f"it:stats:{skey}")
    if html is None:
        pre_mtime = cache_module._sentinel_mtime()
        db = get_db()
        try:
            # Compute current + prior-period stats in one thread dispatch
            if date_range_arg:
                prev_start = range_start - timedelta(days=(range_end - range_start).days + 1)
                prev_range_arg = (prev_start, range_start - timedelta(days=1))
                prev_target = prev_range_arg[1]
            else:
                prev_target = target_date - timedelta(days=1)
                prev_range_arg = None

            def _fetch_stats_pair() -> tuple[dict, dict]:
                cur = queries.get_summary_stats(
                    db, target_date, date_range=date_range_arg,
                    hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
                    codes=effective_codes,
                )
                prev = queries.get_summary_stats(
                    db, prev_target, date_range=prev_range_arg,
                    hide_10b5_1=effective_hide, hide_equity_swap=effective_hide_swap,
                    codes=effective_codes,
                )
                return cur, prev

            stats, prev_stats = await asyncio.to_thread(_fetch_stats_pair)
        finally:
            put_db(db)
        html = templates.env.get_template("_stats_partial.html").render(
            {"stats": stats, "prev_stats": prev_stats}
        )
        cache_module.cache_set(f"it:stats:{skey}", pre_mtime, html)

    return HTMLResponse(html)


@app.get("/htmx/clusters", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def htmx_clusters(
    request: Request,
    d: str | None = Query(default=None),
    start_date: str | None = Query(default=None),
    end_date: str | None = Query(default=None),
    hide_10b5_1: str = Query(default="1"),
    hide_equity_swap: str = Query(default="1"),
    codes: list[str] = Query(default=["P", "S"]),
):
    effective_hide = hide_10b5_1 != "0"
    effective_hide_swap = hide_equity_swap != "0"
    effective_codes = codes or ["P", "S"]

    range_start, range_end, is_range = _resolve_date_range(d, start_date, end_date)
    target_date = range_end
    date_range_arg = (range_start, range_end) if is_range else None

    ckey = _cache_key({
        "target_date": target_date.isoformat(),
        "start_date": range_start.isoformat() if is_range else None,
        "hide_10b5_1": "1" if effective_hide else "0",
        "hide_equity_swap": "1" if effective_hide_swap else "0",
        "codes": sorted(effective_codes),
    })
    html = cache_module.cache_get(f"it:clusters:{ckey}")
    if html is None:
        pre_mtime = cache_module._sentinel_mtime()
        db = get_db()
        try:
            clusters = await asyncio.to_thread(
                queries.get_cluster_activity, db, target_date,
                hide_10b5_1=effective_hide,
                hide_equity_swap=effective_hide_swap,
                date_range=date_range_arg,
                codes=effective_codes,
            )
            clusters = await asyncio.to_thread(
                queries.enrich_clusters_with_quality, db, clusters
            )
        finally:
            put_db(db)
        html = templates.env.get_template("_clusters_partial.html").render({"clusters": clusters})
        cache_module.cache_set(f"it:clusters:{ckey}", pre_mtime, html)

    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# HTMX partial — today's top signals hero strip
# ---------------------------------------------------------------------------

@app.get("/htmx/top-signals", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def htmx_top_signals(request: Request):
    skey = f"it:top-signals:{date.today().isoformat()}"
    html = cache_module.cache_get(skey)
    if html is None:
        pre_mtime = cache_module._sentinel_mtime()
        db = get_db()
        try:
            signals = await asyncio.to_thread(queries.get_top_signals_today, db)
        finally:
            put_db(db)
        html = templates.env.get_template("_top_signals.html").render({"signals": signals})
        cache_module.cache_set(skey, pre_mtime, html)
    return HTMLResponse(html)


# ---------------------------------------------------------------------------
# HTMX partial — watchlist activity hero strip
# ---------------------------------------------------------------------------

def _get_watchlist_feed_sync(db: psycopg.Connection) -> list[dict]:
    tickers = queries.watched_tickers(db)
    insiders = queries.watched_insiders(db)
    return queries.get_watchlist_feed(db, tickers, insiders, lookback_days=14, limit=8)


@app.get("/htmx/watchlist-activity", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def htmx_watchlist_activity(request: Request):
    # Prefixed "it:query:" (not "it:top-signals:") so watchlist add/remove
    # invalidates it immediately via invalidate_query_cache(), same as the
    # main filings query cache — this content is watchlist-dependent, unlike
    # the top-signals strip.
    skey = f"it:query:watchlist-activity:{date.today().isoformat()}"
    html = cache_module.cache_get(skey)
    if html is None:
        pre_mtime = cache_module._sentinel_mtime()
        db = get_db()
        try:
            feed = await asyncio.to_thread(_get_watchlist_feed_sync, db)
        finally:
            put_db(db)
        html = templates.env.get_template("_watchlist_activity.html").render({"feed": feed})
        cache_module.cache_set(skey, pre_mtime, html)
    return HTMLResponse(html)


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
@limiter.limit("3/minute")
async def export_csv(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
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
    hide_entity_filers: str = Query(default="0"),
):
    active_config = _load_config_cached()
    effective_hide = hide_10b5_1 != "0"
    effective_hide_swap = hide_equity_swap != "0"
    effective_hide_funds = hide_funds == "1"
    effective_has_options_only = has_options_only == "1"
    effective_mktcap_tiers = [t for t in market_cap_tiers if t in queries.MARKET_CAP_TIERS]
    effective_hide_entity = hide_entity_filers == "1"
    ceo_cfo_only = ceo_cfo == "1"
    only_watched = watched_only == "1"

    range_start, range_end, is_range = _resolve_date_range(d, start_date, end_date)
    # Clamp CSV exports to 90 days max to prevent runaway queries
    if (range_end - range_start).days > 90:
        range_start = range_end - timedelta(days=90)
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
        hide_entity_filers=effective_hide_entity,
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
    db: psycopg.Connection = Depends(get_request_db),
):
    active_config = _load_config_cached()
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
    db: psycopg.Connection = Depends(get_request_db),
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
    db: psycopg.Connection = Depends(get_request_db),
):
    if not _CIK_RE.match(cik):
        raise HTTPException(status_code=400, detail="Invalid CIK")
    config = _load_config_cached()
    ctx = _make_ctx(db, config)
    history = queries.get_insider_full_history(db, cik, ctx=ctx)
    summary = queries.get_insider_summary(db, cik)
    perf_profile = queries.get_insider_perf_profile(db, cik)
    name = history[0]["insider_name"] if history else cik
    return templates.TemplateResponse(request, "insider.html", {
        "history": history,
        "summary": summary,
        "perf_profile": perf_profile,
        "name": name,
        "cik": cik,
    })


@app.get("/leaderboard", response_class=HTMLResponse)
@limiter.limit("30/minute")
async def leaderboard_view(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
    sort_by: str = Query(default="med_90"),
    min_trades: int = Query(default=5, ge=1),
    role: str | None = Query(default=None),
):
    leaderboard = queries.get_insider_leaderboard(
        db, sort_by=sort_by, min_trades=min_trades, role=role or None, limit=50
    )
    roles = queries.get_insider_leaderboard_roles(db)
    sentiment_series = queries.get_sentiment_index(db, weeks=26)
    sentiment_svg = render_sentiment_chart(sentiment_series)
    cross_company = queries.get_cross_company_buys(db, lookback_days=90, limit=25)
    return templates.TemplateResponse(request, "leaderboard.html", {
        "leaderboard": leaderboard,
        "roles": roles,
        "sort_by": sort_by,
        "min_trades": min_trades,
        "role": role or "",
        "sentiment_svg": sentiment_svg,
        "cross_company": cross_company,
    })


_RANGE_DAYS = {"1m": 30, "3m": 90, "6m": 180, "1y": 365}


@app.get("/chart/{ticker}", response_class=HTMLResponse)
@limiter.limit("30/minute")
async def chart_view(
    request: Request,
    ticker: str,
    db: psycopg.Connection = Depends(get_request_db),
    range: str = Query(default="6m"),
    mode: str = Query(default="both"),   # buys | sells | both
):
    ticker = ticker.upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")
    active_config = _load_config_cached()
    api_key = active_config.get("polygon_api_key", "")
    days = _RANGE_DAYS.get(range, 180)
    from_date = date.today() - timedelta(days=days)
    to_date = date.today()

    full_from = from_date - timedelta(days=PRICE_WARMUP_DAYS)
    bars = polygon_client.get_daily_bars(ticker, full_from, to_date, api_key, limit=700)
    earnings = polygon_client.get_earnings_estimate(ticker, api_key)
    filings = queries.get_issuer_filings(db, ticker, days=days)
    filings = [f for f in filings if f.get("table_type") == "ND"]

    # Signal detection (runs on the extended bar history for warmup)
    _CHART_SIG_DETECTORS = [
        ("gc",  "GC",  "#f59e0b", detect_golden_cross),
        ("rb",  "RB",  "#f97316", detect_resistance_break),
        ("hhl", "HH",  "#22d3ee", detect_hhl),
        ("cb",  "CB",  "#a855f7", detect_channel_break),
    ]
    signal_markers: list[dict] = []
    if bars and api_key:
        sig_bars = [{**b, "date": b["time"]} for b in bars]
        sig_bar_dates = [b["date"] for b in sig_bars]
        nd_buys = [
            f for f in filings
            if f.get("transaction_code") == "P"
            and not f.get("is_10b5_1")
            and f.get("transaction_date")
        ]
        seen_signals: set[tuple[str, str]] = set()
        for f in nd_buys:
            trade_date = f["transaction_date"]
            if hasattr(trade_date, "isoformat"):
                trade_date = trade_date.isoformat()
            trade_idx = next((i for i, d in enumerate(sig_bar_dates) if d >= trade_date), None)
            if trade_idx is None or trade_idx >= len(sig_bars) - 1:
                continue
            for sig_code, label, color, detect_fn in _CHART_SIG_DETECTORS:
                _, days_to_fire = detect_fn(sig_bars, trade_idx)
                if days_to_fire is None:
                    continue
                fire_date = (date.fromisoformat(trade_date) + timedelta(days=days_to_fire)).isoformat()
                key = (sig_code, fire_date)
                if key in seen_signals:
                    continue
                seen_signals.add(key)
                signal_markers.append({
                    "time":     fire_date,
                    "position": "aboveBar",
                    "color":    color,
                    "shape":    "circle",
                    "text":     label,
                })

    # Build marker list for Lightweight Charts.
    # Group same-day, same-direction transactions into a single marker -- tickers
    # with a repeat institutional filer (e.g. many same-day 10b5-1 sale lines)
    # would otherwise stack one full-text label per transaction on the same
    # candle. Above MAX_LABELED_MARKERS, drop per-marker text entirely (arrows
    # only) -- a systematic seller trading near-daily for months (common for
    # PE sponsors post-lockup) still produces one marker per day, and adjacent
    # days' text labels overlap into an unreadable wall even without same-day
    # dupes. Full detail always remains in the transaction table below.
    MAX_LABELED_MARKERS = 25
    code_filter = {"buys": ["P"], "sells": ["S"], "both": ["P", "S"]}.get(mode, ["P", "S"])
    marker_groups: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for f in filings:
        if f.get("transaction_code") not in code_filter:
            continue
        tx_date = f.get("transaction_date") or ""
        if not tx_date:
            continue
        if hasattr(tx_date, "isoformat"):
            tx_date = tx_date.isoformat()
        marker_groups[(tx_date, f["transaction_code"])].append(f)

    # A global count cap alone isn't enough: a handful of same-week clusters
    # (e.g. 4 heavy sell days within a 5-day span) can stay under
    # MAX_LABELED_MARKERS yet still stack full-text labels on top of each other,
    # since the cap doesn't account for how close together the labeled dates
    # are. Thin by minimum spacing (scaled to the visible window), greedily
    # keeping the largest-dollar group in each too-close cluster so the
    # biggest events stay labeled.
    show_labels = len(marker_groups) <= MAX_LABELED_MARKERS
    labeled_keys: set[tuple[str, str]] = set()
    if show_labels:
        min_gap_days = max(1, days // MAX_LABELED_MARKERS)
        shown_dates: list[date] = []
        for key in sorted(
            marker_groups.keys(),
            key=lambda k: sum(g.get("total_value") or 0 for g in marker_groups[k]),
            reverse=True,
        ):
            d = date.fromisoformat(key[0])
            if all(abs((d - sd).days) >= min_gap_days for sd in shown_dates):
                shown_dates.append(d)
                labeled_keys.add(key)

    markers = []
    for (tx_date, code), group in marker_groups.items():
        is_buy = code == "P"
        text = ""
        if (tx_date, code) in labeled_keys:
            if len(group) == 1:
                label_parts = [group[0].get("insider_name", "")]
                if group[0].get("total_value_fmt"):
                    label_parts.append(group[0]["total_value_fmt"])
                text = " ".join(label_parts)
            else:
                names = sorted({g.get("insider_name", "") for g in group if g.get("insider_name")})
                total = sum(g.get("total_value") or 0 for g in group)
                extra = f" +{len(names) - 1} more" if len(names) > 1 else f" ({len(group)}x)"
                text = f"{names[0]}{extra} {queries._fmt_value(total)}".strip()
        markers.append({
            "time":     tx_date,
            "position": "belowBar" if is_buy else "aboveBar",
            "color":    "#22c55e" if is_buy else "#ef4444",
            "shape":    "arrowUp" if is_buy else "arrowDown",
            "text":     text,
        })

    watched = queries.watched_tickers(db)
    return templates.TemplateResponse(request, "chart.html", {
        "ticker": ticker,
        "range": range,
        "mode": mode,
        "bars": bars,
        "markers": markers,
        "signal_markers": signal_markers,
        "chart_from": from_date.isoformat(),
        "filings": filings,
        "earnings": earnings,
        "code_filter": code_filter,
        "has_api_key": bool(api_key),
        "ranges": list(_RANGE_DAYS.keys()),
        "is_watched": ticker in watched,
    })


@app.get("/htmx/chart-preview/{ticker}", response_class=HTMLResponse)
@limiter.limit("120/minute")
async def htmx_chart_preview(
    request: Request,
    ticker: str,
    db: psycopg.Connection = Depends(get_request_db),
):
    """Small price-line + insider markers for the ticker hover card. Cached
    per-ticker in Redis (~4h) since a hover can fire far more often than a
    full /chart/{ticker} page load."""
    ticker = ticker.upper()
    if not _TICKER_RE.match(ticker):
        raise HTTPException(status_code=400, detail="Invalid ticker")

    cache_key = f"it:chartpreview:{ticker}"
    html_out = cache_module.cache_get(cache_key)
    if html_out is None:
        pre_mtime = cache_module._sentinel_mtime()
        active_config = _load_config_cached()
        api_key = active_config.get("polygon_api_key", "")
        to_date = date.today()
        from_date = to_date - timedelta(days=95)

        bars = polygon_client.get_daily_bars(ticker, from_date, to_date, api_key, limit=100)
        filings = queries.get_issuer_filings(db, ticker, days=95)
        filings = [f for f in filings if f.get("table_type") == "ND"]

        last_price = bars[-1]["close"] if bars else None
        pct_change = (
            (bars[-1]["close"] - bars[0]["close"]) / bars[0]["close"] * 100
            if len(bars) >= 2 and bars[0]["close"] else None
        )
        svg = render_price_preview_svg(bars, filings)

        html_out = templates.env.get_template("_chart_preview.html").render({
            "ticker": ticker,
            "svg": svg,
            "last_price": last_price,
            "pct_change": pct_change,
        })
        cache_module.cache_set(cache_key, pre_mtime, html_out, ttl=14400)

    return HTMLResponse(html_out)


# ---------------------------------------------------------------------------
# Run log
# ---------------------------------------------------------------------------

@app.get("/watchlist", response_class=HTMLResponse)
async def watchlist_page(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
):
    wl = queries.list_watchlist(db)
    tickers = [item["value"] for item in wl["tickers"]]
    insider_ciks = [item["value"] for item in wl["insiders"]]
    congress_names_lower = [item["value"].lower() for item in wl["congress_members"]]

    last_by_ticker = queries.get_last_activity_by_ticker(db, tickers)
    last_by_insider = queries.get_last_activity_by_insider(db, insider_ciks)
    last_by_congress = queries.get_last_activity_by_congress_member(db, congress_names_lower)

    for item in wl["tickers"]:
        item["last_activity"] = last_by_ticker.get(item["value"])
    for item in wl["insiders"]:
        item["last_activity"] = last_by_insider.get(item["value"])
    for item in wl["congress_members"]:
        item["last_activity"] = last_by_congress.get(item["value"].lower())

    activity_feed = queries.get_watchlist_feed(db, tickers, insider_ciks, lookback_days=60, limit=20)

    return templates.TemplateResponse(request, "watchlist.html", {
        "watchlist": wl,
        "activity_feed": activity_feed,
    })


@app.post("/watchlist/add")
async def watchlist_add(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
    watch_type: str = Form(default=""),
    value: str = Form(default=""),
    label: str = Form(default=""),
    next: str = Form(default="/watchlist"),
):
    if watch_type not in ("ticker", "insider", "congress_member"):
        raise HTTPException(status_code=400, detail="Invalid watch_type")
    value = value.strip()
    label = label.strip()
    if not value or len(value) > 128 or len(label) > 128:
        raise HTTPException(status_code=400, detail="Invalid value or label")
    if watch_type == "ticker":
        if len(value) > 64:
            raise HTTPException(status_code=400, detail="Invalid value or label")
        value = value.upper()
        if not _TICKER_RE.match(value):
            raise HTTPException(status_code=400, detail="Invalid ticker format")
    elif watch_type == "insider":
        if not _CIK_RE.match(value):
            raise HTTPException(status_code=400, detail="Invalid CIK format")
    # congress_member: value is the politician name — stored as-is, matched case-insensitively
    queries.add_watch(db, watch_type, value, label or value)
    cache_module.invalidate_query_cache()
    safe_next = next if next in ("/watchlist", "/congress") else "/watchlist"
    return RedirectResponse(url=safe_next, status_code=303)


@app.post("/watchlist/remove")
async def watchlist_remove(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
    watch_id: int = Form(...),
):
    queries.remove_watch(db, watch_id)
    cache_module.invalidate_query_cache()
    return RedirectResponse(url="/watchlist", status_code=303)


@app.post("/watchlist/toggle", response_class=HTMLResponse)
async def watchlist_toggle(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
    watch_type: str = Form(default=""),
    value: str = Form(default=""),
    label: str = Form(default=""),
):
    if watch_type not in ("ticker", "insider"):
        raise HTTPException(status_code=400, detail="Invalid watch_type")
    value = value.strip()
    label = label.strip()
    if not value or len(value) > 128:
        raise HTTPException(status_code=400, detail="Invalid value")
    if watch_type == "ticker":
        if len(value) > 64:
            raise HTTPException(status_code=400, detail="Invalid ticker")
        value = value.upper()
        if not _TICKER_RE.match(value):
            raise HTTPException(status_code=400, detail="Invalid ticker format")
    elif watch_type == "insider":
        if not _CIK_RE.match(value):
            raise HTTPException(status_code=400, detail="Invalid CIK format")
    is_watched = queries.toggle_watch(db, watch_type, value, label or value)
    cache_module.invalidate_query_cache()
    watch_star = templates.env.get_template("_macros.html").module.watch_star
    return HTMLResponse(watch_star(watch_type, value, label or value, is_watched))


def _congress_filters_dict(
    *, ticker: str, politician: str, chamber: str, tx_type: str,
    source: str, days: int, sort_by: str, sort_order: str,
) -> dict:
    return {
        "ticker": ticker, "politician": politician, "chamber": chamber,
        "tx_type": tx_type, "source": source, "days": days,
        "sort_by": sort_by, "sort_order": sort_order,
    }


def _load_congress_leaderboard(min_trades: int = 3) -> list[dict]:
    """Read congress_backtest.csv and return per-politician performance stats.
    Result is cached for 1 hour — the CSV only changes when the backtest is re-run.
    """
    cached = _leaderboard_cache.get("leaderboard")
    if cached is not None:
        return cached

    csv_path = BASE_DIR / "data" / "congress_backtest.csv"
    if not csv_path.exists():
        return []

    def _f(s: str) -> float | None:
        try:
            return float(s) if s not in ("", "None") else None
        except ValueError:
            return None

    by_politician: dict[str, list[dict]] = {}
    try:
        with open(csv_path, newline="") as f:
            for row in csv.DictReader(f):
                name = row.get("politician_name") or ""
                if not name:
                    continue
                by_politician.setdefault(name, []).append(row)
    except Exception:
        return []

    ranked = []
    for name, rows in by_politician.items():
        if len(rows) < min_trades:
            continue
        chamber = (rows[0].get("chamber") or "").lower()
        party   = rows[0].get("party") or ""
        src     = "Executive" if chamber == "executive" else "Congress"

        e90  = [v for r in rows if (v := _f(r.get("excess_90d"))) is not None]
        e60  = [v for r in rows if (v := _f(r.get("excess_60d"))) is not None]
        e30  = [v for r in rows if (v := _f(r.get("excess_30d"))) is not None]
        win90 = sum(1 for v in e90 if v > 0) / len(e90) * 100 if e90 else None

        ranked.append({
            "name":    name,
            "source":  src,
            "party":   party,
            "n":       len(rows),
            "exc_30":  round(sum(e30) / len(e30), 1) if e30 else None,
            "exc_60":  round(sum(e60) / len(e60), 1) if e60 else None,
            "exc_90":  round(sum(e90) / len(e90), 1) if e90 else None,
            "win_90":  round(win90) if win90 is not None else None,
        })

    ranked.sort(key=lambda x: x["exc_90"] if x["exc_90"] is not None else -999, reverse=True)
    _leaderboard_cache["leaderboard"] = ranked
    return ranked


@app.get("/congress", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def congress_view(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
    ticker: str = Query(default=""),
    politician: str = Query(default=""),
    chamber: str = Query(default=""),
    tx_type: str = Query(default=""),
    source: str = Query(default=""),
    days: int = Query(default=0),
    sort_by: str = Query(default="transaction_date"),
    sort_order: str = Query(default="desc"),
):
    effective_days = days if days > 0 else None
    watched_members = queries.watched_congress_members(db)
    trades = queries.get_congress_trades(
        db,
        ticker=ticker or None,
        politician=politician or None,
        chamber=chamber or None,
        tx_type=tx_type or None,
        source=source or None,
        days=effective_days,
        sort_by=sort_by,
        sort_order=sort_order,
        watched_members=watched_members,
    )
    summary = queries.get_congress_summary(db, days=effective_days, source=source or None)
    filters = _congress_filters_dict(
        ticker=ticker, politician=politician, chamber=chamber,
        tx_type=tx_type, source=source, days=days,
        sort_by=sort_by, sort_order=sort_order,
    )
    return templates.TemplateResponse(request, "congress.html", {
        "request": request,
        "trades": trades,
        "summary": summary,
        "filters": filters,
        "leaderboard": _load_congress_leaderboard(),
    })


@app.get("/htmx/congress-trades", response_class=HTMLResponse)
@limiter.limit("60/minute")
async def htmx_congress_trades(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
    ticker: str = Query(default=""),
    politician: str = Query(default=""),
    chamber: str = Query(default=""),
    tx_type: str = Query(default=""),
    source: str = Query(default=""),
    days: int = Query(default=0),
    sort_by: str = Query(default="transaction_date"),
    sort_order: str = Query(default="desc"),
):
    effective_days = days if days > 0 else None
    watched_members = queries.watched_congress_members(db)
    trades = queries.get_congress_trades(
        db,
        ticker=ticker or None,
        politician=politician or None,
        chamber=chamber or None,
        tx_type=tx_type or None,
        source=source or None,
        days=effective_days,
        sort_by=sort_by,
        sort_order=sort_order,
        watched_members=watched_members,
    )
    filters = _congress_filters_dict(
        ticker=ticker, politician=politician, chamber=chamber,
        tx_type=tx_type, source=source, days=days,
        sort_by=sort_by, sort_order=sort_order,
    )
    return templates.TemplateResponse(request, "_congress_partial.html", {
        "trades": trades,
        "filters": filters,
    })


# ---------------------------------------------------------------------------
# API — tickers list for search datalist
# ---------------------------------------------------------------------------

@app.get("/api/tickers-list")
@limiter.limit("30/minute")
async def tickers_list(request: Request):
    cached = cache_module.cache_get("it:tickers-list")
    if cached is not None:
        return JSONResponse(cached)
    pre_mtime = cache_module._sentinel_mtime()
    db = get_db()
    try:
        tickers = queries.get_ticker_list(db)
    finally:
        put_db(db)
    cache_module.cache_set("it:tickers-list", pre_mtime, tickers)
    return JSONResponse(tickers)


# ---------------------------------------------------------------------------
# Webhook — auto-diagnosis trigger from Healthchecks.io / BetterStack
# ---------------------------------------------------------------------------

@app.post("/webhook/alert")
async def webhook_alert(request: Request, background_tasks: BackgroundTasks):
    """
    Receives POST from Healthchecks.io or BetterStack when a check fails.
    Validates WEBHOOK_SECRET, then fires auto_diagnose in the background.
    Intentionally exempt from Basic Auth and rate limiting (called by external services).
    """
    secret = os.getenv("WEBHOOK_SECRET", "")
    if secret:
        provided = request.headers.get("X-Webhook-Secret", "")
        if not hmac.compare_digest(provided, secret):
            raise HTTPException(status_code=403, detail="Invalid webhook secret")

    try:
        body = await request.json()
    except Exception:
        body = {}

    check_name = (
        body.get("check_name")
        or body.get("monitor", {}).get("url")
        or body.get("monitor_friendly_name")
        or "unknown"
    )
    alert_info = {"check_name": check_name, "source": "webhook", "payload": body}
    background_tasks.add_task(_run_diagnostic_bg, alert_info)
    return JSONResponse({"ok": True})


def _run_diagnostic_bg(alert_info: dict) -> None:
    try:
        import auto_diagnose
        auto_diagnose.run_diagnostic(alert_info)
    except Exception as e:
        logging.getLogger("auto_diagnose").error("Diagnostic failed: %s", e)


@app.get("/run-log", response_class=HTMLResponse)
@limiter.limit("30/minute")
async def run_log(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
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

    _bt_windows = (15, 30, 45, 60, 90)
    bool_fields = {
        "is_10b5_1", "is_director", "is_officer",
        *(f"{s}_computable" for s in ("gc", "rb", "hhl", "cb")),
        *(f"{s}_{w}d" for s in ("gc", "rb", "hhl", "cb") for w in _bt_windows),
    }
    float_fields = {
        "value", "trade_price",
        "gc_days", "rb_days", "hhl_days", "cb_days",
        *(f"stacked_{w}d"        for w in _bt_windows),
        *(f"return_{w}d"         for w in _bt_windows),
        *(f"{s}_fire_ret_{w}d"   for s in ("gc", "rb", "hhl", "cb") for w in _bt_windows),
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
    WINDOWS = _bt_windows

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


@app.get("/backtest-logic", response_class=HTMLResponse)
async def backtest_logic_page(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
):
    active_config = _load_config_cached()
    rules = active_config.get("alert_rules", {})

    signal_defs = [
        {
            "code": "gc", "label": "Golden Cross", "live_alert": True,
            "what": "50-day MA crosses above the 200-day MA, having been below it at the time of the insider buy.",
            "params": "50/200-day SMA",
        },
        {
            "code": "rb", "label": "Resistance Break", "live_alert": False,
            "what": "Close breaks above a resistance level (2+ local-high touches within ±2%, over the trailing 180 days) by more than 1%.",
            "params": "180d lookback · ±2% cluster · 2+ touches · 1% break",
        },
        {
            "code": "hhl", "label": "Higher Highs + Higher Lows", "live_alert": False,
            "what": "Two consecutive confirmed swing highs and swing lows, each higher than the one before (pivots confirmed 3 bars later).",
            "params": "3-bar pivot confirmation · 2 consecutive HH & HL",
        },
        {
            "code": "cb", "label": "Channel Break", "live_alert": False,
            "what": "Stock trades sideways for 90 days (high–low range ≤ 20%) then closes 1%+ above the channel's high.",
            "params": "90d channel · ≤20% range · 1% break · 20+ bars",
        },
    ]
    signal_stats = alert_module._SIGNAL_STATS

    signal_history = queries.get_signal_alert_history(db, limit=20)

    return templates.TemplateResponse(request, "backtest_logic.html", {
        "signal_defs": signal_defs,
        "signal_stats": signal_stats,
        "signal_history": signal_history,
        "signal_scan_min_value": rules.get("signal_scan_min_value", 500_000),
        "signal_scan_lookback_days": rules.get("signal_scan_lookback_days", 90),
        "signal_scan_max_signal_age_days": rules.get("signal_scan_max_signal_age_days", 5),
        "polygon_configured": bool(active_config.get("polygon_api_key", "")),
        "slack_configured": bool(os.getenv("SLACK_WEBHOOK_URL", "")),
    })


# ---------------------------------------------------------------------------
# Logic & Config
# ---------------------------------------------------------------------------

@app.get("/logic", response_class=HTMLResponse)
async def logic_page(
    request: Request,
    db: psycopg.Connection = Depends(get_request_db),
):
    active_config = _load_config_cached()
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
    big_buy_threshold: float = Form(..., ge=0, le=1_000_000_000),
    insider_buy_threshold: float = Form(..., ge=0, le=1_000_000_000),
    insider_title_keywords: str = Form(...),
    cluster_window_days: int = Form(..., ge=1, le=365),
    cluster_min_insiders: int = Form(..., ge=2, le=50),
    # Filter defaults
    min_value: float = Form(..., ge=0, le=1_000_000_000),
    hide_10b5_1: str = Form(default="off"),
    # Conviction flags (all optional — default None means "keep existing")
    conviction_base_open_market_buy: int | None = Form(default=None, ge=0, le=10),
    conviction_ceo_cfo_bonus: int | None = Form(default=None, ge=0, le=10),
    conviction_director_bonus: int | None = Form(default=None, ge=0, le=10),
    conviction_ten_percent_owner_bonus: int | None = Form(default=None, ge=0, le=10),
    conviction_cluster_bonus: int | None = Form(default=None, ge=0, le=10),
    conviction_non_10b5_1_buy: int | None = Form(default=None, ge=0, le=10),
    # Conviction tier point values
    conviction_value_250k_pts: int | None = Form(default=None, ge=0, le=10),
    conviction_value_1m_pts: int | None = Form(default=None, ge=0, le=10),
    conviction_value_5m_pts: int | None = Form(default=None, ge=0, le=10),
    conviction_pct_20_pts: int | None = Form(default=None, ge=0, le=10),
    conviction_pct_50_pts: int | None = Form(default=None, ge=0, le=10),
    # Insider history baseline (all optional -- default None means "keep existing")
    insider_baseline_min_prior_trades: int | None = Form(default=None, ge=1, le=20),
    insider_baseline_size_multiplier: float | None = Form(default=None, ge=1.0, le=20.0),
    insider_baseline_silence_days: int | None = Form(default=None, ge=30, le=3650),
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
        existing = _load_config_cached()
        conviction_flags = dict(existing.get("conviction_flags") or cfg.CONVICTION_FLAGS)
        conviction_flags.update(submitted_flags)
        # Tier point overrides — update the points in CONVICTION_TIERS structure
        # These are stored separately in config_overrides under "conviction_tier_pts"
        # and applied at load_config time. For now store in conviction_flags with prefix.
        for key, val in submitted_tier_pts.items():
            conviction_flags[f"tier_pts_{key}"] = val

    insider_baseline_map = {
        "min_prior_trades": insider_baseline_min_prior_trades,
        "size_multiplier":  insider_baseline_size_multiplier,
        "silence_days":     insider_baseline_silence_days,
    }
    submitted_insider_baseline = {k: v for k, v in insider_baseline_map.items() if v is not None}

    save_overrides(
        alert_rules, filter_defaults,
        conviction_flags=conviction_flags or None,
        insider_baseline=submitted_insider_baseline or None,
    )
    _config_cache.clear()
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
    active_config = _load_config_cached()
    base_url = active_config.get("alert_base_url", "https://opi-insider.duckdns.org")
    ok = alert_module.send_test_alert(webhook_url, base_url)
    if ok:
        return JSONResponse({"ok": True, "message": "Test alert sent successfully"})
    return JSONResponse(
        {"ok": False, "error": "Slack returned an error — check the webhook URL"},
        status_code=502,
    )
