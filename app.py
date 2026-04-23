from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager
from datetime import date, timedelta
from pathlib import Path

from fastapi import FastAPI, Form, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

import config as cfg
from config import save_overrides
from ingest import get_db
import queries

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = get_db()
    yield
    app.state.db.close()


app = FastAPI(title="Insider Tracker", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(BASE_DIR / "static")), name="static")


def _db(request: Request) -> sqlite3.Connection:
    return request.app.state.db


def _parse_date(d: str | None) -> date:
    if not d:
        return date.today()
    try:
        return date.fromisoformat(d)
    except ValueError:
        return date.today()


# ---------------------------------------------------------------------------
# Main dashboard
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
async def index(
    request: Request,
    d: str | None = Query(default=None),
    min_value: float = Query(default=None),
    hide_10b5_1: str = Query(default=None),
    codes: list[str] = Query(default=None),
    roles: list[str] = Query(default=None),
    search: str | None = Query(default=None),
):
    active_config = cfg.load_config()
    fd = active_config["filter_defaults"]

    target_date = _parse_date(d)
    effective_min = min_value if min_value is not None else fd["min_value"]
    effective_hide = (hide_10b5_1 != "0") if hide_10b5_1 is not None else fd["hide_10b5_1"]
    effective_codes = codes if codes else fd["transaction_codes"]

    db = _db(request)
    buys, sells = queries.get_filings_for_date(
        db, target_date,
        min_value=effective_min,
        transaction_codes=effective_codes,
        hide_10b5_1=effective_hide,
        roles=roles,
        search=search,
    )
    stats = queries.get_summary_stats(db, target_date, hide_10b5_1=effective_hide)
    clusters = queries.get_cluster_activity(db, target_date, hide_10b5_1=effective_hide)

    return templates.TemplateResponse(request, "index.html", {
        "buys": buys,
        "sells": sells,
        "stats": stats,
        "clusters": clusters,
        "target_date": target_date.isoformat(),
        "prev_date": (target_date - timedelta(days=1)).isoformat(),
        "next_date": (target_date + timedelta(days=1)).isoformat(),
        "is_today": target_date == date.today(),
        "filters": {
            "min_value": effective_min,
            "hide_10b5_1": effective_hide,
            "codes": effective_codes,
            "roles": roles or [],
            "search": search or "",
        },
        "config": active_config,
    })


# ---------------------------------------------------------------------------
# HTMX partial — filter updates
# ---------------------------------------------------------------------------

@app.get("/htmx/filings", response_class=HTMLResponse)
async def htmx_filings(
    request: Request,
    d: str | None = Query(default=None),
    min_value: float = Query(default=0),
    hide_10b5_1: str = Query(default="1"),
    codes: list[str] = Query(default=["P", "S"]),
    roles: list[str] = Query(default=None),
    search: str | None = Query(default=None),
):
    target_date = _parse_date(d)
    effective_hide = hide_10b5_1 != "0"
    db = _db(request)
    buys, sells = queries.get_filings_for_date(
        db, target_date,
        min_value=min_value,
        transaction_codes=codes,
        hide_10b5_1=effective_hide,
        roles=roles,
        search=search,
    )
    stats = queries.get_summary_stats(db, target_date, hide_10b5_1=effective_hide)
    clusters = queries.get_cluster_activity(db, target_date, hide_10b5_1=effective_hide)
    return templates.TemplateResponse(request, "_tables_partial.html", {
        "buys": buys,
        "sells": sells,
        "stats": stats,
        "clusters": clusters,
        "target_date": target_date.isoformat(),
    })


# ---------------------------------------------------------------------------
# Filing detail
# ---------------------------------------------------------------------------

@app.get("/filing/{transaction_id:path}", response_class=HTMLResponse)
async def filing_detail(request: Request, transaction_id: str):
    filing = queries.get_filing_detail(_db(request), transaction_id)
    if filing is None:
        raise HTTPException(status_code=404, detail="Filing not found")
    return templates.TemplateResponse(request, "filing.html", {
        "filing": filing,
    })


# ---------------------------------------------------------------------------
# Issuer view
# ---------------------------------------------------------------------------

@app.get("/issuer/{ticker}", response_class=HTMLResponse)
async def issuer_view(request: Request, ticker: str, days: int = Query(default=90)):
    filings = queries.get_issuer_filings(_db(request), ticker, days=days)
    return templates.TemplateResponse(request, "issuer.html", {
        "ticker": ticker.upper(),
        "filings": filings,
        "days": days,
    })


# ---------------------------------------------------------------------------
# Run log
# ---------------------------------------------------------------------------

@app.get("/run-log", response_class=HTMLResponse)
async def run_log(request: Request):
    log = queries.get_run_log(_db(request))
    return templates.TemplateResponse(request, "run_log.html", {
        "log": log,
    })


# ---------------------------------------------------------------------------
# Logic & Config
# ---------------------------------------------------------------------------

@app.get("/logic", response_class=HTMLResponse)
async def logic_page(request: Request):
    active_config = cfg.load_config()
    stats = queries.get_10b5_1_stats(_db(request))
    return templates.TemplateResponse(request, "logic.html", {
        "config": active_config,
        "stats": stats,
        "transaction_codes": cfg.TRANSACTION_CODES,
    })


@app.post("/logic/save")
async def logic_save(
    request: Request,
    big_buy_threshold: float = Form(...),
    insider_buy_threshold: float = Form(...),
    insider_title_keywords: str = Form(...),
    cluster_window_days: int = Form(...),
    cluster_min_insiders: int = Form(...),
    min_value: float = Form(...),
    hide_10b5_1: str = Form(default="off"),
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
    save_overrides(alert_rules, filter_defaults)
    return RedirectResponse(url="/logic?saved=1", status_code=303)
