"""
Polygon.io API client — daily OHLCV bars and earnings estimates for the chart page.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone

import httpx


def get_daily_bars(
    ticker: str,
    from_date: date,
    to_date: date,
    api_key: str,
    limit: int = 365,
) -> list[dict]:
    """
    Fetch daily adjusted OHLCV bars from Polygon.io.
    Returns list of {time, open, high, low, close, volume} dicts
    where time is 'YYYY-MM-DD' (required by Lightweight Charts).
    Returns [] on any error so the chart page degrades gracefully.
    """
    if not api_key:
        return []

    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker.upper()}/range/1/day"
        f"/{from_date.isoformat()}/{to_date.isoformat()}"
    )
    try:
        resp = httpx.get(
            url,
            params={"apiKey": api_key, "limit": limit, "adjusted": "true", "sort": "asc"},
            timeout=10.0,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception:
        return []

    results = data.get("results") or []
    bars = []
    for bar in results:
        t = bar.get("t")
        if t is None:
            continue
        day = datetime.fromtimestamp(t / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
        bars.append({
            "time":   day,
            "open":   bar.get("o", 0),
            "high":   bar.get("h", 0),
            "low":    bar.get("l", 0),
            "close":  bar.get("c", 0),
            "volume": bar.get("v", 0),
        })
    return bars


def get_earnings_estimate(ticker: str, api_key: str) -> dict | None:
    """
    Fetch the most recent quarterly period from Polygon financials and estimate
    the next earnings date (last period + 91 days).
    Returns None on any error or missing API key so the chart degrades gracefully.
    """
    if not api_key:
        return None
    try:
        resp = httpx.get(
            "https://api.polygon.io/vX/reference/financials",
            params={
                "ticker": ticker.upper(),
                "timeframe": "quarterly",
                "limit": 1,
                "sort": "period_of_report_date",
                "order": "desc",
                "apiKey": api_key,
            },
            timeout=5.0,
        )
        resp.raise_for_status()
        results = resp.json().get("results") or []
        if not results:
            return None
        r = results[0]
        last = r.get("period_of_report_date")
        if not last:
            return None
        last_date = date.fromisoformat(last)
        estimated_next = last_date + timedelta(days=91)
        days_until = (estimated_next - date.today()).days
        return {
            "last_period": last,
            "fiscal_period": r.get("fiscal_period", ""),
            "fiscal_year": r.get("fiscal_year", ""),
            "estimated_next": estimated_next.isoformat(),
            "days_until": days_until,
        }
    except Exception:
        return None


def fetch_ticker_metadata(ticker: str, api_key: str) -> dict | None:
    """
    Fetches market cap and options availability for a ticker from Polygon.io.

    Calls two endpoints independently so a failure on one does not block the other.
    Returns {'market_cap': float|None, 'has_options': 0|1|None} or None if both
    calls fail (i.e. both market_cap and has_options are None).
    """
    if not api_key:
        return None

    market_cap: float | None = None
    has_options: int | None = None

    try:
        resp = httpx.get(
            f"https://api.polygon.io/v3/reference/tickers/{ticker.upper()}",
            params={"apiKey": api_key},
            timeout=5.0,
        )
        if resp.status_code == 200:
            market_cap = resp.json().get("results", {}).get("market_cap")
    except Exception:
        pass

    try:
        resp = httpx.get(
            "https://api.polygon.io/v3/reference/options/contracts",
            params={"underlying_ticker": ticker.upper(), "limit": 1, "apiKey": api_key},
            timeout=5.0,
        )
        if resp.status_code == 200:
            has_options = 1 if len(resp.json().get("results", [])) > 0 else 0
    except Exception:
        pass

    if market_cap is None and has_options is None:
        return None
    return {"market_cap": market_cap, "has_options": has_options}


def fetch_latest_close(ticker: str, api_key: str) -> float | None:
    """Most recent daily close price. Returns None on any error or missing data."""
    if not api_key:
        return None
    try:
        from datetime import date, timedelta
        bars = get_daily_bars(
            ticker,
            (date.today() - timedelta(days=7)).strftime("%Y-%m-%d"),
            date.today().strftime("%Y-%m-%d"),
            api_key,
        )
        return bars[-1]["close"] if bars else None
    except Exception:
        return None
