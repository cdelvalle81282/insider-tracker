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
            params={"apiKey": api_key, "limit": 365, "adjusted": "true", "sort": "asc"},
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
