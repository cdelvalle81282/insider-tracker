"""
Polygon.io API client — daily OHLCV bars for the chart page.
"""
from __future__ import annotations

from datetime import date, datetime, timezone

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
