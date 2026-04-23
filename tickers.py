import json
import time
from pathlib import Path

import httpx

from config import SEC_USER_AGENT, TICKER_CACHE_PATH, TICKER_CACHE_DAYS

TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"

_cache: dict[str, str] | None = None


def _is_stale() -> bool:
    path = Path(TICKER_CACHE_PATH)
    if not path.exists():
        return True
    age_days = (time.time() - path.stat().st_mtime) / 86400
    return age_days > TICKER_CACHE_DAYS


def _fetch_and_cache() -> dict[str, str]:
    """Download company_tickers.json and build {padded_cik: ticker} dict."""
    resp = httpx.get(
        TICKERS_URL,
        headers={"User-Agent": SEC_USER_AGENT},
        timeout=30,
        follow_redirects=True,
    )
    resp.raise_for_status()
    raw = resp.json()

    mapping: dict[str, str] = {}
    for entry in raw.values():
        cik = str(entry["cik_str"]).zfill(10)
        ticker = entry.get("ticker", "")
        if ticker:
            mapping[cik] = ticker.upper()

    Path(TICKER_CACHE_PATH).parent.mkdir(parents=True, exist_ok=True)
    with open(TICKER_CACHE_PATH, "w") as f:
        json.dump(mapping, f)

    return mapping


def get_ticker_map() -> dict[str, str]:
    """Return {padded_cik: ticker}. Uses in-memory cache, then file, then fetches."""
    global _cache
    if _cache is not None:
        return _cache

    if not _is_stale():
        with open(TICKER_CACHE_PATH) as f:
            _cache = json.load(f)
        return _cache

    _cache = _fetch_and_cache()
    return _cache


def lookup_ticker(cik: str) -> str | None:
    """Return ticker for a CIK (zero-padded to 10 digits), or None."""
    padded = cik.zfill(10)
    return get_ticker_map().get(padded)


def invalidate_cache() -> None:
    global _cache
    _cache = None
    path = Path(TICKER_CACHE_PATH)
    if path.exists():
        path.unlink()
