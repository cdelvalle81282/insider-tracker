"""
SIC code → sector enrichment.

Fetches SIC codes from EDGAR's company JSON API, maps them to 11 readable
sector labels, and caches results in the `sectors` table.
"""
from __future__ import annotations

import sqlite3
import time
from datetime import date

import httpx

from config import SEC_USER_AGENT, SEC_RATE_LIMIT

RATE_SLEEP = 1.0 / SEC_RATE_LIMIT

# ---------------------------------------------------------------------------
# SIC → sector mapping
# Ranges are (inclusive_start, inclusive_end, sector_label).
# Listed in lookup order — first match wins.
# ---------------------------------------------------------------------------

_SIC_RANGES = [
    # Technology
    (3570, 3579, "Technology"),
    (3660, 3679, "Technology"),
    (3690, 3699, "Technology"),
    (3760, 3769, "Technology"),
    (3810, 3812, "Technology"),
    (3820, 3827, "Technology"),
    (7370, 7379, "Technology"),
    # Healthcare
    (2830, 2836, "Healthcare"),
    (3841, 3851, "Healthcare"),
    (3826, 3826, "Healthcare"),
    (5047, 5047, "Healthcare"),
    (5122, 5122, "Healthcare"),
    (8000, 8099, "Healthcare"),
    # Financials
    (6000, 6099, "Financials"),
    (6100, 6199, "Financials"),
    (6200, 6289, "Financials"),
    (6300, 6411, "Financials"),
    (6700, 6726, "Financials"),
    # Real Estate (before Financials catch-all ends)
    (6500, 6552, "Real Estate"),
    (6798, 6798, "Real Estate"),
    # Energy
    (1300, 1389, "Energy"),
    (2900, 2911, "Energy"),
    # Consumer Discretionary
    (5500, 5599, "Consumer Disc"),
    (5600, 5699, "Consumer Disc"),
    (5700, 5799, "Consumer Disc"),
    (5900, 5999, "Consumer Disc"),
    (7000, 7041, "Consumer Disc"),
    (7200, 7299, "Consumer Disc"),
    (7500, 7599, "Consumer Disc"),
    (7810, 7819, "Consumer Disc"),
    # Consumer Staples
    (2000, 2099, "Consumer Staples"),
    (2100, 2199, "Consumer Staples"),
    (5100, 5199, "Consumer Staples"),
    (5400, 5499, "Consumer Staples"),
    # Industrials
    (1500, 1799, "Industrials"),
    (3400, 3499, "Industrials"),
    (3500, 3569, "Industrials"),
    (3700, 3799, "Industrials"),
    (4210, 4215, "Industrials"),
    (4500, 4599, "Industrials"),
    # Materials
    (1000, 1299, "Materials"),
    (2600, 2679, "Materials"),
    (2800, 2829, "Materials"),
    (3000, 3099, "Materials"),
    (3300, 3399, "Materials"),
    # Utilities
    (4900, 4991, "Utilities"),
    # Communications
    (4800, 4899, "Communications"),
]


def sic_to_sector(sic_code: str | None) -> str:
    if not sic_code:
        return "Other"
    try:
        sic = int(sic_code)
    except ValueError:
        return "Other"
    for lo, hi, label in _SIC_RANGES:
        if lo <= sic <= hi:
            return label
    return "Other"


# ---------------------------------------------------------------------------
# EDGAR fetch
# ---------------------------------------------------------------------------

def fetch_sic_for_cik(cik: str) -> tuple[str, str]:
    """
    Fetch SIC code and description for a CIK from EDGAR.
    Returns ("", "") on any error — caller should treat as unknown.
    """
    padded = cik.zfill(10)
    url = f"https://data.sec.gov/submissions/CIK{padded}.json"
    time.sleep(RATE_SLEEP)
    try:
        resp = httpx.get(
            url,
            headers={"User-Agent": SEC_USER_AGENT},
            timeout=5.0,
            follow_redirects=True,
        )
        if resp.status_code != 200:
            return "", ""
        data = resp.json()
        return str(data.get("sic", "") or ""), str(data.get("sicDescription", "") or "")
    except Exception:
        return "", ""


# ---------------------------------------------------------------------------
# Cache-aside
# ---------------------------------------------------------------------------

_REFRESH_DAYS = 90
_session_cache: dict[str, str] = {}   # in-memory per-process cache


def get_or_fetch_sector(conn: sqlite3.Connection, cik: str) -> str:
    """
    Return sector label for a CIK. Checks in order:
      1. In-memory session cache (instant)
      2. `sectors` DB table (if not stale)
      3. EDGAR API (updates DB + session cache)
    """
    if cik in _session_cache:
        return _session_cache[cik]

    row = conn.execute(
        "SELECT sector, fetched_at FROM sectors WHERE issuer_cik = ?", [cik]
    ).fetchone()

    if row:
        fetched = date.fromisoformat(row["fetched_at"][:10]) if row["fetched_at"] else None
        if fetched and (date.today() - fetched).days < _REFRESH_DAYS:
            sector = row["sector"] or "Other"
            _session_cache[cik] = sector
            return sector

    # Cache miss or stale — fetch from EDGAR
    sic_code, sic_desc = fetch_sic_for_cik(cik)
    sector = sic_to_sector(sic_code)

    conn.execute(
        """INSERT INTO sectors (issuer_cik, sic_code, sic_desc, sector, fetched_at)
           VALUES (?, ?, ?, ?, DATE('now'))
           ON CONFLICT(issuer_cik) DO UPDATE SET
             sic_code=excluded.sic_code, sic_desc=excluded.sic_desc,
             sector=excluded.sector, fetched_at=excluded.fetched_at""",
        [cik, sic_code, sic_desc, sector],
    )
    conn.commit()
    _session_cache[cik] = sector
    return sector


def invalidate_session_cache() -> None:
    _session_cache.clear()
