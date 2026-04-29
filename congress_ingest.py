"""Congressional trade ingester — fetches House and Senate Stock Watcher data."""
from __future__ import annotations

import hashlib
import re
import sys

import httpx

from ingest import get_db

HOUSE_URL = (
    "https://house-stock-watcher-data.s3-us-west-2.amazonaws.com"
    "/data/all_transactions.json"
)
SENATE_URL = (
    "https://raw.githubusercontent.com/timothycarambat"
    "/senate-stock-watcher-data/master/data/all_transactions.json"
)

_AMOUNT_RE = re.compile(r"[\$,]")
_OVER_RE = re.compile(r"[Oo]ver\s+\$?([\d,]+)")


def _make_tx_id(
    source: str,
    politician: str,
    ticker: str,
    date: str,
    amount_label: str,
) -> str:
    raw = f"{source}|{politician}|{ticker}|{date}|{amount_label}"
    return hashlib.sha256(raw.encode()).hexdigest()[:24]


def _parse_amount_range(label: str | None) -> tuple[float | None, float | None]:
    """Parse amount strings like '$1,001 - $15,000' or 'Over $1,000,000'."""
    if not label:
        return None, None
    try:
        over_match = _OVER_RE.search(label)
        if over_match:
            min_val = float(over_match.group(1).replace(",", ""))
            return min_val, None

        cleaned = _AMOUNT_RE.sub("", label)
        parts = [p.strip() for p in cleaned.split("-") if p.strip()]
        if len(parts) == 2:
            return float(parts[0]), float(parts[1])
        if len(parts) == 1:
            return float(parts[0]), None
    except (ValueError, IndexError):
        pass
    return None, None


def _normalize_house(record: dict) -> dict | None:
    """Map a House Stock Watcher record to the congress_trades schema."""
    politician = (record.get("representative") or "").strip()
    tx_date = (record.get("transaction_date") or "").strip()
    if not politician or not tx_date:
        return None

    raw_type = (record.get("type") or "").lower().strip()
    if "sale" in raw_type:
        tx_type = "sale"
    elif "purchase" in raw_type:
        tx_type = "purchase"
    elif "exchange" in raw_type:
        tx_type = "exchange"
    else:
        tx_type = raw_type

    ticker = (record.get("ticker") or "").strip().upper() or None
    amount_label = (record.get("amount") or "").strip() or None
    amount_min, amount_max = _parse_amount_range(amount_label)

    tx_id = _make_tx_id("house", politician, ticker or "", tx_date, amount_label or "")

    return {
        "source": "house",
        "transaction_id": tx_id,
        "politician_name": politician,
        "chamber": "house",
        "party": None,
        "state": None,
        "ticker": ticker,
        "asset_description": (record.get("asset_description") or "").strip() or None,
        "transaction_type": tx_type or None,
        "transaction_date": tx_date,
        "disclosure_date": (record.get("disclosure_date") or "").strip() or None,
        "amount_min": amount_min,
        "amount_max": amount_max,
        "amount_label": amount_label,
        "raw_url": (record.get("link") or "").strip() or None,
    }


def _normalize_senate(record: dict) -> dict | None:
    """Map a Senate Stock Watcher record to the congress_trades schema."""
    first = (record.get("first_name") or "").strip()
    last = (record.get("last_name") or "").strip()
    politician = f"{first} {last}".strip()
    tx_date = (record.get("transaction_date") or "").strip()
    if not politician or not tx_date:
        return None

    raw_type = (record.get("type") or "").lower().strip()
    if "sale" in raw_type:
        tx_type = "sale"
    elif "purchase" in raw_type:
        tx_type = "purchase"
    elif "exchange" in raw_type:
        tx_type = "exchange"
    else:
        tx_type = raw_type

    ticker = (record.get("ticker") or "").strip().upper() or None
    amount_label = (record.get("amount") or "").strip() or None
    amount_min, amount_max = _parse_amount_range(amount_label)

    tx_id = _make_tx_id("senate", politician, ticker or "", tx_date, amount_label or "")

    return {
        "source": "senate",
        "transaction_id": tx_id,
        "politician_name": politician,
        "chamber": "senate",
        "party": (record.get("party") or "").strip() or None,
        "state": (record.get("state") or "").strip() or None,
        "ticker": ticker,
        "asset_description": (record.get("asset_description") or "").strip() or None,
        "transaction_type": tx_type or None,
        "transaction_date": tx_date,
        "disclosure_date": (record.get("disclosure_date") or "").strip() or None,
        "amount_min": amount_min,
        "amount_max": amount_max,
        "amount_label": amount_label,
        "raw_url": (record.get("link") or "").strip() or None,
    }


def fetch_house() -> list[dict]:
    """Fetch and normalize all House transactions."""
    try:
        resp = httpx.get(HOUSE_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"[congress_ingest] ERROR fetching house data: {exc}", file=sys.stderr)
        return []

    records = resp.json()
    result = []
    for rec in records:
        normalized = _normalize_house(rec)
        if normalized is not None:
            result.append(normalized)
    return result


def fetch_senate() -> list[dict]:
    """Fetch and normalize all Senate transactions."""
    try:
        resp = httpx.get(SENATE_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"[congress_ingest] ERROR fetching senate data: {exc}", file=sys.stderr)
        return []

    records = resp.json()
    result = []
    for rec in records:
        normalized = _normalize_senate(rec)
        if normalized is not None:
            result.append(normalized)
    return result


def ingest(source: str = "all") -> None:
    """Fetch and upsert congressional trades. source in ('house', 'senate', 'all')."""
    sources_to_run: list[str] = []
    if source in ("house", "all"):
        sources_to_run.append("house")
    if source in ("senate", "all"):
        sources_to_run.append("senate")

    conn = get_db()

    for src in sources_to_run:
        print(f"[congress_ingest] Fetching {src} trades...")
        records = fetch_house() if src == "house" else fetch_senate()
        print(f"[congress_ingest] {src}: {len(records)} records fetched")

        inserted = 0
        skipped = 0

        conn.execute("BEGIN")
        for rec in records:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO congress_trades (
                    source, transaction_id, politician_name, chamber, party, state,
                    ticker, asset_description, transaction_type,
                    transaction_date, disclosure_date,
                    amount_min, amount_max, amount_label, raw_url
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    rec["source"],
                    rec["transaction_id"],
                    rec["politician_name"],
                    rec["chamber"],
                    rec["party"],
                    rec["state"],
                    rec["ticker"],
                    rec["asset_description"],
                    rec["transaction_type"],
                    rec["transaction_date"],
                    rec["disclosure_date"],
                    rec["amount_min"],
                    rec["amount_max"],
                    rec["amount_label"],
                    rec["raw_url"],
                ],
            )
            if cur.rowcount:
                inserted += 1
            else:
                skipped += 1

        conn.commit()
        print(
            f"[congress_ingest] {src}: {inserted} inserted, {skipped} skipped (already existed)"
        )


if __name__ == "__main__":
    source = sys.argv[1] if len(sys.argv) > 1 else "all"
    ingest(source)
