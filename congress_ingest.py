"""Congressional trade ingester — uses AInvest API (ticker-based)."""
from __future__ import annotations

import os
import sys
import time

import httpx

from ingest import get_db

AINVEST_BASE = "https://openapi.ainvest.com/open/ownership/congress"
PAGE_SIZE = 100
RATE_SLEEP = 0.5
_VALID_TICKER = __import__("re").compile(r"^[A-Z]{1,5}$")


def _get_api_key() -> str:
    key = os.getenv("AINVEST_API_KEY", "")
    if not key:
        print("[congress_ingest] ERROR: AINVEST_API_KEY not set.", file=sys.stderr)
        sys.exit(1)
    return key


def _chamber(trade_id: str) -> str:
    return "senate" if trade_id.startswith("S") else "house"


def _parse_amount_range(size_label: str | None) -> tuple[float | None, float | None]:
    """Parse AInvest size labels like '$1K-$15K', '$1M-$5M', 'Over $1M'."""
    if not size_label:
        return None, None

    def _parse_val(s: str) -> float | None:
        s = s.strip().lstrip("$").upper()
        try:
            if s.endswith("M"):
                return float(s[:-1]) * 1_000_000
            if s.endswith("K"):
                return float(s[:-1]) * 1_000
            return float(s.replace(",", ""))
        except ValueError:
            return None

    label = size_label.strip()
    if "-" in label:
        parts = label.split("-", 1)
        return _parse_val(parts[0]), _parse_val(parts[1])
    if label.lower().startswith("over"):
        return _parse_val(label[4:].strip()), None
    return _parse_val(label), None


def fetch_ticker(ticker: str, api_key: str) -> list[dict]:
    """Fetch all congressional trades for a ticker, paginating until exhausted."""
    headers = {"Authorization": f"Bearer {api_key}"}
    records = []
    page = 1

    while True:
        try:
            resp = httpx.get(
                AINVEST_BASE,
                params={"ticker": ticker, "page": page, "size": PAGE_SIZE},
                headers=headers,
                timeout=10.0,
            )
            resp.raise_for_status()
            outer = resp.json().get("data") or {}
            data = outer.get("data") or []
        except Exception as exc:
            print(f"[congress_ingest] WARN {ticker} page {page}: {exc}", file=sys.stderr)
            break

        for rec in data:
            trade_id = rec.get("id") or ""
            amount_min, amount_max = _parse_amount_range(rec.get("size"))
            records.append({
                "source": "ainvest",
                "transaction_id": trade_id,
                "politician_name": (rec.get("name") or "").strip(),
                "chamber": _chamber(trade_id),
                "party": rec.get("party") or None,
                "state": rec.get("state") or None,
                "ticker": ticker,
                "asset_description": None,
                "transaction_type": rec.get("trade_type") or None,
                "transaction_date": rec.get("trade_date") or None,
                "disclosure_date": rec.get("filing_date") or None,
                "amount_min": amount_min,
                "amount_max": amount_max,
                "amount_label": rec.get("size") or None,
                "raw_url": None,
            })

        if len(data) < PAGE_SIZE:
            break
        page += 1
        time.sleep(RATE_SLEEP)

    return records


def ingest_ticker(conn, ticker: str, api_key: str) -> tuple[int, int]:
    """Fetch and upsert all trades for one ticker. Returns (inserted, skipped)."""
    records = fetch_ticker(ticker, api_key)
    inserted = skipped = 0
    for rec in records:
        if not rec["transaction_id"]:
            continue
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO congress_trades (
                source, transaction_id, politician_name, chamber, party, state,
                ticker, asset_description, transaction_type,
                transaction_date, disclosure_date,
                amount_min, amount_max, amount_label, raw_url
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            [
                rec["source"], rec["transaction_id"], rec["politician_name"],
                rec["chamber"], rec["party"], rec["state"],
                rec["ticker"], rec["asset_description"], rec["transaction_type"],
                rec["transaction_date"], rec["disclosure_date"],
                rec["amount_min"], rec["amount_max"], rec["amount_label"], rec["raw_url"],
            ],
        )
        if cur.rowcount:
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped


def backfill(limit: int | None = None, stale_days: int = 7) -> None:
    """Fetch congressional trades for every distinct ticker in the filings DB."""
    api_key = _get_api_key()
    conn = get_db()

    # All distinct tickers from insider filings
    tickers = [
        r[0] for r in conn.execute(
            "SELECT DISTINCT issuer_ticker FROM filings "
            "WHERE issuer_ticker IS NOT NULL AND issuer_ticker != '' "
            "ORDER BY issuer_ticker"
        ).fetchall()
    ]

    # Skip tickers already ingested recently
    fresh = {
        r[0] for r in conn.execute(
            "SELECT DISTINCT ticker FROM congress_trades "
            "WHERE source='ainvest' AND ingested_at >= datetime('now', ?)",
            [f"-{stale_days} days"],
        ).fetchall()
    }

    work = [t for t in tickers if t not in fresh and _VALID_TICKER.match(t)]
    if limit:
        work = work[:limit]

    total = len(work)
    print(f"[congress_ingest] Fetching for {total} tickers (skipping {len(fresh)} fresh) ...")

    all_inserted = all_skipped = 0
    for i, ticker in enumerate(work, 1):
        inserted, skipped = ingest_ticker(conn, ticker, api_key)
        if inserted or skipped:
            print(f"[{i}/{total}] {ticker} → {inserted} inserted, {skipped} skipped")
        conn.commit()
        time.sleep(RATE_SLEEP)

    print(f"[congress_ingest] Done. {all_inserted} inserted, {all_skipped} skipped total.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest congressional trades from AInvest")
    parser.add_argument("--ticker", help="Fetch for a single ticker")
    parser.add_argument("--limit", type=int, help="Max tickers to process in backfill")
    parser.add_argument("--stale-days", type=int, default=7,
                        help="Skip tickers ingested within this many days (default: 7)")
    args = parser.parse_args()

    if args.ticker:
        api_key = _get_api_key()
        conn = get_db()
        ins, skip = ingest_ticker(conn, args.ticker.upper(), api_key)
        conn.commit()
        print(f"{args.ticker}: {ins} inserted, {skip} skipped")
    else:
        backfill(limit=args.limit, stale_days=args.stale_days)
