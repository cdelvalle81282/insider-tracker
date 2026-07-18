"""Trump 278-T ingester — uses corrected CSV from trump-portfolio-tracker on GitHub.

The GitHub repo (HerringtonDarkholme/trump-portfolio-tracker) rebuilt Trump's OGE Form
278-T filing using AI vision extraction, fixing OCR errors in 47% of dates and 663
description corruptions. It also ships a ticker-seed.json with 1,028 company→ticker
mappings covering every holding in the filing.

This script:
  1. Fetches the corrected CSV + ticker-seed from GitHub
  2. Resolves company descriptions → tickers via alias prefix matching
  3. Deletes stale Open Cabinet rows for Trump (wrong dates, no tickers)
  4. Inserts clean rows with real per-transaction dates into congress_trades
  5. Fires co-buy alerts for any new ticker overlaps with corporate insiders

Usage:
    python trump_ingest.py            # full refresh
    python trump_ingest.py --dry-run  # parse and report match stats without inserting
"""
from __future__ import annotations

import csv
import hashlib
import io
import json
import sys
from datetime import datetime

import httpx

import alerts as alert_module
from congress_ingest import _parse_amount_range
from db import get_cli_db

# Pinned to a specific commit (not `main`) — this data lands directly in the public
# dashboard via congress_trades, and main is an unauthenticated third-party branch
# with no integrity check otherwise. To pick up an upstream correction, review the
# new commit's diff for trump_278T.csv/data/ticker-seed.json, then bump this SHA.
PINNED_SHA = "5c439b55138713ba3d9f93688e1e688d5341ccab"

CSV_URL = (
    "https://raw.githubusercontent.com/HerringtonDarkholme"
    f"/trump-portfolio-tracker/{PINNED_SHA}/trump_278T.csv"
)
TICKER_SEED_URL = (
    "https://raw.githubusercontent.com/HerringtonDarkholme"
    f"/trump-portfolio-tracker/{PINNED_SHA}/data/ticker-seed.json"
)

FILING_DATE     = "2026-05-08"   # OGE receipt date for this batch filing
POLITICIAN_NAME = "Donald J. Trump"
SOURCE          = "trump_278t"
CHAMBER         = "executive"


# ---------------------------------------------------------------------------
# Fetch
# ---------------------------------------------------------------------------

def _get(url: str) -> bytes:
    try:
        resp = httpx.get(url, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        return resp.content
    except Exception as exc:
        print(f"[trump_ingest] ERROR fetching {url}: {exc}", file=sys.stderr)
        raise


# ---------------------------------------------------------------------------
# Ticker resolution
# ---------------------------------------------------------------------------

def _build_ticker_lookup(seed: dict) -> dict[str, str]:
    """Invert ticker-seed: lowercase alias/name → ticker."""
    lookup: dict[str, str] = {}
    for ticker, info in seed.items():
        name = (info.get("name") or "").lower().strip()
        if name:
            lookup[name] = ticker
        for alias in info.get("aliases") or []:
            a = alias.lower().strip()
            if a:
                lookup[a] = ticker
    return lookup


def _resolve_ticker(description: str, lookup: dict[str, str]) -> str | None:
    """
    Match a 278-T description to a ticker via:
      1. Exact match on lowercased description
      2. Prefix match — find all aliases that are a prefix of the description,
         take the longest (most specific) to avoid false positives on short aliases.
    """
    desc = description.lower().strip()
    if desc in lookup:
        return lookup[desc]

    # All aliases that are a prefix of this description
    matches = [(alias, t) for alias, t in lookup.items() if desc.startswith(alias)]
    if matches:
        matches.sort(key=lambda x: len(x[0]), reverse=True)
        return matches[0][1]

    return None


# ---------------------------------------------------------------------------
# Date parsing
# ---------------------------------------------------------------------------

def _parse_date(s: str) -> str | None:
    """Parse MM/DD/YYYY → ISO YYYY-MM-DD. Correct OCR year errors (e.g. 2028→2026)."""
    try:
        dt = datetime.strptime(s.strip(), "%m/%d/%Y")
        if dt.year > 2026:          # OCR misread 6 as 8; all trades are from ≤2026
            dt = dt.replace(year=2026)
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Parse
# ---------------------------------------------------------------------------

def parse_csv(raw_csv: bytes, ticker_lookup: dict[str, str]) -> tuple[list[dict], dict]:
    """Parse trump_278T.csv into congress_trades rows. Returns (rows, stats)."""
    rows = []
    stats = {"total": 0, "ticker_resolved": 0, "no_ticker": 0, "bad_date": 0}

    reader = csv.DictReader(io.TextIOWrapper(io.BytesIO(raw_csv), encoding="utf-8"))
    for record in reader:
        stats["total"] += 1

        description = (record.get("Description") or "").strip()
        tx_type     = (record.get("Type") or "").strip()
        date_str    = (record.get("Date") or "").strip()
        amount_str  = (record.get("Amount") or "").strip()

        tx_date = _parse_date(date_str)
        if not tx_date:
            stats["bad_date"] += 1
            continue

        ticker = _resolve_ticker(description, ticker_lookup)
        if ticker:
            stats["ticker_resolved"] += 1
        else:
            stats["no_ticker"] += 1

        amount_min, amount_max = _parse_amount_range(amount_str)

        raw = f"{description}|{tx_date}|{tx_type.lower()}"
        transaction_id = hashlib.md5(f"trump|{raw}".encode()).hexdigest()

        rows.append({
            "source":           SOURCE,
            "transaction_id":   transaction_id,
            "politician_name":  POLITICIAN_NAME,
            "chamber":          CHAMBER,
            "party":            "Republican",
            "state":            None,
            "ticker":           ticker,
            "asset_description": description,
            "transaction_type": tx_type,
            "transaction_date": tx_date,
            "disclosure_date":  FILING_DATE,
            "amount_min":       amount_min,
            "amount_max":       amount_max,
            "amount_label":     amount_str,
            "raw_url":          "https://open-cabinet.org/officials/donald-j-trump",
        })

    return rows, stats


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------

def ingest(conn, rows: list[dict], dry_run: bool = False) -> tuple[int, int]:
    inserted = skipped = 0
    for row in rows:
        if dry_run:
            inserted += 1
            continue
        cur = conn.execute(
            """
            INSERT INTO congress_trades (
                source, transaction_id, politician_name, chamber, party, state,
                ticker, asset_description, transaction_type,
                transaction_date, disclosure_date,
                amount_min, amount_max, amount_label, raw_url
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT DO NOTHING
            """,
            [
                row["source"], row["transaction_id"], row["politician_name"],
                row["chamber"], row["party"], row["state"],
                row["ticker"], row["asset_description"], row["transaction_type"],
                row["transaction_date"], row["disclosure_date"],
                row["amount_min"], row["amount_max"], row["amount_label"], row["raw_url"],
            ],
        )
        if cur.rowcount:
            inserted += 1
        else:
            skipped += 1
    return inserted, skipped


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(dry_run: bool = False) -> None:
    print("[trump_ingest] Fetching CSV + ticker-seed from GitHub...")
    raw_csv    = _get(CSV_URL)
    seed       = json.loads(_get(TICKER_SEED_URL))
    ticker_lookup = _build_ticker_lookup(seed)
    print(f"[trump_ingest] Ticker lookup: {len(ticker_lookup):,} entries from {len(seed):,} tickers")

    rows, stats = parse_csv(raw_csv, ticker_lookup)
    resolved_pct = stats["ticker_resolved"] / max(stats["total"], 1) * 100
    print(
        f"[trump_ingest] Parsed {stats['total']:,} rows — "
        f"ticker resolved: {stats['ticker_resolved']:,} ({resolved_pct:.0f}%), "
        f"no ticker: {stats['no_ticker']:,}, "
        f"bad date: {stats['bad_date']:,}"
    )

    if dry_run:
        print("[trump_ingest] DRY RUN — no DB changes. Sample rows:")
        for r in rows[:5]:
            print(f"  {r['ticker'] or '—':6}  {r['transaction_date']}  {r['transaction_type']:10}  {r['amount_label']:25}  {r['asset_description'][:50]}")
        # Top unresolved
        unresolved = [r["asset_description"] for r in rows if not r["ticker"]]
        if unresolved:
            print("\n  Unresolved descriptions (first 10):")
            for d in unresolved[:10]:
                print(f"    {d[:70]}")
        return

    conn = get_cli_db()
    try:
        # Remove stale Open Cabinet rows — wrong dates, no tickers, useless
        deleted = conn.execute(
            "DELETE FROM congress_trades WHERE source = 'open_cabinet' "
            "AND politician_name ILIKE %s",
            ["%trump%"],
        ).rowcount
        conn.commit()
        if deleted:
            print(f"[trump_ingest] Removed {deleted:,} stale Open Cabinet Trump rows")

        inserted, skipped = ingest(conn, rows)
        conn.commit()
        print(f"[trump_ingest] Done — {inserted:,} inserted, {skipped:,} skipped (already existed)")

        if inserted > 0:
            sent = alert_module.check_congress_cobuy_alerts(conn)
            if sent:
                print(f"[trump_ingest] Sent {sent} co-buy alert(s).")
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest Trump 278-T from GitHub")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and report without inserting")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
