"""Executive branch trade ingester — uses Open Cabinet public dataset.

Open Cabinet (https://open-cabinet.org) parses OGE Form 278-T filings for
senior executive branch officials (36 Cabinet / sub-Cabinet officials as of
June 2026) and publishes a free structured JSON dataset updated ~monthly.

Trades are stored in the same congress_trades table as AInvest data:
  source    = 'open_cabinet'
  chamber   = 'executive'
  party     = NULL  (not provided by Open Cabinet)
  state     = NULL  (not provided)

Open Cabinet provides no per-transaction IDs, so we generate a stable one:
  transaction_id = md5("{name}|{ticker_or_desc}|{date}|{type}")
This is deterministic — identical trades always hash to the same ID, enabling
ON CONFLICT DO NOTHING deduplication across weekly refreshes.

Usage:
    python exec_ingest.py            # full refresh from Open Cabinet
    python exec_ingest.py --dry-run  # parse and count without inserting
"""
from __future__ import annotations

import hashlib
import sys

import httpx

import alerts as alert_module
from congress_ingest import _parse_amount_range
from db import get_cli_db

OPEN_CABINET_URL = "https://open-cabinet.org/data/full-dataset.json"
OPEN_CABINET_PAGE = "https://open-cabinet.org/officials/{slug}"


def fetch_full_dataset() -> dict:
    """Download the Open Cabinet full JSON dataset. Raises on HTTP error."""
    try:
        resp = httpx.get(OPEN_CABINET_URL, timeout=30.0, follow_redirects=True)
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        print(f"[exec_ingest] ERROR fetching Open Cabinet dataset: {exc}", file=sys.stderr)
        raise


def _make_transaction_id(name: str, ticker_or_desc: str, date: str, tx_type: str) -> str:
    """Stable dedup key — md5 of pipe-joined fields."""
    raw = f"{name}|{ticker_or_desc}|{date}|{tx_type}"
    return hashlib.md5(raw.encode()).hexdigest()


def parse_transactions(officials: list[dict]) -> list[dict]:
    """Flatten Open Cabinet officials + transactions into congress_trades rows."""
    rows = []
    for official in officials:
        name     = (official.get("name") or "").strip()
        title    = (official.get("title") or "").strip()
        agency   = (official.get("agency") or "").strip()
        slug     = official.get("slug") or ""
        raw_url  = OPEN_CABINET_PAGE.format(slug=slug) if slug else None

        # Use mostRecentFilingDate as disclosure_date proxy — it's the
        # date OGE received the batch filing, not per-transaction.
        filing_date = (official.get("mostRecentFilingDate") or "")[:10] or None

        for tx in official.get("transactions") or []:
            ticker      = (tx.get("ticker") or "").strip().upper() or None
            description = (tx.get("description") or "").strip() or None
            tx_type     = (tx.get("type") or "").strip()      # "Purchase" / "Sale" / "Exchange"
            tx_date     = (tx.get("date") or "")[:10] or None
            amount_str  = (tx.get("amount") or "").strip() or None

            # Stable ID from most-identifying fields
            id_key = ticker or description or ""
            transaction_id = _make_transaction_id(name, id_key, tx_date or "", tx_type)

            amount_min, amount_max = _parse_amount_range(amount_str)

            # politician_name carries title + agency so it's searchable
            # from the congress tab without a separate column
            politician_name = f"{name} ({title}, {agency})" if title and agency else name

            rows.append({
                "source":           "open_cabinet",
                "transaction_id":   transaction_id,
                "politician_name":  politician_name,
                "chamber":          "executive",
                "party":            None,
                "state":            None,
                "ticker":           ticker,
                "asset_description": description,
                "transaction_type": tx_type,
                "transaction_date": tx_date,
                "disclosure_date":  filing_date,
                "amount_min":       amount_min,
                "amount_max":       amount_max,
                "amount_label":     amount_str,
                "raw_url":          raw_url,
            })

    return rows


def ingest_all(conn, rows: list[dict], dry_run: bool = False) -> tuple[int, int]:
    """Insert rows into congress_trades. Returns (inserted, skipped)."""
    inserted = skipped = 0
    for row in rows:
        if not row["transaction_id"]:
            continue
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


def main(dry_run: bool = False) -> None:
    print("[exec_ingest] Fetching Open Cabinet full dataset...")
    data      = fetch_full_dataset()
    officials = data.get("officials") or []
    exported  = data.get("exportedAt", "unknown")[:10]
    print(f"[exec_ingest] Dataset: {len(officials)} officials, "
          f"{data.get('transactionCount', '?')} transactions, exported {exported}")

    rows = parse_transactions(officials)
    print(f"[exec_ingest] Parsed {len(rows)} transaction rows")

    if dry_run:
        print("[exec_ingest] DRY RUN — no inserts. Sample row:")
        if rows:
            for k, v in rows[0].items():
                print(f"  {k}: {v!r}")
        return

    conn = get_cli_db()
    try:
        inserted, skipped = ingest_all(conn, rows)
        conn.commit()
        print(f"[exec_ingest] Done — {inserted} inserted, {skipped} skipped (already existed)")

        if inserted > 0:
            sent = alert_module.check_congress_alerts(conn)
            if sent:
                print(f"[exec_ingest] Sent {sent} congress watchlist alert(s).")
            sent = alert_module.check_congress_cobuy_alerts(conn)
            if sent:
                print(f"[exec_ingest] Sent {sent} co-buy alert(s).")
    finally:
        conn.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Ingest executive branch trades from Open Cabinet")
    parser.add_argument("--dry-run", action="store_true",
                        help="Parse and count rows without inserting")
    args = parser.parse_args()
    main(dry_run=args.dry_run)
