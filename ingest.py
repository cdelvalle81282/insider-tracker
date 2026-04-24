"""
SEC Form 4 ingester CLI.

Usage:
  python ingest.py --date today
  python ingest.py --date 2026-04-22
  python ingest.py --backfill 2024-01-01 2026-04-22
  python ingest.py --backfill-days 730
  python ingest.py --since-last-run
"""
from __future__ import annotations

import sqlite3
import time
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import click
import httpx

from config import DB_PATH, SEC_USER_AGENT, SEC_RATE_LIMIT, load_config
from parser import parse_form4
from tickers import lookup_ticker
import alerts as alert_module

EDGAR_BASE = "https://www.sec.gov"
RATE_SLEEP = 1.0 / SEC_RATE_LIMIT  # seconds between requests


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------

SCHEMA = """
CREATE TABLE IF NOT EXISTS filings (
  transaction_id       TEXT PRIMARY KEY,
  accession_no         TEXT NOT NULL,
  filed_at             TIMESTAMP NOT NULL,
  form_type            TEXT NOT NULL,

  issuer_cik           TEXT NOT NULL,
  issuer_name          TEXT NOT NULL,
  issuer_ticker        TEXT,

  insider_cik          TEXT NOT NULL,
  insider_name         TEXT NOT NULL,
  insider_title        TEXT,
  is_director          INTEGER NOT NULL DEFAULT 0,
  is_officer           INTEGER NOT NULL DEFAULT 0,
  is_ten_percent_owner INTEGER NOT NULL DEFAULT 0,
  is_other             INTEGER NOT NULL DEFAULT 0,

  transaction_date     DATE NOT NULL,
  transaction_code     TEXT NOT NULL,
  equity_swap          INTEGER NOT NULL DEFAULT 0,
  table_type           TEXT NOT NULL,

  shares               REAL NOT NULL DEFAULT 0,
  price_per_share      REAL,
  total_value          REAL,

  shares_owned_after   REAL,
  ownership_type       TEXT,

  is_10b5_1            INTEGER NOT NULL DEFAULT 0,
  footnote_text        TEXT,
  raw_xml_url          TEXT NOT NULL,
  ingested_at          TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_filed_at   ON filings(filed_at);
CREATE INDEX IF NOT EXISTS idx_accession  ON filings(accession_no);
CREATE INDEX IF NOT EXISTS idx_ticker     ON filings(issuer_ticker);
CREATE INDEX IF NOT EXISTS idx_insider    ON filings(insider_cik);
CREATE INDEX IF NOT EXISTS idx_tx_code    ON filings(transaction_code);
CREATE INDEX IF NOT EXISTS idx_issuer_cik ON filings(issuer_cik);
CREATE INDEX IF NOT EXISTS idx_tx_date    ON filings(transaction_date);

CREATE TABLE IF NOT EXISTS run_log (
  id              INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at      TIMESTAMP NOT NULL,
  finished_at     TIMESTAMP,
  date_processed  TEXT NOT NULL,
  filings_found   INTEGER DEFAULT 0,
  rows_inserted   INTEGER DEFAULT 0,
  errors          INTEGER DEFAULT 0,
  error_detail    TEXT
);

CREATE TABLE IF NOT EXISTS alerts_sent (
  id          INTEGER PRIMARY KEY AUTOINCREMENT,
  alert_key   TEXT NOT NULL UNIQUE,
  alert_type  TEXT NOT NULL,
  sent_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""


def _migrate(conn: sqlite3.Connection) -> None:
    """Idempotent schema migrations. Uses PRAGMA to check before ALTER TABLE."""
    cols = {r[1] for r in conn.execute("PRAGMA table_info(filings)")}
    if "superseded_by" not in cols:
        conn.execute("ALTER TABLE filings ADD COLUMN superseded_by TEXT")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_superseded ON filings(superseded_by)"
        )
        conn.commit()


def get_db(path: str | None = None) -> sqlite3.Connection:
    db_path = path or DB_PATH
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.executescript(SCHEMA)
    conn.commit()
    _migrate(conn)
    return conn


# ---------------------------------------------------------------------------
# EDGAR fetching
# ---------------------------------------------------------------------------

def _make_client() -> httpx.Client:
    return httpx.Client(
        headers={"User-Agent": SEC_USER_AGENT},
        timeout=30,
        follow_redirects=True,
    )


def _quarter(d: date) -> int:
    return (d.month - 1) // 3 + 1


def fetch_index_for_date(client: httpx.Client, target_date: date) -> list[dict]:
    """
    Fetch EDGAR full-index for the quarter containing target_date and filter
    to Form 4 / Form 4/A filings on that exact date.
    Returns list of dicts with keys: form_type, company, cik, date_filed, filename
    """
    year = target_date.year
    qtr = _quarter(target_date)
    url = f"{EDGAR_BASE}/Archives/edgar/full-index/{year}/QTR{qtr}/form.idx"

    time.sleep(RATE_SLEEP)
    resp = client.get(url)
    resp.raise_for_status()

    date_str = target_date.strftime("%Y-%m-%d")
    entries = []
    lines = resp.text.splitlines()
    in_data = False
    for line in lines:
        if line.startswith("---"):
            in_data = True
            continue
        if not in_data:
            continue
        if "edgar/data" not in line:
            continue

        # Anchor on "edgar/data" — column positions vary slightly across quarterly files
        file_col = line.index("edgar/data")
        filename = line[file_col:].strip()
        # Date is 12 chars before the filename, right-padded with spaces
        date_filed = line[file_col - 12:file_col].strip()
        # Remaining prefix: form_type (first token) + company + CIK (last token)
        prefix = line[:file_col - 12].strip()
        tokens = prefix.split()
        if len(tokens) < 2:
            continue
        form_type = tokens[0]
        cik = tokens[-1]
        company = " ".join(tokens[1:-1])

        if date_filed != date_str:
            continue
        if form_type not in ("4", "4/A"):
            continue

        entries.append({
            "form_type": form_type,
            "company": company,
            "cik": cik.zfill(10),
            "date_filed": date_filed,
            "filename": filename,
        })

    return entries


def fetch_xml_url(client: httpx.Client, filename: str) -> tuple[str, bytes]:
    """
    Given an index filename like edgar/data/123/0001234-26-000001.txt,
    fetch the filing index page to find the actual XML document URL,
    then fetch and return (xml_url, xml_bytes).
    """
    index_url = f"{EDGAR_BASE}/Archives/{filename}".replace(".txt", "-index.htm")
    time.sleep(RATE_SLEEP)
    resp = client.get(index_url)
    resp.raise_for_status()

    # Parse href attributes to find the raw XML doc (not XSLT-rendered, not XSD)
    xml_url = None
    for line in resp.text.splitlines():
        if 'href=' not in line or '.xml' not in line.lower():
            continue
        # Skip XSLT viewer links (contain xslF345 or similar)
        if 'xsl' in line.lower():
            continue
        # Extract href value
        for chunk in line.split('href="'):
            if not chunk.startswith('/'):
                continue
            href = chunk.split('"')[0]
            if href.lower().endswith('.xml') and 'xsd' not in href.lower():
                xml_url = f"{EDGAR_BASE}{href}"
                break
        if xml_url:
            break

    # Fallback: accession-number.xml inside the accession folder
    if xml_url is None:
        accession = filename.split("/")[-1].replace(".txt", "")
        accession_nodash = accession.replace("-", "")
        cik = filename.split("/")[2]
        xml_url = f"{EDGAR_BASE}/Archives/edgar/data/{cik}/{accession_nodash}/{accession}.xml"

    time.sleep(RATE_SLEEP)
    xml_resp = client.get(xml_url)
    xml_resp.raise_for_status()
    return xml_url, xml_resp.content


def accession_from_filename(filename: str) -> str:
    """Extract accession number like 0001234567-26-000001 from a filename path."""
    base = filename.split("/")[-1]
    return base.replace(".txt", "")


# ---------------------------------------------------------------------------
# Core ingest logic
# ---------------------------------------------------------------------------

def ingest_date(conn: sqlite3.Connection, target_date: date) -> tuple[int, int, int]:
    """
    Ingest all Form 4 filings for target_date.
    Returns (filings_found, rows_inserted, errors).
    """
    client = _make_client()
    entries = fetch_index_for_date(client, target_date)
    filings_found = len(entries)
    rows_inserted = 0
    errors = 0
    error_lines = []

    for entry in entries:
        accession_no = accession_from_filename(entry["filename"])
        try:
            xml_url, xml_bytes = fetch_xml_url(client, entry["filename"])
            rows = parse_form4(xml_bytes, accession_no, entry["date_filed"], xml_url)

            # Enrich ticker from EDGAR map if the XML didn't include one
            for row in rows:
                if not row.get("issuer_ticker"):
                    row["issuer_ticker"] = lookup_ticker(row["issuer_cik"])

            inserted = _upsert_rows(conn, rows)
            rows_inserted += inserted
        except Exception as e:
            errors += 1
            error_lines.append(f"{accession_no}: {e}")

    return filings_found, rows_inserted, errors, "; ".join(error_lines[-10:])


def _resolve_amendment(conn: sqlite3.Connection, row: dict) -> int:
    """
    If a newly inserted row is a Form 4/A amendment, find and mark the original
    row(s) it supersedes. Returns 1 if a row was superseded, 0 otherwise.
    Ambiguous matches (0 or 2+) are skipped to avoid mis-attribution.
    """
    if row.get("form_type") != "4/A":
        return 0

    candidates = conn.execute(
        """
        SELECT transaction_id FROM filings
        WHERE issuer_cik = ?
          AND insider_cik = ?
          AND transaction_date = ?
          AND transaction_code = ?
          AND shares = ?
          AND form_type = '4'
          AND superseded_by IS NULL
        """,
        [
            row["issuer_cik"],
            row["insider_cik"],
            row["transaction_date"],
            row["transaction_code"],
            row["shares"],
        ],
    ).fetchall()

    if len(candidates) == 1:
        cur = conn.execute(
            "UPDATE filings SET superseded_by = ? WHERE transaction_id = ?",
            [row["accession_no"], candidates[0][0]],
        )
        return cur.rowcount
    return 0


def _upsert_rows(conn: sqlite3.Connection, rows: list[dict]) -> int:
    """Insert rows, skipping duplicates. Returns count of new rows inserted."""
    inserted = 0
    for row in rows:
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO filings (
                  transaction_id, accession_no, filed_at, form_type,
                  issuer_cik, issuer_name, issuer_ticker,
                  insider_cik, insider_name, insider_title,
                  is_director, is_officer, is_ten_percent_owner, is_other,
                  transaction_date, transaction_code, equity_swap, table_type,
                  shares, price_per_share, total_value,
                  shares_owned_after, ownership_type,
                  is_10b5_1, footnote_text, raw_xml_url
                ) VALUES (
                  :transaction_id, :accession_no, :filed_at, :form_type,
                  :issuer_cik, :issuer_name, :issuer_ticker,
                  :insider_cik, :insider_name, :insider_title,
                  :is_director, :is_officer, :is_ten_percent_owner, :is_other,
                  :transaction_date, :transaction_code, :equity_swap, :table_type,
                  :shares, :price_per_share, :total_value,
                  :shares_owned_after, :ownership_type,
                  :is_10b5_1, :footnote_text, :raw_xml_url
                )
                """,
                row,
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                inserted += 1
                _resolve_amendment(conn, row)
        except sqlite3.Error:
            pass
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--date", "target_date", default=None, help="Date to ingest (YYYY-MM-DD or 'today')")
@click.option("--backfill", nargs=2, default=None, metavar="START END", help="Date range YYYY-MM-DD YYYY-MM-DD")
@click.option("--backfill-days", default=None, type=int, help="Ingest last N days")
@click.option("--since-last-run", is_flag=True, default=False, help="Ingest only since the most recent filing in DB")
@click.option("--resolve-amendments", is_flag=True, default=False, help="Backfill amendment resolution for all existing 4/A rows")
def main(target_date, backfill, backfill_days, since_last_run, resolve_amendments):
    conn = get_db()
    config = load_config()

    # Backfills suppress alerts — only real-time runs fire Slack
    suppress_alerts = bool(backfill or backfill_days)

    # Record run start for alert since_ts window
    run_started_at = datetime.now(timezone.utc).isoformat()

    if resolve_amendments:
        click.echo("Resolving amendments in existing data ...", nl=False)
        amendments = conn.execute(
            "SELECT transaction_id, issuer_cik, insider_cik, transaction_date, "
            "transaction_code, shares, accession_no, form_type "
            "FROM filings WHERE form_type = '4/A'"
        ).fetchall()
        resolved = sum(_resolve_amendment(conn, dict(row)) for row in amendments)
        conn.commit()
        click.echo(f" {len(amendments)} amendments processed, {resolved} rows superseded")
        return

    dates: list[date] = []

    if since_last_run:
        row = conn.execute("SELECT MAX(DATE(filed_at)) FROM filings").fetchone()
        last = row[0]
        if last:
            start = date.fromisoformat(last)
            end = date.today()
            d = start
            while d <= end:
                dates.append(d)
                d += timedelta(days=1)
        else:
            dates = [date.today()]

    elif backfill:
        start = date.fromisoformat(backfill[0])
        end = date.fromisoformat(backfill[1])
        d = start
        while d <= end:
            dates.append(d)
            d += timedelta(days=1)

    elif backfill_days:
        end = date.today()
        start = end - timedelta(days=backfill_days)
        d = start
        while d <= end:
            dates.append(d)
            d += timedelta(days=1)

    elif target_date:
        if target_date.lower() == "today":
            dates = [date.today()]
        else:
            dates = [date.fromisoformat(target_date)]
    else:
        dates = [date.today()]

    for d in dates:
        # Skip weekends — EDGAR has no filings
        if d.weekday() >= 5:
            continue

        started_at = datetime.now(timezone.utc).isoformat()
        click.echo(f"Ingesting {d} ...", nl=False)

        try:
            found, inserted, errors, error_detail = ingest_date(conn, d)
            finished_at = datetime.now(timezone.utc).isoformat()
            conn.execute(
                """INSERT INTO run_log (started_at, finished_at, date_processed,
                   filings_found, rows_inserted, errors, error_detail)
                   VALUES (?, ?, ?, ?, ?, ?, ?)""",
                (started_at, finished_at, d.isoformat(), found, inserted, errors, error_detail or None),
            )
            conn.commit()
            click.echo(f" {found} filings, {inserted} rows inserted, {errors} errors")
        except Exception as e:
            finished_at = datetime.now(timezone.utc).isoformat()
            detail = traceback.format_exc()
            conn.execute(
                """INSERT INTO run_log (started_at, finished_at, date_processed,
                   filings_found, rows_inserted, errors, error_detail)
                   VALUES (?, ?, ?, 0, 0, 1, ?)""",
                (started_at, finished_at, d.isoformat(), str(e)),
            )
            conn.commit()
            click.echo(f" ERROR: {e}")

    # Fire Slack alerts for newly ingested rows (real-time runs only)
    if not suppress_alerts:
        try:
            n = alert_module.check_and_send(
                conn, config, since_ts=run_started_at, suppress=False
            )
            if n:
                click.echo(f"Sent {n} Slack alert(s)")
        except Exception as e:
            click.echo(f"Alert error (non-fatal): {e}")


if __name__ == "__main__":
    main()
