"""
SEC Form 4 ingester CLI.

Usage:
  python ingest.py --date today
  python ingest.py --date 2026-04-22
  python ingest.py --backfill 2024-01-01 2026-04-22
  python ingest.py --backfill-days 730
  python ingest.py --since-last-run
  python ingest.py --mark-joint-filers     # one-time backfill: dedup joint-filer pairs
  python ingest.py --update-prices
"""
from __future__ import annotations

import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import click
import httpx
import psycopg

import alerts as alert_module
import polygon_client
import queries
import sector as sector_module
from config import POLYGON_API_KEY, SEC_RATE_LIMIT, SEC_USER_AGENT, load_config
from db import get_cli_db
from parser import normalize_ticker, parse_form4
from tickers import lookup_ticker

EDGAR_BASE = "https://www.sec.gov"
RATE_SLEEP = 1.0 / SEC_RATE_LIMIT  # seconds between requests

INGEST_SENTINEL = Path(__file__).parent / "data" / ".last_ingest"


def _write_sentinel() -> None:
    """Touch sentinel so app workers know DB was updated and should invalidate cache."""
    try:
        INGEST_SENTINEL.touch()
    except Exception:
        pass


def _ping_heartbeat(url: str | None) -> None:
    """Ping an UptimeRobot (or similar) heartbeat URL. Failure is silently swallowed."""
    if not url:
        return
    try:
        import urllib.request
        urllib.request.urlopen(url, timeout=10)
    except Exception:
        pass  # heartbeat failure must never break ingest


# ---------------------------------------------------------------------------
# Database setup
# ---------------------------------------------------------------------------
#
# Schema is now managed by Alembic — see migrations/versions/0001_initial_schema.py.
# The SQLite SCHEMA string and _migrate() that used to live here have been removed.
# The legacy SQLite DDL is preserved below as a comment for historical reference
# only — it is NOT used to create tables anymore.
#
# Legacy SQLite schema (do not use; superseded by Alembic):
#
#     CREATE TABLE IF NOT EXISTS filings (
#       transaction_id TEXT PRIMARY KEY, ... )
#     CREATE TABLE IF NOT EXISTS run_log (
#       id INTEGER PRIMARY KEY AUTOINCREMENT, ... )
#     -- etc.  See git history for full SQLite definition.
#
# ---------------------------------------------------------------------------


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
    Fetch EDGAR index for target_date and return Form 4 / Form 4/A filings.
    Tries the daily index first (updated same-day); falls back to the quarterly
    full-index (updated with a multi-day lag) for older dates or if daily 404s.
    Returns list of dicts with keys: form_type, company, cik, date_filed, filename
    """
    year = target_date.year
    qtr = _quarter(target_date)

    # Daily index: updated same day, needed for dates within the last ~2 weeks
    daily_url = (
        f"{EDGAR_BASE}/Archives/edgar/daily-index/{year}/QTR{qtr}"
        f"/form.{target_date.strftime('%Y%m%d')}.idx"
    )
    quarterly_url = f"{EDGAR_BASE}/Archives/edgar/full-index/{year}/QTR{qtr}/form.idx"

    time.sleep(RATE_SLEEP)
    resp = client.get(daily_url)
    if resp.status_code != 200:
        # Daily index not available (404, 403, etc.) — fall back to quarterly
        resp = client.get(quarterly_url)
    resp.raise_for_status()

    # SEC returns 200 with an HTML rate-limit page instead of plain text when
    # the server IP exceeds the request threshold. Detect and raise so the
    # error is recorded in run_log rather than silently returning 0 filings.
    if resp.text.lstrip().startswith("<"):
        raise RuntimeError(
            f"SEC returned HTML instead of plain-text index (rate-limited?): {resp.url}"
        )

    # Daily index uses YYYYMMDD; quarterly uses YYYY-MM-DD — accept both
    date_str = target_date.strftime("%Y-%m-%d")
    date_str_compact = target_date.strftime("%Y%m%d")
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

        if date_filed not in (date_str, date_str_compact):
            continue
        if form_type not in ("4", "4/A"):
            continue

        # Normalize compact YYYYMMDD → YYYY-MM-DD so filed_at is always ISO format
        if date_filed == date_str_compact:
            date_filed = date_str

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

def ingest_date(conn: psycopg.Connection, target_date: date) -> tuple[int, int, int, str]:
    """
    Ingest all Form 4 filings for target_date.
    Returns (filings_found, rows_inserted, errors, error_detail).
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

            # Enrich sector for this issuer (session-cached, one EDGAR call per CIK per run)
            if rows:
                cik = rows[0]["issuer_cik"]
                try:
                    sec = sector_module.get_or_fetch_sector(conn, cik)
                    if sec:
                        conn.execute(
                            "UPDATE filings SET sector=%s WHERE issuer_cik=%s AND sector IS NULL",
                            [sec, cik],
                        )
                        conn.commit()
                except Exception:
                    pass  # sector enrichment failure must never break ingest
        except Exception as e:
            errors += 1
            error_lines.append(f"{accession_no}: {e}")

    return filings_found, rows_inserted, errors, "; ".join(error_lines[-10:])


def _resolve_amendment(conn: psycopg.Connection, row: dict) -> int:
    """
    If a newly inserted row is a Form 4/A amendment, find and mark the original
    row(s) it supersedes. Returns 1 if a row was superseded, 0 otherwise.
    Ambiguous matches (0 or 2+) are skipped to avoid mis-attribution.
    """
    if row.get("form_type") != "4/A":
        return 0

    base_params = [
        row["issuer_cik"],
        row["insider_cik"],
        row["transaction_date"],
        row["transaction_code"],
    ]
    # Pass 1: exact share match — handles unchanged rows in a multi-row 4/A
    candidates = conn.execute(
        """
        SELECT transaction_id FROM filings
        WHERE issuer_cik = %s AND insider_cik = %s AND transaction_date = %s
          AND transaction_code = %s AND shares = %s AND form_type = '4'
          AND superseded_by IS NULL
        """,
        [*base_params, row["shares"]],
    ).fetchall()

    if len(candidates) != 1:
        # Pass 2: share count may have been corrected in this 4/A
        candidates = conn.execute(
            """
            SELECT transaction_id FROM filings
            WHERE issuer_cik = %s AND insider_cik = %s AND transaction_date = %s
              AND transaction_code = %s AND form_type = '4'
              AND superseded_by IS NULL
            """,
            base_params,
        ).fetchall()

    if len(candidates) == 1:
        cur = conn.execute(
            "UPDATE filings SET superseded_by = %s WHERE transaction_id = %s",
            [row["accession_no"], candidates[0]["transaction_id"]],
        )
        return cur.rowcount
    return 0


def _upsert_rows(conn: psycopg.Connection, rows: list[dict]) -> int:
    """Insert rows, skipping duplicates. Returns count of new rows inserted.

    psycopg3 named-parameter placeholders use `%(name)s`, not SQLite's `:name`.

    Each row is wrapped in its own `conn.transaction()` block so a DB error
    on one row does not abort the whole batch — PG aborts the entire
    transaction on any error, unlike SQLite which keeps going. When called
    at the top level this is a per-row BEGIN/COMMIT; when called inside an
    existing transaction it becomes a SAVEPOINT. Either way, a failing row
    rolls back only that row.

    `ON CONFLICT DO NOTHING` already handles duplicate-key cases without
    raising, so this guard mostly protects against genuinely malformed
    rows (e.g. NULL violations from a schema drift).
    """
    inserted = 0
    for row in rows:
        try:
            with conn.transaction():  # per-row tx — failures don't cascade across the batch
                cur = conn.execute(
                    """
                    INSERT INTO filings (
                      transaction_id, accession_no, filed_at, form_type,
                      issuer_cik, issuer_name, issuer_ticker,
                      insider_cik, insider_name, insider_title,
                      is_director, is_officer, is_ten_percent_owner, is_other,
                      transaction_date, transaction_code, equity_swap, table_type,
                      shares, price_per_share, total_value,
                      shares_owned_after, ownership_type,
                      is_10b5_1, footnote_text, raw_xml_url
                    ) VALUES (
                      %(transaction_id)s, %(accession_no)s, %(filed_at)s, %(form_type)s,
                      %(issuer_cik)s, %(issuer_name)s, %(issuer_ticker)s,
                      %(insider_cik)s, %(insider_name)s, %(insider_title)s,
                      %(is_director)s, %(is_officer)s, %(is_ten_percent_owner)s, %(is_other)s,
                      %(transaction_date)s, %(transaction_code)s, %(equity_swap)s, %(table_type)s,
                      %(shares)s, %(price_per_share)s, %(total_value)s,
                      %(shares_owned_after)s, %(ownership_type)s,
                      %(is_10b5_1)s, %(footnote_text)s, %(raw_xml_url)s
                    )
                    ON CONFLICT DO NOTHING
                    """,
                    row,
                )
                if cur.rowcount:
                    inserted += 1
                    _resolve_amendment(conn, row)
        except psycopg.Error:
            # per-row transaction auto-rolled back; preserve already-inserted rows.
            pass
    # `conn.transaction()` already committed each row; this commit is a
    # belt-and-suspenders no-op for the case where psycopg's autocommit
    # state has been left open by some caller.
    conn.commit()
    return inserted


# ---------------------------------------------------------------------------
# Joint-filer deduplication
# ---------------------------------------------------------------------------

def mark_joint_filers(conn: psycopg.Connection) -> int:
    """
    Detect joint-filer duplicate transactions and mark secondary rows.

    When a person and their controlled entity (fund/LLC/trust) file separate
    Form 4s for the same economic transaction, each gets its own accession
    number but identical (issuer_ticker, transaction_date, transaction_code,
    shares, total_value, table_type).  We keep the earliest-filed row as
    primary, mark the rest with joint_filer_of = primary_transaction_id, and
    update the primary's insider_name to "Name A / Name B".

    Returns the number of rows newly marked.
    """
    # CHR(31)/CHR(30) = ASCII unit/record separators — safe in EDGAR names.
    # PG: STRING_AGG (vs SQLite GROUP_CONCAT); CHR() (vs SQLite char()).
    # STRING_AGG ordering is not guaranteed without an ORDER BY clause, but
    # Python re-sorts by filed_at after splitting, so source order doesn't matter.
    groups = conn.execute("""
        SELECT STRING_AGG(
                   transaction_id || CHR(31) || insider_name || CHR(31) || filed_at::text,
                   CHR(30)
               ) AS row_data
        FROM filings
        WHERE superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND transaction_code IN ('P', 'S')
          AND total_value IS NOT NULL
          AND shares > 0
        GROUP BY issuer_cik, transaction_date, transaction_code,
                 shares, total_value, table_type
        HAVING COUNT(DISTINCT insider_cik) > 1
    """).fetchall()

    marked = 0
    for group in groups:
        items = sorted(
            [item.split(chr(31)) for item in group["row_data"].split(chr(30))],
            key=lambda x: x[2],  # sort by filed_at
        )
        primary_id, primary_name = items[0][0], items[0][1]
        secondary_ids = [item[0] for item in items[1:]]
        combined = " / ".join(dict.fromkeys(item[1] for item in items))

        if combined != primary_name:
            conn.execute(
                "UPDATE filings SET insider_name = %s WHERE transaction_id = %s",
                [combined, primary_id],
            )

        if secondary_ids:
            placeholders = ",".join(["%s"] * len(secondary_ids))
            cur = conn.execute(
                f"UPDATE filings SET joint_filer_of = %s "
                f"WHERE transaction_id IN ({placeholders}) AND joint_filer_of IS NULL",
                [primary_id, *secondary_ids],
            )
            marked += cur.rowcount

    conn.commit()
    return marked


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

@click.command()
@click.option("--date", "target_date", default=None, help="Date to ingest (YYYY-MM-DD or 'today')")
@click.option("--backfill", nargs=2, default=None, metavar="START END", help="Date range YYYY-MM-DD YYYY-MM-DD")
@click.option("--backfill-days", default=None, type=int, help="Ingest last N days")
@click.option("--since-last-run", is_flag=True, default=False, help="Ingest only since the most recent filing in DB")
@click.option("--resolve-amendments", is_flag=True, default=False, help="Backfill amendment resolution for all existing 4/A rows")
@click.option("--backfill-sectors", is_flag=True, default=False, help="Fetch missing sector labels for all issuers in DB")
@click.option("--mark-joint-filers", "do_joint_filers", is_flag=True, default=False, help="Detect and deduplicate joint-filer Form 4 duplicates")
@click.option("--backfill-metadata", "do_backfill_metadata", is_flag=True, default=False, help="Fetch Polygon.io market cap and options flag for all tickers")
@click.option("--limit", "metadata_limit", default=None, type=int, help="Max tickers to process in --backfill-metadata")
@click.option("--stale-days", "metadata_stale_days", default=30, type=int, show_default=True, help="Re-fetch metadata older than N days")
@click.option("--update-prices", "do_update_prices", is_flag=True, default=False,
              help="Fetch latest close for tickers in ticker_metadata where last_close_at < today or NULL")
@click.option("--normalize-tickers", "do_normalize_tickers", is_flag=True, default=False,
              help="Clean malformed issuer_ticker values in existing rows (NONE→NULL, NYSE:X→X, etc.)")
def main(target_date, backfill, backfill_days, since_last_run, resolve_amendments, backfill_sectors, do_joint_filers, do_backfill_metadata, metadata_limit, metadata_stale_days, do_update_prices, do_normalize_tickers):
    conn = get_cli_db()
    try:
        config = load_config()

        # Backfills suppress alerts — only real-time runs fire Slack
        suppress_alerts = bool(backfill or backfill_days)

        # Record run start for alert since_ts window
        run_started_at = datetime.now(timezone.utc).isoformat()

        if do_joint_filers:
            click.echo("Marking joint-filer duplicates ...", nl=False)
            n = mark_joint_filers(conn)
            click.echo(f" {n} rows marked")
            _write_sentinel()
            return

        if do_backfill_metadata:
            api_key = POLYGON_API_KEY
            if not api_key:
                click.echo("Error: POLYGON_API_KEY is not set. Export the environment variable and retry.")
                return

            # All distinct tickers in the filings table
            all_tickers: list[str] = [
                r["issuer_ticker"] for r in conn.execute(
                    "SELECT DISTINCT issuer_ticker FROM filings"
                    " WHERE issuer_ticker IS NOT NULL AND issuer_ticker != ''"
                ).fetchall()
            ]

            # Skip tickers whose metadata is already fresh.
            # PG: fetched_at is TEXT, so compare against an ISO timestamp string.
            fresh_cutoff = (
                datetime.now(timezone.utc) - timedelta(days=metadata_stale_days)
            ).isoformat()
            fresh: set[str] = {
                r["ticker"] for r in conn.execute(
                    "SELECT ticker FROM ticker_metadata WHERE fetched_at > %s",
                    [fresh_cutoff],
                ).fetchall()
            }
            work_list = [t for t in all_tickers if t not in fresh]

            if metadata_limit is not None:
                work_list = work_list[:metadata_limit]

            total = len(work_list)
            click.echo(
                f"Fetching Polygon metadata for {total} tickers"
                f" (stale_days={metadata_stale_days}, limit={metadata_limit}) ..."
            )

            fetched = 0
            skipped = 0
            for i, ticker in enumerate(work_list, start=1):
                result = polygon_client.fetch_ticker_metadata(ticker, api_key)
                if result is not None:
                    queries.upsert_ticker_metadata(
                        conn, ticker, result["market_cap"], result["has_options"]
                    )
                    fetched += 1
                    click.echo(
                        f"[{i}/{total}] {ticker} → "
                        f"market_cap={result['market_cap']}, "
                        f"has_options={result['has_options']}"
                    )
                else:
                    skipped += 1
                    click.echo(f"[{i}/{total}] {ticker} → no data (skipped)")

                if fetched % 10 == 0 and fetched > 0:
                    conn.commit()

                if i < total:
                    time.sleep(12)  # Polygon free tier: 5 req/min per endpoint

            conn.commit()
            click.echo(f"Done — {fetched} upserted, {skipped} skipped (no data)")
            _write_sentinel()
            return

        if do_update_prices:
            if not POLYGON_API_KEY:
                click.echo("Error: POLYGON_API_KEY is not set.", err=True)
                return
            today_iso = date.today().isoformat()
            work = [
                r["ticker"] for r in conn.execute(
                    "SELECT ticker FROM ticker_metadata WHERE last_close_at IS NULL OR last_close_at < %s",
                    [today_iso],
                ).fetchall()
            ]
            total = len(work)
            click.echo(f"Updating last_close for {total} tickers ...")
            updated = 0
            for i, ticker in enumerate(work, start=1):
                close = polygon_client.fetch_latest_close(ticker, POLYGON_API_KEY)
                if close is not None:
                    conn.execute(
                        "UPDATE ticker_metadata SET last_close=%s, last_close_at=%s WHERE ticker=%s",
                        [close, today_iso, ticker],
                    )
                    updated += 1
                    click.echo(f"[{i}/{total}] {ticker} → {close}")
                else:
                    click.echo(f"[{i}/{total}] {ticker} → no data")
                if i % 10 == 0:
                    conn.commit()
                if i < total:
                    time.sleep(12)  # Polygon free tier: ~5 req/min
            conn.commit()
            click.echo(f"Done. Updated {updated}/{total} tickers.")
            _write_sentinel()
            _ping_heartbeat(os.getenv("PRICES_HEARTBEAT_URL"))
            return

        if do_normalize_tickers:
            rows = conn.execute(
                "SELECT DISTINCT issuer_cik, issuer_ticker FROM filings WHERE issuer_ticker IS NOT NULL"
            ).fetchall()
            click.echo(f"Checking {len(rows)} distinct (issuer_cik, ticker) pairs ...")
            updated = 0
            for row in rows:
                original = row["issuer_ticker"]
                normalized = normalize_ticker(original)
                if normalized != original:
                    conn.execute(
                        "UPDATE filings SET issuer_ticker = %s WHERE issuer_cik = %s AND issuer_ticker = %s",
                        [normalized, row["issuer_cik"], original],
                    )
                    updated += 1
                    click.echo(f"  {original!r} → {normalized!r} (issuer_cik={row['issuer_cik']})")
            if updated:
                conn.commit()
            click.echo(f"Done. Normalized {updated} distinct ticker values.")
            _write_sentinel()
            return


        if backfill_sectors:
            ciks = [r["issuer_cik"] for r in conn.execute(
                "SELECT DISTINCT issuer_cik FROM filings WHERE sector IS NULL"
            ).fetchall()]
            click.echo(f"Fetching sectors for {len(ciks)} issuers ...")
            done = 0
            for cik in ciks:
                try:
                    sector_module.get_or_fetch_sector(conn, cik)
                    conn.execute(
                        "UPDATE filings SET sector=(SELECT sector FROM sectors WHERE issuer_cik=%s)"
                        " WHERE issuer_cik=%s AND sector IS NULL",
                        [cik, cik],
                    )
                    conn.commit()
                    done += 1
                except Exception:
                    pass
            click.echo(f"Done — enriched {done}/{len(ciks)} issuers")
            _write_sentinel()
            return

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
            _write_sentinel()
            return

        dates: list[date] = []

        if since_last_run:
            row = conn.execute("SELECT MAX(filed_at::date) AS d FROM filings").fetchone()
            last = row["d"] if row else None
            if last:
                start = last if isinstance(last, date) else date.fromisoformat(str(last))
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

        # Determine run_kind once for this invocation
        if since_last_run:
            _run_kind = "nightly"
        elif backfill or backfill_days:
            _run_kind = "backfill"
        elif target_date:
            _run_kind = "intraday"
        else:
            _run_kind = "intraday"

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
                       filings_found, rows_inserted, errors, error_detail, run_kind)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (started_at, finished_at, d.isoformat(), found, inserted, errors, error_detail or None, _run_kind),
                )
                conn.commit()
                click.echo(f" {found} filings, {inserted} rows inserted, {errors} errors")
            except Exception as e:
                # Aborted txns must be rolled back before the next statement.
                conn.rollback()
                finished_at = datetime.now(timezone.utc).isoformat()
                conn.execute(
                    """INSERT INTO run_log (started_at, finished_at, date_processed,
                       filings_found, rows_inserted, errors, error_detail, run_kind)
                       VALUES (%s, %s, %s, 0, 0, 1, %s, %s)""",
                    (started_at, finished_at, d.isoformat(), str(e), _run_kind),
                )
                conn.commit()
                click.echo(f" ERROR: {e}")

        # Mark joint-filer duplicates introduced by this ingest
        mark_joint_filers(conn)
        _write_sentinel()

        # Heartbeat ping — only for nightly runs (since_last_run path)
        if since_last_run:
            _ping_heartbeat(os.getenv("INGEST_HEARTBEAT_URL"))

        # Health check — only for nightly runs; backfills skip alerts anyway
        if since_last_run:
            try:
                import health_check
                n_health = health_check.send_health_alerts(conn, os.getenv("SLACK_WEBHOOK_URL"))
                if n_health:
                    click.echo(f"Sent {n_health} health alert(s)")
            except Exception as e:
                click.echo(f"Health check error (non-fatal): {e}")

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

            if POLYGON_API_KEY:
                try:
                    n = alert_module.check_and_send_signals(conn, config, POLYGON_API_KEY)
                    if n:
                        click.echo(f"Sent {n} signal alert(s)")
                except Exception as e:
                    click.echo(f"Signal scan error (non-fatal): {e}")
    finally:
        conn.close()  # returns to pool (psycopg_pool overrides close())


if __name__ == "__main__":
    main()
