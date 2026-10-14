"""
SEC Schedule 13D ingester CLI.

Usage:
  python ingest_13d.py --date today
  python ingest_13d.py --date 2026-10-13
  python ingest_13d.py --backfill 2024-12-18 2026-10-13
  python ingest_13d.py --since-last-run          # used by the systemd timer
  python ingest_13d.py --report-cik 0001494730   # what does this person hold

Structured 13D XML starts 2024-12-18; earlier filings are free text and are
not parsed. Writes `ownership_filing` + `ownership_filing_person` (migration
0008) and logs to run_log with run_kind='13d' so health_check's nightly
Form 4 alarm is not affected.
"""
from __future__ import annotations

import logging
import re
import time
from datetime import date, datetime, timedelta, timezone

import click
import psycopg

import queries
from db import get_cli_db
from ingest import (
    EDGAR_BASE,
    RATE_SLEEP,
    IngestResult,
    _make_client,
    _quarter,
    _write_sentinel,
    accession_from_filename,
    fetch_xml_url,
)
from parser_13d import parse_13d
from tickers import lookup_ticker

_LOG = logging.getLogger("ingest_13d")

STRUCTURED_START = date(2024, 12, 18)
RUN_KIND = "13d"

# "SCHEDULE 13D" has a space, so the Form 4 index parser (which takes the first
# whitespace token as the form type) cannot be reused. Anchor on the form type,
# then the trailing "<cik> <date> <path>" columns.
_INDEX_ROW = re.compile(
    r"^(SCHEDULE 13D(?:/A)?)\s+(.*?)\s+(\d+)\s+(\d{8}|\d{4}-\d{2}-\d{2})\s+(edgar/data/\S+)"
)


def fetch_13d_index(client, target_date: date) -> list[dict]:
    """Return 13D / 13D/A filings for target_date, deduped by accession.

    Every filing appears once per CIK it is indexed under (subject company plus
    each filer), so the same accession shows up two or more times.
    """
    year, qtr = target_date.year, _quarter(target_date)
    daily = (
        f"{EDGAR_BASE}/Archives/edgar/daily-index/{year}/QTR{qtr}"
        f"/form.{target_date.strftime('%Y%m%d')}.idx"
    )
    quarterly = f"{EDGAR_BASE}/Archives/edgar/full-index/{year}/QTR{qtr}/form.idx"

    time.sleep(RATE_SLEEP)
    resp = client.get(daily)
    if resp.status_code != 200:
        resp = client.get(quarterly)
    resp.raise_for_status()
    if resp.text.lstrip().startswith("<"):
        raise RuntimeError(
            f"SEC returned HTML instead of plain-text index (rate-limited?): {resp.url}"
        )

    want = {target_date.strftime("%Y-%m-%d"), target_date.strftime("%Y%m%d")}
    seen: set[str] = set()
    entries: list[dict] = []
    for line in resp.text.splitlines():
        m = _INDEX_ROW.match(line)
        if not m:
            continue
        form_type, _company, _cik, date_filed, filename = m.groups()
        if date_filed not in want:
            continue
        accession = accession_from_filename(filename)
        if accession in seen:
            continue
        seen.add(accession)
        entries.append({
            "form_type": form_type,
            "filename": filename,
            "date_filed": target_date.isoformat(),
        })
    return entries


def _store(conn: psycopg.Connection, filing: dict, persons: list[dict]) -> bool:
    """Insert one filing and its persons atomically. Returns False if it was a duplicate."""
    with conn.transaction():
        cur = conn.execute(
            """
            INSERT INTO ownership_filing (
              accession_no, schedule, form_type, is_amendment, amendment_no,
              previous_accession, filed_at, event_date, filer_cik,
              issuer_cik, issuer_name, issuer_cusip, issuer_ticker,
              class_title, funds_source, purpose_text, raw_xml_url
            ) VALUES (
              %(accession_no)s, %(schedule)s, %(form_type)s, %(is_amendment)s, %(amendment_no)s,
              %(previous_accession)s, %(filed_at)s, %(event_date)s, %(filer_cik)s,
              %(issuer_cik)s, %(issuer_name)s, %(issuer_cusip)s, %(issuer_ticker)s,
              %(class_title)s, %(funds_source)s, %(purpose_text)s, %(raw_xml_url)s
            )
            ON CONFLICT (accession_no) DO NOTHING
            """,
            filing,
        )
        if not cur.rowcount:
            return False
        for p in persons:
            conn.execute(
                """
                INSERT INTO ownership_filing_person (
                  accession_no, seq, person_cik, person_name, no_cik,
                  member_of_group, fund_type, person_type,
                  sole_voting, shared_voting, sole_dispositive, shared_dispositive,
                  aggregate_owned, pct_class, comment_text
                ) VALUES (
                  %(accession_no)s, %(seq)s, %(person_cik)s, %(person_name)s, %(no_cik)s,
                  %(member_of_group)s, %(fund_type)s, %(person_type)s,
                  %(sole_voting)s, %(shared_voting)s, %(sole_dispositive)s, %(shared_dispositive)s,
                  %(aggregate_owned)s, %(pct_class)s, %(comment_text)s
                )
                """,
                p,
            )
    return True


def ingest_date(conn: psycopg.Connection, target_date: date) -> IngestResult:
    client = _make_client()
    entries = fetch_13d_index(client, target_date)
    result = IngestResult(filings_found=len(entries))

    for entry in entries:
        accession_no = accession_from_filename(entry["filename"])
        try:
            xml_url, xml_bytes = fetch_xml_url(client, entry["filename"])
            filing, persons = parse_13d(xml_bytes, accession_no, entry["date_filed"], xml_url)
            filing["issuer_ticker"] = lookup_ticker(filing["issuer_cik"])
            try:
                if _store(conn, filing, persons):
                    result.rows_inserted += 1
            except psycopg.Error as exc:
                # A row the database refused is not an operational blip; count it
                # the same way ingest.py does so the heartbeat is withheld.
                detail = getattr(getattr(exc, "diag", None), "message_primary", None) or str(exc)
                result.errors += 1
                result.insert_failures += 1
                result.error_lines.append(f"insert: {accession_no} {detail}")
        except Exception as exc:
            result.errors += 1
            result.error_lines.append(f"{accession_no}: {exc}")
    conn.commit()
    return result


def _report(conn: psycopg.Connection, cik: str) -> None:
    rows = queries.get_ownership_positions(conn, cik.zfill(10))
    if not rows:
        click.echo(f"No 13D filings found for CIK {cik}")
        return
    click.echo(f"{'event':<11}{'issuer':<34}{'form':<15}{'shares':>16}{'pct':>7}  change")
    for r in rows:
        shares = f"{int(r['aggregate_owned']):,}" if r["aggregate_owned"] is not None else "?"
        pct = f"{float(r['pct_class']):.1f}%" if r["pct_class"] is not None else "?"
        click.echo(
            f"{str(r['event_date'] or r['filed_at']):<11}"
            f"{(r['issuer_name'] or r['issuer_cik'])[:32]:<34}"
            f"{r['form_type']:<15}{shares:>16}{pct:>7}  {r['change_label']}"
        )


@click.command()
@click.option("--date", "target_date", default=None, help="YYYY-MM-DD or 'today'")
@click.option("--backfill", nargs=2, default=None, help="START END (YYYY-MM-DD)")
@click.option("--since-last-run", is_flag=True)
@click.option("--report-cik", default=None, help="Print a person's 13D positions and exit")
def main(target_date, backfill, since_last_run, report_cik):
    conn = get_cli_db()
    try:
        if report_cik:
            _report(conn, report_cik)
            return

        dates: list[date] = []
        if since_last_run:
            row = conn.execute("SELECT MAX(filed_at) AS d FROM ownership_filing").fetchone()
            start = row["d"] if row and row["d"] else date.today() - timedelta(days=3)
            d = start
            while d <= date.today():
                dates.append(d)
                d += timedelta(days=1)
            run_kind = RUN_KIND
        elif backfill:
            d, end = date.fromisoformat(backfill[0]), date.fromisoformat(backfill[1])
            while d <= end:
                dates.append(d)
                d += timedelta(days=1)
            run_kind = f"{RUN_KIND}-backfill"
        else:
            dates = [date.today() if not target_date or target_date.lower() == "today"
                     else date.fromisoformat(target_date)]
            run_kind = RUN_KIND

        for d in dates:
            if d.weekday() >= 5:
                continue
            if d < STRUCTURED_START:
                click.echo(f"Skipping {d}: before structured 13D XML ({STRUCTURED_START})")
                continue
            started_at = datetime.now(timezone.utc).isoformat()
            click.echo(f"Ingesting 13D {d} ...", nl=False)
            try:
                res = ingest_date(conn, d)
                err, found, ins, detail = res.errors, res.filings_found, res.rows_inserted, res.error_detail
                click.echo(f" {found} filings, {ins} stored, {err} errors")
            except Exception as exc:
                conn.rollback()
                err, found, ins, detail = 1, 0, 0, str(exc)
                click.echo(f" ERROR: {exc}")
            conn.execute(
                """INSERT INTO run_log (started_at, finished_at, date_processed,
                   filings_found, rows_inserted, errors, error_detail, run_kind)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                (started_at, datetime.now(timezone.utc).isoformat(), d.isoformat(),
                 found, ins, err, detail or None, run_kind),
            )
            conn.commit()

        _write_sentinel()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
