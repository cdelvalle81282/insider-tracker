"""
Ingest health checker — detects silent failures in the nightly ingest.

Run standalone: python health_check.py (checks without alerting)
Called by ingest.py after --since-last-run completes.
"""
from __future__ import annotations

import os
import sqlite3
from datetime import date, datetime
from pathlib import Path

INGEST_SENTINEL = Path(__file__).parent / "data" / ".last_ingest"
STALE_HOURS = 36  # alert if sentinel older than this


def check_ingest_health(conn: sqlite3.Connection) -> list[dict]:
    """
    Return a list of health findings. Empty list = healthy.
    Each finding: {"kind": str, "message": str}

    Only considers 'nightly' runs — daytime runs returning 0 filings is expected
    (EDGAR daily index not published until ~22:00 ET).
    """
    findings = []

    # --- Check: 2+ consecutive nightly weekday runs with 0 filings ---
    rows = conn.execute(
        """
        SELECT date_processed, filings_found, errors, started_at
        FROM run_log
        WHERE run_kind = 'nightly'
        ORDER BY started_at DESC
        LIMIT 3
        """
    ).fetchall()

    if len(rows) >= 2:
        # Most recent two nightly runs
        recent = rows[:2]
        both_zero = all(r[1] == 0 for r in recent)
        both_no_error = all(r[2] == 0 for r in recent)  # errors=0 means silent failure
        # Only alert on weekday runs (Mon=0 ... Fri=4)
        both_weekday = all(
            datetime.fromisoformat(r[3]).weekday() < 5
            for r in recent
        )
        if both_zero and both_no_error and both_weekday:
            dates = [r[0] for r in recent]
            findings.append({
                "kind": "no_filings",
                "message": (
                    f"Nightly ingest returned 0 filings for 2 consecutive weekday runs "
                    f"({', '.join(dates)}) with no errors. EDGAR index may be unavailable "
                    f"or the ingest is silently broken."
                ),
            })

    # --- Check: consecutive nightly errors ---
    if len(rows) >= 2:
        recent = rows[:2]
        both_errors = all(r[2] > 0 for r in recent)
        if both_errors:
            findings.append({
                "kind": "consecutive_errors",
                "message": (
                    "Nightly ingest has errors in the last 2 consecutive runs. "
                    "Check run_log for details."
                ),
            })

    # --- Check: stale sentinel ---
    try:
        mtime = os.path.getmtime(INGEST_SENTINEL)
        age_hours = (datetime.now().timestamp() - mtime) / 3600
        if age_hours > STALE_HOURS:
            findings.append({
                "kind": "stale_sentinel",
                "message": (
                    f"Last ingest sentinel is {age_hours:.1f}h old "
                    f"(threshold: {STALE_HOURS}h). Ingest may have stopped running."
                ),
            })
    except FileNotFoundError:
        findings.append({
            "kind": "no_sentinel",
            "message": "Ingest sentinel file data/.last_ingest not found. Has the ingester ever run?",
        })

    return findings


def send_health_alerts(conn: sqlite3.Connection, slack_webhook_url: str | None) -> int:
    """
    Check health and send Slack alerts for new findings. Returns count of alerts sent.
    Uses alerts_sent table for dedup — same pattern as existing alert system.
    """
    if not slack_webhook_url:
        return 0

    findings = check_ingest_health(conn)
    if not findings:
        return 0

    sent = 0
    today = date.today().isoformat()

    for finding in findings:
        alert_key = f"health:{finding['kind']}:{today}"

        # Dedup: INSERT OR IGNORE, check rowcount — alert_type is NOT NULL in schema
        cur = conn.execute(
            "INSERT OR IGNORE INTO alerts_sent (alert_key, alert_type) VALUES (?, ?)",
            (alert_key, "ingest_health"),
        )
        conn.commit()
        if cur.rowcount == 0:
            continue  # already sent today

        # Send to Slack
        try:
            import json
            import urllib.request
            payload = json.dumps({
                "text": f":warning: *Insider Tracker — Ingest Health Alert*\n{finding['message']}"
            }).encode()
            req = urllib.request.Request(
                slack_webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
            )
            urllib.request.urlopen(req, timeout=10)
            sent += 1
        except Exception as e:
            print(f"Health alert send failed: {e}")

    return sent


if __name__ == "__main__":
    from ingest import get_db
    conn = get_db()
    findings = check_ingest_health(conn)
    if findings:
        print(f"Found {len(findings)} health issue(s):")
        for f in findings:
            print(f"  [{f['kind']}] {f['message']}")
    else:
        print("All healthy.")
    conn.close()
