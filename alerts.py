"""
Slack push alerts for insider trading signals.

Called at the end of each real-time ingest run (not backfills).
Uses insert-first deduplication: claim the alert_key slot before posting
to Slack, so multiple concurrent runs never double-fire.
"""
from __future__ import annotations

import json
import os
import sqlite3
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone

import polygon_client
from backtest import (
    detect_channel_break,
    detect_golden_cross,
    detect_hhl,
    detect_resistance_break,
)
from queries import _fmt_value as _fmt_money


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _try_claim_alert(conn: sqlite3.Connection, alert_key: str, alert_type: str) -> bool:
    """
    Attempt to claim an alert slot. Returns True only if this process
    is the first to claim it (INSERT succeeded). Uses rowcount before commit
    to avoid the changes() race condition.
    """
    cur = conn.execute(
        "INSERT OR IGNORE INTO alerts_sent (alert_key, alert_type) VALUES (?, ?)",
        [alert_key, alert_type],
    )
    claimed = cur.rowcount == 1
    conn.commit()
    return claimed


# ---------------------------------------------------------------------------
# Slack HTTP POST
# ---------------------------------------------------------------------------

def _post_to_slack(webhook_url: str, payload: dict, timeout: float = 5.0) -> bool:
    """POST a Block Kit payload to a Slack incoming webhook. Returns True on success."""
    body = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        webhook_url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.HTTPError, urllib.error.URLError):
        return False


# ---------------------------------------------------------------------------
# Message formatting
# ---------------------------------------------------------------------------

def _fmt(v: float | None) -> str:
    return _fmt_money(v) or "?"


def _format_buy_message(alert_type: str, row: dict, base_url: str) -> dict:
    ticker = row.get("issuer_ticker") or "?"
    company = row.get("issuer_name") or ""
    insider = row.get("insider_name") or "Unknown"
    title = row.get("insider_title") or "Insider"
    value = _fmt(row.get("total_value"))
    shares = f"{row.get('shares', 0):,.0f}" if row.get("shares") else "?"
    price = _fmt(row.get("price_per_share"))
    is_plan = row.get("is_10b5_1", 0)
    conviction = row.get("conviction")

    emoji = "🟢"
    label = "BIG BUY" if alert_type == "big_buy" else "C-SUITE BUY"
    plan_note = " · 10b5-1 plan" if is_plan else " · Open market"
    score_note = f" · Score {conviction}/10" if conviction else ""

    filing_url = f"{base_url}/filing/{row.get('transaction_id', '')}"

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"{emoji} {label} — ${ticker}"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{insider}* ({title})\n"
                        f"{shares} shares @ {price} = *{value}*"
                        f"{plan_note}{score_note}"
                    ),
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Filing"},
                    "url": filing_url,
                },
            },
            {
                "type": "context",
                "elements": [
                    {"type": "mrkdwn", "text": f"_{company}_"}
                ],
            },
        ]
    }


def _format_cluster_message(row: dict, base_url: str) -> dict:
    ticker = row.get("issuer_ticker") or "?"
    company = row.get("issuer_name") or ""
    count = row.get("insider_count", 0)
    value = _fmt(row.get("total_value"))
    issuer_url = f"{base_url}/issuer/{ticker}"

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"⚡ CLUSTER BUY — ${ticker}"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{count} insiders* bought at {company}\n"
                        f"Combined value: *{value}*"
                    ),
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Issuer"},
                    "url": issuer_url,
                },
            },
        ]
    }


# ---------------------------------------------------------------------------
# Alert matchers
# ---------------------------------------------------------------------------

def _match_big_buy(
    conn: sqlite3.Connection,
    since_ts: str,
    threshold: float,
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT * FROM filings
        WHERE transaction_code = 'P'
          AND total_value >= ?
          AND ingested_at >= ?
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        ORDER BY total_value DESC
        """,
        [threshold, since_ts],
    ).fetchall()
    return [dict(r) for r in rows]


def _match_insider_buy(
    conn: sqlite3.Connection,
    since_ts: str,
    threshold: float,
    keywords: list[str],
) -> list[dict]:
    if not keywords:
        return []
    kw_clauses = " OR ".join("insider_title LIKE ?" for _ in keywords)
    params = [threshold, since_ts] + [f"%{kw}%" for kw in keywords]
    rows = conn.execute(
        f"""
        SELECT * FROM filings
        WHERE transaction_code = 'P'
          AND total_value >= ?
          AND ingested_at >= ?
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND ({kw_clauses})
        ORDER BY total_value DESC
        """,
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def _match_cluster(
    conn: sqlite3.Connection,
    since_ts: str,
    min_insiders: int,
    window_days: int,
) -> list[dict]:
    """
    Find issuers with min_insiders+ distinct buyers within window_days,
    where at least one buy was ingested since since_ts (new activity).
    """
    cutoff = (datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat()
    rows = conn.execute(
        """
        SELECT issuer_cik, issuer_name, issuer_ticker,
               COUNT(DISTINCT insider_cik) AS insider_count,
               COALESCE(SUM(total_value), 0) AS total_value,
               MAX(ingested_at) AS latest_ingested_at
        FROM filings
        WHERE transaction_code = 'P'
          AND transaction_date >= ?
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        GROUP BY issuer_cik, issuer_name, issuer_ticker
        HAVING COUNT(DISTINCT insider_cik) >= ?
           AND MAX(ingested_at) >= ?
        ORDER BY total_value DESC
        """,
        [cutoff, min_insiders, since_ts],
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Alert key builders (amendment-stable)
# ---------------------------------------------------------------------------

def _buy_alert_key(row: dict) -> str:
    # Shared key across all buy alert types so a trade that matches both
    # big_buy and insider_buy thresholds only fires one Slack message.
    return (
        f"buy:"
        f"{row.get('issuer_cik','')}:"
        f"{row.get('insider_cik','')}:"
        f"{row.get('transaction_date','')}:"
        f"{row.get('transaction_code','')}"
    )


def _cluster_alert_key(row: dict) -> str:
    # One cluster alert per issuer per calendar week
    week = datetime.now(timezone.utc).strftime("%G-W%V")
    return f"cluster:{row.get('issuer_cik','')}:{week}"


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def check_and_send(
    conn: sqlite3.Connection,
    config: dict,
    since_ts: str | None = None,
    suppress: bool = False,
) -> int:
    """
    Query for alert matches and post to Slack.
    Returns count of alerts sent.
    suppress=True skips all sending (used for backfills).
    """
    if suppress:
        return 0

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return 0

    # Default: look back 25 hours to catch any late filings from previous run
    if since_ts is None:
        since_ts = (
            datetime.now(timezone.utc) - timedelta(hours=25)
        ).isoformat()

    rules = config.get("alert_rules", {})
    base_url = config.get("alert_base_url", "https://opi-insider.duckdns.org")
    keywords = rules.get("insider_title_keywords", [])
    sent = 0

    # 1. Big buy
    big_threshold = rules.get("big_buy_threshold", 1_000_000)
    for row in _match_big_buy(conn, since_ts, big_threshold):
        key = _buy_alert_key(row)
        if _try_claim_alert(conn, key, "big_buy"):
            payload = _format_buy_message("big_buy", row, base_url)
            if _post_to_slack(webhook_url, payload):
                sent += 1

    # 2. C-suite buy (lower threshold)
    insider_threshold = rules.get("insider_buy_threshold", 250_000)
    for row in _match_insider_buy(conn, since_ts, insider_threshold, keywords):
        key = _buy_alert_key(row)
        if _try_claim_alert(conn, key, "insider_buy"):
            payload = _format_buy_message("insider_buy", row, base_url)
            if _post_to_slack(webhook_url, payload):
                sent += 1

    # 3. Cluster
    min_insiders = rules.get("cluster_min_insiders", 3)
    window_days = rules.get("cluster_window_days", 10)
    for row in _match_cluster(conn, since_ts, min_insiders, window_days):
        key = _cluster_alert_key(row)
        if _try_claim_alert(conn, key, "cluster"):
            payload = _format_cluster_message(row, base_url)
            if _post_to_slack(webhook_url, payload):
                sent += 1

    return sent


_SIGNAL_DETECTORS = {
    "gc":  ("Golden Cross",     detect_golden_cross),
    "rb":  ("Resistance Break", detect_resistance_break),
    "hhl": ("HH+HL",            detect_hhl),
    "cb":  ("Channel Break",    detect_channel_break),
}


def _format_signal_message(
    signal_label: str,
    trade: dict,
    days_to_fire: int,
    base_url: str,
) -> dict:
    ticker  = trade.get("issuer_ticker") or "?"
    company = trade.get("issuer_name") or ""
    insider = trade.get("insider_name") or "Unknown"
    title   = trade.get("insider_title") or "Insider"
    value   = _fmt(trade.get("total_value"))
    trade_date = trade.get("transaction_date", "")
    chart_url  = f"{base_url}/chart/{ticker}"

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"📡 Signal: {signal_label} — ${ticker}"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"Fired *{days_to_fire}d* after insider buy on {trade_date}\n"
                        f"*{insider}* ({title}) · {value}"
                    ),
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Chart"},
                    "url": chart_url,
                },
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"_{company}_"}],
            },
        ]
    }


def check_and_send_signals(
    conn: sqlite3.Connection,
    config: dict,
    polygon_api_key: str,
    suppress: bool = False,
) -> int:
    """
    Scan recent insider buys for technical signals that have fired since the trade.
    Groups by ticker to minimize Polygon API calls. Returns count of alerts sent.
    One alert per (signal, trade) — deduped via alerts_sent table.
    """
    if suppress or not polygon_api_key:
        return 0

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return 0

    rules      = config.get("alert_rules", {})
    base_url   = config.get("alert_base_url", "https://opi-insider.duckdns.org")
    min_value  = rules.get("signal_scan_min_value", 500_000)
    lookback   = rules.get("signal_scan_lookback_days", 90)

    today  = datetime.now(timezone.utc).date()
    cutoff = (today - timedelta(days=lookback)).isoformat()

    rows = conn.execute("""
        SELECT issuer_ticker, issuer_cik, issuer_name,
               insider_cik, insider_name, insider_title,
               transaction_date, total_value, price_per_share, transaction_id
        FROM filings
        WHERE transaction_code = 'P'
          AND total_value >= ?
          AND is_10b5_1 = 0
          AND transaction_date >= ?
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND issuer_ticker IS NOT NULL
          AND TRIM(issuer_ticker) NOT IN ('', 'NONE', 'N/A')
        ORDER BY issuer_ticker, transaction_date
    """, [min_value, cutoff]).fetchall()

    if not rows:
        return 0

    by_ticker: dict[str, list[dict]] = {}
    for r in rows:
        t = dict(r)
        by_ticker.setdefault(t["issuer_ticker"], []).append(t)

    # 300-day warmup needed for 200-bar MA + up to 90 days post-trade
    price_from = today - timedelta(days=lookback + 310)
    sent = 0

    for ticker, trades in by_ticker.items():
        raw_bars = polygon_client.get_daily_bars(ticker, price_from, today, polygon_api_key, limit=600)
        if len(raw_bars) < 50:
            continue
        # polygon_client uses "time" key; backtest detectors expect "date"
        bars = [{**b, "date": b["time"]} for b in raw_bars]
        bar_dates = [b["date"] for b in bars]

        for trade in trades:
            trade_date = trade["transaction_date"]
            trade_idx = next((i for i, d in enumerate(bar_dates) if d >= trade_date), None)
            if trade_idx is None or trade_idx >= len(bars) - 1:
                continue

            for sig_code, (sig_label, detect_fn) in _SIGNAL_DETECTORS.items():
                _, days_to_fire = detect_fn(bars, trade_idx)
                if days_to_fire is None:
                    continue

                alert_key = (
                    f"signal:{sig_code}:"
                    f"{trade['issuer_cik']}:{trade['insider_cik']}:{trade_date}"
                )
                if not _try_claim_alert(conn, alert_key, "signal"):
                    continue

                payload = _format_signal_message(sig_label, trade, days_to_fire, base_url)
                if _post_to_slack(webhook_url, payload):
                    sent += 1

    return sent


def send_test_alert(webhook_url: str, base_url: str) -> bool:
    """Send a test message to verify the webhook is wired up."""
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🔔 Insider Tracker — Test Alert"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"Webhook is connected. Real-time alerts are active.\n"
                        f"<{base_url}|Open Dashboard>"
                    ),
                },
            },
        ]
    }
    return _post_to_slack(webhook_url, payload)
