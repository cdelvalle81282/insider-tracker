"""
Slack push alerts for insider trading signals.

Called at the end of each real-time ingest run (not backfills).
Uses insert-first deduplication: claim the alert_key slot before posting
to Slack, so multiple concurrent runs never double-fire.
"""
from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from datetime import date, datetime, timedelta, timezone

import psycopg

import polygon_client
from backtest import (
    PRICE_WARMUP_DAYS,
    detect_channel_break,
    detect_golden_cross,
    detect_hhl,
    detect_resistance_break,
)
import queries
from queries import _fmt_value as _fmt_money


# ---------------------------------------------------------------------------
# Deduplication
# ---------------------------------------------------------------------------

def _try_claim_alert(conn: psycopg.Connection, alert_key: str, alert_type: str) -> bool:
    """
    Attempt to claim an alert slot. Returns True only if this process
    is the first to claim it (INSERT succeeded). Uses rowcount before commit
    to avoid the changes() race condition.
    """
    cur = conn.execute(
        "INSERT INTO alerts_sent (alert_key, alert_type) VALUES (%s, %s)"
        " ON CONFLICT DO NOTHING",
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


def _slack_escape(s: str) -> str:
    """Escape Slack mrkdwn special characters in external text (insider/issuer/
    politician names, titles) so it can't be used to inject a live @channel-style
    mention or a spoofed <url|label> link. & must be replaced first, or the
    &lt;/&gt; entities produced below would themselves get double-escaped."""
    return s.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def _format_buy_message(alert_type: str, row: dict, base_url: str) -> dict:
    ticker = row.get("issuer_ticker") or "?"
    company = _slack_escape(row.get("issuer_name") or "")
    insider = _slack_escape(row.get("insider_name") or "Unknown")
    title = _slack_escape(row.get("insider_title") or "Insider")
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


def _format_watchlist_message(row: dict, base_url: str) -> dict:
    ticker = row.get("issuer_ticker") or "?"
    company = _slack_escape(row.get("issuer_name") or "")
    insider = _slack_escape(row.get("insider_name") or "Unknown")
    title = _slack_escape(row.get("insider_title") or "Insider")
    value = _fmt(row.get("total_value"))
    shares = f"{row.get('shares', 0):,.0f}" if row.get("shares") else "?"
    price = _fmt(row.get("price_per_share"))
    is_plan = row.get("is_10b5_1", 0)
    conviction = row.get("conviction")
    is_sell = row.get("transaction_code") == "S"

    emoji = "🔴" if is_sell else "🟢"
    direction = "SELL" if is_sell else "BUY"
    plan_note = " · 10b5-1 plan" if is_plan else " · Open market"
    score_note = f" · Score {conviction}/10" if conviction else ""

    filing_url = f"{base_url}/filing/{row.get('transaction_id', '')}"

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"⭐ WATCHLIST {direction} — ${ticker}"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"{emoji} *{insider}* ({title})\n"
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
    company = _slack_escape(row.get("issuer_name") or "")
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
    conn: psycopg.Connection,
    since_ts: str,
    threshold: float,
) -> list[dict]:
    rows = conn.execute(
        """
        SELECT * FROM filings
        WHERE transaction_code = 'P'
          AND total_value >= %s
          AND ingested_at >= %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND table_type = 'ND'
          AND issuer_ticker IS NOT NULL
        ORDER BY total_value DESC
        """,
        [threshold, since_ts],
    ).fetchall()
    return [dict(r) for r in rows]


def _match_watchlist_activity(
    conn: psycopg.Connection,
    since_ts: str,
    watched_tickers: list[str],
    watched_insiders: list[str],
) -> list[dict]:
    """Any buy or sell (no $ threshold) on a watched ticker or insider — watching
    something means the user cares about it regardless of size. Checked before the
    generic thresholds in check_and_send() so a matching trade gets the
    ⭐ WATCHLIST message instead of (or as well as) BIG BUY / C-SUITE BUY."""
    if not watched_tickers and not watched_insiders:
        return []
    rows = conn.execute(
        """
        SELECT * FROM filings
        WHERE transaction_code IN ('P', 'S')
          AND ingested_at >= %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND table_type = 'ND'
          AND issuer_ticker IS NOT NULL
          AND (issuer_ticker = ANY(%s) OR insider_cik = ANY(%s))
        ORDER BY total_value DESC NULLS LAST
        """,
        [since_ts, watched_tickers, watched_insiders],
    ).fetchall()
    return [dict(r) for r in rows]


def _match_insider_buy(
    conn: psycopg.Connection,
    since_ts: str,
    threshold: float,
    keywords: list[str],
) -> list[dict]:
    if not keywords:
        return []
    kw_clauses = " OR ".join("insider_title ILIKE %s" for _ in keywords)
    params = [threshold, since_ts] + [f"%{kw}%" for kw in keywords]
    rows = conn.execute(
        f"""
        SELECT * FROM filings
        WHERE transaction_code = 'P'
          AND total_value >= %s
          AND ingested_at >= %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND table_type = 'ND'
          AND issuer_ticker IS NOT NULL
          AND ({kw_clauses})
        ORDER BY total_value DESC
        """,
        params,
    ).fetchall()
    return [dict(r) for r in rows]


def _match_cluster(
    conn: psycopg.Connection,
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
          AND transaction_date >= %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND table_type = 'ND'
          AND issuer_ticker IS NOT NULL
        GROUP BY issuer_cik, issuer_name, issuer_ticker
        HAVING COUNT(DISTINCT insider_cik) >= %s
           AND MAX(ingested_at) >= %s
        ORDER BY total_value DESC
        """,
        [cutoff, min_insiders, since_ts],
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# Alert key builders (amendment-stable)
# ---------------------------------------------------------------------------

def _buy_alert_key(row: dict) -> str:
    # Shared key across all buy/sell alert types (watchlist, big_buy, insider_buy)
    # so a trade that matches more than one only fires one Slack message.
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
    conn: psycopg.Connection,
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

    # 0. Watchlist activity — any size, buy or sell. Claimed under the same
    # shared key as big_buy/insider_buy, so a watched trade that also clears
    # a generic threshold gets exactly one alert (this one, checked first).
    watched_tickers = list(queries.watched_tickers(conn))
    watched_insiders = list(queries.watched_insiders(conn))
    for row in _match_watchlist_activity(conn, since_ts, watched_tickers, watched_insiders):
        key = _buy_alert_key(row)
        if _try_claim_alert(conn, key, "watchlist"):
            payload = _format_watchlist_message(row, base_url)
            if _post_to_slack(webhook_url, payload):
                sent += 1

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

    # 4. Congress / executive co-buy stacking
    sent += check_congress_cobuy_alerts(conn, base_url=base_url)

    return sent


_SIGNAL_DETECTORS = {
    "gc":  ("Golden Cross (50MA > 200MA)", detect_golden_cross),
    "rb":  ("Resistance Break",            detect_resistance_break),
    "hhl": ("Higher Highs + Higher Lows",  detect_hhl),
    "cb":  ("Channel Break",               detect_channel_break),
}

# Win rate and avg return from backtest at the most informative window per signal.
# Format: sig_code → (window_days, win_rate_pct, avg_return_pct, n_fired)
_SIGNAL_STATS = {
    "gc":  (90, 87.0, 22.6, 75),
    "rb":  (15, 85.4, 17.9, 219),
    "hhl": (30, 85.7, 14.8, 105),
    "cb":  (90, 80.6,  7.1,  31),
}


def _format_signal_message(
    sig_code: str,
    signal_label: str,
    trade: dict,
    days_to_fire: int,
    base_url: str,
) -> dict:
    ticker     = trade.get("issuer_ticker") or "?"
    company    = _slack_escape(trade.get("issuer_name") or "")
    insider    = _slack_escape(trade.get("insider_name") or "Unknown")
    title      = _slack_escape(trade.get("insider_title") or "Insider")
    value      = _fmt(trade.get("total_value"))
    trade_date = trade.get("transaction_date", "")
    chart_url  = f"{base_url}/chart/{ticker}"

    win_win, win_rate, avg_ret, n = _SIGNAL_STATS.get(sig_code, (90, None, None, 0))
    stat_line = (
        f"Historical ({n} trades): *{win_rate:.0f}% win rate* · avg *+{avg_ret:.1f}%* at {win_win}d from buy"
        if win_rate is not None else ""
    )

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"📡 Signal Fire — ${ticker}"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{signal_label}* fired *{days_to_fire}d* after insider buy\n"
                        f"*{insider}* ({title}) bought {value} on {trade_date}"
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
                "elements": [{"type": "mrkdwn", "text": f"{stat_line}  ·  _{company}_"}],
            },
        ]
    }


def _signal_alert_key(sig_code: str, issuer_cik: str, fire_date: str) -> str:
    # Keyed on (signal, issuer, fire date) — not per-insider — so a cluster buy
    # on the same stock only fires one alert when the technical signal triggers.
    return f"signal:{sig_code}:{issuer_cik}:{fire_date}"


def _log_signal_trigger(
    conn: psycopg.Connection,
    signal_code: str,
    fire_date: date,
    days_to_fire: int,
    trade: dict,
) -> None:
    """Record a fired signal for the dashboard trigger feed / performance tracking.
    Independent of Slack alerting -- logs regardless of whether a webhook is
    configured or whether this signal type is live-alerted."""
    conn.execute(
        """
        INSERT INTO signal_triggers
            (issuer_ticker, issuer_cik, issuer_name, signal_code, trigger_date,
             trade_transaction_id, insider_name, insider_title, trade_date,
             trade_value, days_to_fire)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (issuer_ticker, signal_code, trigger_date) DO NOTHING
        """,
        [
            trade.get("issuer_ticker"), trade.get("issuer_cik"), trade.get("issuer_name"),
            signal_code, fire_date, trade.get("transaction_id"),
            trade.get("insider_name"), trade.get("insider_title"),
            trade.get("transaction_date"), trade.get("total_value"), days_to_fire,
        ],
    )
    conn.commit()


def check_and_send_signals(
    conn: psycopg.Connection,
    config: dict,
    polygon_api_key: str,
    suppress: bool = False,
    send_alerts: bool = True,
    max_age_override: int | None = None,
) -> int:
    """Scan recent insider buys for technical signals firing post-trade.

    Logs every fired signal (all 4 types) to signal_triggers for the dashboard
    trigger feed / performance tracking -- independent of Slack. Only Golden
    Cross pushes a live Slack alert (see gotchas.md: backtest data showed no
    consistent return edge for RB/HHL/CB measured from their own fire date).
    Returns count of Slack alerts sent (not signals logged).

    send_alerts=False logs signals without posting to Slack (used by the
    one-time --backfill-signal-triggers CLI seed). max_age_override widens
    the freshness cutoff for that same backfill so it can seed further back
    than the live scan's default staleness window.
    """
    if suppress or not polygon_api_key:
        return 0

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "") if send_alerts else ""

    rules      = config.get("alert_rules", {})
    base_url   = config.get("alert_base_url", "https://opi-insider.duckdns.org")
    min_value  = rules.get("signal_scan_min_value", 500_000)
    lookback   = rules.get("signal_scan_lookback_days", 90)
    max_age    = max_age_override if max_age_override is not None else rules.get("signal_scan_max_signal_age_days", 5)

    today  = datetime.now(timezone.utc).date()
    cutoff = (today - timedelta(days=lookback)).isoformat()

    rows = conn.execute("""
        SELECT transaction_id, issuer_ticker, issuer_cik, issuer_name,
               insider_cik, insider_name, insider_title,
               transaction_date::text AS transaction_date, total_value
        FROM filings
        WHERE transaction_code = 'P'
          AND total_value >= %s
          AND is_10b5_1 = 0
          AND transaction_date >= %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND table_type = 'ND'
          AND issuer_ticker IS NOT NULL
          AND TRIM(issuer_ticker) NOT IN ('', 'NONE', 'N/A')
        ORDER BY issuer_ticker, transaction_date
    """, [min_value, cutoff]).fetchall()

    if not rows:
        return 0

    by_ticker: dict[str, list[dict]] = {}
    for r in rows:
        t = dict(r)
        by_ticker.setdefault(t["issuer_ticker"].strip(), []).append(t)

    price_from = today - timedelta(days=lookback + PRICE_WARMUP_DAYS)
    sent = 0

    for ticker, trades in by_ticker.items():
        raw_bars = polygon_client.get_daily_bars(ticker, price_from, today, polygon_api_key, limit=600)
        if len(raw_bars) < 50:
            continue
        # polygon_client uses "time" key; backtest detectors expect "date"
        bars = [{**b, "date": b["time"]} for b in raw_bars]
        bar_dates = [b["date"] for b in bars]

        for sig_code, (label, detect_fn) in _SIGNAL_DETECTORS.items():
            # Find all trades where this signal fired within 15 days and is
            # still fresh. Signals are stock-level events — log/alert once per
            # (ticker, signal, fire date) using the largest buy as the
            # representative trade.
            qualifying: list[tuple[date, int, dict]] = []
            for trade in trades:
                trade_date = trade["transaction_date"]
                trade_idx = next((i for i, d in enumerate(bar_dates) if d >= trade_date), None)
                if trade_idx is None or trade_idx >= len(bars) - 1:
                    continue
                _, days_to_fire = detect_fn(bars, trade_idx)
                if days_to_fire is None or days_to_fire > 15:
                    continue
                fire_date = date.fromisoformat(trade_date) + timedelta(days=days_to_fire)
                if (today - fire_date).days > max_age:
                    continue
                qualifying.append((fire_date, days_to_fire, trade))

            if not qualifying:
                continue

            # One entry per fire date; pick the largest buy as the representative
            fire_date = min(fd for fd, _, _ in qualifying)
            largest = max(qualifying, key=lambda x: x[2].get("total_value") or 0)
            days_to_fire_val, best_trade = largest[1], largest[2]

            _log_signal_trigger(conn, sig_code, fire_date, days_to_fire_val, best_trade)

            if sig_code != "gc" or not webhook_url:
                continue

            issuer_cik = best_trade.get("issuer_cik", "")
            alert_key = _signal_alert_key("gc", issuer_cik, fire_date.isoformat())
            if not _try_claim_alert(conn, alert_key, "signal"):
                continue
            payload = _format_signal_message("gc", label, best_trade, days_to_fire_val, base_url)
            if _post_to_slack(webhook_url, payload):
                sent += 1

    return sent


COBUY_WINDOW_DAYS  = 14       # ±days around congressional disclosure to scan for corp buys
COBUY_MIN_AMOUNT   = 100_000  # floor to exclude diversified portfolio rebalance noise


def _format_cobuy_message(cong_row: dict, corp_buys: list[dict], base_url: str) -> dict:
    ticker   = cong_row.get("ticker") or "?"
    name     = cong_row.get("politician_name") or "Unknown"
    amount   = cong_row.get("amount_label") or "?"
    disc     = str(cong_row.get("disclosure_date") or "?")[:10]
    source   = cong_row.get("source", "")
    src_label = "Executive" if source == "open_cabinet" else "Congress"

    corp_lines = []
    for b in corp_buys[:3]:       # cap at 3 names to keep message readable
        bname  = b.get("insider_name") or "Insider"
        btitle = b.get("insider_title") or ""
        bval   = _fmt(b.get("total_value"))
        bdate  = str(b.get("transaction_date") or "?")[:10]
        corp_lines.append(f"• *{_slack_escape(bname)}* ({_slack_escape(btitle)}) — {bval} on {bdate}")

    overflow = len(corp_buys) - 3
    if overflow > 0:
        corp_lines.append(f"_…and {overflow} more corporate insider(s)_")

    congress_url = f"{base_url}/congress?politician={urllib.parse.quote(name)}"
    chart_url    = f"{base_url}/chart/{ticker}"

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"⚡ CO-BUY SIGNAL — ${ticker}"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{src_label}: {_slack_escape(name)}* bought *{_slack_escape(amount)}* (disclosed {disc})\n"
                        f"*Corporate insider(s) also bought within ±{COBUY_WINDOW_DAYS}d:*\n"
                        + "\n".join(corp_lines)
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
                "elements": [{"type": "mrkdwn", "text": f"<{congress_url}|View {src_label} trades>"}],
            },
        ]
    }


def check_congress_cobuy_alerts(
    conn: psycopg.Connection,
    suppress: bool = False,
    base_url: str = "https://opi-insider.duckdns.org",
) -> int:
    """
    Fire alert when a congress/exec purchase and a corporate insider buy
    occurred within ±COBUY_WINDOW_DAYS of each other on the same ticker.

    Keyed per congress trade (cobuy:{transaction_id}) so each political trade
    fires at most one alert regardless of how many corp insiders overlapped.
    Called from check_and_send() (corp ingest direction) and directly from
    congress_ingest / exec_ingest (political ingest direction).
    """
    if suppress:
        return 0

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return 0

    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).date().isoformat()

    cong_rows = conn.execute("""
        SELECT transaction_id, politician_name, source, chamber, party,
               ticker, transaction_type,
               transaction_date, disclosure_date,
               amount_label, amount_min
        FROM congress_trades
        WHERE LOWER(transaction_type) IN ('purchase', 'buy')
          AND disclosure_date >= %s
          AND ticker IS NOT NULL AND ticker != ''
          AND transaction_id IS NOT NULL
          AND (amount_min IS NULL OR amount_min >= %s)
        ORDER BY disclosure_date DESC
    """, [cutoff, COBUY_MIN_AMOUNT]).fetchall()

    sent = 0
    for cr in cong_rows:
        cong = dict(cr)
        alert_key = f"cobuy:{cong['transaction_id']}"
        disc = cong.get("disclosure_date") or ""
        if not disc:
            continue
        try:
            disc_date = date.fromisoformat(disc[:10])
        except ValueError:
            continue

        lo = (disc_date - timedelta(days=COBUY_WINDOW_DAYS)).isoformat()
        hi = (disc_date + timedelta(days=COBUY_WINDOW_DAYS)).isoformat()

        corp_buys = conn.execute("""
            SELECT insider_name, insider_title, total_value,
                   transaction_date::text AS transaction_date
            FROM filings
            WHERE issuer_ticker = %s
              AND transaction_code = 'P'
              AND table_type = 'ND'
              AND superseded_by IS NULL
              AND joint_filer_of IS NULL
              AND transaction_date >= %s
              AND transaction_date <= %s
            ORDER BY total_value DESC NULLS LAST
        """, [cong["ticker"], lo, hi]).fetchall()

        if not corp_buys:
            continue

        if _try_claim_alert(conn, alert_key, "congress_cobuy"):
            payload = _format_cobuy_message(cong, [dict(r) for r in corp_buys], base_url)
            if _post_to_slack(webhook_url, payload):
                sent += 1

    return sent


def _format_congress_message(row: dict, base_url: str) -> dict:
    name      = row.get("politician_name") or "Unknown"
    ticker    = row.get("ticker") or "?"
    amount    = row.get("amount_label") or "?"
    tx_date   = str(row.get("transaction_date") or "?")[:10]
    disc_date = str(row.get("disclosure_date") or "?")[:10]
    party     = row.get("party") or ""
    lag_days  = row.get("lag_days")

    party_emoji = "🔵" if "Democrat" in party else "🔴" if "Republican" in party else "⚪"
    lag_note    = f" · disclosed {lag_days}d later" if lag_days is not None else ""
    congress_url = f"{base_url}/congress?politician={urllib.parse.quote(name)}"

    return {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": f"🏛️ WATCHED CONGRESS BUY — ${ticker}"},
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": (
                        f"*{_slack_escape(name)}* {party_emoji} {_slack_escape(party)}\n"
                        f"*{_slack_escape(amount)}* · Traded {tx_date}{lag_note}"
                    ),
                },
                "accessory": {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "View Trades"},
                    "url": congress_url,
                },
            },
            {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": f"Disclosed {disc_date}"}],
            },
        ]
    }


def check_congress_alerts(
    conn: psycopg.Connection,
    suppress: bool = False,
) -> int:
    """
    Fire Slack alerts for new purchases by watched congress members.
    Called at the end of each congress_ingest backfill that inserted new rows.
    Returns count of alerts sent.
    """
    if suppress:
        return 0

    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        return 0

    watched_lower = list(queries.watched_congress_members(conn))
    if not watched_lower:
        return 0

    base_url = os.getenv("ALERT_BASE_URL", "https://opi-insider.duckdns.org")

    # Look back 14 days so a weekend ingest gap doesn't miss anything
    cutoff = (datetime.now(timezone.utc) - timedelta(days=14)).date().isoformat()

    rows = conn.execute("""
        SELECT transaction_id, politician_name, chamber, party,
               ticker, transaction_type,
               transaction_date, disclosure_date,
               amount_label, amount_min,
               (disclosure_date::date - transaction_date::date) AS lag_days
        FROM congress_trades
        WHERE LOWER(transaction_type) IN ('purchase', 'buy')
          AND disclosure_date >= %s
          AND LOWER(politician_name) = ANY(%s)
          AND transaction_id IS NOT NULL
        ORDER BY disclosure_date DESC
    """, [cutoff, watched_lower]).fetchall()

    sent = 0
    for r in rows:
        row      = dict(r)
        alert_key = f"congress:{row['transaction_id']}"
        if not _try_claim_alert(conn, alert_key, "congress_buy"):
            continue
        payload = _format_congress_message(row, base_url)
        if _post_to_slack(webhook_url, payload):
            sent += 1

    return sent


def send_test_alert(webhook_url: str, base_url: str) -> bool:
    """Send a test message to verify the webhook is wired up."""
    payload = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🔔 Insider Scanner — Test Alert"},
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
