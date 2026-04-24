"""All SQL queries for the dashboard. No SQL in app.py."""
from __future__ import annotations

import sqlite3
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone


# ---------------------------------------------------------------------------
# Enrichment context — passed to _enrich; add new fields here each session
# rather than adding positional parameters. Callers passing ctx=None are
# unaffected by new fields.
# ---------------------------------------------------------------------------

@dataclass
class EnrichContext:
    conn: sqlite3.Connection | None = None
    conviction_flags: dict | None = None      # CONVICTION_FLAGS values
    conviction_tiers: dict | None = None      # CONVICTION_TIERS values
    conviction_max: int = 10
    conviction_thresholds: dict | None = None
    cluster_window_days: int = 14
    ceo_cfo_keywords: list[str] = field(default_factory=list)
    watched_tickers: set[str] = field(default_factory=set)     # Session 6
    watched_insiders: set[str] = field(default_factory=set)    # Session 6
    compute_conviction: bool = False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fmt_value(v: float | None) -> str:
    if v is None:
        return ""
    if abs(v) >= 1_000_000:
        return f"${v/1_000_000:.1f}M"
    if abs(v) >= 1_000:
        return f"${v/1_000:.0f}K"
    return f"${v:,.0f}"


def _relative_time(ts: str | None) -> str:
    if not ts:
        return ""
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        now = datetime.now(tz=timezone.utc)
        delta = now - dt
        if delta.total_seconds() < 3600:
            mins = int(delta.total_seconds() / 60)
            return f"{mins}m ago"
        if delta.total_seconds() < 86400:
            hrs = int(delta.total_seconds() / 3600)
            return f"{hrs}h ago"
        if delta.days == 1:
            return f"yesterday {dt.strftime('%-I:%M%p').lower()}"
        return dt.strftime("%b %-d")
    except Exception:
        return ts or ""


def _pct_holdings(row: dict) -> str | None:
    """
    % of position this transaction represents.
    Buy:  shares_bought / shares_owned_after
    Sell: shares_sold   / (shares_owned_after + shares_sold)
    """
    shares = row.get("shares")
    after = row.get("shares_owned_after")
    code = row.get("transaction_code")
    if not shares or not after or after <= 0:
        return None
    if code == "P":
        pct = shares / after * 100
    elif code == "S":
        pct = shares / (after + shares) * 100
    else:
        return None
    if pct < 0.1:
        return "<0.1%"
    if pct >= 100:
        return "100%"
    return f"{pct:.1f}%"


def _batch_cluster_counts(
    conn: sqlite3.Connection,
    rows: list[dict],
    window_days: int,
) -> dict[tuple, int]:
    """
    Count distinct buying insiders per (issuer_cik, transaction_date) pair in one query.
    Uses the broadest date window across all pairs so a single SQL round-trip covers all.
    Python groups results back to the per-pair granularity.
    """
    pairs = {(r.get("issuer_cik"), r.get("transaction_date")) for r in rows
             if r.get("issuer_cik") and r.get("transaction_date")}
    if not pairs:
        return {}

    all_ciks = list({p[0] for p in pairs})
    all_dates = [p[1] for p in pairs]
    global_end = max(all_dates)
    global_start = (date.fromisoformat(min(all_dates)) - timedelta(days=window_days)).isoformat()

    placeholders = ",".join("?" * len(all_ciks))
    db_rows = conn.execute(
        f"""SELECT issuer_cik, transaction_date, insider_cik
            FROM filings
            WHERE transaction_code = 'P'
              AND issuer_cik IN ({placeholders})
              AND transaction_date BETWEEN ? AND ?
              AND superseded_by IS NULL""",
        all_ciks + [global_start, global_end],
    ).fetchall()

    by_cik_date: dict[tuple, set] = defaultdict(set)
    for r in db_rows:
        by_cik_date[(r[0], r[1])].add(r[2])

    result: dict[tuple, int] = {}
    for (cik, dt) in pairs:
        d0 = (date.fromisoformat(dt) - timedelta(days=window_days)).isoformat()
        insiders: set = set()
        for (c, d), iset in by_cik_date.items():
            if c == cik and d0 <= d <= dt:
                insiders |= iset
        result[(cik, dt)] = len(insiders)
    return result


def _conviction_score(
    row: dict,
    tiers: dict,
    flags: dict,
    cluster_count: int,
    keywords: list[str],
    max_score: int = 10,
) -> tuple[int, list[str]]:
    """
    Returns (score, reasons) for a single filing row.
    Only P (open market purchase) transactions receive a non-zero score.
    """
    if row.get("transaction_code") != "P":
        return 0, []

    score = 0
    reasons: list[str] = []

    base = flags.get("base_open_market_buy", 0)
    if base:
        score += base
        reasons.append(f"Open market buy (+{base})")

    # Tiered bonuses — highest matching threshold only per family
    for family, brackets in tiers.items():
        if family == "value":
            v = row.get("total_value") or 0
            for threshold, points, label in brackets:  # descending order assumed
                if v >= threshold:
                    score += points
                    reasons.append(f"Trade value >= {_fmt_value(threshold)} (+{points})")
                    break
        elif family == "pct_holdings":
            raw = row.get("pct_holdings") or ""
            try:
                pct = float(raw.replace("%", "").replace("<", "").replace(">", ""))
            except ValueError:
                continue
            for threshold, points, label in brackets:
                if pct >= threshold:
                    score += points
                    reasons.append(f"% of holdings >= {threshold}% (+{points})")
                    break

    title = (row.get("insider_title") or "").lower()
    flag_checks = [
        (any(kw.lower() in title for kw in keywords), "ceo_cfo_bonus",           "C-suite insider"),
        (bool(row.get("is_director")),                 "director_bonus",          "Director"),
        (bool(row.get("is_ten_percent_owner")),        "ten_percent_owner_bonus", "10%+ owner"),
        (cluster_count >= 3,                           "cluster_bonus",           f"Cluster: {cluster_count} insiders buying"),
        (not row.get("is_10b5_1"),                     "non_10b5_1_buy",          "Not a 10b5-1 plan"),
    ]
    for condition, flag_key, label in flag_checks:
        if condition:
            pts = flags.get(flag_key, 0)
            if pts:
                score += pts
                reasons.append(f"{label} (+{pts})")

    final = min(score, max_score)
    return final, reasons


def _enrich(rows: list[sqlite3.Row], ctx: EnrichContext | None = None) -> list[dict]:
    result = []
    raw_dicts = [dict(r) for r in rows]

    cluster_counts: dict[tuple, int] = {}
    if ctx and ctx.compute_conviction and ctx.conn and ctx.conviction_flags:
        cluster_counts = _batch_cluster_counts(
            ctx.conn, raw_dicts, ctx.cluster_window_days
        )
    rows_to_process = raw_dicts

    thresholds = (ctx.conviction_thresholds or {}) if ctx else {}
    high_t = thresholds.get("high", 8)
    med_t = thresholds.get("medium", 5)

    for d in rows_to_process:
        d["total_value_fmt"] = _fmt_value(d.get("total_value"))
        d["price_fmt"] = _fmt_value(d.get("price_per_share"))
        d["filed_rel"] = _relative_time(d.get("filed_at"))
        d["pct_holdings"] = _pct_holdings(d)

        if ctx and ctx.compute_conviction and ctx.conviction_flags and ctx.conviction_tiers:
            cluster_n = cluster_counts.get(
                (d.get("issuer_cik"), d.get("transaction_date")), 0
            )
            score, reasons = _conviction_score(
                d,
                ctx.conviction_tiers,
                ctx.conviction_flags,
                cluster_n,
                ctx.ceo_cfo_keywords,
                ctx.conviction_max,
            )
            d["conviction"] = score
            d["conviction_reasons"] = reasons
            if score >= high_t:
                d["conviction_tier"] = "high"
            elif score >= med_t:
                d["conviction_tier"] = "medium"
            else:
                d["conviction_tier"] = "low"
        else:
            d["conviction"] = None
            d["conviction_reasons"] = []
            d["conviction_tier"] = "low"

        # Watchlist flag
        if ctx:
            ticker = d.get("issuer_ticker") or ""
            insider = d.get("insider_cik") or ""
            d["is_watched"] = (
                ticker in ctx.watched_tickers or insider in ctx.watched_insiders
            )
        else:
            d["is_watched"] = False

        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Dashboard queries
# ---------------------------------------------------------------------------

# SQL-sortable columns only. "conviction" is NOT here — it's computed in Python.
_SORT_COLUMNS = {
    "value":   "total_value",
    "shares":  "shares",
    "price":   "price_per_share",
    "ticker":  "issuer_ticker",
    "insider": "insider_name",
    "filed":   "filed_at",
}


def list_watchlist(conn: sqlite3.Connection) -> dict[str, list[dict]]:
    """Return {'tickers': [...], 'insiders': [...]} for the watchlist page."""
    rows = conn.execute(
        "SELECT id, type, value, label, created_at FROM watchlist ORDER BY created_at DESC"
    ).fetchall()
    tickers = [dict(r) for r in rows if r["type"] == "ticker"]
    insiders = [dict(r) for r in rows if r["type"] == "insider"]
    return {"tickers": tickers, "insiders": insiders}


def add_watch(conn: sqlite3.Connection, watch_type: str, value: str, label: str) -> None:
    conn.execute(
        "INSERT OR IGNORE INTO watchlist (type, value, label) VALUES (?, ?, ?)",
        [watch_type, value.strip(), label.strip()],
    )
    conn.commit()


def remove_watch(conn: sqlite3.Connection, watch_id: int) -> None:
    conn.execute("DELETE FROM watchlist WHERE id = ?", [watch_id])
    conn.commit()


def watched_tickers(conn: sqlite3.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT value FROM watchlist WHERE type = 'ticker'"
    ).fetchall()
    return {r[0] for r in rows}


def watched_insiders(conn: sqlite3.Connection) -> set[str]:
    """Returns a set of insider_cik values."""
    rows = conn.execute(
        "SELECT value FROM watchlist WHERE type = 'insider'"
    ).fetchall()
    return {r[0] for r in rows}


def get_all_sectors(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT sector FROM filings WHERE sector IS NOT NULL ORDER BY sector"
    ).fetchall()
    return [r[0] for r in rows]


def get_daily_summary(
    conn: sqlite3.Connection,
    start_date: date,
    end_date: date,
    hide_10b5_1: bool = True,
    min_value: float = 0,
    transaction_codes: list[str] | None = None,
) -> list[dict]:
    """Per-day aggregates for the date range summary view (shown when range > 7 days)."""
    codes = transaction_codes or ["P", "S"]
    ten_b = "AND is_10b5_1 = 0" if hide_10b5_1 else ""
    code_placeholders = ",".join("?" * len(codes))
    rows = conn.execute(f"""
        SELECT
            DATE(filed_at) AS day,
            SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) AS buy_count,
            COALESCE(SUM(CASE WHEN transaction_code='P' THEN total_value END), 0) AS buy_total,
            SUM(CASE WHEN transaction_code='S' THEN 1 ELSE 0 END) AS sell_count,
            COALESCE(SUM(CASE WHEN transaction_code='S' THEN total_value END), 0) AS sell_total,
            COUNT(DISTINCT CASE WHEN transaction_code='P' THEN issuer_cik END) AS issuers
        FROM filings
        WHERE DATE(filed_at) BETWEEN ? AND ?
          AND transaction_code IN ({code_placeholders})
          AND (total_value IS NULL OR total_value >= ?)
          AND superseded_by IS NULL
          {ten_b}
        GROUP BY DATE(filed_at)
        ORDER BY day DESC
    """, [start_date.isoformat(), end_date.isoformat(), *codes, min_value]).fetchall()

    result = []
    for r in rows:
        d = dict(r)
        d["buy_total_fmt"] = _fmt_value(d["buy_total"]) if d["buy_total"] else ""
        d["sell_total_fmt"] = _fmt_value(d["sell_total"]) if d["sell_total"] else ""
        net = (d["buy_total"] or 0) - (d["sell_total"] or 0)
        d["net_fmt"] = ("+" if net >= 0 else "") + (_fmt_value(abs(net)) or "")
        d["net_positive"] = net >= 0
        result.append(d)
    return result


def get_filings_for_date(
    conn: sqlite3.Connection,
    target_date: date,
    min_value: float = 0,
    transaction_codes: list[str] | None = None,
    hide_10b5_1: bool = True,
    roles: list[str] | None = None,
    search: str | None = None,
    ceo_cfo_only: bool = False,
    ceo_cfo_keywords: list[str] | None = None,
    sort_by: str = "value",
    sort_order: str = "desc",
    sector: str | None = None,
    watched_only: bool = False,
    date_range: tuple[date, date] | None = None,
    limit: int | None = None,
    ctx: EnrichContext | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (buys, sells) for a date or date range, applying all filters."""
    codes = transaction_codes or ["P", "S"]

    # Date condition: single date or range
    if date_range:
        date_condition = "DATE(filed_at) BETWEEN ? AND ?"
        params: list = [date_range[0].isoformat(), date_range[1].isoformat()]
    else:
        date_condition = "DATE(filed_at) = ?"
        params = [target_date.isoformat()]

    role_clauses = []
    if roles:
        if "director" in roles:
            role_clauses.append("is_director = 1")
        if "officer" in roles:
            role_clauses.append("is_officer = 1")
        if "ten_pct" in roles:
            role_clauses.append("is_ten_percent_owner = 1")

    order_col = _SORT_COLUMNS.get(sort_by, "total_value")
    order_dir = "ASC" if sort_order == "asc" else "DESC"
    sql_sort = f"ORDER BY {order_col} {order_dir} NULLS LAST"
    # Conviction sort happens in Python after enrichment
    if sort_by == "conviction":
        sql_sort = "ORDER BY total_value DESC NULLS LAST"

    base_where = f"""
        WHERE {date_condition}
          AND transaction_code IN ({{codes}})
          AND (total_value IS NULL OR total_value >= ?)
          AND superseded_by IS NULL
          {{ten_b}}
          {{role}}
          {{ceo}}
          {{search}}
          {{sec}}
          {{watched}}
    """.format(
        codes=",".join("?" * len(codes)),
        ten_b="AND is_10b5_1 = 0" if hide_10b5_1 else "",
        role=("AND (" + " OR ".join(role_clauses) + ")") if role_clauses else "",
        ceo=("AND (" + " OR ".join("insider_title LIKE ?" for _ in (ceo_cfo_keywords or [])) + ")") if ceo_cfo_only and ceo_cfo_keywords else "",
        search="AND (issuer_ticker LIKE ? OR issuer_name LIKE ? OR insider_name LIKE ?)" if search else "",
        sec="AND sector = ?" if sector else "",
        watched="" if not watched_only else (
            "AND (issuer_ticker IN (SELECT value FROM watchlist WHERE type='ticker') "
            "OR insider_cik IN (SELECT value FROM watchlist WHERE type='insider'))"
        ),
    )

    params += codes
    params.append(min_value)
    if ceo_cfo_only and ceo_cfo_keywords:
        params += [f"%{kw}%" for kw in ceo_cfo_keywords]
    if search:
        s = f"%{search}%"
        params += [s, s, s]
    if sector:
        params.append(sector)

    sql = f"""
        SELECT transaction_id, accession_no, filed_at,
               issuer_cik, issuer_ticker, issuer_name,
               insider_cik, insider_name, insider_title,
               transaction_code, shares, price_per_share, total_value,
               shares_owned_after, is_10b5_1, is_director, is_officer,
               is_ten_percent_owner, ownership_type, table_type,
               transaction_date
        FROM filings
        {base_where}
        {sql_sort}
        {"LIMIT ?" if limit else ""}
    """

    if limit:
        params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    enriched = _enrich(rows, ctx=ctx)

    if sort_by == "conviction":
        reverse = sort_order != "asc"
        enriched.sort(key=lambda r: r.get("conviction") or -1, reverse=reverse)

    buys = [r for r in enriched if r["transaction_code"] == "P"]
    sells = [r for r in enriched if r["transaction_code"] == "S"]
    return buys, sells


def get_summary_stats(conn: sqlite3.Connection, target_date: date, hide_10b5_1: bool = True) -> dict:
    d = target_date.isoformat()
    ten_b = "AND is_10b5_1 = 0" if hide_10b5_1 else ""

    buys = conn.execute(f"""
        SELECT COUNT(*), COALESCE(SUM(total_value),0)
        FROM filings WHERE DATE(filed_at)=? AND transaction_code='P' {ten_b}
    """, [d]).fetchone()

    sells = conn.execute(f"""
        SELECT COUNT(*), COALESCE(SUM(total_value),0)
        FROM filings WHERE DATE(filed_at)=? AND transaction_code='S' {ten_b}
    """, [d]).fetchone()

    issuers = conn.execute(f"""
        SELECT COUNT(DISTINCT issuer_cik) FROM filings
        WHERE DATE(filed_at)=? AND transaction_code IN ('P','S') {ten_b}
    """, [d]).fetchone()

    clusters = conn.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT issuer_cik FROM filings
          WHERE DATE(filed_at)=? AND transaction_code='P' {ten_b}
          GROUP BY issuer_cik HAVING COUNT(DISTINCT insider_cik) >= 2
        )
    """, [d]).fetchone()

    net = (buys[1] or 0) - (sells[1] or 0)
    return {
        "buy_count": buys[0] or 0,
        "sell_count": sells[0] or 0,
        "net_flow": net,
        "net_flow_fmt": _fmt_value(net),
        "issuer_count": issuers[0] or 0,
        "cluster_count": clusters[0] or 0,
        "buy_total_fmt": _fmt_value(buys[1]),
        "sell_total_fmt": _fmt_value(sells[1]),
    }


def get_cluster_activity(
    conn: sqlite3.Connection,
    target_date: date,
    min_insiders: int = 2,
    hide_10b5_1: bool = True,
    date_range: tuple[date, date] | None = None,
) -> list[dict]:
    if date_range:
        date_condition = "DATE(filed_at) BETWEEN ? AND ?"
        date_params: list = [date_range[0].isoformat(), date_range[1].isoformat()]
    else:
        date_condition = "DATE(filed_at) = ?"
        date_params = [target_date.isoformat()]
    ten_b = "AND is_10b5_1 = 0" if hide_10b5_1 else ""

    rows = conn.execute(f"""
        SELECT
            issuer_ticker, issuer_name,
            MAX(sector) AS sector,
            CASE WHEN SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) > 0
                  AND SUM(CASE WHEN transaction_code='S' THEN 1 ELSE 0 END) > 0
                 THEN 'mixed'
                 WHEN SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) > 0
                 THEN 'buy' ELSE 'sell'
            END AS direction,
            COUNT(DISTINCT insider_cik) AS insider_count,
            COUNT(*) AS tx_count,
            COALESCE(SUM(total_value), 0) AS total_value,
            GROUP_CONCAT(DISTINCT insider_name) AS insider_names,
            GROUP_CONCAT(DISTINCT COALESCE(insider_title, '')) AS insider_titles
        FROM filings
        WHERE {date_condition}
          AND transaction_code IN ('P', 'S')
          AND superseded_by IS NULL
          {ten_b}
          AND issuer_ticker IS NOT NULL
        GROUP BY issuer_ticker, issuer_name
        HAVING COUNT(DISTINCT insider_cik) >= ?
        ORDER BY total_value DESC
    """, [*date_params, min_insiders]).fetchall()

    result = []
    for r in rows:
        d_row = dict(r)
        d_row["total_value_fmt"] = _fmt_value(d_row["total_value"])

        tx_rows = conn.execute(f"""
            SELECT transaction_id, insider_name, insider_title,
                   transaction_code, shares, price_per_share, total_value, is_10b5_1
            FROM filings
            WHERE {date_condition} AND issuer_ticker = ?
              AND transaction_code IN ('P','S') {ten_b}
            ORDER BY total_value DESC NULLS LAST
        """, [*date_params, d_row["issuer_ticker"]]).fetchall()

        d_row["transactions"] = [
            {**dict(tx),
             "total_value_fmt": _fmt_value(tx["total_value"]),
             "price_fmt": _fmt_value(tx["price_per_share"])}
            for tx in tx_rows
        ]
        result.append(d_row)
    return result


def get_filing_detail(
    conn: sqlite3.Connection,
    transaction_id: str,
    ctx: EnrichContext | None = None,
) -> dict | None:
    row = conn.execute(
        "SELECT * FROM filings WHERE transaction_id = ?", [transaction_id]
    ).fetchone()
    if row is None:
        return None
    enriched = _enrich([row], ctx=ctx)
    d = enriched[0]
    # If this row was superseded, attach the amendment accession for display
    if d.get("superseded_by"):
        amendment = conn.execute(
            "SELECT transaction_id FROM filings WHERE accession_no = ? LIMIT 1",
            [d["superseded_by"]],
        ).fetchone()
        d["amended_by_transaction_id"] = amendment[0] if amendment else None
    return d


def get_issuer_filings(
    conn: sqlite3.Connection,
    ticker: str,
    days: int = 90,
) -> list[dict]:
    since = (date.today() - timedelta(days=days)).isoformat()
    rows = conn.execute(
        """
        SELECT * FROM filings
        WHERE issuer_ticker = ? AND DATE(filed_at) >= ?
          AND superseded_by IS NULL
        ORDER BY filed_at DESC
""",
        [ticker.upper(), since],
    ).fetchall()
    return _enrich(rows)


def _week_start(d: date) -> str:
    """ISO date string of the Monday of the week containing d. Portable across all SQLite versions."""
    return (d - timedelta(days=d.weekday())).isoformat()


def get_issuer_trend(conn: sqlite3.Connection, ticker: str) -> list[dict]:
    """Returns 26 weekly data points (≈ 6 months) for the buy/sell sparkline."""
    today = date.today()
    cutoff = (today - timedelta(weeks=26)).isoformat()

    # Use Monday-date keys to avoid %Y-%W year-boundary collisions across all SQLite versions.
    # (strftime('%G-%V') would be cleaner but requires SQLite 3.38+; Ubuntu 22.04 ships 3.37)
    rows = conn.execute("""
        SELECT DATE(filed_at, printf('-%d days', (strftime('%w', filed_at) + 6) % 7)) AS week_start,
               COALESCE(SUM(CASE WHEN transaction_code='P' THEN total_value END), 0) AS buy_total,
               COALESCE(SUM(CASE WHEN transaction_code='S' THEN total_value END), 0) AS sell_total
        FROM filings
        WHERE issuer_ticker = ? AND DATE(filed_at) >= ?
          AND superseded_by IS NULL
        GROUP BY week_start
        ORDER BY week_start
    """, [ticker.upper(), cutoff]).fetchall()

    lookup = {r[0]: (r[1] or 0, r[2] or 0) for r in rows}

    series = []
    for i in range(25, -1, -1):
        key = _week_start(today - timedelta(weeks=i))
        buy, sell = lookup.get(key, (0, 0))
        series.append({'week': key, 'buy_total': buy, 'sell_total': sell})

    return series


def get_run_log(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM run_log ORDER BY started_at DESC LIMIT ?", [limit]
    ).fetchall()
    return [dict(r) for r in rows]


def get_insider_history(
    conn: sqlite3.Connection,
    insider_cik: str,
    limit: int = 10,
) -> list[dict]:
    """
    Last N transactions by this insider across all companies.
    Uses a window function to flag the largest buy ever so it can be highlighted.
    Requires SQLite 3.25+ (Ubuntu 22.04 ships 3.37).
    """
    rows = conn.execute(
        """
        SELECT *,
          CASE
            WHEN transaction_code = 'P' AND total_value IS NOT NULL
              AND total_value = MAX(
                    CASE WHEN transaction_code = 'P' THEN total_value END
                  ) OVER (PARTITION BY insider_cik)
            THEN 1 ELSE 0
          END AS is_largest_buy
        FROM filings
        WHERE insider_cik = ?
          AND superseded_by IS NULL
        ORDER BY transaction_date DESC
        LIMIT ?
        """,
        [insider_cik, limit],
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["total_value_fmt"] = _fmt_value(d.get("total_value"))
        d["price_fmt"] = _fmt_value(d.get("price_per_share"))
        result.append(d)
    return result


def get_issuer_recent_insiders(
    conn: sqlite3.Connection,
    issuer_cik: str,
    days: int = 90,
    exclude_transaction_id: str | None = None,
) -> list[dict]:
    """
    All distinct insiders active at this issuer in the last N days,
    grouped by insider with buy/sell aggregates. Used for the
    'Other insiders at X' sidebar on the filing detail page.
    """
    since = (date.today() - timedelta(days=days)).isoformat()
    rows = conn.execute(
        """
        SELECT
            insider_cik, insider_name, insider_title,
            SUM(CASE WHEN transaction_code='P' THEN COALESCE(total_value,0) ELSE 0 END) AS total_bought,
            SUM(CASE WHEN transaction_code='S' THEN COALESCE(total_value,0) ELSE 0 END) AS total_sold,
            MAX(transaction_date) AS last_date,
            MAX(transaction_id) AS latest_transaction_id
        FROM filings
        WHERE issuer_cik = ?
          AND DATE(filed_at) >= ?
          AND (? IS NULL OR transaction_id != ?)
          AND superseded_by IS NULL
        GROUP BY insider_cik, insider_name, insider_title
        ORDER BY total_bought DESC
        """,
        [issuer_cik, since, exclude_transaction_id, exclude_transaction_id],
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["total_bought_fmt"] = _fmt_value(d.get("total_bought") or None)
        d["total_sold_fmt"] = _fmt_value(d.get("total_sold") or None)
        result.append(d)
    return result


def get_recent_alerts(conn: sqlite3.Connection, limit: int = 10) -> list[dict]:
    rows = conn.execute(
        "SELECT alert_key, alert_type, sent_at FROM alerts_sent ORDER BY sent_at DESC LIMIT ?",
        [limit],
    ).fetchall()
    return [dict(r) for r in rows]


def get_10b5_1_stats(conn: sqlite3.Connection) -> dict:
    total = conn.execute("SELECT COUNT(*) FROM filings").fetchone()[0]
    flagged_xml = conn.execute(
        "SELECT COUNT(*) FROM filings WHERE is_10b5_1=1"
    ).fetchone()[0]
    footnote_only = conn.execute(
        """SELECT COUNT(*) FROM filings
           WHERE is_10b5_1=1 AND footnote_text LIKE '%10b5-1%'"""
    ).fetchone()[0]
    return {
        "total_filings": total,
        "flagged": flagged_xml,
        "footnote_only": footnote_only,
    }
