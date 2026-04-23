"""All SQL queries for the dashboard. No SQL in app.py."""
from __future__ import annotations

import sqlite3
from datetime import date, datetime, timedelta, timezone


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


def _enrich(rows: list[sqlite3.Row]) -> list[dict]:
    result = []
    for r in rows:
        d = dict(r)
        d["total_value_fmt"] = _fmt_value(d.get("total_value"))
        d["price_fmt"] = _fmt_value(d.get("price_per_share"))
        d["filed_rel"] = _relative_time(d.get("filed_at"))
        result.append(d)
    return result


# ---------------------------------------------------------------------------
# Dashboard queries
# ---------------------------------------------------------------------------

def get_filings_for_date(
    conn: sqlite3.Connection,
    target_date: date,
    min_value: float = 0,
    transaction_codes: list[str] | None = None,
    hide_10b5_1: bool = True,
    roles: list[str] | None = None,
    search: str | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (buys, sells) for the given date, applying all filters."""
    codes = transaction_codes or ["P", "S"]
    params: list = [target_date.isoformat()]

    role_clauses = []
    if roles:
        if "director" in roles:
            role_clauses.append("is_director = 1")
        if "officer" in roles:
            role_clauses.append("is_officer = 1")
        if "ten_pct" in roles:
            role_clauses.append("is_ten_percent_owner = 1")

    base_where = """
        WHERE DATE(filed_at) = ?
          AND transaction_code IN ({codes})
          AND (total_value IS NULL OR total_value >= ?)
          {ten_b}
          {role}
          {search}
    """.format(
        codes=",".join("?" * len(codes)),
        ten_b="AND is_10b5_1 = 0" if hide_10b5_1 else "",
        role=("AND (" + " OR ".join(role_clauses) + ")") if role_clauses else "",
        search="AND (issuer_ticker LIKE ? OR issuer_name LIKE ? OR insider_name LIKE ?)" if search else "",
    )

    params += codes
    params.append(min_value)
    if search:
        s = f"%{search}%"
        params += [s, s, s]

    sql = f"""
        SELECT transaction_id, accession_no, filed_at,
               issuer_ticker, issuer_name,
               insider_name, insider_title,
               transaction_code, shares, price_per_share, total_value,
               is_10b5_1, is_director, is_officer, is_ten_percent_owner,
               ownership_type, table_type
        FROM filings
        {base_where}
        ORDER BY total_value DESC NULLS LAST
    """

    rows = conn.execute(sql, params).fetchall()
    enriched = _enrich(rows)
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

    # Cluster: 2+ distinct insiders buying at same issuer on this date
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
) -> list[dict]:
    """
    Return tickers where 2+ distinct insiders traded on the same day.
    Each row covers one ticker+direction combo (buys separate from sells).
    Sorted by total value desc.
    """
    d = target_date.isoformat()
    ten_b = "AND is_10b5_1 = 0" if hide_10b5_1 else ""

    rows = conn.execute(f"""
        SELECT
            issuer_ticker,
            issuer_name,
            CASE WHEN SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) > 0
                  AND SUM(CASE WHEN transaction_code='S' THEN 1 ELSE 0 END) > 0
                 THEN 'mixed'
                 WHEN SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) > 0
                 THEN 'buy'
                 ELSE 'sell'
            END AS direction,
            COUNT(DISTINCT insider_cik) AS insider_count,
            COUNT(*) AS tx_count,
            COALESCE(SUM(total_value), 0) AS total_value,
            GROUP_CONCAT(DISTINCT insider_name) AS insider_names,
            GROUP_CONCAT(DISTINCT COALESCE(insider_title, '')) AS insider_titles
        FROM filings
        WHERE DATE(filed_at) = ?
          AND transaction_code IN ('P', 'S')
          {ten_b}
          AND issuer_ticker IS NOT NULL
        GROUP BY issuer_ticker, issuer_name
        HAVING COUNT(DISTINCT insider_cik) >= ?
        ORDER BY total_value DESC
    """, [d, min_insiders]).fetchall()

    result = []
    for r in rows:
        d_row = dict(r)
        d_row["total_value_fmt"] = _fmt_value(d_row["total_value"])

        # Fetch the individual transactions for the expanded view
        tx_rows = conn.execute(f"""
            SELECT transaction_id, insider_name, insider_title,
                   transaction_code, shares, price_per_share, total_value, is_10b5_1
            FROM filings
            WHERE DATE(filed_at) = ? AND issuer_ticker = ?
              AND transaction_code IN ('P','S') {ten_b}
            ORDER BY total_value DESC NULLS LAST
        """, [d, d_row["issuer_ticker"]]).fetchall()

        d_row["transactions"] = [
            {**dict(tx),
             "total_value_fmt": _fmt_value(tx["total_value"]),
             "price_fmt": _fmt_value(tx["price_per_share"])}
            for tx in tx_rows
        ]
        result.append(d_row)
    return result


def get_filing_detail(conn: sqlite3.Connection, transaction_id: str) -> dict | None:
    row = conn.execute(
        "SELECT * FROM filings WHERE transaction_id = ?", [transaction_id]
    ).fetchone()
    if row is None:
        return None
    d = dict(row)
    d["total_value_fmt"] = _fmt_value(d.get("total_value"))
    d["price_fmt"] = _fmt_value(d.get("price_per_share"))
    d["filed_rel"] = _relative_time(d.get("filed_at"))
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
        ORDER BY filed_at DESC
        """,
        [ticker.upper(), since],
    ).fetchall()
    return _enrich(rows)


def get_run_log(conn: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        "SELECT * FROM run_log ORDER BY started_at DESC LIMIT ?", [limit]
    ).fetchall()
    return [dict(r) for r in rows]


def get_10b5_1_stats(conn: sqlite3.Connection) -> dict:
    """Stats shown on the Logic page."""
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
