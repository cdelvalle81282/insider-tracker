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
# Market cap tier definitions — used by get_filings_for_date() and app.py
# ---------------------------------------------------------------------------

MARKET_CAP_TIERS = {
    "micro": (0, 300_000_000),
    "small": (300_000_000, 2_000_000_000),
    "mid":   (2_000_000_000, 10_000_000_000),
    "large": (10_000_000_000, 200_000_000_000),
    "mega":  (200_000_000_000, 1e15),
}


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
    if row.get("table_type") == "D":
        return None  # derivative units ≠ actual shares; ratio is meaningless
    shares = row.get("shares")
    after = row.get("shares_owned_after")
    code = row.get("transaction_code")
    if not shares or not after or after <= 0:
        return None
    if code == "P":
        if after <= shares:
            return None  # shares_owned_after == shares_bought: initial/standalone position, ratio not meaningful
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
              AND superseded_by IS NULL
              AND joint_filer_of IS NULL""",
        all_ciks + [global_start, global_end],
    ).fetchall()

    by_cik: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in db_rows:
        by_cik[r[0]][r[1]].add(r[2])

    result: dict[tuple, int] = {}
    for (cik, dt) in pairs:
        d0 = (date.fromisoformat(dt) - timedelta(days=window_days)).isoformat()
        insiders: set = set()
        for d_key, iset in by_cik.get(cik, {}).items():
            if d0 <= d_key <= dt:
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

    # Batch-fetch last_close prices for all tickers in this result set
    prices: dict[str, float] = {}
    if ctx and ctx.conn:
        _tickers = {r.get("issuer_ticker") for r in raw_dicts if r.get("issuer_ticker")}
        if _tickers:
            _ph = ",".join("?" * len(_tickers))
            _price_rows = ctx.conn.execute(
                f"SELECT ticker, last_close FROM ticker_metadata WHERE ticker IN ({_ph})",
                list(_tickers),
            ).fetchall()
            prices = {r[0]: r[1] for r in _price_rows if r[1] is not None}

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
        try:
            filed_d = datetime.fromisoformat(str(d.get("filed_at") or "")).date()
            tx_d = date.fromisoformat(str(d.get("transaction_date") or ""))
            d["disclosure_lag"] = (filed_d - tx_d).days
        except (ValueError, TypeError):
            d["disclosure_lag"] = None
        d["pct_holdings"] = _pct_holdings(d)

        last = prices.get(d.get("issuer_ticker") or "")
        pps = d.get("price_per_share")
        if last and pps and pps > 0 and d.get("transaction_code") == "P":
            d["price_perf_pct"] = round((last - pps) / pps * 100, 1)
        else:
            d["price_perf_pct"] = None

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
    hide_equity_swap: bool = True,
) -> list[dict]:
    """Per-day aggregates for the date range summary view (shown when range > 7 days)."""
    codes = transaction_codes or ["P", "S"]
    ten_b = "AND is_10b5_1 = 0" if hide_10b5_1 else ""
    swap_f = "AND equity_swap = 0" if hide_equity_swap else ""
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
          AND joint_filer_of IS NULL
          {ten_b}
          {swap_f}
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


def _build_filings_where(
    target_date: date,
    *,
    transaction_codes: list[str],
    min_value: float = 0,
    hide_10b5_1: bool = True,
    hide_equity_swap: bool = True,
    roles: list[str] | None = None,
    search: str | None = None,
    ceo_cfo_only: bool = False,
    ceo_cfo_keywords: list[str] | None = None,
    sector: str | None = None,
    watched_only: bool = False,
    date_range: tuple[date, date] | None = None,
    hide_funds: bool = False,
    has_options_only: bool = False,
    market_cap_tiers: list[str] | None = None,
) -> tuple[str, list]:
    """Build WHERE clause and params for filings queries.
    Returns (where_sql, params) where where_sql starts with 'WHERE ...'."""
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

    # Build market cap tier SQL.
    # valid_tiers is filtered strictly against MARKET_CAP_TIERS keys — unknown values are dropped.
    # The SQL fragment is built from the *count* of tiers (an int), not from the tier name strings,
    # so no user-supplied value ever reaches the SQL string.
    valid_tiers = [t for t in (market_cap_tiers or []) if t in MARKET_CAP_TIERS]
    _n_tiers = len(valid_tiers)  # int only — taint path from user input ends here
    if _n_tiers:
        _single_range = "(market_cap >= ? AND market_cap < ?)"
        tier_range_clauses = (" OR ".join([_single_range] * _n_tiers))
        mktcap_sql = (
            "AND (\n"
            "    issuer_ticker NOT IN (SELECT ticker FROM ticker_metadata WHERE market_cap IS NOT NULL)\n"
            "    OR issuer_ticker IN (\n"
            "        SELECT ticker FROM ticker_metadata WHERE\n"
            "        " + tier_range_clauses + "\n"
            "    )\n"
            ")"
        )
        mktcap_params = [v for t in valid_tiers for v in MARKET_CAP_TIERS[t]]
    else:
        mktcap_sql = ""
        mktcap_params = []

    # All SQL fragments below are built from fixed string constants, never from user input.
    # Boolean flags (hide_10b5_1, has_options_only, etc.) are already bool by the time
    # they arrive here — the ternary outcomes are literal constant strings.
    _frag_ten_b    = "AND is_10b5_1 = 0" if hide_10b5_1 else ""
    _frag_swap     = "AND equity_swap = 0" if hide_equity_swap else ""
    _frag_role     = ("AND (" + " OR ".join(role_clauses) + ")") if role_clauses else ""
    _frag_ceo      = (
        "AND (" + " OR ".join("insider_title LIKE ?" for _ in (ceo_cfo_keywords or [])) + ")"
        if ceo_cfo_only and ceo_cfo_keywords else ""
    )
    _frag_search   = "AND (issuer_ticker LIKE ? OR issuer_name LIKE ? OR insider_name LIKE ?)" if search else ""
    _frag_sec      = "AND sector = ?" if sector else ""
    _frag_watched  = (
        "AND (issuer_ticker IN (SELECT value FROM watchlist WHERE type='ticker') "
        "OR insider_cik IN (SELECT value FROM watchlist WHERE type='insider'))"
        if watched_only else ""
    )
    _frag_funds    = (
        "AND issuer_cik NOT IN (SELECT issuer_cik FROM sectors WHERE sic_code IN ('6726','6798'))"
        if hide_funds else ""
    )
    _frag_options  = (
        "AND issuer_ticker IN (SELECT ticker FROM ticker_metadata WHERE has_options = 1)"
        if has_options_only else ""
    )

    where_sql = f"""
        WHERE {date_condition}
          AND transaction_code IN ({{codes}})
          AND (total_value IS NULL OR total_value >= ?)
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          {_frag_ten_b}
          {_frag_swap}
          {_frag_role}
          {_frag_ceo}
          {_frag_search}
          {_frag_sec}
          {_frag_watched}
          {_frag_funds}
          {_frag_options}
          {mktcap_sql}
    """.format(codes=",".join("?" * len(transaction_codes)))

    # Param order must mirror placeholder order in the WHERE clause exactly:
    # date -> codes -> min_value -> ceo_keywords -> search -> sector -> mktcap.
    params += transaction_codes
    params.append(min_value)
    if ceo_cfo_only and ceo_cfo_keywords:
        params += [f"%{kw}%" for kw in ceo_cfo_keywords]
    if search:
        s = f"%{search}%"
        params += [s, s, s]
    if sector:
        params.append(sector)
    params += mktcap_params

    return where_sql, params


def get_filings_for_date(
    conn: sqlite3.Connection,
    target_date: date,
    min_value: float = 0,
    transaction_codes: list[str] | None = None,
    hide_10b5_1: bool = True,
    hide_equity_swap: bool = True,
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
    hide_funds: bool = False,
    has_options_only: bool = False,
    market_cap_tiers: list[str] | None = None,
    buys_page: int = 1,
    sells_page: int = 1,
    page_size: int | None = None,
) -> tuple[list[dict], list[dict]]:
    """Return (buys, sells) for a date or date range, applying all filters.

    When page_size is provided, P-side and S-side queries are paginated independently
    via buys_page and sells_page. When page_size is None (export/backtest path), the
    legacy `limit` param is honored as a top-level LIMIT.
    """
    codes = transaction_codes or ["P", "S"]

    # Both values come from fixed constant maps — no user input reaches the SQL string.
    _safe_col = _SORT_COLUMNS.get(sort_by, "total_value")
    assert _safe_col in _SORT_COLUMNS.values(), f"Unexpected sort column: {_safe_col!r}"
    _safe_dir = "ASC" if sort_order == "asc" else "DESC"
    use_conviction = (sort_by == "conviction")
    if use_conviction:
        sql_sort = "ORDER BY total_value DESC NULLS LAST"
    else:
        sql_sort = f"ORDER BY {_safe_col} {_safe_dir} NULLS LAST"

    select_cols = """
        SELECT transaction_id, accession_no, filed_at,
               issuer_cik, issuer_ticker, issuer_name,
               insider_cik, insider_name, insider_title,
               transaction_code, shares, price_per_share, total_value,
               shares_owned_after, is_10b5_1, is_director, is_officer,
               is_ten_percent_owner, ownership_type, table_type,
               transaction_date
        FROM filings
    """

    # Iterate per-side (P, S) so each side can be paginated independently.
    sides: list[tuple[str, int]] = []
    if "P" in codes:
        sides.append(("P", buys_page))
    if "S" in codes:
        sides.append(("S", sells_page))

    all_rows: list = []
    for side_code, page in sides:
        where_sql, side_params = _build_filings_where(
            target_date,
            transaction_codes=[side_code],
            min_value=min_value,
            hide_10b5_1=hide_10b5_1,
            hide_equity_swap=hide_equity_swap,
            roles=roles,
            search=search,
            ceo_cfo_only=ceo_cfo_only,
            ceo_cfo_keywords=ceo_cfo_keywords,
            sector=sector,
            watched_only=watched_only,
            date_range=date_range,
            hide_funds=hide_funds,
            has_options_only=has_options_only,
            market_cap_tiers=market_cap_tiers,
        )

        # Pagination vs legacy limit:
        #   - page_size provided => paginated path (LIMIT ? OFFSET ?), legacy limit ignored
        #   - page_size None     => export/backtest path; legacy limit applied if set
        if page_size is not None and not use_conviction:
            sql = f"{select_cols}\n{where_sql}\n{sql_sort}\nLIMIT ? OFFSET ?"
            side_params.append(page_size)
            side_params.append((page - 1) * page_size)
        else:
            sql = f"{select_cols}\n{where_sql}\n{sql_sort}"
            if page_size is None and limit:
                sql += "\nLIMIT ?"
                side_params.append(limit)

        rows = conn.execute(sql, side_params).fetchall()
        all_rows.extend(rows)

    # Single _enrich call across both sides so cluster counting stays batched.
    enriched = _enrich(all_rows, ctx=ctx)

    buys = [r for r in enriched if r["transaction_code"] == "P"]
    sells = [r for r in enriched if r["transaction_code"] == "S"]

    if use_conviction:
        # Python-sort each side by conviction desc, then slice for pagination.
        buys = sorted(buys, key=lambda r: r.get("conviction") or 0, reverse=True)
        sells = sorted(sells, key=lambda r: r.get("conviction") or 0, reverse=True)
        if page_size is not None:
            b_start = (buys_page - 1) * page_size
            s_start = (sells_page - 1) * page_size
            buys = buys[b_start : b_start + page_size]
            sells = sells[s_start : s_start + page_size]

    return buys, sells


def get_filings_count(
    conn: sqlite3.Connection,
    target_date: date,
    *,
    min_value: float = 0,
    transaction_codes: list[str] | None = None,
    hide_10b5_1: bool = True,
    hide_equity_swap: bool = True,
    roles: list[str] | None = None,
    search: str | None = None,
    ceo_cfo_only: bool = False,
    ceo_cfo_keywords: list[str] | None = None,
    sector: str | None = None,
    watched_only: bool = False,
    date_range: tuple[date, date] | None = None,
    hide_funds: bool = False,
    has_options_only: bool = False,
    market_cap_tiers: list[str] | None = None,
) -> tuple[int, int]:
    """Returns (buy_count, sell_count) using the same WHERE clauses as get_filings_for_date.
    Must accept the exact same filter kwargs (minus pagination/sort/ctx/limit)."""
    codes = transaction_codes or ["P", "S"]

    common_kwargs = dict(
        min_value=min_value,
        hide_10b5_1=hide_10b5_1,
        hide_equity_swap=hide_equity_swap,
        roles=roles,
        search=search,
        ceo_cfo_only=ceo_cfo_only,
        ceo_cfo_keywords=ceo_cfo_keywords,
        sector=sector,
        watched_only=watched_only,
        date_range=date_range,
        hide_funds=hide_funds,
        has_options_only=has_options_only,
        market_cap_tiers=market_cap_tiers,
    )

    if "P" in codes:
        buy_where, buy_params = _build_filings_where(
            target_date, transaction_codes=["P"], **common_kwargs
        )
        buy_count = conn.execute(
            f"SELECT COUNT(*) FROM filings {buy_where}", buy_params
        ).fetchone()[0]
    else:
        buy_count = 0

    if "S" in codes:
        sell_where, sell_params = _build_filings_where(
            target_date, transaction_codes=["S"], **common_kwargs
        )
        sell_count = conn.execute(
            f"SELECT COUNT(*) FROM filings {sell_where}", sell_params
        ).fetchone()[0]
    else:
        sell_count = 0

    return buy_count, sell_count


def get_summary_stats(
    conn: sqlite3.Connection,
    target_date: date,
    hide_10b5_1: bool = True,
    hide_equity_swap: bool = True,
) -> dict:
    d = target_date.isoformat()
    ten_b  = "AND is_10b5_1 = 0"   if hide_10b5_1    else ""
    swap_f = "AND equity_swap = 0"  if hide_equity_swap else ""

    row = conn.execute(f"""
        SELECT
            SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) AS buy_count,
            COALESCE(SUM(CASE WHEN transaction_code='P' THEN total_value END), 0) AS buy_total,
            SUM(CASE WHEN transaction_code='S' THEN 1 ELSE 0 END) AS sell_count,
            COALESCE(SUM(CASE WHEN transaction_code='S' THEN total_value END), 0) AS sell_total,
            COUNT(DISTINCT issuer_cik) AS issuer_count
        FROM filings
        WHERE DATE(filed_at)=? AND transaction_code IN ('P','S')
          AND superseded_by IS NULL AND joint_filer_of IS NULL {ten_b} {swap_f}
    """, [d]).fetchone()

    clusters = conn.execute(f"""
        SELECT COUNT(*) FROM (
          SELECT issuer_cik FROM filings
          WHERE DATE(filed_at)=? AND transaction_code='P'
            AND superseded_by IS NULL AND joint_filer_of IS NULL {ten_b} {swap_f}
          GROUP BY issuer_cik HAVING COUNT(DISTINCT insider_cik) >= 2
        )
    """, [d]).fetchone()

    buy_total = row[1] or 0
    sell_total = row[3] or 0
    net = buy_total - sell_total
    return {
        "buy_count": row[0] or 0,
        "sell_count": row[2] or 0,
        "net_flow": net,
        "net_flow_fmt": _fmt_value(net),
        "issuer_count": row[4] or 0,
        "cluster_count": clusters[0] or 0,
        "buy_total_fmt": _fmt_value(buy_total),
        "sell_total_fmt": _fmt_value(sell_total),
    }


def get_cluster_activity(
    conn: sqlite3.Connection,
    target_date: date,
    min_insiders: int = 2,
    hide_10b5_1: bool = True,
    hide_equity_swap: bool = True,
    date_range: tuple[date, date] | None = None,
) -> list[dict]:
    if date_range:
        date_condition = "DATE(filed_at) BETWEEN ? AND ?"
        date_params: list = [date_range[0].isoformat(), date_range[1].isoformat()]
    else:
        date_condition = "DATE(filed_at) = ?"
        date_params = [target_date.isoformat()]
    ten_b  = "AND is_10b5_1 = 0"  if hide_10b5_1    else ""
    swap_f = "AND equity_swap = 0" if hide_equity_swap else ""

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
          AND joint_filer_of IS NULL
          {ten_b}
          {swap_f}
          AND issuer_ticker IS NOT NULL
        GROUP BY issuer_ticker, issuer_name
        HAVING COUNT(DISTINCT insider_cik) >= ?
        ORDER BY total_value DESC
    """, [*date_params, min_insiders]).fetchall()

    if not rows:
        return []

    cluster_tickers = [dict(r)["issuer_ticker"] for r in rows]
    ticker_placeholders = ",".join("?" * len(cluster_tickers))
    all_tx = conn.execute(f"""
        SELECT transaction_id, insider_name, insider_title,
               transaction_code, shares, price_per_share, total_value, is_10b5_1,
               issuer_ticker
        FROM filings
        WHERE {date_condition}
          AND issuer_ticker IN ({ticker_placeholders})
          AND transaction_code IN ('P','S') {ten_b} {swap_f}
        ORDER BY total_value DESC NULLS LAST
    """, [*date_params, *cluster_tickers]).fetchall()

    tx_by_ticker: dict[str, list[dict]] = defaultdict(list)
    for tx in all_tx:
        tx_d = dict(tx)
        tx_d["total_value_fmt"] = _fmt_value(tx_d["total_value"])
        tx_d["price_fmt"] = _fmt_value(tx_d["price_per_share"])
        tx_by_ticker[tx_d["issuer_ticker"]].append(tx_d)

    result = []
    for r in rows:
        d_row = dict(r)
        d_row["total_value_fmt"] = _fmt_value(d_row["total_value"])
        d_row["transactions"] = tx_by_ticker.get(d_row["issuer_ticker"], [])
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
          AND joint_filer_of IS NULL
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
          AND joint_filer_of IS NULL
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
          AND joint_filer_of IS NULL
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


def get_insider_full_history(
    conn: sqlite3.Connection,
    insider_cik: str,
    ctx: EnrichContext | None = None,
) -> list[dict]:
    """All transactions by this insider across all companies, newest first."""
    rows = conn.execute(
        """
        SELECT *
        FROM filings
        WHERE insider_cik = ?
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        ORDER BY transaction_date DESC, filed_at DESC
        """,
        [insider_cik],
    ).fetchall()
    return _enrich(rows, ctx=ctx)


def get_insider_summary(conn: sqlite3.Connection, insider_cik: str) -> dict:
    """Aggregate stats for a single insider across all filings."""
    row = conn.execute(
        """
        SELECT
            (SELECT insider_name FROM filings
             WHERE insider_cik = ? ORDER BY filed_at DESC LIMIT 1) AS name,
            SUM(CASE WHEN transaction_code = 'P' THEN COALESCE(total_value, 0) ELSE 0 END) AS total_bought,
            SUM(CASE WHEN transaction_code = 'S' THEN COALESCE(total_value, 0) ELSE 0 END) AS total_sold,
            COUNT(DISTINCT issuer_cik) AS distinct_issuers,
            MIN(transaction_date) AS first_trade,
            MAX(transaction_date) AS last_trade
        FROM filings
        WHERE insider_cik = ?
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        """,
        [insider_cik, insider_cik],
    ).fetchone()
    if row is None:
        return {
            "name": None,
            "total_bought": 0,
            "total_sold": 0,
            "distinct_issuers": 0,
            "first_trade": None,
            "last_trade": None,
        }
    return dict(row)


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
          AND joint_filer_of IS NULL
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


# ---------------------------------------------------------------------------
# Ticker metadata (market cap, options availability)
# ---------------------------------------------------------------------------

def get_ticker_metadata_map(conn: sqlite3.Connection, tickers: list[str]) -> dict[str, dict]:
    """
    Batch-fetch ticker metadata rows for the given tickers in a single query.
    Returns {ticker: {'market_cap': float|None, 'has_options': int|None, 'fetched_at': str|None}}.
    Returns {} when tickers is empty.
    """
    if not tickers:
        return {}
    placeholders = ",".join("?" * len(tickers))
    rows = conn.execute(
        f"SELECT ticker, market_cap, has_options, fetched_at FROM ticker_metadata"
        f" WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchall()
    return {r["ticker"]: dict(r) for r in rows}


def upsert_ticker_metadata(
    conn: sqlite3.Connection,
    ticker: str,
    market_cap: float | None,
    has_options: int | None,
) -> None:
    """Insert or update ticker metadata, setting fetched_at to the current UTC time."""
    conn.execute(
        """
        INSERT INTO ticker_metadata (ticker, has_options, market_cap, fetched_at)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(ticker) DO UPDATE SET
            has_options = excluded.has_options,
            market_cap  = excluded.market_cap,
            fetched_at  = excluded.fetched_at
        """,
        [ticker, has_options, market_cap],
    )


# ---------------------------------------------------------------------------
# Congressional trades queries
# ---------------------------------------------------------------------------

_CONGRESS_SORT_COLUMNS = {
    "disclosure_date",
    "transaction_date",
    "politician_name",
    "ticker",
    "amount_min",
}


def get_congress_trades(
    conn: sqlite3.Connection,
    ticker: str | None = None,
    politician: str | None = None,
    chamber: str | None = None,
    tx_type: str | None = None,
    days: int = 90,
    sort_by: str = "disclosure_date",
    sort_order: str = "desc",
    limit: int = 500,
) -> list[dict]:
    """Return congress_trades rows matching the given filters."""
    _safe_col = sort_by if sort_by in _CONGRESS_SORT_COLUMNS else "disclosure_date"
    _safe_dir = "ASC" if sort_order == "asc" else "DESC"

    clauses: list[str] = []
    params: list = []

    if days and days > 0:
        clauses.append("disclosure_date >= date('now', ?)")
        params.append(f"-{days} days")

    if ticker:
        clauses.append("ticker LIKE ?")
        params.append(ticker.upper())

    if politician:
        clauses.append("politician_name LIKE ?")
        params.append(f"%{politician}%")

    if chamber:
        clauses.append("chamber = ?")
        params.append(chamber)

    if tx_type:
        clauses.append("transaction_type = ?")
        params.append(tx_type)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT id, source, transaction_id, politician_name, chamber, party, state,
               ticker, asset_description, transaction_type,
               transaction_date, disclosure_date,
               amount_min, amount_max, amount_label, raw_url, ingested_at
        FROM congress_trades
        {where}
        ORDER BY {_safe_col} {_safe_dir} NULLS LAST
        LIMIT ?
    """
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def get_congress_summary(conn: sqlite3.Connection, days: int = 30) -> dict:
    """Return aggregate KPIs for the congress trades tab."""
    use_date = days and days > 0
    date_clause = "AND disclosure_date >= date('now', ?)" if use_date else ""
    date_param: list = [f"-{days} days"] if use_date else []

    totals = conn.execute(f"""
        SELECT
            COUNT(*) AS total_trades,
            COUNT(DISTINCT politician_name) AS unique_politicians,
            COUNT(DISTINCT ticker) AS unique_tickers,
            SUM(CASE WHEN transaction_type = 'purchase' THEN 1 ELSE 0 END) AS purchase_count,
            SUM(CASE WHEN transaction_type = 'sale' THEN 1 ELSE 0 END) AS sale_count
        FROM congress_trades
        WHERE 1=1 {date_clause}
    """, date_param).fetchone()

    top_tickers = conn.execute(f"""
        SELECT ticker, COUNT(*) AS cnt
        FROM congress_trades
        WHERE ticker IS NOT NULL AND ticker != '' {date_clause}
        GROUP BY ticker
        ORDER BY cnt DESC
        LIMIT 10
    """, date_param).fetchall()

    top_politicians = conn.execute(f"""
        SELECT politician_name AS name, COUNT(*) AS cnt
        FROM congress_trades
        WHERE 1=1 {date_clause}
        GROUP BY politician_name
        ORDER BY cnt DESC
        LIMIT 10
    """, date_param).fetchall()

    return {
        "total_trades": totals["total_trades"] or 0,
        "unique_politicians": totals["unique_politicians"] or 0,
        "unique_tickers": totals["unique_tickers"] or 0,
        "purchase_count": totals["purchase_count"] or 0,
        "sale_count": totals["sale_count"] or 0,
        "top_tickers": [{"ticker": r["ticker"], "count": r["cnt"]} for r in top_tickers],
        "top_politicians": [{"name": r["name"], "count": r["cnt"]} for r in top_politicians],
    }
