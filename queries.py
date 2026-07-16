"""All SQL queries for the dashboard. No SQL in app.py.

PostgreSQL (psycopg3) port — rows are dict-like (`dict_row` factory in db.py),
placeholders are `%s`, INSERT OR IGNORE is `ON CONFLICT DO NOTHING`, date
arithmetic uses `INTERVAL` / `CURRENT_DATE`, and string search uses `ILIKE`
for case-insensitive matching (SQLite's `LIKE` was case-insensitive by default).
"""
from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone

import psycopg


# ---------------------------------------------------------------------------
# Enrichment context — passed to _enrich; add new fields here each session
# rather than adding positional parameters. Callers passing ctx=None are
# unaffected by new fields.
# ---------------------------------------------------------------------------

@dataclass
class EnrichContext:
    conn: psycopg.Connection | None = None
    conviction_flags: dict | None = None      # CONVICTION_FLAGS values
    conviction_tiers: dict | None = None      # CONVICTION_TIERS values
    conviction_max: int = 10
    conviction_thresholds: dict | None = None
    cluster_window_days: int = 14
    ceo_cfo_keywords: list[str] = field(default_factory=list)
    watched_tickers: set[str] = field(default_factory=set)     # Session 6
    watched_insiders: set[str] = field(default_factory=set)    # Session 6
    compute_conviction: bool = False
    insider_baseline_cfg: dict | None = None    # INSIDER_BASELINE values
    compute_insider_baseline: bool = False


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

def _sanitize_codes(codes: list[str] | None) -> tuple[list[str], str]:
    result = [c for c in (codes or ["P", "S"]) if c in ("P", "S")] or ["P", "S"]
    return result, ",".join(["%s"] * len(result))


def _iso_date(v) -> str | None:
    if v is None:
        return None
    return v.isoformat() if isinstance(v, date) else str(v)


def _fmt_value(v: float | None) -> str:
    if v is None:
        return ""
    sign = "-" if v < 0 else ""
    av = abs(v)
    if av >= 1_000_000_000_000:
        return f"{sign}${av/1_000_000_000_000:,.2f}T"
    if av >= 1_000_000_000:
        return f"{sign}${av/1_000_000_000:,.2f}B"
    if av >= 1_000_000:
        return f"{sign}${av/1_000_000:,.1f}M"
    if av >= 1_000:
        return f"{sign}${av/1_000:,.0f}K"
    return f"{sign}${av:,.0f}"


def _relative_time(ts: str | None) -> str:
    if not ts:
        return ""
    try:
        # psycopg may hand us a datetime directly; tolerate both
        if isinstance(ts, datetime):
            dt = ts
        else:
            dt = datetime.fromisoformat(str(ts))
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
        return str(ts) if ts else ""


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
    conn: psycopg.Connection,
    rows: list[dict],
    window_days: int,
) -> dict[tuple, int]:
    """
    Count distinct buying insiders per (issuer_cik, transaction_date) pair in one query.
    Uses the broadest date window across all pairs so a single SQL round-trip covers all.
    Python groups results back to the per-pair granularity.
    """
    # Normalize transaction_date values to ISO strings up-front so all dict
    # keys + comparisons are string-vs-string. psycopg returns DATE columns as
    # Python `date` objects; SQLite returned them as strings — normalize here.
    def _to_iso(v) -> str | None:
        if v is None:
            return None
        if isinstance(v, date):
            return v.isoformat()
        return str(v)

    pairs_norm: set[tuple[str, str]] = set()
    for r in rows:
        cik = r.get("issuer_cik")
        td = _to_iso(r.get("transaction_date"))
        if cik and td:
            pairs_norm.add((cik, td))
    if not pairs_norm:
        return {}

    all_ciks = list({p[0] for p in pairs_norm})
    all_dates = [p[1] for p in pairs_norm]
    global_end = max(all_dates)
    global_start = (date.fromisoformat(min(all_dates)) - timedelta(days=window_days)).isoformat()

    placeholders = ",".join(["%s"] * len(all_ciks))
    db_rows = conn.execute(
        f"""SELECT issuer_cik, transaction_date, insider_cik
            FROM filings
            WHERE transaction_code = 'P'
              AND issuer_cik IN ({placeholders})
              AND transaction_date BETWEEN %s AND %s
              AND superseded_by IS NULL
              AND joint_filer_of IS NULL""",
        all_ciks + [global_start, global_end],
    ).fetchall()

    by_cik: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    for r in db_rows:
        td_key = _to_iso(r["transaction_date"])
        by_cik[r["issuer_cik"]][td_key].add(r["insider_cik"])

    # Original keys may include date objects from the caller; preserve them
    # so callers (which look up by the same row dict's transaction_date) hit.
    result: dict[tuple, int] = {}
    for r in rows:
        cik = r.get("issuer_cik")
        td_orig = r.get("transaction_date")
        td_key = _to_iso(td_orig)
        if not cik or not td_key:
            continue
        d0 = (date.fromisoformat(td_key) - timedelta(days=window_days)).isoformat()
        insiders: set = set()
        for d_key, iset in by_cik.get(cik, {}).items():
            if d0 <= d_key <= td_key:
                insiders |= iset
        result[(cik, td_orig)] = len(insiders)
    return result


def _batch_insider_baseline(
    conn: psycopg.Connection,
    rows: list[dict],
    baseline_cfg: dict,
) -> dict[tuple, dict]:
    """
    Flag P-code buys that are unusual vs. this insider's own trade history --
    either size (>= size_multiplier x their own median prior buy) or timing
    (>= silence_days since their last P-trade). One query fetches each
    insider's full P-trade history; the per-trade comparison happens in
    Python so a trade is only measured against trades strictly before it in
    that insider's own timeline (point-in-time correct, not all-time-inclusive).
    """
    min_prior  = baseline_cfg.get("min_prior_trades", 2)
    multiplier = baseline_cfg.get("size_multiplier", 3.0)
    silence    = baseline_cfg.get("silence_days", 365)

    ciks = {
        r.get("insider_cik") for r in rows
        if r.get("insider_cik") and r.get("transaction_code") == "P"
    }
    if not ciks:
        return {}

    placeholders = ",".join(["%s"] * len(ciks))
    history = conn.execute(
        f"""SELECT insider_cik, transaction_date, total_value
            FROM filings
            WHERE insider_cik IN ({placeholders})
              AND transaction_code = 'P'
              AND superseded_by IS NULL
              AND joint_filer_of IS NULL
              AND table_type = 'ND'
              AND total_value IS NOT NULL
            ORDER BY insider_cik, transaction_date""",
        list(ciks),
    ).fetchall()

    by_insider: dict[str, list[tuple[str, float]]] = defaultdict(list)
    for h in history:
        by_insider[h["insider_cik"]].append((_iso_date(h["transaction_date"]), h["total_value"]))

    # Original (insider_cik, transaction_date) keys may carry date objects from
    # the caller's rows -- key the output the same way _batch_cluster_counts
    # does, so a lookup with the row's own (possibly non-string) value hits.
    trade_date_by_key: dict[tuple[str, str], object] = {
        (r.get("insider_cik"), _iso_date(r.get("transaction_date"))): r.get("transaction_date")
        for r in rows
    }

    flags: dict[tuple, dict] = {}
    for cik, trades in by_insider.items():
        for i, (td, value) in enumerate(trades):
            prior = trades[:i]
            if len(prior) < min_prior:
                continue
            prior_values = sorted(v for _, v in prior)
            mid = len(prior_values) // 2
            median_val = (
                prior_values[mid] if len(prior_values) % 2
                else (prior_values[mid - 1] + prior_values[mid]) / 2
            )
            gap_days = (date.fromisoformat(td) - date.fromisoformat(prior[-1][0])).days

            is_size_outlier = median_val > 0 and value >= median_val * multiplier
            is_silence_outlier = gap_days >= silence
            if not (is_size_outlier or is_silence_outlier):
                continue
            td_orig = trade_date_by_key.get((cik, td))
            if td_orig is None:
                continue  # this historical trade isn't in the current result set
            flags[(cik, td_orig)] = {
                "size_outlier": is_size_outlier,
                "silence_outlier": is_silence_outlier,
                "median_value": median_val,
                "multiple": (value / median_val) if median_val > 0 else None,
                "gap_days": gap_days,
            }
    return flags


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


def _enrich(rows: list[dict], ctx: EnrichContext | None = None) -> list[dict]:
    result = []
    # psycopg3 dict_row already returns dicts; dict(r) is idempotent.
    raw_dicts = [dict(r) for r in rows]

    # Batch-fetch last_close prices for all tickers in this result set
    prices: dict[str, float] = {}
    if ctx and ctx.conn:
        _tickers = {r.get("issuer_ticker") for r in raw_dicts if r.get("issuer_ticker")}
        if _tickers:
            _ph = ",".join(["%s"] * len(_tickers))
            _price_rows = ctx.conn.execute(
                f"SELECT ticker, last_close FROM ticker_metadata WHERE ticker IN ({_ph})",
                list(_tickers),
            ).fetchall()
            prices = {r["ticker"]: r["last_close"] for r in _price_rows if r["last_close"] is not None}

    cluster_counts: dict[tuple, int] = {}
    if ctx and ctx.compute_conviction and ctx.conn and ctx.conviction_flags:
        cluster_counts = _batch_cluster_counts(
            ctx.conn, raw_dicts, ctx.cluster_window_days
        )

    baseline_flags: dict[tuple, dict] = {}
    if ctx and ctx.compute_insider_baseline and ctx.conn and ctx.insider_baseline_cfg:
        baseline_flags = _batch_insider_baseline(
            ctx.conn, raw_dicts, ctx.insider_baseline_cfg
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
            # filed_at may be a datetime (psycopg) or str (legacy)
            filed_raw = d.get("filed_at") or ""
            if isinstance(filed_raw, datetime):
                filed_d = filed_raw.date()
            else:
                filed_d = datetime.fromisoformat(str(filed_raw)).date()
            tx_raw = d.get("transaction_date") or ""
            if isinstance(tx_raw, date):
                tx_d = tx_raw
            else:
                tx_d = date.fromisoformat(str(tx_raw))
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

        d["baseline_flag"] = baseline_flags.get((d.get("insider_cik"), d.get("transaction_date")))

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

# Regex used by hide_entity_filers filter — drops entity-named filers (funds, institutions)
# who are NOT officers. Keeps officers filing through personal LLCs/trusts (is_officer=1).
# Includes common foreign corporate suffixes (Mexican S.A. de C.V., Dutch N.V./B.V., Italian
# S.p.A., German GmbH, UK PLC, Brazilian/Portuguese Ltda) alongside the English-only terms —
# without these, foreign 10%-owner entities (e.g. "Control Empresarial de Capitales S.A. de
# C.V.", PBF Energy's largest holder) slipped through "Individuals Only" undetected.
_ENTITY_FILER_RE = (
    r"\y(llc|lp|ltd|inc|corp|fund|trust|capital|partners|management|holdings|"
    r"investment|investments|advisors|advisor|group|equity|ventures|associates|"
    r"foundation|partnership|company|securities|financial|lending|realty|properties|"
    r"gmbh|plc|ltda|s\.?a\.?(b\.?)?\s+de\s+c\.?v|s\.a|s\.p\.a|n\.v|b\.v)\y"
)


def list_watchlist(conn: psycopg.Connection) -> dict[str, list[dict]]:
    """Return {'tickers': [...], 'insiders': [...], 'congress_members': [...]} for the watchlist page."""
    rows = conn.execute(
        "SELECT id, type, value, label, created_at FROM watchlist ORDER BY created_at DESC"
    ).fetchall()
    tickers = [dict(r) for r in rows if r["type"] == "ticker"]
    insiders = [dict(r) for r in rows if r["type"] == "insider"]
    congress_members = [dict(r) for r in rows if r["type"] == "congress_member"]
    return {"tickers": tickers, "insiders": insiders, "congress_members": congress_members}


def add_watch(conn: psycopg.Connection, watch_type: str, value: str, label: str) -> None:
    assert conn.autocommit, "add_watch requires an autocommit connection"
    conn.execute(
        "INSERT INTO watchlist (type, value, label) VALUES (%s, %s, %s)"
        " ON CONFLICT DO NOTHING",
        [watch_type, value.strip(), label.strip()],
    )


def remove_watch(conn: psycopg.Connection, watch_id: int) -> None:
    assert conn.autocommit, "remove_watch requires an autocommit connection"
    conn.execute("DELETE FROM watchlist WHERE id = %s", [watch_id])


def toggle_watch(
    conn: psycopg.Connection,
    watch_type: str,
    value: str,
    label: str,
) -> bool:
    """Toggles watch status. Returns True if now watched, False if removed."""
    assert conn.autocommit, "toggle_watch requires an autocommit connection"
    row = conn.execute(
        "SELECT id FROM watchlist WHERE type = %s AND value = %s",
        [watch_type, value],
    ).fetchone()
    if row:
        remove_watch(conn, row["id"])
        return False
    add_watch(conn, watch_type, value, label)
    return True


def watched_tickers(conn: psycopg.Connection) -> set[str]:
    rows = conn.execute(
        "SELECT value FROM watchlist WHERE type = 'ticker'"
    ).fetchall()
    return {r["value"] for r in rows}


def watched_insiders(conn: psycopg.Connection) -> set[str]:
    """Returns a set of insider_cik values."""
    rows = conn.execute(
        "SELECT value FROM watchlist WHERE type = 'insider'"
    ).fetchall()
    return {r["value"] for r in rows}


def watched_congress_members(conn: psycopg.Connection) -> set[str]:
    """Returns a set of lowercase politician_name values for watched congress members."""
    rows = conn.execute(
        "SELECT value FROM watchlist WHERE type = 'congress_member'"
    ).fetchall()
    return {r["value"].lower() for r in rows}


def get_watchlist_feed(
    conn: psycopg.Connection,
    watched_tickers,
    watched_insiders,
    lookback_days: int = 14,
    limit: int = 8,
) -> list[dict]:
    """Flat, most-recent-first feed of buy/sell activity for watched tickers/insiders.
    Used by the dashboard hero strip (short lookback) and the /watchlist activity
    feed (longer lookback) — this is "what happened" across the whole watchlist,
    not per-item status (see get_last_activity_by_* for that)."""
    tickers = list(watched_tickers or [])
    insiders = list(watched_insiders or [])
    if not tickers and not insiders:
        return []
    cutoff = (date.today() - timedelta(days=lookback_days)).isoformat()
    rows = conn.execute(
        """
        SELECT issuer_ticker, issuer_name, insider_name, insider_title, insider_cik,
               transaction_code, total_value, transaction_date, filed_at, transaction_id
        FROM filings
        WHERE table_type = 'ND'
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND transaction_code IN ('P', 'S')
          AND transaction_date >= %s
          AND (issuer_ticker = ANY(%s) OR insider_cik = ANY(%s))
        ORDER BY filed_at DESC
        LIMIT %s
        """,
        [cutoff, tickers, insiders, limit],
    ).fetchall()
    today = date.today()
    result = []
    for r in rows:
        d = dict(r)
        d["value_fmt"] = _fmt_value(d.get("total_value") or 0)
        d["days_ago"] = (today - d["transaction_date"]).days if d.get("transaction_date") else None
        result.append(d)
    return result


def get_last_activity_by_ticker(conn: psycopg.Connection, tickers: list[str]) -> dict[str, dict]:
    """Most recent buy/sell per watched ticker — for the /watchlist status view."""
    if not tickers:
        return {}
    rows = conn.execute(
        """
        SELECT DISTINCT ON (issuer_ticker)
               issuer_ticker, transaction_code, total_value, transaction_date, insider_name
        FROM filings
        WHERE table_type = 'ND'
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND transaction_code IN ('P', 'S')
          AND issuer_ticker = ANY(%s)
        ORDER BY issuer_ticker, transaction_date DESC, filed_at DESC
        """,
        [tickers],
    ).fetchall()
    today = date.today()
    out = {}
    for r in rows:
        d = dict(r)
        d["value_fmt"] = _fmt_value(d.get("total_value") or 0)
        d["days_ago"] = (today - d["transaction_date"]).days if d.get("transaction_date") else None
        out[d["issuer_ticker"]] = d
    return out


def get_last_activity_by_insider(conn: psycopg.Connection, ciks: list[str]) -> dict[str, dict]:
    """Most recent buy/sell per watched insider (across all companies) — for the /watchlist status view."""
    if not ciks:
        return {}
    rows = conn.execute(
        """
        SELECT DISTINCT ON (insider_cik)
               insider_cik, issuer_ticker, issuer_name, transaction_code, total_value, transaction_date
        FROM filings
        WHERE table_type = 'ND'
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND transaction_code IN ('P', 'S')
          AND insider_cik = ANY(%s)
        ORDER BY insider_cik, transaction_date DESC, filed_at DESC
        """,
        [ciks],
    ).fetchall()
    today = date.today()
    out = {}
    for r in rows:
        d = dict(r)
        d["value_fmt"] = _fmt_value(d.get("total_value") or 0)
        d["days_ago"] = (today - d["transaction_date"]).days if d.get("transaction_date") else None
        out[d["insider_cik"]] = d
    return out


def get_last_activity_by_congress_member(conn: psycopg.Connection, names_lower: list[str]) -> dict[str, dict]:
    """Most recent trade per watched politician — for the /watchlist status view."""
    if not names_lower:
        return {}
    rows = conn.execute(
        """
        SELECT DISTINCT ON (LOWER(politician_name))
               LOWER(politician_name) AS name_lower, ticker, transaction_type,
               amount_label, transaction_date::date AS transaction_date
        FROM congress_trades
        WHERE LOWER(politician_name) = ANY(%s)
        ORDER BY LOWER(politician_name), transaction_date::date DESC
        """,
        [names_lower],
    ).fetchall()
    today = date.today()
    out = {}
    for r in rows:
        d = dict(r)
        d["days_ago"] = (today - d["transaction_date"]).days if d.get("transaction_date") else None
        out[d["name_lower"]] = d
    return out


def get_all_sectors(conn: psycopg.Connection) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT sector FROM filings WHERE sector IS NOT NULL ORDER BY sector"
    ).fetchall()
    return [r["sector"] for r in rows]


def get_ticker_list(conn: psycopg.Connection, limit: int = 6000) -> list[str]:
    rows = conn.execute(
        "SELECT DISTINCT issuer_ticker FROM filings "
        "WHERE issuer_ticker IS NOT NULL AND issuer_ticker NOT IN ('NONE','N/A') "
        "ORDER BY issuer_ticker LIMIT %s",
        [limit],
    ).fetchall()
    return [r["issuer_ticker"] for r in rows if r["issuer_ticker"]]


def get_top_signals_today(
    conn: psycopg.Connection,
    limit: int = 5,
    min_value: float = 50000,
) -> list[dict]:
    """Return top insider buys filed today, ordered by value descending, for the hero strip."""
    rows = conn.execute(
        """
        SELECT issuer_ticker, issuer_name, insider_name, insider_title,
               total_value, transaction_date
        FROM filings
        WHERE filed_at::date = CURRENT_DATE
          AND transaction_code = 'P'
          AND table_type = 'ND'
          AND superseded_by IS NULL AND joint_filer_of IS NULL
          AND is_10b5_1 = 0 AND equity_swap = 0
          AND total_value >= %s
        ORDER BY total_value DESC
        LIMIT %s
        """,
        [min_value, limit],
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["value_fmt"] = _fmt_value(d.get("total_value") or 0)
        result.append(d)
    return result


def get_daily_summary(
    conn: psycopg.Connection,
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
    code_placeholders = ",".join(["%s"] * len(codes))
    rows = conn.execute(f"""
        SELECT
            filed_at::date AS day,
            SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) AS buy_count,
            COALESCE(SUM(CASE WHEN transaction_code='P' THEN total_value END), 0) AS buy_total,
            SUM(CASE WHEN transaction_code='S' THEN 1 ELSE 0 END) AS sell_count,
            COALESCE(SUM(CASE WHEN transaction_code='S' THEN total_value END), 0) AS sell_total,
            COUNT(DISTINCT CASE WHEN transaction_code='P' THEN issuer_cik END) AS issuers
        FROM filings
        WHERE filed_at::date BETWEEN %s AND %s
          AND transaction_code IN ({code_placeholders})
          AND (total_value IS NULL OR total_value >= %s)
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          {ten_b}
          {swap_f}
        GROUP BY filed_at::date
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
    hide_entity_filers: bool = False,
) -> tuple[str, list]:
    """Build WHERE clause and params for filings queries.
    Returns (where_sql, params) where where_sql starts with 'WHERE ...'."""
    # Date condition: single date or range
    if date_range:
        date_condition = "filed_at::date BETWEEN %s AND %s"
        params: list = [date_range[0].isoformat(), date_range[1].isoformat()]
    else:
        date_condition = "filed_at::date = %s"
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
        _single_range = "(market_cap >= %s AND market_cap < %s)"
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
        "AND (" + " OR ".join("insider_title ILIKE %s" for _ in (ceo_cfo_keywords or [])) + ")"
        if ceo_cfo_only and ceo_cfo_keywords else ""
    )
    _frag_search   = "AND (issuer_ticker ILIKE %s OR issuer_name ILIKE %s OR insider_name ILIKE %s)" if search else ""
    _frag_sec      = "AND sector = %s" if sector else ""
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
    # Drop entity-named filers (funds, institutions) who aren't officers.
    # Keeps executives who file through personal LLCs/trusts (is_officer=1).
    _frag_entity   = (
        "AND NOT (insider_name ~* %s AND is_officer = 0)"
        if hide_entity_filers else ""
    )

    where_sql = f"""
        WHERE {date_condition}
          AND transaction_code IN ({{codes}})
          AND (total_value IS NULL OR total_value >= %s)
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
          {_frag_entity}
    """.format(codes=",".join(["%s"] * len(transaction_codes)))

    # Param order must mirror placeholder order in the WHERE clause exactly:
    # date -> codes -> min_value -> ceo_keywords -> search -> sector -> mktcap -> entity_re.
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
    if hide_entity_filers:
        params.append(_ENTITY_FILER_RE)

    return where_sql, params


def get_filings_for_date(
    conn: psycopg.Connection,
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
    hide_entity_filers: bool = False,
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
            hide_entity_filers=hide_entity_filers,
        )

        # Pagination vs legacy limit:
        #   - page_size provided => paginated path (LIMIT %s OFFSET %s), legacy limit ignored
        #   - page_size None     => export/backtest path; legacy limit applied if set
        if page_size is not None and not use_conviction:
            sql = f"{select_cols}\n{where_sql}\n{sql_sort}\nLIMIT %s OFFSET %s"
            side_params.append(page_size)
            side_params.append((page - 1) * page_size)
        else:
            sql = f"{select_cols}\n{where_sql}\n{sql_sort}"
            if page_size is None and limit:
                sql += "\nLIMIT %s"
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
    conn: psycopg.Connection,
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
    hide_entity_filers: bool = False,
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
        hide_entity_filers=hide_entity_filers,
    )

    if "P" in codes:
        buy_where, buy_params = _build_filings_where(
            target_date, transaction_codes=["P"], **common_kwargs
        )
        buy_count = conn.execute(
            f"SELECT COUNT(*) AS n FROM filings {buy_where}", buy_params
        ).fetchone()["n"]
    else:
        buy_count = 0

    if "S" in codes:
        sell_where, sell_params = _build_filings_where(
            target_date, transaction_codes=["S"], **common_kwargs
        )
        sell_count = conn.execute(
            f"SELECT COUNT(*) AS n FROM filings {sell_where}", sell_params
        ).fetchone()["n"]
    else:
        sell_count = 0

    return buy_count, sell_count


def get_summary_stats(
    conn: psycopg.Connection,
    target_date: date,
    date_range: tuple[date, date] | None = None,
    hide_10b5_1: bool = True,
    hide_equity_swap: bool = True,
    codes: list[str] | None = None,
) -> dict:
    ten_b  = "AND is_10b5_1 = 0"   if hide_10b5_1    else ""
    swap_f = "AND equity_swap = 0"  if hide_equity_swap else ""
    codes_list, codes_ph = _sanitize_codes(codes)

    if date_range:
        date_filter = "filed_at::date BETWEEN %s AND %s"
        date_params = [date_range[0].isoformat(), date_range[1].isoformat()]
    else:
        date_filter = "filed_at::date = %s"
        date_params = [target_date.isoformat()]

    row = conn.execute(f"""
        SELECT
            SUM(CASE WHEN transaction_code='P' THEN 1 ELSE 0 END) AS buy_count,
            COALESCE(SUM(CASE WHEN transaction_code='P' THEN total_value END), 0) AS buy_total,
            SUM(CASE WHEN transaction_code='S' THEN 1 ELSE 0 END) AS sell_count,
            COALESCE(SUM(CASE WHEN transaction_code='S' THEN total_value END), 0) AS sell_total,
            COUNT(DISTINCT issuer_cik) AS issuer_count
        FROM filings
        WHERE {date_filter} AND transaction_code IN ({codes_ph})
          AND table_type = 'ND'
          AND superseded_by IS NULL AND joint_filer_of IS NULL {ten_b} {swap_f}
    """, [*date_params, *codes_list]).fetchone()

    # Clusters are always buy-only — a cluster is 2+ distinct insiders buying
    # the same stock. Sells are excluded regardless of the caller's codes filter.
    # PostgreSQL requires an alias on subqueries — hence `AS sub`.
    clusters = conn.execute(f"""
        SELECT COUNT(*) AS n FROM (
          SELECT issuer_cik FROM filings
          WHERE {date_filter} AND transaction_code = 'P'
            AND table_type = 'ND'
            AND superseded_by IS NULL AND joint_filer_of IS NULL {ten_b} {swap_f}
          GROUP BY issuer_cik HAVING COUNT(DISTINCT insider_cik) >= 2
        ) AS sub
    """, date_params).fetchone()

    buy_total = row["buy_total"] or 0
    sell_total = row["sell_total"] or 0
    net = buy_total - sell_total
    return {
        "buy_count": row["buy_count"] or 0,
        "sell_count": row["sell_count"] or 0,
        "net_flow": net,
        "net_flow_fmt": _fmt_value(net),
        "issuer_count": row["issuer_count"] or 0,
        "cluster_count": clusters["n"] or 0,
        "buy_total_fmt": _fmt_value(buy_total),
        "sell_total_fmt": _fmt_value(sell_total),
    }


def get_cluster_activity(
    conn: psycopg.Connection,
    target_date: date,
    min_insiders: int = 2,
    hide_10b5_1: bool = True,
    hide_equity_swap: bool = True,
    date_range: tuple[date, date] | None = None,
    codes: list[str] | None = None,
) -> list[dict]:
    if date_range:
        date_condition = "filed_at::date BETWEEN %s AND %s"
        date_params: list = [date_range[0].isoformat(), date_range[1].isoformat()]
    else:
        date_condition = "filed_at::date = %s"
        date_params = [target_date.isoformat()]
    ten_b  = "AND is_10b5_1 = 0"  if hide_10b5_1    else ""
    swap_f = "AND equity_swap = 0" if hide_equity_swap else ""
    codes_list, codes_ph = _sanitize_codes(codes)

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
            STRING_AGG(DISTINCT insider_name, ', ') AS insider_names,
            STRING_AGG(DISTINCT COALESCE(insider_title, ''), ', ') AS insider_titles
        FROM filings
        WHERE {date_condition}
          AND transaction_code IN ({codes_ph})
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          {ten_b}
          {swap_f}
          AND issuer_ticker IS NOT NULL
        GROUP BY issuer_ticker, issuer_name
        HAVING COUNT(DISTINCT insider_cik) >= %s
        ORDER BY total_value DESC
    """, [*date_params, *codes_list, min_insiders]).fetchall()

    if not rows:
        return []

    cluster_tickers = [r["issuer_ticker"] for r in rows]
    ticker_placeholders = ",".join(["%s"] * len(cluster_tickers))
    all_tx = conn.execute(f"""
        SELECT transaction_id, insider_cik, insider_name, insider_title,
               transaction_code, shares, price_per_share, total_value, is_10b5_1,
               issuer_ticker
        FROM filings
        WHERE {date_condition}
          AND issuer_ticker IN ({ticker_placeholders})
          AND transaction_code IN ({codes_ph}) {ten_b} {swap_f}
        ORDER BY total_value DESC NULLS LAST
    """, [*date_params, *cluster_tickers, *codes_list]).fetchall()

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
    conn: psycopg.Connection,
    transaction_id: str,
    ctx: EnrichContext | None = None,
) -> dict | None:
    row = conn.execute(
        "SELECT * FROM filings WHERE transaction_id = %s", [transaction_id]
    ).fetchone()
    if row is None:
        return None
    enriched = _enrich([row], ctx=ctx)
    d = enriched[0]
    # If this row was superseded, attach the amendment accession for display
    if d.get("superseded_by"):
        amendment = conn.execute(
            "SELECT transaction_id FROM filings WHERE accession_no = %s LIMIT 1",
            [d["superseded_by"]],
        ).fetchone()
        d["amended_by_transaction_id"] = amendment["transaction_id"] if amendment else None
    return d


def get_issuer_filings(
    conn: psycopg.Connection,
    ticker: str,
    days: int = 90,
) -> list[dict]:
    since = (date.today() - timedelta(days=days)).isoformat()
    rows = conn.execute(
        """
        SELECT * FROM filings
        WHERE issuer_ticker = %s AND filed_at::date >= %s
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


def get_issuer_trend(conn: psycopg.Connection, ticker: str) -> list[dict]:
    """Returns 26 weekly data points (≈ 6 months) for the buy/sell sparkline.

    PG: `date_trunc('week', ...)` returns Monday by definition, so the
    Monday-of-week ISO date is `date_trunc('week', filed_at)::date`.
    """
    today = date.today()
    cutoff = (today - timedelta(weeks=26)).isoformat()

    rows = conn.execute("""
        SELECT date_trunc('week', filed_at)::date AS week_start,
               COALESCE(SUM(CASE WHEN transaction_code='P' THEN total_value END), 0) AS buy_total,
               COALESCE(SUM(CASE WHEN transaction_code='S' THEN total_value END), 0) AS sell_total
        FROM filings
        WHERE issuer_ticker = %s AND filed_at::date >= %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        GROUP BY week_start
        ORDER BY week_start
    """, [ticker.upper(), cutoff]).fetchall()

    lookup = {}
    for r in rows:
        ws = r["week_start"]
        key = ws.isoformat() if isinstance(ws, date) else str(ws)
        lookup[key] = (r["buy_total"] or 0, r["sell_total"] or 0)

    series = []
    for i in range(25, -1, -1):
        key = _week_start(today - timedelta(weeks=i))
        buy, sell = lookup.get(key, (0, 0))
        series.append({'week': key, 'buy_total': buy, 'sell_total': sell})

    return series


def get_run_log(conn: psycopg.Connection, limit: int = 50) -> list[dict]:
    rows = conn.execute(
        """
        SELECT id, run_kind, date_processed,
               started_at::text AS started_at,
               finished_at::text AS finished_at,
               filings_found, rows_inserted, errors, error_detail
        FROM run_log ORDER BY started_at DESC LIMIT %s
        """,
        [limit],
    ).fetchall()
    return [dict(r) for r in rows]


def get_insider_history(
    conn: psycopg.Connection,
    insider_cik: str,
    limit: int = 10,
) -> list[dict]:
    """
    Last N transactions by this insider across all companies.
    Uses a window function to flag the largest buy ever so it can be highlighted.
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
        WHERE insider_cik = %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        ORDER BY transaction_date DESC
        LIMIT %s
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
    conn: psycopg.Connection,
    insider_cik: str,
    ctx: EnrichContext | None = None,
) -> list[dict]:
    """All transactions by this insider across all companies, newest first."""
    rows = conn.execute(
        """
        SELECT *
        FROM filings
        WHERE insider_cik = %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        ORDER BY transaction_date DESC, filed_at DESC
        LIMIT 500
        """,
        [insider_cik],
    ).fetchall()
    return _enrich(rows, ctx=ctx)


def get_insider_perf_profile(conn: psycopg.Connection, insider_cik: str) -> dict | None:
    """Return backtest performance profile for an insider, or None if not profiled.
    Table is optional (populated by load_insider_profiles.py) — returns None if absent."""
    try:
        row = conn.execute(
            """
            SELECT insider_cik, insider_name, role, n_trades,
                   win_30, avg_30, med_30,
                   win_60, avg_60, med_60,
                   win_90, avg_90, med_90,
                   peak_window, profile_label, updated_at
            FROM insider_perf_profile
            WHERE insider_cik = %s
            """,
            [insider_cik],
        ).fetchone()
        return dict(row) if row else None
    except psycopg.errors.UndefinedTable:
        conn.rollback()
        return None


def get_insider_summary(conn: psycopg.Connection, insider_cik: str) -> dict:
    """Aggregate stats for a single insider across all filings."""
    row = conn.execute(
        """
        SELECT
            (SELECT insider_name FROM filings
             WHERE insider_cik = %s ORDER BY filed_at DESC LIMIT 1) AS name,
            SUM(CASE WHEN transaction_code = 'P' THEN COALESCE(total_value, 0) ELSE 0 END) AS total_bought,
            SUM(CASE WHEN transaction_code = 'S' THEN COALESCE(total_value, 0) ELSE 0 END) AS total_sold,
            COUNT(DISTINCT issuer_cik) AS distinct_issuers,
            MIN(transaction_date) AS first_trade,
            MAX(transaction_date) AS last_trade
        FROM filings
        WHERE insider_cik = %s
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
    conn: psycopg.Connection,
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
    exclude_clause = "AND transaction_id != %s" if exclude_transaction_id else ""
    params: list = [issuer_cik, since]
    if exclude_transaction_id:
        params.append(exclude_transaction_id)
    rows = conn.execute(
        f"""
        SELECT
            insider_cik, insider_name, insider_title,
            SUM(CASE WHEN transaction_code='P' THEN COALESCE(total_value,0) ELSE 0 END) AS total_bought,
            SUM(CASE WHEN transaction_code='S' THEN COALESCE(total_value,0) ELSE 0 END) AS total_sold,
            MAX(transaction_date) AS last_date,
            MAX(transaction_id) AS latest_transaction_id
        FROM filings
        WHERE issuer_cik = %s
          AND filed_at::date >= %s
          {exclude_clause}
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        GROUP BY insider_cik, insider_name, insider_title
        ORDER BY total_bought DESC
        """,
        params,
    ).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d["total_bought_fmt"] = _fmt_value(d.get("total_bought") or None)
        d["total_sold_fmt"] = _fmt_value(d.get("total_sold") or None)
        result.append(d)
    return result


def get_recent_alerts(conn: psycopg.Connection, limit: int = 10) -> list[dict]:
    rows = conn.execute(
        "SELECT alert_key, alert_type, sent_at::text AS sent_at FROM alerts_sent ORDER BY sent_at DESC LIMIT %s",
        [limit],
    ).fetchall()
    return [dict(r) for r in rows]


def get_signal_alert_history(conn: psycopg.Connection, limit: int = 20) -> list[dict]:
    """
    Recent 'signal' alerts (technical signal fires). alert_key format is
    'signal:{sig_code}:{issuer_cik}:{fire_date}' -- split it out and join to
    filings for a ticker/company label, best-effort (issuer may have no
    matching row if data has since changed).
    """
    rows = conn.execute(
        """
        SELECT
            a.alert_key, a.sent_at::text AS sent_at,
            split_part(a.alert_key, ':', 2) AS sig_code,
            split_part(a.alert_key, ':', 3) AS issuer_cik,
            split_part(a.alert_key, ':', 4) AS fire_date,
            f.issuer_ticker, f.issuer_name
        FROM alerts_sent a
        LEFT JOIN LATERAL (
            SELECT issuer_ticker, issuer_name FROM filings
            WHERE issuer_cik = split_part(a.alert_key, ':', 3)
            ORDER BY ingested_at DESC LIMIT 1
        ) f ON true
        WHERE a.alert_type = 'signal'
        ORDER BY a.sent_at DESC
        LIMIT %s
        """,
        [limit],
    ).fetchall()
    return [dict(r) for r in rows]


def get_10b5_1_stats(conn: psycopg.Connection) -> dict:
    total = conn.execute("SELECT COUNT(*) AS n FROM filings").fetchone()["n"]
    flagged_xml = conn.execute(
        "SELECT COUNT(*) AS n FROM filings WHERE is_10b5_1=1"
    ).fetchone()["n"]
    footnote_only = conn.execute(
        """SELECT COUNT(*) AS n FROM filings
           WHERE is_10b5_1=1 AND footnote_text ILIKE '%10b5-1%'"""
    ).fetchone()["n"]
    return {
        "total_filings": total,
        "flagged": flagged_xml,
        "footnote_only": footnote_only,
    }


# ---------------------------------------------------------------------------
# Ticker metadata (market cap, options availability)
# ---------------------------------------------------------------------------

def get_ticker_metadata_map(conn: psycopg.Connection, tickers: list[str]) -> dict[str, dict]:
    """
    Batch-fetch ticker metadata rows for the given tickers in a single query.
    Returns {ticker: {'market_cap': float|None, 'has_options': int|None, 'fetched_at': str|None}}.
    Returns {} when tickers is empty.
    """
    if not tickers:
        return {}
    placeholders = ",".join(["%s"] * len(tickers))
    rows = conn.execute(
        f"SELECT ticker, market_cap, has_options, fetched_at FROM ticker_metadata"
        f" WHERE ticker IN ({placeholders})",
        tickers,
    ).fetchall()
    return {r["ticker"]: dict(r) for r in rows}


def upsert_ticker_metadata(
    conn: psycopg.Connection,
    ticker: str,
    market_cap: float | None,
    has_options: int | None,
) -> None:
    """Insert or update ticker metadata, setting fetched_at to the current UTC time."""
    conn.execute(
        """
        INSERT INTO ticker_metadata (ticker, has_options, market_cap, fetched_at)
        VALUES (%s, %s, %s, NOW())
        ON CONFLICT (ticker) DO UPDATE SET
            has_options = EXCLUDED.has_options,
            market_cap  = EXCLUDED.market_cap,
            fetched_at  = EXCLUDED.fetched_at
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
    conn: psycopg.Connection,
    ticker: str | None = None,
    politician: str | None = None,
    chamber: str | None = None,
    tx_type: str | None = None,
    source: str | None = None,
    days: int = 90,
    sort_by: str = "disclosure_date",
    sort_order: str = "desc",
    limit: int = 500,
    watched_members: set[str] | None = None,
) -> list[dict]:
    """Return congress_trades rows matching the given filters.

    Note: disclosure_date is stored as TEXT (ISO YYYY-MM-DD), so comparison
    is lexicographic against a string. PG cast `CURRENT_DATE - INTERVAL %s`
    produces a `date` value; psycopg adapts it back to text for comparison
    via the implicit text < text rule, which works correctly for ISO dates.
    """
    _safe_col = sort_by if sort_by in _CONGRESS_SORT_COLUMNS else "disclosure_date"
    _safe_dir = "ASC" if sort_order == "asc" else "DESC"

    clauses: list[str] = []
    params: list = []

    if days and days > 0:
        # disclosure_date is stored as ISO YYYY-MM-DD TEXT; compute the cutoff
        # in Python so we don't need to wrestle with PG INTERVAL syntax.
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        clauses.append("disclosure_date >= %s")
        params.append(cutoff)

    if ticker:
        clauses.append("ticker ILIKE %s")
        params.append(ticker.upper())

    if politician:
        clauses.append("politician_name ILIKE %s")
        params.append(f"%{politician}%")

    if chamber:
        clauses.append("chamber = %s")
        params.append(chamber)

    if tx_type:
        clauses.append("LOWER(transaction_type) = %s")
        params.append(tx_type.lower())

    if source:
        clauses.append("source = %s")
        params.append(source)

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    sql = f"""
        SELECT id, source, transaction_id, politician_name, chamber, party, state,
               ticker, asset_description, transaction_type,
               transaction_date, disclosure_date,
               amount_min, amount_max, amount_label, raw_url, ingested_at
        FROM congress_trades
        {where}
        ORDER BY {_safe_col} {_safe_dir} NULLS LAST
        LIMIT %s
    """
    params.append(limit)
    rows = conn.execute(sql, params).fetchall()
    result = []
    for r in rows:
        row = dict(r)
        row["is_watched"] = (
            (row.get("politician_name") or "").lower() in watched_members
            if watched_members is not None else False
        )
        result.append(row)
    return result


def get_chart_buys(
    conn: psycopg.Connection,
    ticker: str,
    days: int,
    min_value: float,
) -> list[dict]:
    """Open-market buys for a ticker within `days` days, for chart overlays."""
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = conn.execute(
        """
        SELECT transaction_date::text AS transaction_date,
               insider_name, insider_title, total_value
        FROM filings
        WHERE issuer_ticker = %s
          AND transaction_code = 'P'
          AND table_type = 'ND'
          AND is_10b5_1 = 0
          AND total_value >= %s
          AND transaction_date >= %s
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        ORDER BY transaction_date
        """,
        [ticker.upper(), min_value, cutoff],
    ).fetchall()
    return [dict(r) for r in rows]


def get_congress_summary(conn: psycopg.Connection, days: int = 30, source: str | None = None) -> dict:
    """Return aggregate KPIs for the congress trades tab."""
    use_date = bool(days and days > 0)
    if use_date:
        cutoff = (date.today() - timedelta(days=days)).isoformat()
        date_clause = "AND disclosure_date >= %s"
        date_param: list = [cutoff]
    else:
        date_clause = ""
        date_param = []

    source_clause = "AND source = %s" if source else ""
    source_param  = [source] if source else []

    totals = conn.execute(f"""
        SELECT
            COUNT(*) AS total_trades,
            COUNT(DISTINCT politician_name) AS unique_politicians,
            COUNT(DISTINCT ticker) AS unique_tickers,
            SUM(CASE WHEN LOWER(transaction_type) IN ('purchase', 'buy') THEN 1 ELSE 0 END) AS purchase_count,
            SUM(CASE WHEN LOWER(transaction_type) IN ('sale', 'sell') THEN 1 ELSE 0 END) AS sale_count
        FROM congress_trades
        WHERE 1=1 {date_clause} {source_clause}
    """, date_param + source_param).fetchone()

    top_tickers = conn.execute(f"""
        SELECT ticker, COUNT(*) AS cnt
        FROM congress_trades
        WHERE ticker IS NOT NULL AND ticker != '' {date_clause} {source_clause}
        GROUP BY ticker
        ORDER BY cnt DESC
        LIMIT 10
    """, date_param + source_param).fetchall()

    top_politicians = conn.execute(f"""
        SELECT politician_name AS name, COUNT(*) AS cnt
        FROM congress_trades
        WHERE 1=1 {date_clause} {source_clause}
        GROUP BY politician_name
        ORDER BY cnt DESC
        LIMIT 10
    """, date_param + source_param).fetchall()

    return {
        "total_trades": totals["total_trades"] or 0,
        "unique_politicians": totals["unique_politicians"] or 0,
        "unique_tickers": totals["unique_tickers"] or 0,
        "purchase_count": totals["purchase_count"] or 0,
        "sale_count": totals["sale_count"] or 0,
        "top_tickers": [{"ticker": r["ticker"], "count": r["cnt"]} for r in top_tickers],
        "top_politicians": [{"name": r["name"], "count": r["cnt"]} for r in top_politicians],
    }
