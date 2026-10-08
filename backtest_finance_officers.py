"""
Backtest: open-market buys by finance officers (CFO, Controller / Comptroller,
Chief Accounting Officer) on stocks that trade listed options, any market cap.

Same mechanics as backtest_smallcap_system.py (and reuses its helpers):
entry at the OPEN of the first trading day strictly after filed_at, exit at
the close after a fixed hold, one open position per ticker at a time, trades
filed more than --max-filing-lag days after the transaction are dropped.

Differences from the small-cap script:
  * Role filter is applied in SQL on insider_title, and the role buckets are
    finance-specific. A title that is BOTH CEO and CFO (common at tiny
    companies) goes in its own "CEO+CFO" bucket so it does not pollute "CFO".
  * Universe is ticker_metadata.has_options = 1 with no market cap ceiling;
    results are broken out by cap tier up to large cap.
  * Hold windows add 14d and 180d. The SQL end date only guarantees the
    shortest window has forward data; longer windows simply have fewer
    positions near the end of the sample (compute_position returns None).
  * Every position also carries the SPY return over the same entry-open to
    exit-close span, and excess_pct = return_pct - spy_pct.

Usage:
    python backtest_finance_officers.py [--min-value 25000] [--start 2021-01-01]
        [--max-filing-lag 2,3,4] [--output data/finance_officer_backtest.csv]

Requires POLYGON_API_KEY in environment.
"""
from __future__ import annotations

import argparse
import csv
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from backtest_smallcap_system import (
    FETCH_WORKERS,
    RATE_LIMIT_SLEEP,
    RateLimiter,
    _is_likely_entity,
    _parse_lag_list,
    _stats,
    dedup_positions,
    fetch_bars,
)
from db import get_cli_db

HOLD_WINDOWS = [14, 30, 60, 90, 180]
TRADE_START_DEFAULT = "2021-01-01"
MIN_VALUE_DEFAULT = 25_000
MAX_FILING_LAG_DEFAULT = "2,3,4"
REPORT_LAG = 3   # the cutoff the printed tables use; the CSV carries all of them

AMOUNT_TIERS = [
    (25_000,    100_000, "25k-100k"),
    (100_000,   250_000, "100k-250k"),
    (250_000,      None, "250k+"),
]

MCAP_TIERS = [
    (0,               300_000_000, "micro <300m"),
    (300_000_000,   1_000_000_000, "small 300m-1b"),
    (1_000_000_000, 10_000_000_000, "mid 1b-10b"),
    (10_000_000_000,         None, "large 10b+"),
]

# SQL-side prefilter: anything that could be a finance officer. The Python
# classifier below decides the bucket (and drops titles that only matched
# because of a substring, if any show up).
TITLE_SQL = """(
       f.insider_title ILIKE '%%chief financial%%'
    OR f.insider_title ILIKE '%%cfo%%'
    OR f.insider_title ILIKE '%%controller%%'
    OR f.insider_title ILIKE '%%comptroller%%'
    OR f.insider_title ILIKE '%%chief accounting%%'
)"""


def classify_finance_role(title: str | None) -> str | None:
    if not title:
        return None
    t = title.lower()
    is_cfo = "chief financial" in t or "cfo" in t
    is_ceo = "chief executive" in t or "ceo" in t
    if is_cfo and is_ceo:
        return "CEO+CFO"
    if is_cfo:
        return "CFO"
    if "controller" in t or "comptroller" in t:
        return "Controller"
    if "chief accounting" in t:
        return "Chief Accounting"
    return None


def _tier(value: float | None, tiers) -> str:
    if value is None:
        return "unknown"
    for lo, hi, label in tiers:
        if value >= lo and (hi is None or value < hi):
            return label
    return "unknown"


def _spy_return(spy_by_date: dict[str, dict], entry_date: str, exit_date: str) -> float | None:
    e, x = spy_by_date.get(entry_date), spy_by_date.get(exit_date)
    if not e or not x or not e.get("open"):
        return None
    return round((x["close"] - e["open"]) / e["open"] * 100, 2)


def _segment(label: str, positions: list[dict], key: str = "return_pct") -> None:
    vals = [p[key] for p in positions if p.get(key) is not None]
    if vals:
        print(f"    {label:<26} {_stats(vals)}")


def _breakdown(title: str, positions: list[dict], field: str, order: list[str] | None = None) -> None:
    print(f"\n  {title}")
    groups: dict[str, list[dict]] = defaultdict(list)
    for p in positions:
        groups[p[field]].append(p)
    keys = order or sorted(groups)
    for k in keys:
        if k in groups:
            _segment(k, groups[k])


def main() -> None:
    parser = argparse.ArgumentParser(description="Finance-officer insider buy backtest, optionable stocks")
    parser.add_argument("--min-value", type=float, default=MIN_VALUE_DEFAULT)
    parser.add_argument("--start", default=TRADE_START_DEFAULT)
    parser.add_argument("--max-filing-lag", type=_parse_lag_list, default=_parse_lag_list(MAX_FILING_LAG_DEFAULT))
    parser.add_argument("--fetch-workers", type=int, default=FETCH_WORKERS)
    parser.add_argument("--output", default="data/finance_officer_backtest.csv")
    args = parser.parse_args()

    api_key = os.environ.get("POLYGON_API_KEY", "")
    if not api_key:
        raise SystemExit("POLYGON_API_KEY not set")

    lag_values = args.max_filing_lag
    widest_lag = max(lag_values)
    trade_end = (date.today() - timedelta(days=min(HOLD_WINDOWS) + 5)).isoformat()

    conn = get_cli_db()
    rows = conn.execute(f"""
        SELECT
            f.insider_cik, f.insider_name, f.insider_title,
            f.issuer_ticker, f.issuer_name, f.issuer_cik,
            f.transaction_date::text AS transaction_date,
            f.filed_at::date::text   AS filed_at,
            f.total_value, f.price_per_share,
            tm.market_cap
        FROM filings f
        JOIN ticker_metadata tm ON tm.ticker = f.issuer_ticker
        WHERE f.transaction_code = 'P'
          AND f.table_type = 'ND'
          AND f.superseded_by IS NULL
          AND f.joint_filer_of IS NULL
          AND f.is_officer = 1
          AND f.is_10b5_1 = 0
          AND tm.has_options = 1
          AND TRIM(f.issuer_ticker) IS NOT NULL
          AND TRIM(f.issuer_ticker) NOT IN ('NONE', 'N/A', '')
          AND f.total_value >= %s
          AND {TITLE_SQL}
          AND (f.filed_at::date - f.transaction_date::date) BETWEEN 0 AND %s
          AND f.filed_at::date >= %s::date
          AND f.filed_at::date <= %s::date
        ORDER BY f.issuer_ticker, f.filed_at
    """, [args.min_value, widest_lag, args.start, trade_end]).fetchall()
    conn.close()

    trades: list[dict] = []
    dropped_entity = dropped_role = 0
    for r in rows:
        t = dict(r)
        if _is_likely_entity(t["insider_name"]):
            dropped_entity += 1
            continue
        role = classify_finance_role(t["insider_title"])
        if role is None:
            dropped_role += 1
            continue
        t["role"] = role
        t["filing_lag_days"] = (date.fromisoformat(t["filed_at"]) - date.fromisoformat(t["transaction_date"])).days
        t["amount_tier"] = _tier(t["total_value"], AMOUNT_TIERS)
        t["mcap_tier"] = _tier(t["market_cap"], MCAP_TIERS)
        trades.append(t)

    ticker_list = sorted({t["issuer_ticker"].strip() for t in trades})
    print(f"Trades from DB (optionable, >= ${args.min_value:,.0f}, filed within {widest_lag}d): {len(rows)}")
    print(f"Dropped: {dropped_entity} entity filers, {dropped_role} unclassifiable titles")
    print(f"Finance-officer trades: {len(trades)} across {len(ticker_list)} tickers")
    print(f"Filed window: {args.start} -> {trade_end}\n")

    fetch_start, fetch_end = args.start, date.today().isoformat()
    limiter = RateLimiter(RATE_LIMIT_SLEEP)
    bars_by_ticker: dict[str, list[dict]] = {}
    no_data: list[str] = []
    with ThreadPoolExecutor(max_workers=args.fetch_workers) as pool:
        futures = {pool.submit(fetch_bars, tk, fetch_start, fetch_end, api_key, limiter): tk
                   for tk in ticker_list + ["SPY"]}
        for i, fut in enumerate(as_completed(futures), 1):
            tk = futures[fut]
            try:
                bars, cached = fut.result()
            except Exception as e:
                print(f"[{i}/{len(futures)}] {tk}... [WARN] {e}")
                no_data.append(tk)
                continue
            if bars:
                bars_by_ticker[tk] = bars
                print(f"[{i}/{len(futures)}] {tk}... ok ({len(bars)} bars, {'cached' if cached else 'live'})")
            else:
                no_data.append(tk)
                print(f"[{i}/{len(futures)}] {tk}... NO DATA")

    spy_by_date = {b["date"]: b for b in bars_by_ticker.get("SPY", [])}
    if not spy_by_date:
        raise SystemExit("No SPY bars; cannot compute excess returns")

    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_ticker[t["issuer_ticker"].strip()].append(t)

    csv_rows: list[dict] = []
    report: dict[int, list[dict]] = {}
    for max_lag in lag_values:
        for hold in HOLD_WINDOWS:
            positions: list[dict] = []
            for tk, tts in by_ticker.items():
                bars = bars_by_ticker.get(tk)
                if not bars:
                    continue
                kept = sorted((t for t in tts if t["filing_lag_days"] <= max_lag), key=lambda t: t["filed_at"])
                for pos in dedup_positions(kept, bars, hold):
                    pos["hold_days"] = hold
                    pos["max_lag"] = max_lag
                    pos["spy_pct"] = _spy_return(spy_by_date, pos["entry_date"], pos["exit_date"])
                    pos["excess_pct"] = (round(pos["return_pct"] - pos["spy_pct"], 2)
                                         if pos["spy_pct"] is not None else None)
                    positions.append(pos)
            if max_lag == REPORT_LAG:
                report[hold] = positions
            for p in positions:
                csv_rows.append({k: p[k] for k in (
                    "max_lag", "hold_days", "issuer_ticker", "issuer_name", "insider_name", "insider_title",
                    "role", "amount_tier", "mcap_tier", "market_cap", "total_value",
                    "transaction_date", "filed_at", "filing_lag_days",
                    "entry_date", "entry_price", "exit_date", "exit_price",
                    "return_pct", "spy_pct", "excess_pct", "peak_pct", "peak_day", "trough_pct", "trough_day",
                )})

    sep = "=" * 80
    role_order = ["CFO", "Controller", "Chief Accounting", "CEO+CFO"]
    for hold in HOLD_WINDOWS:
        positions = report.get(hold, [])
        print(f"\n{sep}\n  FILED WITHIN {REPORT_LAG}d -- {hold}-DAY HOLD  (raw return; excess vs SPY on the next line)\n{sep}")
        _segment("All finance officers", positions)
        _segment("  excess vs SPY", positions, key="excess_pct")
        _breakdown("BY ROLE", positions, "role", role_order)
        _breakdown("BY MARKET CAP", positions, "mcap_tier", [t[2] for t in MCAP_TIERS])
        _breakdown("BY BUY SIZE", positions, "amount_tier", [t[2] for t in AMOUNT_TIERS])

    if not csv_rows:
        print("\nNo results.")
        return
    Path(args.output).parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(csv_rows[0].keys()))
        w.writeheader()
        w.writerows(csv_rows)
    print(f"\nWrote {len(csv_rows)} rows -> {args.output}")
    if no_data:
        print(f"Tickers with no price data ({len(no_data)}): {', '.join(no_data[:30])}{'...' if len(no_data) > 30 else ''}")
    print("\nNOTES: entry = next-morning open after filed_at; market_cap and has_options are CURRENT")
    print("snapshots from ticker_metadata, not point-in-time; delisted tickers are absent (survivorship).")


if __name__ == "__main__":
    main()
