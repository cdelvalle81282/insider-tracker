"""
Backtest: congressional stock purchases → forward excess return vs. SPY.

STRATEGY RATIONALE (based on adversarial literature review):
  The famous Senate +12%/yr and House +6%/yr numbers are methodological artifacts
  (Eggers & Hainmueller 2013, J. Politics). A holdings-based study of 2004-2008
  found congressional portfolios underperformed a passive index by -2.8%/yr.
  Post-STOCK-Act (2012+), multiple peer-reviewed studies find null results in aggregate.

  Two signals with surviving evidence:
    1. LEADERSHIP / COMMITTEE POWER — pre-Act committee chairs earned 13.5%/yr
       Carhart alpha (Huang & Xuan); congressional leaders outperform matched peers
       after ascension, not before (Wei & Zhou, NBER w34524, 2025). The edge appears
       to be causally tied to holding power over legislation, not skill.
    2. CORPORATE CO-BUY STACKING — not in the academic literature, but structurally
       distinct: if a corporate insider and a congress member both bought the same
       stock within ±14 days, that is two independent information channels converging.

  Technical signals (golden cross, resistance break, etc.) are carried over from
  backtest.py for continuity but are NOT the primary hypothesis here. The key test is
  whether EXCESS RETURN (vs. SPY over the same window from the same entry date)
  differs meaningfully across the leadership, co-buy, and disclosure-lag segments.

Entry date: disclosure_date (the first day the trade was publicly known — the only
  date a real trader could act on). transaction_date is included for lag analysis only.

Usage:
    python backtest_congress.py [--min-amount 15000] [--output data/congress_backtest.csv]

Requires POLYGON_API_KEY in environment (or .env file).
"""
from __future__ import annotations

import argparse
import bisect
import csv
import os
import time
from collections import defaultdict
from datetime import date, timedelta

from backtest import (
    CACHE_DIR, CB_MIN_BARS_PRE, GC_MIN_BARS, HHL_PIVOT_WINDOW,
    PRICE_WARMUP_DAYS, RATE_LIMIT_SLEEP, RB_MIN_BARS, WINDOWS,
    _fire_returns,
    detect_channel_break, detect_golden_cross, detect_hhl,
    detect_resistance_break, fetch_bars, forward_return,
)

from db import get_cli_db

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

TRADE_START = "2021-01-01"
TRADE_END   = (date.today() - timedelta(days=max(WINDOWS))).isoformat()

CORPORATE_STACK_WINDOW = 14  # ±calendar days around disclosure_date

# Known congressional leaders (sparse list — improves over time as trades accumulate).
# Based on Wei & Zhou (NBER w34524): edge concentrates in members who hold actual
# legislative power. Names matched case-insensitively against politician_name.
KNOWN_LEADERS: set[str] = {
    "nancy pelosi",          # House Speaker / Minority Leader
    "mike johnson",          # House Speaker (2023-)
    "hakeem jeffries",       # House Minority Leader
    "chuck schumer",         # Senate Majority Leader
    "mitch mcconnell",       # Senate Minority Leader
    "john thune",            # Senate Majority Leader (2025-)
    "steve scalise",         # House Majority Leader
    "tom emmer",             # House Majority Whip
    "kevin mccarthy",        # House Speaker (2023)
    "steny hoyer",           # House Majority Leader (former)
    "richard durbin",        # Senate Majority Whip (former)
    "john cornyn",           # Senate Majority Whip
}

AMOUNT_BUCKETS = [
    (0,         15_000,    "<15k"),
    (15_000,    50_000,    "15k-50k"),
    (50_000,    250_000,   "50k-250k"),
    (250_000, 1_000_000,   "250k-1m"),
    (1_000_000,     None,  ">1m"),
]


def _amount_bucket(amount_min: float | None) -> str:
    if amount_min is None:
        return "unknown"
    for lo, hi, label in AMOUNT_BUCKETS:
        if amount_min >= lo and (hi is None or amount_min < hi):
            return label
    return ">1m"


def _is_leader(name: str | None) -> bool:
    if not name:
        return False
    return name.strip().lower() in KNOWN_LEADERS


# ---------------------------------------------------------------------------
# SPY benchmark
# ---------------------------------------------------------------------------

def build_spy_return_lookup(spy_bars: list[dict]) -> tuple[dict, list[str]]:
    """Pre-compute SPY forward returns for every bar date × every window.
    Returns (lookup, sorted_dates) — pass both to spy_return_on().
    """
    bar_by_date = {b["date"]: i for i, b in enumerate(spy_bars)}
    lookup: dict[str, dict[int, float | None]] = {}
    for entry_date, idx in bar_by_date.items():
        lookup[entry_date] = {w: forward_return(spy_bars, idx, w) for w in WINDOWS}
    return lookup, sorted(lookup)


def spy_return_on(
    spy_lookup: dict,
    spy_dates: list[str],
    entry_date: str,
    window: int,
) -> float | None:
    """SPY forward return from the first trading day on or after entry_date.
    spy_dates must be the sorted list returned by build_spy_return_lookup().
    """
    if entry_date in spy_lookup:
        return spy_lookup[entry_date].get(window)
    idx = bisect.bisect_left(spy_dates, entry_date)
    if idx < len(spy_dates):
        return spy_lookup[spy_dates[idx]].get(window)
    return None


# ---------------------------------------------------------------------------
# Corporate co-buy stacking
# ---------------------------------------------------------------------------

def _fetch_corporate_buys(conn, tickers: list[str]) -> dict[str, list[str]]:
    if not tickers:
        return {}
    placeholders = ",".join(["%s"] * len(tickers))
    rows = conn.execute(
        f"""
        SELECT issuer_ticker, transaction_date::text AS td
        FROM filings
        WHERE issuer_ticker IN ({placeholders})
          AND transaction_code = 'P'
          AND table_type = 'ND'
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
        ORDER BY issuer_ticker, td
        """,
        tickers,
    ).fetchall()
    result: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        result[r["issuer_ticker"]].append(r["td"])
    return dict(result)


def _is_stacked(disclosure_date: str, corp_buy_dates: list[str]) -> bool:
    d  = date.fromisoformat(disclosure_date)
    lo = (d - timedelta(days=CORPORATE_STACK_WINDOW)).isoformat()
    hi = (d + timedelta(days=CORPORATE_STACK_WINDOW)).isoformat()
    i = bisect.bisect_left(corp_buy_dates, lo)
    return i < len(corp_buy_dates) and corp_buy_dates[i] <= hi


# ---------------------------------------------------------------------------
# Summary helpers
# ---------------------------------------------------------------------------

def _stats(vals: list[float], label: str = "") -> str:
    if not vals:
        return "—"
    wins = sum(1 for v in vals if v > 0)
    avg  = sum(vals) / len(vals)
    med  = sorted(vals)[len(vals) // 2]
    pfx  = f"{label} " if label else ""
    return f"{pfx}win={wins/len(vals)*100:.0f}%  avg={avg:+.1f}%  med={med:+.1f}%"


def _segment_row(label: str, sub: list[dict]) -> None:
    if not sub:
        return
    print(f"\n    {label} [{len(sub):>4} trades]")
    for w in [30, 60, 90]:
        raw    = [r[f"return_{w}d"]  for r in sub if r[f"return_{w}d"]  is not None]
        excess = [r[f"excess_{w}d"]  for r in sub if r[f"excess_{w}d"]  is not None]
        spy    = [r[f"spy_return_{w}d"] for r in sub if r[f"spy_return_{w}d"] is not None]
        print(f"      {w:>2}d  raw:{_stats(raw):<42}  vs SPY:{_stats(excess):<42}  SPY:{_stats(spy)}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Congressional buy forward-return backtest")
    parser.add_argument("--min-amount", type=float, default=15_000,
                        help="Min amount_min to include (default: $15,000)")
    parser.add_argument("--output", default="data/congress_backtest.csv")
    args = parser.parse_args()

    api_key = os.environ.get("POLYGON_API_KEY", "")
    if not api_key:
        raise SystemExit("POLYGON_API_KEY not set")

    conn       = get_cli_db()
    min_amount = args.min_amount

    rows = conn.execute("""
        SELECT politician_name, chamber, party, state, ticker,
               transaction_date, disclosure_date,
               amount_min, amount_max, amount_label,
               (disclosure_date::date - transaction_date::date) AS disclosure_lag_days
        FROM congress_trades
        WHERE LOWER(transaction_type) IN ('purchase', 'buy')
          AND ticker IS NOT NULL AND ticker != ''
          AND ticker ~ '^[A-Z]{1,5}$'
          AND disclosure_date IS NOT NULL
          AND disclosure_date >= %s
          AND disclosure_date <= %s
          AND (%s = 0 OR amount_min IS NULL OR amount_min >= %s)
        ORDER BY ticker, disclosure_date
    """, [TRADE_START, TRADE_END, min_amount, min_amount]).fetchall()

    trades    = [dict(r) for r in rows]
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_ticker[t["ticker"].strip()].append(t)

    ticker_list   = sorted(by_ticker)
    total_tickers = len(ticker_list)
    print(f"Trades: {len(trades)} | Tickers: {total_tickers} | Min amount: ${min_amount:,.0f}")
    print(f"Disclosure window: {TRADE_START} → {TRADE_END}")
    print(f"Cache dir: {CACHE_DIR}\n")

    print("Loading corporate insider buys for co-buy stacking check...")
    corp_buys = _fetch_corporate_buys(conn, ticker_list)
    conn.close()

    fetch_start = (date.fromisoformat(TRADE_START) - timedelta(days=PRICE_WARMUP_DAYS)).isoformat()
    fetch_end   = date.today().isoformat()

    # Fetch SPY benchmark bars first
    print("Fetching SPY benchmark bars...", end=" ", flush=True)
    spy_bars, spy_cached = fetch_bars("SPY", fetch_start, fetch_end, api_key)
    if not spy_bars:
        raise SystemExit("Could not fetch SPY bars — cannot compute excess returns.")
    spy_lookup, spy_dates = build_spy_return_lookup(spy_bars)
    print(f"ok ({len(spy_bars)} bars, {'cached' if spy_cached else 'live'})\n")
    if not spy_cached:
        time.sleep(RATE_LIMIT_SLEEP)

    results: list[dict] = []
    no_data: list[str]  = []

    for n, ticker in enumerate(ticker_list, 1):
        ticker_trades = by_ticker[ticker]
        print(f"[{n}/{total_tickers}] {ticker} ({len(ticker_trades)} trade{'s' if len(ticker_trades)>1 else ''})...",
              end=" ", flush=True)

        bars, was_cached = fetch_bars(ticker, fetch_start, fetch_end, api_key)

        if not bars:
            print("NO DATA")
            no_data.append(ticker)
            if not was_cached:
                time.sleep(RATE_LIMIT_SLEEP)
            continue

        bar_by_date = {b["date"]: i for i, b in enumerate(bars)}
        corp_dates  = corp_buys.get(ticker, [])

        for trade in ticker_trades:
            entry_date = trade["disclosure_date"]   # public signal date

            trade_idx = bar_by_date.get(entry_date)
            if trade_idx is None:
                for i, b in enumerate(bars):
                    if b["date"] >= entry_date:
                        trade_idx = i
                        break
            if trade_idx is None or trade_idx >= len(bars) - 5:
                continue

            gc_ok  = trade_idx >= GC_MIN_BARS
            rb_ok  = trade_idx >= RB_MIN_BARS
            cb_ok  = trade_idx >= CB_MIN_BARS_PRE
            hhl_ok = (len(bars) - trade_idx) >= max(WINDOWS) + HHL_PIVOT_WINDOW * 4

            gc_fired,  gc_days  = detect_golden_cross(bars, trade_idx)     if gc_ok  else ({w: None for w in WINDOWS}, None)
            rb_fired,  rb_days  = detect_resistance_break(bars, trade_idx)  if rb_ok  else ({w: None for w in WINDOWS}, None)
            hhl_fired, hhl_days = detect_hhl(bars, trade_idx)               if hhl_ok else ({w: None for w in WINDOWS}, None)
            cb_fired,  cb_days  = detect_channel_break(bars, trade_idx)     if cb_ok  else ({w: None for w in WINDOWS}, None)

            def _tech_stacked(w):
                return sum(1 for v in [gc_fired[w], rb_fired[w], hhl_fired[w], cb_fired[w]] if v is True)

            lag = trade.get("disclosure_lag_days")
            try:
                lag_int = int(lag) if lag is not None else None
            except (TypeError, ValueError):
                lag_int = None

            row = {
                "ticker":               ticker,
                "politician_name":      trade["politician_name"],
                "chamber":              trade["chamber"] or "",
                "party":                trade["party"] or "",
                "state":                trade["state"] or "",
                "is_leader":            _is_leader(trade["politician_name"]),
                "transaction_date":     trade["transaction_date"],
                "disclosure_date":      entry_date,
                "disclosure_lag_days":  lag_int,
                "amount_label":         trade["amount_label"] or "",
                "amount_min":           trade["amount_min"],
                "amount_bucket":        _amount_bucket(trade["amount_min"]),
                "stacked_w_corporate":  _is_stacked(entry_date, corp_dates),
                "entry_price":          bars[trade_idx]["close"],
                "gc_computable":        gc_ok,
                "gc_days":              gc_days,
                "rb_computable":        rb_ok,
                "rb_days":              rb_days,
                "hhl_computable":       hhl_ok,
                "hhl_days":             hhl_days,
                "cb_computable":        cb_ok,
                "cb_days":              cb_days,
            }

            gc_fire_rets  = _fire_returns(bars, entry_date, gc_days)
            rb_fire_rets  = _fire_returns(bars, entry_date, rb_days)
            hhl_fire_rets = _fire_returns(bars, entry_date, hhl_days)
            cb_fire_rets  = _fire_returns(bars, entry_date, cb_days)

            for w in WINDOWS:
                raw_ret = forward_return(bars, trade_idx, w)
                spy_ret = spy_return_on(spy_lookup, spy_dates, entry_date, w)
                excess  = (
                    round(raw_ret - spy_ret, 2)
                    if raw_ret is not None and spy_ret is not None
                    else None
                )
                row[f"return_{w}d"]      = raw_ret
                row[f"spy_return_{w}d"]  = spy_ret
                row[f"excess_{w}d"]      = excess        # primary output
                row[f"gc_{w}d"]          = gc_fired[w]
                row[f"rb_{w}d"]          = rb_fired[w]
                row[f"hhl_{w}d"]         = hhl_fired[w]
                row[f"cb_{w}d"]          = cb_fired[w]
                row[f"tech_stacked_{w}d"]= _tech_stacked(w)
                row[f"gc_fire_ret_{w}d"] = gc_fire_rets[w]
                row[f"rb_fire_ret_{w}d"] = rb_fire_rets[w]
                row[f"hhl_fire_ret_{w}d"]= hhl_fire_rets[w]
                row[f"cb_fire_ret_{w}d"] = cb_fire_rets[w]

            results.append(row)

        print(f"ok ({len(bars)} bars)")
        if not was_cached:
            time.sleep(RATE_LIMIT_SLEEP)

    # Write CSV
    if results:
        with open(args.output, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
            writer.writeheader()
            writer.writerows(results)
        print(f"\nWrote {len(results)} rows → {args.output}")
    else:
        print("\nNo results.")
        return

    # ---------------------------------------------------------------------------
    # Summary — lead with excess returns (raw return minus SPY same window)
    # ---------------------------------------------------------------------------
    print(f"\n{'='*70}")
    print(f"CONGRESS BACKTEST  (min_amount=${min_amount:,.0f}  entry=disclosure_date)")
    print(f"{'='*70}")
    print(f"  Trades scored:      {len(results)}")
    print(f"  Tickers w/ no data: {len(no_data)}")
    if no_data:
        print(f"  No-data tickers:    {', '.join(no_data[:20])}{'...' if len(no_data)>20 else ''}")

    # ── PRIMARY HYPOTHESIS 1: Leadership ────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  PRIMARY: Leadership filter  (Wei & Zhou NBER w34524 hypothesis)")
    print("  Hypothesis: members holding legislative power outperform post-disclosure")
    _segment_row("Leaders (known)",  [r for r in results if r["is_leader"]])
    _segment_row("Rank-and-file",    [r for r in results if not r["is_leader"]])

    # ── PRIMARY HYPOTHESIS 2: Corporate co-buy stacking ─────────────────────
    print(f"\n{'─'*70}")
    print(f"  PRIMARY: Corporate co-buy stacking  (±{CORPORATE_STACK_WINDOW}d of disclosure_date)")
    print("  Hypothesis: convergence of corporate + congressional buys = dual-channel signal")
    _segment_row("Co-buy (stacked)", [r for r in results if r["stacked_w_corporate"]])
    _segment_row("Congress only",    [r for r in results if not r["stacked_w_corporate"]])

    # ── SEGMENTATION: Disclosure lag ────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SEGMENTATION: Disclosure lag  (transaction → public filing)")
    print("  Late disclosures (>30d) may indicate deliberate/conviction positions")
    for label, lo, hi in [("≤7d", 0, 7), ("8-30d", 8, 30), ("31-90d", 31, 90), (">90d", 91, 9999)]:
        sub = [r for r in results
               if r["disclosure_lag_days"] is not None and lo <= r["disclosure_lag_days"] <= hi]
        _segment_row(label, sub)

    # ── SEGMENTATION: Chamber ───────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SEGMENTATION: Chamber")
    for grp in ("house", "senate"):
        sub = [r for r in results if (r["chamber"] or "").lower() == grp]
        _segment_row(grp.title(), sub)

    # ── SEGMENTATION: Party ─────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SEGMENTATION: Party")
    for p in sorted({r["party"] for r in results if r["party"]}):
        _segment_row(p, [r for r in results if r["party"] == p])

    # ── SEGMENTATION: Trade size ────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SEGMENTATION: Trade size (amount_bucket)")
    for _, _, bucket in AMOUNT_BUCKETS:
        sub = [r for r in results if r["amount_bucket"] == bucket]
        _segment_row(bucket, sub)

    # ── SECONDARY: Technical signals ────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  SECONDARY: Technical signals (supplemental — not primary hypothesis)")
    for sig, label in [("gc", "Golden Cross"), ("rb", "Resistance Break"),
                        ("hhl", "HH+HL"), ("cb", "Channel Break")]:
        comp = [r for r in results if r[f"{sig}_computable"]]
        if not comp:
            continue
        print(f"\n    {label}:  ({len(comp)}/{len(results)} computable)")
        for w in WINDOWS:
            fired = [r for r in comp if r[f"{sig}_{w}d"] is True]
            if not fired:
                continue
            raw    = [r[f"return_{w}d"]  for r in fired if r[f"return_{w}d"]  is not None]
            excess = [r[f"excess_{w}d"]  for r in fired if r[f"excess_{w}d"]  is not None]
            print(f"      {w:>2}d [{len(fired):>4}]  raw:{_stats(raw):<40}  vs SPY:{_stats(excess)}")

    # ── COMBINED: Leader + co-buy ────────────────────────────────────────────
    combined = [r for r in results if r["is_leader"] and r["stacked_w_corporate"]]
    if combined:
        print(f"\n{'─'*70}")
        print("  COMBINED: Leader AND co-buy (highest-conviction filter)")
        _segment_row("Leader + co-buy", combined)

    # ── PER-POLITICIAN RANKING ────────────────────────────────────────────────
    print(f"\n{'─'*70}")
    print("  PER-POLITICIAN RANKING  (min 3 trades, sorted by 90d excess vs SPY)")
    print(f"  {'Politician':<30} {'Ch':>2}  {'Pty':>3}  {'N':>4}  {'30d exc':>8}  {'60d exc':>8}  {'90d exc':>8}  {'90d win':>8}")
    print(f"  {'─'*30} {'─'*2}  {'─'*3}  {'─'*4}  {'─'*8}  {'─'*8}  {'─'*8}  {'─'*8}")

    politicians: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        politicians[r["politician_name"]].append(r)

    ranked = []
    for name, trades in politicians.items():
        if len(trades) < 3:
            continue
        chamber = (trades[0]["chamber"] or "")[:1].upper()
        party   = (trades[0]["party"] or "")[:1].upper()
        e30  = [t["excess_30d"]  for t in trades if t.get("excess_30d")  is not None]
        e60  = [t["excess_60d"]  for t in trades if t.get("excess_60d")  is not None]
        e90  = [t["excess_90d"]  for t in trades if t.get("excess_90d")  is not None]
        w90  = sum(1 for v in e90 if v > 0) / len(e90) if e90 else None
        avg30 = sum(e30) / len(e30) if e30 else None
        avg60 = sum(e60) / len(e60) if e60 else None
        avg90 = sum(e90) / len(e90) if e90 else None
        ranked.append((name, chamber, party, len(trades), avg30, avg60, avg90, w90))

    ranked.sort(key=lambda x: x[6] if x[6] is not None else -999, reverse=True)

    def _fmt_exc(v: float | None) -> str:
        return f"{v:>+7.1f}%" if v is not None else f"{'—':>8}"

    for name, ch, pty, n, a30, a60, a90, w90 in ranked:
        wfmt = f"{w90*100:>7.0f}%" if w90 is not None else f"{'—':>8}"
        print(f"  {name:<30} {ch:>2}  {pty:>3}  {n:>4}  {_fmt_exc(a30)}  {_fmt_exc(a60)}  {_fmt_exc(a90)}  {wfmt}")

    print(f"\n{'='*70}")
    print("  NOTE: 'vs SPY' = raw return minus SPY return over the same window from")
    print("  the same entry date. Positive = outperformed the market. This is the")
    print("  correct benchmark — the original Ziobrowski papers omitted it.")


if __name__ == "__main__":
    main()
