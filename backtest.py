"""
Backtest: insider open-market buy → trend change signal.

Signals tested per trade, within 30/60/90-day forward windows:
  gc  — Golden cross (50MA crosses above 200MA)
  rb  — Resistance break (close > resistance zone touched 2+ times pre-trade)
  hhl — Higher highs + higher lows (2 consecutive confirmed swing HH + HL)
  cb  — Channel break (close > upper bound of a sideways pre-trade channel)

Usage:
    python backtest.py [--min-value 1000000] [--output data/backtest_results.csv]

Requires POLYGON_API_KEY in environment (or .env file loaded by caller).
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import sqlite3
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx

# ---------------------------------------------------------------------------
# Config — all fuzzy signal knobs in one place
# ---------------------------------------------------------------------------

TRADE_START = "2024-08-01"   # earliest trade (90-day channel lookback feasible)
TRADE_END   = "2026-01-25"   # latest trade with 90d of forward price data

CACHE_DIR = Path("data/polygon_cache")
RATE_LIMIT_SLEEP = 13        # seconds between live API calls (free tier ≈ 5/min)

WINDOWS = [30, 60, 90]       # forward windows (calendar days)

# Golden cross
GC_FAST  = 50
GC_SLOW  = 200

# Resistance break
RB_LOOKBACK_DAYS   = 180   # calendar days of pre-trade history to scan
RB_PEAK_WINDOW     = 3     # bars each side to qualify as a local high
RB_CLUSTER_PCT     = 0.02  # ±2% tolerance to cluster peaks into one level
RB_MIN_TOUCHES     = 2     # minimum peaks in a cluster to call it resistance
RB_BREAK_PCT       = 0.01  # close must exceed resistance by this fraction

# Higher highs / higher lows
HHL_PIVOT_WINDOW   = 3     # bars each side for swing pivot confirmation
HHL_MIN_CONSEC     = 2     # consecutive higher highs AND higher lows required

# Channel break
CB_LOOKBACK_DAYS   = 90    # calendar days of pre-trade history to measure channel
CB_MAX_RANGE_PCT   = 0.20  # high-low range ≤ this → stock is "sideways"
CB_BREAK_PCT       = 0.01  # close must exceed channel top by this fraction
CB_MIN_BARS        = 20    # minimum bars in channel window

# Computable thresholds (in trading bars, not calendar days)
GC_MIN_BARS  = GC_SLOW          # 200 bars for 200-day MA
RB_MIN_BARS  = 130              # ~180 calendar days at 5/7 trading ratio
CB_MIN_BARS_PRE = 65            # ~90 calendar days


# ---------------------------------------------------------------------------
# Price data
# ---------------------------------------------------------------------------

def _fetch_live(ticker: str, from_date: str, to_date: str, api_key: str) -> list[dict]:
    url = (
        f"https://api.polygon.io/v2/aggs/ticker/{ticker.upper()}"
        f"/range/1/day/{from_date}/{to_date}"
    )
    for attempt in range(3):
        try:
            resp = httpx.get(
                url,
                params={"apiKey": api_key, "limit": 5000, "adjusted": "true", "sort": "asc"},
                timeout=15.0,
            )
            if resp.status_code == 429:
                print(f"    [429 rate-limit] waiting 60s (attempt {attempt+1}/3)...")
                time.sleep(60)
                continue
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"    [WARN] fetch error: {e}")
            return []

        results = data.get("results") or []
        bars = []
        for bar in results:
            t = bar.get("t")
            if t is None:
                continue
            day = datetime.fromtimestamp(t / 1000, tz=timezone.utc).strftime("%Y-%m-%d")
            bars.append({
                "date":   day,
                "open":   bar.get("o", 0.0),
                "high":   bar.get("h", 0.0),
                "low":    bar.get("l", 0.0),
                "close":  bar.get("c", 0.0),
                "volume": bar.get("v", 0.0),
            })
        return bars

    return []


def fetch_bars(ticker: str, from_date: str, to_date: str, api_key: str) -> tuple[list[dict], bool]:
    """Return (bars, was_cached). Caches to CACHE_DIR/{ticker}.json."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{ticker}.json"

    if cache_file.exists():
        try:
            bars = json.loads(cache_file.read_text())
            return bars, True
        except Exception:
            pass

    bars = _fetch_live(ticker, from_date, to_date, api_key)
    if bars:
        cache_file.write_text(json.dumps(bars))
    return bars, False


# ---------------------------------------------------------------------------
# Signal helpers
# ---------------------------------------------------------------------------

def _sma(bars: list[dict], idx: int, window: int) -> float | None:
    if idx < window - 1:
        return None
    return sum(b["close"] for b in bars[idx - window + 1: idx + 1]) / window


def _local_peaks(bars: list[dict], start_i: int, end_i: int, window: int) -> list[tuple[int, float]]:
    """Return (bar_idx, high) for local highs in [start_i, end_i), no look-ahead."""
    peaks = []
    for i in range(start_i + window, min(end_i, len(bars)) - window):
        h = bars[i]["high"]
        if (all(h > bars[i - j]["high"] for j in range(1, window + 1)) and
                all(h > bars[i + j]["high"] for j in range(1, window + 1))):
            peaks.append((i, h))
    return peaks


def _local_troughs(bars: list[dict], start_i: int, end_i: int, window: int) -> list[tuple[int, float]]:
    """Return (bar_idx, low) for local lows in [start_i, end_i), no look-ahead."""
    troughs = []
    for i in range(start_i + window, min(end_i, len(bars)) - window):
        lo = bars[i]["low"]
        if (all(lo < bars[i - j]["low"] for j in range(1, window + 1)) and
                all(lo < bars[i + j]["low"] for j in range(1, window + 1))):
            troughs.append((i, lo))
    return troughs


# ---------------------------------------------------------------------------
# Signal detectors  (each returns ({30: bool, 60: bool, 90: bool}, days_int|None))
# ---------------------------------------------------------------------------

def detect_golden_cross(bars: list[dict], trade_idx: int) -> tuple[dict, int | None]:
    fired = {w: False for w in WINDOWS}
    days_to_fire = None

    ma50_t  = _sma(bars, trade_idx, GC_FAST)
    ma200_t = _sma(bars, trade_idx, GC_SLOW)
    if ma50_t is None or ma200_t is None or ma50_t >= ma200_t:
        return fired, days_to_fire   # not below the slow MA at trade — no crossover coming

    trade_date = bars[trade_idx]["date"]
    for i in range(trade_idx + 1, len(bars)):
        ma50  = _sma(bars, i, GC_FAST)
        ma200 = _sma(bars, i, GC_SLOW)
        if ma50 is None or ma200 is None:
            continue
        if ma50 > ma200:
            days = (date.fromisoformat(bars[i]["date"]) - date.fromisoformat(trade_date)).days
            days_to_fire = days
            for w in WINDOWS:
                fired[w] = days <= w
            break

    return fired, days_to_fire


def detect_resistance_break(bars: list[dict], trade_idx: int) -> tuple[dict, int | None]:
    fired = {w: False for w in WINDOWS}
    days_to_fire = None

    trade_date_obj = date.fromisoformat(bars[trade_idx]["date"])
    cutoff = (trade_date_obj - timedelta(days=RB_LOOKBACK_DAYS)).isoformat()

    # Find local peaks in pre-trade window only
    start_i = next((i for i, b in enumerate(bars) if b["date"] >= cutoff), 0)
    raw_peaks = _local_peaks(bars, start_i, trade_idx, RB_PEAK_WINDOW)

    if not raw_peaks:
        return fired, days_to_fire

    # Cluster by price proximity
    prices = sorted(p[1] for p in raw_peaks)
    clusters: list[list[float]] = [[prices[0]]]
    for p in prices[1:]:
        if (p - clusters[-1][0]) / clusters[-1][0] <= RB_CLUSTER_PCT:
            clusters[-1].append(p)
        else:
            clusters.append([p])

    levels = [sum(c) / len(c) for c in clusters if len(c) >= RB_MIN_TOUCHES]
    if not levels:
        return fired, days_to_fire

    trade_price = bars[trade_idx]["close"]
    above = [lvl for lvl in levels if lvl > trade_price]
    if not above:
        return fired, days_to_fire

    nearest  = min(above)
    break_at = nearest * (1 + RB_BREAK_PCT)
    trade_date = bars[trade_idx]["date"]

    for i in range(trade_idx + 1, len(bars)):
        if bars[i]["close"] >= break_at:
            days = (date.fromisoformat(bars[i]["date"]) - date.fromisoformat(trade_date)).days
            days_to_fire = days
            for w in WINDOWS:
                fired[w] = days <= w
            break

    return fired, days_to_fire


def detect_hhl(bars: list[dict], trade_idx: int) -> tuple[dict, int | None]:
    """Higher highs + higher lows. Pivot confirmed at bar i + HHL_PIVOT_WINDOW."""
    fired = {w: False for w in WINDOWS}
    days_to_fire = None

    trade_date = bars[trade_idx]["date"]
    scan_end = min(trade_idx + max(WINDOWS) + HHL_PIVOT_WINDOW * 4, len(bars))

    highs   = _local_peaks(bars, trade_idx, scan_end, HHL_PIVOT_WINDOW)
    troughs = _local_troughs(bars, trade_idx, scan_end, HHL_PIVOT_WINDOW)

    # Find when 2 consecutive HH are confirmed (confirmation = pivot_idx + WINDOW)
    hh_confirmed_bar = None
    for k in range(1, len(highs)):
        if highs[k][1] > highs[k - 1][1]:
            hh_confirmed_bar = highs[k][0] + HHL_PIVOT_WINDOW
            break

    hl_confirmed_bar = None
    for k in range(1, len(troughs)):
        if troughs[k][1] > troughs[k - 1][1]:
            hl_confirmed_bar = troughs[k][0] + HHL_PIVOT_WINDOW
            break

    if hh_confirmed_bar is None or hl_confirmed_bar is None:
        return fired, days_to_fire

    fire_bar = max(hh_confirmed_bar, hl_confirmed_bar)
    if fire_bar >= len(bars):
        return fired, days_to_fire

    days = (date.fromisoformat(bars[fire_bar]["date"]) - date.fromisoformat(trade_date)).days
    days_to_fire = days
    for w in WINDOWS:
        fired[w] = days <= w

    return fired, days_to_fire


def detect_channel_break(bars: list[dict], trade_idx: int) -> tuple[dict, int | None]:
    fired = {w: False for w in WINDOWS}
    days_to_fire = None

    trade_date_obj = date.fromisoformat(bars[trade_idx]["date"])
    cutoff = (trade_date_obj - timedelta(days=CB_LOOKBACK_DAYS)).isoformat()

    pre = [b for b in bars[:trade_idx] if b["date"] >= cutoff]
    if len(pre) < CB_MIN_BARS:
        return fired, days_to_fire

    ch_high = max(b["high"] for b in pre)
    ch_low  = min(b["low"]  for b in pre)
    if ch_low == 0:
        return fired, days_to_fire

    if (ch_high - ch_low) / ch_low > CB_MAX_RANGE_PCT:
        return fired, days_to_fire  # not sideways

    break_at   = ch_high * (1 + CB_BREAK_PCT)
    trade_date = bars[trade_idx]["date"]

    for i in range(trade_idx + 1, len(bars)):
        if bars[i]["close"] >= break_at:
            days = (date.fromisoformat(bars[i]["date"]) - date.fromisoformat(trade_date)).days
            days_to_fire = days
            for w in WINDOWS:
                fired[w] = days <= w
            break

    return fired, days_to_fire


def forward_return(bars: list[dict], trade_idx: int, days: int) -> float | None:
    trade_price = bars[trade_idx]["close"]
    if not trade_price:
        return None
    target = date.fromisoformat(bars[trade_idx]["date"]) + timedelta(days=days)
    for i in range(trade_idx + 1, len(bars)):
        if date.fromisoformat(bars[i]["date"]) >= target:
            return round((bars[i]["close"] - trade_price) / trade_price * 100, 2)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Insider buy trend-signal backtest")
    parser.add_argument("--min-value", type=float, default=1_000_000, help="Min insider buy $")
    parser.add_argument("--output", default="data/backtest_results.csv")
    args = parser.parse_args()

    api_key = os.environ.get("POLYGON_API_KEY", "")
    if not api_key:
        raise SystemExit("POLYGON_API_KEY not set")

    conn = sqlite3.connect("data/insider_tracker.db")
    conn.row_factory = sqlite3.Row
    rows = conn.execute("""
        SELECT issuer_ticker, issuer_name, insider_name, insider_title,
               transaction_date, total_value, price_per_share,
               is_10b5_1, is_director, is_officer, is_ten_percent_owner
        FROM filings
        WHERE transaction_code = 'P'
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND TRIM(issuer_ticker) IS NOT NULL
          AND TRIM(issuer_ticker) NOT IN ('NONE', 'N/A', '')
          AND total_value >= ?
          AND transaction_date >= ?
          AND transaction_date <= ?
        ORDER BY issuer_ticker, transaction_date
    """, [args.min_value, TRADE_START, TRADE_END]).fetchall()

    trades = [dict(r) for r in rows]
    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_ticker[t["issuer_ticker"].strip()].append(t)

    ticker_list = sorted(by_ticker)
    total_tickers = len(ticker_list)
    print(f"Trades: {len(trades)} | Tickers: {total_tickers} | Min value: ${args.min_value:,.0f}")
    print(f"Trade window: {TRADE_START} → {TRADE_END}")
    print(f"Cache dir: {CACHE_DIR}\n")

    # Fetch bars back 300 days before earliest qualifying trade
    fetch_start = (date.fromisoformat(TRADE_START) - timedelta(days=300)).isoformat()
    fetch_end   = date.today().isoformat()

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

        for trade in ticker_trades:
            td = trade["transaction_date"]

            # Snap to nearest bar on or after trade date
            trade_idx = bar_by_date.get(td)
            if trade_idx is None:
                for i, b in enumerate(bars):
                    if b["date"] >= td:
                        trade_idx = i
                        break
            if trade_idx is None or trade_idx >= len(bars) - 5:
                continue

            # Per-signal computability
            gc_ok  = trade_idx >= GC_MIN_BARS
            rb_ok  = trade_idx >= RB_MIN_BARS
            cb_ok  = trade_idx >= CB_MIN_BARS_PRE
            hhl_ok = (len(bars) - trade_idx) >= max(WINDOWS) + HHL_PIVOT_WINDOW * 4

            gc_fired,  gc_days  = detect_golden_cross(bars, trade_idx)    if gc_ok  else ({w: None for w in WINDOWS}, None)
            rb_fired,  rb_days  = detect_resistance_break(bars, trade_idx) if rb_ok  else ({w: None for w in WINDOWS}, None)
            hhl_fired, hhl_days = detect_hhl(bars, trade_idx)              if hhl_ok else ({w: None for w in WINDOWS}, None)
            cb_fired,  cb_days  = detect_channel_break(bars, trade_idx)    if cb_ok  else ({w: None for w in WINDOWS}, None)

            def _stacked(w):
                return sum(1 for v in [gc_fired[w], rb_fired[w], hhl_fired[w], cb_fired[w]] if v is True)

            results.append({
                "ticker":         ticker,
                "issuer_name":    trade["issuer_name"],
                "trade_date":     td,
                "insider_name":   trade["insider_name"],
                "title":          trade["insider_title"] or "",
                "value":          trade["total_value"],
                "trade_price":    trade["price_per_share"],
                "is_10b5_1":      trade["is_10b5_1"],
                "is_director":    trade["is_director"],
                "is_officer":     trade["is_officer"],
                # Golden cross
                "gc_computable":  gc_ok,
                "gc_30d": gc_fired[30], "gc_60d": gc_fired[60], "gc_90d": gc_fired[90],
                "gc_days": gc_days,
                # Resistance break
                "rb_computable":  rb_ok,
                "rb_30d": rb_fired[30], "rb_60d": rb_fired[60], "rb_90d": rb_fired[90],
                "rb_days": rb_days,
                # Higher highs / lows
                "hhl_computable": hhl_ok,
                "hhl_30d": hhl_fired[30], "hhl_60d": hhl_fired[60], "hhl_90d": hhl_fired[90],
                "hhl_days": hhl_days,
                # Channel break
                "cb_computable":  cb_ok,
                "cb_30d": cb_fired[30], "cb_60d": cb_fired[60], "cb_90d": cb_fired[90],
                "cb_days": cb_days,
                # Stacked signal count
                "stacked_30d": _stacked(30),
                "stacked_60d": _stacked(60),
                "stacked_90d": _stacked(90),
                # Forward returns
                "return_30d": forward_return(bars, trade_idx, 30),
                "return_60d": forward_return(bars, trade_idx, 60),
                "return_90d": forward_return(bars, trade_idx, 90),
            })

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

    # Summary
    print(f"\n{'='*60}")
    print(f"BACKTEST SUMMARY  (min_value=${args.min_value:,.0f})")
    print(f"{'='*60}")
    print(f"  Trades scored:      {len(results)}")
    print(f"  Tickers w/ no data: {len(no_data)}")
    if no_data:
        print(f"  No-data tickers:  {', '.join(no_data[:20])}{'...' if len(no_data)>20 else ''}")

    for sig, label in [("gc","Golden Cross"), ("rb","Resistance Break"), ("hhl","HH+HL"), ("cb","Channel Break")]:
        print(f"\n  {label}:")
        comp = [r for r in results if r[f"{sig}_computable"]]
        print(f"    Computable trades: {len(comp)}/{len(results)}")
        for w in WINDOWS:
            fired = [r for r in comp if r[f"{sig}_{w}d"] is True]
            rets  = [r[f"return_{w}d"] for r in fired if r[f"return_{w}d"] is not None]
            if fired:
                avg = sum(rets) / len(rets) if rets else float("nan")
                med = sorted(rets)[len(rets)//2] if rets else float("nan")
                print(f"    {w:>2}d: {len(fired):>4}/{len(comp)} ({len(fired)/len(comp)*100:5.1f}%) "
                      f"| avg ret {avg:+.1f}%  median {med:+.1f}%")

    print(f"\n  Stacked signals (of {len(results)} trades):")
    for w in WINDOWS:
        for n_sig in range(1, 5):
            cnt = sum(1 for r in results if r[f"stacked_{w}d"] >= n_sig)
            if cnt:
                rets = [r[f"return_{w}d"] for r in results
                        if r[f"stacked_{w}d"] >= n_sig and r[f"return_{w}d"] is not None]
                avg = sum(rets)/len(rets) if rets else float("nan")
                print(f"    {w}d ≥{n_sig} signal(s): {cnt} trades | avg return {avg:+.1f}%")


if __name__ == "__main__":
    main()
