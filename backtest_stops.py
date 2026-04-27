"""
Trailing stop analysis for Golden Cross + Resistance Break signals.

For each trade where GC or RB fired:
  - Entry = close on signal fire date
  - Walk forward day by day, maintaining a trailing stop
  - Exit when close drops below (peak_close * (1 - trail_pct))
  - Also exit if no exit triggered within MAX_HOLD_DAYS (let it run)

Tests TRAIL_LEVELS from 5% to 25% and reports:
  - Avg / median exit return
  - Avg max gain achieved (peak return before exit)
  - Avg days held
  - % of max gain captured (exit_return / max_gain)
  - Win rate (exit_return > 0)
  - How often stopped out vs held to MAX_HOLD_DAYS

Usage:
    python backtest_stops.py
"""
from __future__ import annotations

import csv
import json
import statistics
from datetime import date, timedelta
from pathlib import Path

CACHE_DIR    = Path("data/polygon_cache")
CSV_PATH     = Path("data/backtest_results.csv")
TRAIL_LEVELS = [0.05, 0.08, 0.10, 0.12, 0.15, 0.20, 0.25]
MAX_HOLD_DAYS = 365   # walk up to a year forward before declaring "still holding"
MIN_DAYS_AFTER_SIGNAL = 3   # don't exit in the first 3 days (avoid whipsaws)


def load_bars(ticker: str) -> dict[str, float]:
    """Return {date_str: close} from cache."""
    f = CACHE_DIR / f"{ticker}.json"
    if not f.exists():
        return {}
    bars = json.loads(f.read_text())
    return {b["date"]: b["close"] for b in bars if b.get("close")}


def simulate_trail(
    closes: list[tuple[str, float]],   # [(date, close), ...] from signal fire onward
    trail_pct: float,
    min_days: int,
) -> dict:
    """
    Walk closes forward. Returns dict with:
      entry_price, exit_price, exit_date, exit_return_pct,
      max_price, max_return_pct, pct_of_max_captured,
      days_held, stopped_out (bool)
    """
    if not closes:
        return {}

    entry_price = closes[0][1]
    if not entry_price:
        return {}

    peak = entry_price
    stop = peak * (1 - trail_pct)

    for i, (dt, close) in enumerate(closes):
        if close > peak:
            peak = close
            stop = peak * (1 - trail_pct)

        # Don't stop out in the first MIN_DAYS_AFTER_SIGNAL bars
        if i >= min_days and close <= stop:
            exit_price = close
            exit_date  = dt
            stopped_out = True
            break
    else:
        # Never stopped — use last available close
        exit_price  = closes[-1][1]
        exit_date   = closes[-1][0]
        stopped_out = False

    exit_ret = (exit_price - entry_price) / entry_price * 100
    max_ret  = (peak - entry_price)       / entry_price * 100
    pct_captured = (exit_ret / max_ret * 100) if max_ret > 0 else (100.0 if exit_ret >= 0 else 0.0)
    days_held = (date.fromisoformat(exit_date) - date.fromisoformat(closes[0][0])).days

    return {
        "entry_price":      entry_price,
        "exit_price":       exit_price,
        "exit_date":        exit_date,
        "exit_return_pct":  round(exit_ret, 2),
        "max_return_pct":   round(max_ret, 2),
        "pct_of_max":       round(pct_captured, 1),
        "days_held":        days_held,
        "stopped_out":      stopped_out,
    }


def main() -> None:
    if not CSV_PATH.exists():
        raise SystemExit(f"Backtest CSV not found: {CSV_PATH}")

    rows = list(csv.DictReader(open(CSV_PATH)))
    print(f"Loaded {len(rows)} trades from {CSV_PATH}\n")

    # Filter to GC or RB fired trades
    gc_rb = [
        r for r in rows
        if r["gc_days"] not in ("", "None") or r["rb_days"] not in ("", "None")
    ]
    print(f"Trades with GC or RB fired: {len(gc_rb)}")

    # For stacked (both GC and RB)
    stacked = [
        r for r in rows
        if r["gc_days"] not in ("", "None") and r["rb_days"] not in ("", "None")
    ]
    print(f"Trades with BOTH GC + RB fired: {len(stacked)}\n")

    for label, subset in [("GC or RB (either)", gc_rb), ("GC + RB (stacked)", stacked)]:
        print(f"{'='*60}")
        print(f"  {label}  —  {len(subset)} trades")
        print(f"{'='*60}")

        # Build signal entries: (ticker, signal_fire_date)
        entries: list[tuple[str, str, str]] = []   # (ticker, trade_date, signal_date)
        for r in subset:
            ticker = r["ticker"]
            trade_date = r["trade_date"]

            # Use whichever signal fired first (or earliest of the two)
            days_list = []
            for sig in ("gc_days", "rb_days"):
                if r[sig] not in ("", "None"):
                    days_list.append(int(float(r[sig])))
            if not days_list:
                continue
            signal_days = min(days_list)
            signal_date = (date.fromisoformat(trade_date[:10]) + timedelta(days=signal_days)).isoformat()
            entries.append((ticker, trade_date, signal_date))

        # Simulate each trail level
        results_by_trail: dict[float, list[dict]] = {t: [] for t in TRAIL_LEVELS}

        for ticker, trade_date, signal_date in entries:
            closes_map = load_bars(ticker)
            if not closes_map:
                continue

            # All bars from signal fire date onward, up to MAX_HOLD_DAYS
            max_date = (date.fromisoformat(signal_date) + timedelta(days=MAX_HOLD_DAYS)).isoformat()
            forward = sorted(
                [(d, c) for d, c in closes_map.items() if signal_date <= d <= max_date],
                key=lambda x: x[0],
            )
            if len(forward) < 5:
                continue

            for trail_pct in TRAIL_LEVELS:
                res = simulate_trail(forward, trail_pct, MIN_DAYS_AFTER_SIGNAL)
                if res:
                    results_by_trail[trail_pct].append(res)

        print(f"\n{'Trail':>6}  {'Trades':>7}  {'AvgRet':>8}  {'MedRet':>8}  "
              f"{'AvgMaxGn':>9}  {'%MaxCapt':>9}  {'AvgDays':>8}  "
              f"{'WinRate':>8}  {'StopRate':>9}")
        print("-" * 90)

        for trail_pct in TRAIL_LEVELS:
            res = results_by_trail[trail_pct]
            if not res:
                continue

            exit_rets  = [r["exit_return_pct"] for r in res]
            max_rets   = [r["max_return_pct"]  for r in res]
            pct_max    = [r["pct_of_max"]       for r in res]
            days_held  = [r["days_held"]         for r in res]
            wins       = sum(1 for r in res if r["exit_return_pct"] > 0)
            stops      = sum(1 for r in res if r["stopped_out"])

            avg_ret  = statistics.mean(exit_rets)
            med_ret  = statistics.median(exit_rets)
            avg_max  = statistics.mean(max_rets)
            avg_capt = statistics.mean(pct_max)
            avg_days = statistics.mean(days_held)
            win_rate = wins / len(res) * 100
            stop_rate= stops / len(res) * 100

            print(f"  {trail_pct*100:4.0f}%  {len(res):>7}  {avg_ret:>+8.1f}%  {med_ret:>+8.1f}%  "
                  f"{avg_max:>+9.1f}%  {avg_capt:>8.1f}%  {avg_days:>8.0f}  "
                  f"{win_rate:>7.1f}%  {stop_rate:>8.1f}%")

        # Best avg return for this subset
        best_trail = max(TRAIL_LEVELS, key=lambda t: statistics.mean(
            r["exit_return_pct"] for r in results_by_trail[t]
        ) if results_by_trail[t] else -999)
        best = results_by_trail[best_trail]
        print(f"\n  Best avg return: {best_trail*100:.0f}% trail → "
              f"avg {statistics.mean(r['exit_return_pct'] for r in best):+.1f}%  "
              f"median {statistics.median(r['exit_return_pct'] for r in best):+.1f}%\n")


if __name__ == "__main__":
    main()
