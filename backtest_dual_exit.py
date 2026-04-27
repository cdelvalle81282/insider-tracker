"""
Dual-exit system: Three Black Crows + trailing stop.

For each stacked GC+RB trade, simulates both exits simultaneously and
exits on whichever fires first. Tests different trailing stop widths
alongside the pattern to find the optimal pairing.

Also breaks down: what happened on the trades where the pattern fired
vs the trades where only the trailing stop decided the exit.
"""
from __future__ import annotations

import csv
import json
import statistics
from datetime import date, timedelta
from pathlib import Path

CACHE_DIR     = Path("data/polygon_cache")
MAX_HOLD_DAYS = 365
MIN_DAYS      = 3

TRAIL_WIDTHS  = [0.12, 0.15, 0.18, 0.20, 0.25]   # trailing stop variants to test

# Three Black Crows params (same as backtest_candles.py)
CROW_MIN_BODY       = 0.60
CROW_MIN_CLOSE_DROP = 0.002


def load_ohlcv(ticker: str) -> list[dict]:
    f = CACHE_DIR / f"{ticker}.json"
    if not f.exists():
        return []
    return json.loads(f.read_text())


def _body(bar: dict) -> float:
    return abs(bar["close"] - bar["open"])

def _range(bar: dict) -> float:
    return max(bar["high"] - bar["low"], 0.0001)

def _is_bearish(bar: dict) -> bool:
    return bar["close"] < bar["open"]


def is_three_crows(bars: list[dict], i: int) -> bool:
    if i < 2:
        return False
    w1, w2, w3 = bars[i - 2], bars[i - 1], bars[i]
    for w in (w1, w2, w3):
        if not _is_bearish(w):
            return False
        if _body(w) < _range(w) * CROW_MIN_BODY:
            return False
    return (w2["close"] < w1["close"] * (1 - CROW_MIN_CLOSE_DROP) and
            w3["close"] < w2["close"] * (1 - CROW_MIN_CLOSE_DROP))


def simulate_dual(
    bars: list[dict],
    entry_idx: int,
    entry_price: float,
    trail_pct: float,
) -> dict:
    """
    Walk forward simultaneously tracking:
      - Trailing stop (always active)
      - Three Black Crows (only triggers when price > entry)
    Exit on whichever fires first.
    """
    peak       = entry_price
    entry_date = date.fromisoformat(bars[entry_idx]["date"])

    for i in range(entry_idx + MIN_DAYS, len(bars)):
        bar      = bars[i]
        bar_date = date.fromisoformat(bar["date"])

        if bar["close"] > peak:
            peak = bar["close"]

        trail_stop = peak * (1 - trail_pct)
        days       = (bar_date - entry_date).days

        # Three Black Crows — only fire above entry
        if bar["close"] > entry_price and is_three_crows(bars, i):
            return {
                "exit_ret":    (bar["close"] - entry_price) / entry_price * 100,
                "peak_ret":    (peak - entry_price) / entry_price * 100,
                "days":        days,
                "exit_by":     "crows",
            }

        # Trailing stop
        if bar["close"] <= trail_stop:
            return {
                "exit_ret":    (bar["close"] - entry_price) / entry_price * 100,
                "peak_ret":    (peak - entry_price) / entry_price * 100,
                "days":        days,
                "exit_by":     "trail",
            }

    lc = bars[-1]["close"]
    return {
        "exit_ret":    (lc - entry_price) / entry_price * 100,
        "peak_ret":    (peak - entry_price) / entry_price * 100,
        "days":        (date.fromisoformat(bars[-1]["date"]) - entry_date).days,
        "exit_by":     "held",
    }


def simulate_trail_only(
    bars: list[dict],
    entry_idx: int,
    entry_price: float,
    trail_pct: float,
) -> dict:
    peak       = entry_price
    entry_date = date.fromisoformat(bars[entry_idx]["date"])
    for i in range(entry_idx + MIN_DAYS, len(bars)):
        bar = bars[i]
        if bar["close"] > peak:
            peak = bar["close"]
        if bar["close"] <= peak * (1 - trail_pct):
            return {
                "exit_ret":  (bar["close"] - entry_price) / entry_price * 100,
                "peak_ret":  (peak - entry_price) / entry_price * 100,
                "days":      (date.fromisoformat(bar["date"]) - entry_date).days,
                "exit_by":   "trail",
            }
    lc = bars[-1]["close"]
    return {
        "exit_ret":  (lc - entry_price) / entry_price * 100,
        "peak_ret":  (peak - entry_price) / entry_price * 100,
        "days":      (date.fromisoformat(bars[-1]["date"]) - entry_date).days,
        "exit_by":   "held",
    }


def summarize(results: list[dict]) -> dict:
    exits = [r["exit_ret"] for r in results]
    wins  = sum(1 for x in exits if x > 0)
    return {
        "n":      len(results),
        "avg":    statistics.mean(exits),
        "median": statistics.median(exits),
        "win":    wins / len(exits) * 100,
        "avg_peak": statistics.mean(r["peak_ret"] for r in results),
        "avg_days": statistics.mean(r["days"] for r in results),
        "by_crows": sum(1 for r in results if r["exit_by"] == "crows"),
        "by_trail": sum(1 for r in results if r["exit_by"] == "trail"),
        "by_held":  sum(1 for r in results if r["exit_by"] == "held"),
    }


def build_entries(rows: list[dict]) -> list[tuple[str, str]]:
    entries = []
    for r in rows:
        if r["gc_days"] in ("", "None") or r["rb_days"] in ("", "None"):
            continue
        td = r["trade_date"][:10]
        sd = min(int(float(r[s])) for s in ("gc_days", "rb_days"))
        sig = (date.fromisoformat(td) + timedelta(days=sd)).isoformat()
        entries.append((r["ticker"], sig))
    return entries


def main() -> None:
    rows    = list(csv.DictReader(open("data/backtest_results.csv")))
    entries = build_entries(rows)
    print(f"Stacked GC+RB entries: {len(entries)}\n")

    # Pre-load bars + resolve entry index per trade once
    trade_data: list[tuple[list[dict], int, float]] = []
    for ticker, sig_date in entries:
        bars = load_ohlcv(ticker)
        if not bars:
            continue
        entry_idx = next((i for i, b in enumerate(bars) if b["date"] >= sig_date), None)
        if entry_idx is None or entry_idx >= len(bars) - 5:
            continue
        entry_price = bars[entry_idx]["close"]
        if not entry_price:
            continue
        max_date = (date.fromisoformat(sig_date) + timedelta(days=MAX_HOLD_DAYS)).isoformat()
        end_idx  = next((i for i, b in enumerate(bars) if b["date"] > max_date), len(bars))
        trade_data.append((bars[:end_idx], entry_idx, entry_price))

    print(f"Trades with price data: {len(trade_data)}\n")

    # Header
    print(f"{'System':>28}  {'Avg':>8}  {'Median':>8}  {'Win':>7}  "
          f"{'AvgPeak':>8}  {'AvgDays':>8}  {'Crows':>7}  {'Trail':>7}  {'Held':>6}")
    print("-" * 105)

    best_avg  = -999.0
    best_label = ""
    best_detail: dict | None = None

    for trail_pct in TRAIL_WIDTHS:
        # Trail only baseline
        trail_res = [simulate_trail_only(b, ei, ep, trail_pct)
                     for b, ei, ep in trade_data]
        ts = summarize(trail_res)
        label = f"Trail {trail_pct*100:.0f}% only"
        print(f"  {label:>26}  {ts['avg']:>+8.1f}%  {ts['median']:>+8.1f}%  "
              f"{ts['win']:>6.0f}%  {ts['avg_peak']:>+7.1f}%  {ts['avg_days']:>8.0f}  "
              f"{'—':>7}  {ts['by_trail']:>7}  {ts['by_held']:>6}")

        # Dual: Crows + trail
        dual_res = [simulate_dual(b, ei, ep, trail_pct)
                    for b, ei, ep in trade_data]
        ds = summarize(dual_res)
        dlabel = f"Crows + {trail_pct*100:.0f}% trail"
        delta  = ds["avg"] - ts["avg"]
        flag   = " <-- best" if ds["avg"] > best_avg else ""
        if ds["avg"] > best_avg:
            best_avg   = ds["avg"]
            best_label = dlabel
            best_detail = {"dual": dual_res, "trail": trail_res, "trail_pct": trail_pct, "s": ds, "ts": ts}
        print(f"  {dlabel:>26}  {ds['avg']:>+8.1f}%  {ds['median']:>+8.1f}%  "
              f"{ds['win']:>6.0f}%  {ds['avg_peak']:>+7.1f}%  {ds['avg_days']:>8.0f}  "
              f"{ds['by_crows']:>7}  {ds['by_trail']:>7}  {ds['by_held']:>6}  "
              f"(vs trail only: {delta:>+.1f}%){flag}")
        print()

    # Deep dive on best config
    if best_detail:
        ds     = best_detail["s"]
        ts_    = best_detail["ts"]
        dual   = best_detail["dual"]
        trail  = best_detail["trail"]

        print(f"\n{'='*65}")
        print(f"BEST CONFIG: {best_label}")
        print(f"{'='*65}")
        print(f"  Avg return:   {ds['avg']:+.1f}%  (trail-only: {ts_['avg']:+.1f}%,  delta {ds['avg']-ts_['avg']:+.1f}%)")
        print(f"  Median:       {ds['median']:+.1f}%  (trail-only: {ts_['median']:+.1f}%)")
        print(f"  Win rate:     {ds['win']:.0f}%   (trail-only: {ts_['win']:.0f}%)")
        print(f"  Avg days:     {ds['avg_days']:.0f}d  (trail-only: {ts_['avg_days']:.0f}d)")
        print(f"  Crows exits:  {ds['by_crows']} trades ({ds['by_crows']/ds['n']*100:.0f}%)")
        print(f"  Trail exits:  {ds['by_trail']} trades ({ds['by_trail']/ds['n']*100:.0f}%)")
        print(f"  Held to end:  {ds['by_held']} trades ({ds['by_held']/ds['n']*100:.0f}%)")

        # Breakdown by exit type
        crows_grp = [r for r in dual if r["exit_by"] == "crows"]
        trail_grp = [r for r in dual if r["exit_by"] == "trail"]
        held_grp  = [r for r in dual if r["exit_by"] == "held"]

        print()
        for grp, lbl in [(crows_grp, "Crows exits"), (trail_grp, "Trail exits"), (held_grp, "Held to end")]:
            if not grp:
                continue
            exits = [r["exit_ret"] for r in grp]
            peaks = [r["peak_ret"] for r in grp]
            wins  = sum(1 for x in exits if x > 0)
            print(f"  {lbl} ({len(grp)}):")
            print(f"    Avg exit {statistics.mean(exits):+.1f}%  "
                  f"Median {statistics.median(exits):+.1f}%  "
                  f"Win {wins/len(grp)*100:.0f}%  "
                  f"Avg peak {statistics.mean(peaks):+.1f}%  "
                  f"Avg days {statistics.mean(r['days'] for r in grp):.0f}")

        # Trade-level comparison: dual vs trail-only for same trades
        print(f"\n  Trade-level comparison (dual vs trail-only, same {len(dual)} trades):")
        improvements = [d["exit_ret"] - t["exit_ret"] for d, t in zip(dual, trail)]
        better = [(d, t, imp) for d, t, imp in zip(dual, trail, improvements) if imp > 0]
        worse  = [(d, t, imp) for d, t, imp in zip(dual, trail, improvements) if imp < 0]
        same   = [(d, t, imp) for d, t, imp in zip(dual, trail, improvements) if imp == 0]

        print(f"    Dual better: {len(better)} trades  avg improvement {statistics.mean(i for _,_,i in better):+.1f}%")
        print(f"    Trail better:{len(worse)} trades  avg cost        {statistics.mean(i for _,_,i in worse):+.1f}%")
        print(f"    Same result: {len(same)} trades")

        if better:
            # Where crows got you out better — what would trail have done?
            crow_saved = [(d, t, imp) for d, t, imp in better if d["exit_by"] == "crows"]
            if crow_saved:
                print(f"\n    Crows fired BEFORE trail and got better exit: {len(crow_saved)} trades")
                print(f"      Crows exit avg:  {statistics.mean(d['exit_ret'] for d,_,_ in crow_saved):+.1f}%")
                print(f"      Trail would have:{statistics.mean(t['exit_ret'] for _,t,_ in crow_saved):+.1f}%")
                print(f"      Avg saved:        {statistics.mean(i for _,_,i in crow_saved):+.1f}% per trade")
                print(f"      Avg days earlier: {statistics.mean(t['days']-d['days'] for d,t,_ in crow_saved):.0f}d")

        print("\n  Return distribution:")
        buckets = [(-999,-20,"< -20%"),(-20,-10,"-20 to -10%"),(-10,0,"-10 to 0%"),
                   (0,10,"0 to +10%"),(10,20,"+10 to +20%"),(20,50,"+20 to +50%"),(50,999,"> +50%")]
        for lo, hi, lbl in buckets:
            nd = sum(1 for r in dual  if lo <= r["exit_ret"] < hi)
            nt = sum(1 for r in trail if lo <= r["exit_ret"] < hi)
            diff = nd - nt
            sign = "+" if diff > 0 else ""
            bar  = "#" * (nd // 2)
            print(f"    {lbl:>14}: dual {nd:>3}  trail {nt:>3}  ({sign}{diff})  {bar}")


if __name__ == "__main__":
    main()
