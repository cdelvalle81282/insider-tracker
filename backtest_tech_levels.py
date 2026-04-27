"""
Technical level analysis of 20% trailing stop exits.

For each stacked GC+RB trade stopped out by the 20% trail, checks whether
the exit price coincides with:
  1. Moving averages at exit bar (20MA, 50MA, 200MA)
  2. Fibonacci retracements of the entry→peak move (23.6%, 38.2%, 50%, 61.8%, 78.6%)
  3. Prior resistance zones (price levels tested 2+ times pre-entry)

Proximity tolerance: within PROX_PCT of the level counts as a "hit".
"""
from __future__ import annotations

import csv
import json
import statistics
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path

CACHE_DIR     = Path("data/polygon_cache")
MAX_HOLD_DAYS = 365
MIN_DAYS      = 3
TRAIL_PCT     = 0.20
PROX_PCT      = 0.02   # within 2% of a level counts as a hit

# Fibonacci retracement levels (of the entry→peak move)
FIB_LEVELS = {
    "23.6%": 0.236,
    "38.2%": 0.382,
    "50.0%": 0.500,
    "61.8%": 0.618,
    "78.6%": 0.786,
}

# Resistance detection params (same as backtest.py)
RES_LOOKBACK_DAYS  = 180
RES_PEAK_WINDOW    = 3
RES_CLUSTER_PCT    = 0.02
RES_MIN_TOUCHES    = 2


def load_ohlcv(ticker: str) -> list[dict]:
    f = CACHE_DIR / f"{ticker}.json"
    if not f.exists():
        return []
    return json.loads(f.read_text())


def sma(bars: list[dict], idx: int, window: int) -> float | None:
    if idx < window - 1:
        return None
    return sum(b["close"] for b in bars[idx - window + 1: idx + 1]) / window


def near(price: float, level: float, pct: float = PROX_PCT) -> bool:
    return abs(price - level) / level <= pct


def find_resistance_levels(bars: list[dict], entry_idx: int) -> list[float]:
    """Prior resistance zones in the 180 days before entry."""
    entry_date = date.fromisoformat(bars[entry_idx]["date"])
    cutoff     = (entry_date - timedelta(days=RES_LOOKBACK_DAYS)).isoformat()

    peaks = []
    start = next((i for i, b in enumerate(bars) if b["date"] >= cutoff), 0)
    for i in range(start + RES_PEAK_WINDOW, entry_idx - RES_PEAK_WINDOW):
        h = bars[i]["high"]
        if (all(h > bars[i - j]["high"] for j in range(1, RES_PEAK_WINDOW + 1)) and
                all(h > bars[i + j]["high"] for j in range(1, RES_PEAK_WINDOW + 1))):
            peaks.append(h)

    if not peaks:
        return []

    prices = sorted(peaks)
    clusters: list[list[float]] = [[prices[0]]]
    for p in prices[1:]:
        if (p - clusters[-1][0]) / clusters[-1][0] <= RES_CLUSTER_PCT:
            clusters[-1].append(p)
        else:
            clusters.append([p])

    return [sum(c) / len(c) for c in clusters if len(c) >= RES_MIN_TOUCHES]


def simulate_trail(
    bars: list[dict],
    entry_idx: int,
    entry_price: float,
) -> dict | None:
    """Run 20% trailing stop, return exit info including peak."""
    peak       = entry_price
    entry_date = date.fromisoformat(bars[entry_idx]["date"])
    for i in range(entry_idx + MIN_DAYS, len(bars)):
        b = bars[i]
        if b["close"] > peak:
            peak = b["close"]
        if b["close"] <= peak * (1 - TRAIL_PCT):
            return {
                "exit_price": b["close"],
                "exit_idx":   i,
                "exit_date":  b["date"],
                "exit_ret":   (b["close"] - entry_price) / entry_price * 100,
                "peak":       peak,
                "peak_ret":   (peak - entry_price) / entry_price * 100,
                "days":       (date.fromisoformat(b["date"]) - entry_date).days,
                "stopped":    True,
            }
    return None   # not stopped — only analyze stopped trades


def analyze_exit(
    bars: list[dict],
    exit_idx: int,
    exit_price: float,
    entry_price: float,
    peak: float,
    entry_idx: int,
) -> dict:
    """
    At the exit bar, check proximity to each technical level.
    Returns a dict of {level_name: bool}.
    """
    hits: dict[str, bool] = {}

    # Moving averages at exit bar
    for window, name in [(20, "MA20"), (50, "MA50"), (200, "MA200")]:
        ma = sma(bars, exit_idx, window)
        hits[name] = ma is not None and near(exit_price, ma)

    # Fibonacci retracements of entry→peak move
    move = peak - entry_price
    if move > 0:
        for label, ratio in FIB_LEVELS.items():
            fib_price = peak - move * ratio
            hits[f"Fib{label}"] = near(exit_price, fib_price)
    else:
        for label in FIB_LEVELS:
            hits[f"Fib{label}"] = False

    # Prior resistance
    res_levels = find_resistance_levels(bars, entry_idx)
    hits["Resistance"] = any(near(exit_price, lvl) for lvl in res_levels)

    return hits


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
    print(f"Stacked GC+RB entries: {len(entries)}")

    level_names = (
        ["MA20", "MA50", "MA200"] +
        [f"Fib{k}" for k in FIB_LEVELS] +
        ["Resistance"]
    )

    # Counters
    hits_total:   dict[str, int]           = defaultdict(int)
    hits_by_ret:  dict[str, list[float]]   = defaultdict(list)  # exit_ret when hit
    no_hit_rets:  list[float]              = []
    any_hit_rets: list[float]              = []
    stopped_exits: list[dict]             = []

    # Per-level combination: which levels co-occur?
    combos: list[frozenset] = []

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
        window   = bars[:end_idx]

        result = simulate_trail(window, entry_idx, entry_price)
        if not result:
            continue   # not stopped — skip

        analysis = analyze_exit(
            window,
            result["exit_idx"],
            result["exit_price"],
            entry_price,
            result["peak"],
            entry_idx,
        )

        stopped_exits.append({**result, **analysis})
        fired = [name for name in level_names if analysis.get(name)]
        combos.append(frozenset(fired))

        for name in level_names:
            if analysis.get(name):
                hits_total[name] += 1
                hits_by_ret[name].append(result["exit_ret"])

        if fired:
            any_hit_rets.append(result["exit_ret"])
        else:
            no_hit_rets.append(result["exit_ret"])

    n = len(stopped_exits)
    print(f"Trail-stopped trades analyzed: {n}\n")

    # Level hit rates
    print(f"{'Level':>14}  {'Hits':>6}  {'Hit%':>6}  {'AvgExitRet':>12}  {'MedExitRet':>12}")
    print("-" * 60)
    for name in level_names:
        h = hits_total[name]
        if h == 0:
            print(f"  {name:>12}     0      0%           —              —")
            continue
        rets = hits_by_ret[name]
        print(f"  {name:>12}  {h:>5}  {h/n*100:>5.0f}%  "
              f"{statistics.mean(rets):>+11.1f}%  {statistics.median(rets):>+11.1f}%")

    # Any level hit vs no level hit
    print(f"\nHit at least one level: {len(any_hit_rets)}/{n} ({len(any_hit_rets)/n*100:.0f}%)")
    if any_hit_rets:
        print(f"  Avg exit ret: {statistics.mean(any_hit_rets):+.1f}%  "
              f"Median: {statistics.median(any_hit_rets):+.1f}%")
    print(f"No level hit:           {len(no_hit_rets)}/{n} ({len(no_hit_rets)/n*100:.0f}%)")
    if no_hit_rets:
        print(f"  Avg exit ret: {statistics.mean(no_hit_rets):+.1f}%  "
              f"Median: {statistics.median(no_hit_rets):+.1f}%")

    # Most common combinations
    combo_counts: dict[frozenset, list[float]] = defaultdict(list)
    for i, combo in enumerate(combos):
        combo_counts[combo].append(stopped_exits[i]["exit_ret"])

    print("\nMost common level combinations at exit (top 10):")
    sorted_combos = sorted(combo_counts.items(), key=lambda x: len(x[1]), reverse=True)[:10]
    for combo, rets in sorted_combos:
        label = ", ".join(sorted(combo)) if combo else "(none)"
        print(f"  {len(rets):>3}x  avg {statistics.mean(rets):>+6.1f}%  — {label}")

    # Fib distribution: which Fib level is hit most often?
    fib_names = [f"Fib{k}" for k in FIB_LEVELS]
    print("\nFibonacci proximity by gain-at-peak bucket:")
    print("  (How does the Fib hit rate vary by how much the stock gained before stopping?)")
    peak_buckets = [(0, 15, "peak  0-15%"), (15, 30, "peak 15-30%"),
                    (30, 50, "peak 30-50%"), (50, 100, "peak 50-100%"), (100, 999, "peak  >100%")]
    for lo, hi, lbl in peak_buckets:
        group = [r for r in stopped_exits if lo <= r["peak_ret"] < hi]
        if not group:
            continue
        fib_hits = {fn: sum(1 for r in group if r.get(fn)) for fn in fib_names}
        best_fib = max(fib_hits, key=lambda k: fib_hits[k])
        hit_str  = "  ".join(f"{fn.replace('Fib','')}: {fib_hits[fn]}/{len(group)}" for fn in fib_names)
        print(f"  {lbl} ({len(group)} trades)  {hit_str}  → closest: {best_fib}")

    # MA proximity: which MA is most often near the exit?
    print("\nMA proximity by exit return bucket:")
    ret_buckets = [(-999, -10, "exit < -10%"), (-10, 0, "exit -10–0%"),
                   (0, 10,    "exit 0–10%"), (10, 999, "exit > 10%")]
    for lo, hi, lbl in ret_buckets:
        group = [r for r in stopped_exits if lo <= r["exit_ret"] < hi]
        if not group:
            continue
        ma_hits = {ma: sum(1 for r in group if r.get(ma)) for ma in ["MA20", "MA50", "MA200"]}
        hit_str = "  ".join(f"{ma}: {ma_hits[ma]}/{len(group)}" for ma in ["MA20","MA50","MA200"])
        print(f"  {lbl:>14} ({len(group):>3} trades)  {hit_str}")


if __name__ == "__main__":
    main()
