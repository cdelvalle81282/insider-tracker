"""
Candlestick exit signal analysis for stacked GC + RB trades.

For each trade, walks forward from signal entry and scans for bearish
reversal patterns. Compares pattern exit price to the 20% trailing stop
exit price to see if patterns would have gotten you out better.

Patterns tested (all require price > entry to trigger — no exiting losers):
  1. Bearish Engulfing  — red candle body fully engulfs prior green body
  2. Shooting Star      — upper shadow >= 2x body, tiny lower shadow, after uptrend
  3. Evening Star       — 3-bar: large green, small-body indecision, large red
  4. Dark Cloud Cover   — opens above prior high, closes below prior midpoint
  5. Three Black Crows  — 3 consecutive large bearish closes, each lower

Exit assumption: close of the pattern's final candle (end-of-day exit).
Baseline: 20% trailing stop from signal entry.
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
TRAIL_PCT     = 0.20

# Pattern params
ENGULF_MIN_BODY_RATIO = 0.50    # engulfing body must be >= this * prior body
STAR_SHADOW_RATIO     = 2.0     # upper shadow >= this * body size
STAR_LOWER_MAX        = 0.30    # lower shadow <= this * body size
STAR_UPTREND_BARS     = 5       # bars of net-positive closes before shooting star
EVENING_STAR_MIN_BODY = 0.50    # first candle body >= this * bar range
EVENING_SMALL_MAX     = 0.30    # middle candle body <= this * bar range
DCC_PENETRATION       = 0.50    # close must be below this fraction into prior body
CROW_MIN_BODY         = 0.60    # each crow body >= this * bar range
CROW_MIN_CLOSE_DROP   = 0.002   # each close must be lower than previous (0.2%)


def load_ohlcv(ticker: str) -> list[dict]:
    f = CACHE_DIR / f"{ticker}.json"
    if not f.exists():
        return []
    return json.loads(f.read_text())


# ---------------------------------------------------------------------------
# Pattern detectors — return True/False given a window of bars
# ---------------------------------------------------------------------------

def _body(bar: dict) -> float:
    return abs(bar["close"] - bar["open"])

def _range(bar: dict) -> float:
    return bar["high"] - bar["low"] if bar["high"] > bar["low"] else 0.0001

def _is_bullish(bar: dict) -> bool:
    return bar["close"] > bar["open"]

def _is_bearish(bar: dict) -> bool:
    return bar["close"] < bar["open"]


def bearish_engulfing(bars: list[dict], i: int) -> bool:
    """Bar i is a large bearish candle that fully engulfs bar i-1's body."""
    if i < 1:
        return False
    prev, curr = bars[i - 1], bars[i]
    if not _is_bullish(prev) or not _is_bearish(curr):
        return False
    pb = _body(prev)
    if pb == 0:
        return False
    # Current body fully wraps prior body
    if curr["open"] < prev["close"] or curr["close"] > prev["open"]:
        return False
    # Engulfing body is meaningfully larger
    return _body(curr) >= pb * ENGULF_MIN_BODY_RATIO


def shooting_star(bars: list[dict], i: int) -> bool:
    """Long upper shadow, tiny body near bottom of range, after uptrend."""
    if i < STAR_UPTREND_BARS:
        return False
    bar = bars[i]
    body = _body(bar)
    if body == 0:
        return False
    upper_low  = max(bar["open"], bar["close"])
    lower_high = min(bar["open"], bar["close"])
    upper_shadow = bar["high"] - upper_low
    lower_shadow = lower_high - bar["low"]
    if upper_shadow < body * STAR_SHADOW_RATIO:
        return False
    if lower_shadow > body * STAR_LOWER_MAX:
        return False
    # Needs prior uptrend: net close-to-close gain over last N bars
    prior = bars[i - STAR_UPTREND_BARS: i]
    if prior[-1]["close"] <= prior[0]["close"]:
        return False
    return True


def evening_star(bars: list[dict], i: int) -> bool:
    """3-bar pattern: large bullish, small-body indecision, large bearish."""
    if i < 2:
        return False
    a, b, c = bars[i - 2], bars[i - 1], bars[i]
    # Bar A: large bullish
    if not _is_bullish(a):
        return False
    if _body(a) < _range(a) * EVENING_STAR_MIN_BODY:
        return False
    # Bar B: small body (star) — gap up preferred but not required
    if _body(b) > _range(b) * EVENING_SMALL_MAX:
        return False
    # Bar C: large bearish, closes into A's body
    if not _is_bearish(c):
        return False
    if _body(c) < _range(a) * EVENING_STAR_MIN_BODY * 0.6:
        return False
    midpoint_a = a["open"] + (a["close"] - a["open"]) * 0.5
    return c["close"] <= midpoint_a


def dark_cloud_cover(bars: list[dict], i: int) -> bool:
    """Opens above prior high, closes below midpoint of prior bullish body."""
    if i < 1:
        return False
    prev, curr = bars[i - 1], bars[i]
    if not _is_bullish(prev) or not _is_bearish(curr):
        return False
    if curr["open"] <= prev["high"]:
        return False
    midpoint = prev["open"] + (prev["close"] - prev["open"]) * DCC_PENETRATION
    return curr["close"] <= midpoint


def three_black_crows(bars: list[dict], i: int) -> bool:
    """3 consecutive large bearish closes, each lower than the previous."""
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


PATTERNS = {
    "engulfing":     bearish_engulfing,
    "shooting_star": shooting_star,
    "evening_star":  evening_star,
    "dark_cloud":    dark_cloud_cover,
    "three_crows":   three_black_crows,
}


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

def find_pattern_exit(
    bars: list[dict],
    entry_idx: int,
    entry_price: float,
) -> dict[str, dict | None]:
    """
    For each pattern, find the first signal after entry where price > entry.
    Returns {pattern_name: {exit_price, exit_date, exit_ret, days} or None}.
    """
    results = {p: None for p in PATTERNS}
    for i in range(entry_idx + MIN_DAYS, len(bars)):
        bar = bars[i]
        # Only exit at profit — don't take a pattern exit below entry
        if bar["close"] <= entry_price:
            continue
        for name, fn in PATTERNS.items():
            if results[name] is not None:
                continue
            if fn(bars, i):
                days = (date.fromisoformat(bar["date"]) -
                        date.fromisoformat(bars[entry_idx]["date"])).days
                results[name] = {
                    "exit_price": bar["close"],
                    "exit_ret":   (bar["close"] - entry_price) / entry_price * 100,
                    "exit_date":  bar["date"],
                    "days":       days,
                }
    return results


def trail_exit(bars: list[dict], entry_idx: int, entry_price: float) -> dict:
    """20% trailing stop exit."""
    peak = entry_price
    entry_date = date.fromisoformat(bars[entry_idx]["date"])
    for i in range(entry_idx + MIN_DAYS, len(bars)):
        b = bars[i]
        if b["close"] > peak:
            peak = b["close"]
        if b["close"] <= peak * (1 - TRAIL_PCT):
            return {
                "exit_price": b["close"],
                "exit_ret":   (b["close"] - entry_price) / entry_price * 100,
                "exit_date":  b["date"],
                "days":       (date.fromisoformat(b["date"]) - entry_date).days,
                "peak_ret":   (peak - entry_price) / entry_price * 100,
            }
    lc = bars[-1]["close"]
    return {
        "exit_price": lc,
        "exit_ret":   (lc - entry_price) / entry_price * 100,
        "exit_date":  bars[-1]["date"],
        "days":       (date.fromisoformat(bars[-1]["date"]) - entry_date).days,
        "peak_ret":   (peak - entry_price) / entry_price * 100,
    }


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    rows    = list(csv.DictReader(open("data/backtest_results.csv")))
    stacked = [r for r in rows
               if r["gc_days"] not in ("", "None") and r["rb_days"] not in ("", "None")]
    print(f"Stacked GC+RB trades: {len(stacked)}\n")

    # Per-pattern accumulators
    pattern_exits: dict[str, list[dict]] = {p: [] for p in PATTERNS}
    trail_exits:   list[dict] = []
    comparisons:   dict[str, list[float]] = {p: [] for p in PATTERNS}  # improvement vs trail

    for r in stacked:
        td = r["trade_date"][:10]
        sd = min(int(float(r[s])) for s in ("gc_days", "rb_days")
                 if r[s] not in ("", "None"))
        sig_date = (date.fromisoformat(td) + timedelta(days=sd)).isoformat()

        bars = load_ohlcv(r["ticker"])
        if not bars:
            continue

        # Find entry index
        entry_idx = None
        for idx, b in enumerate(bars):
            if b["date"] >= sig_date:
                entry_idx = idx
                break
        if entry_idx is None or entry_idx >= len(bars) - 5:
            continue

        entry_price = bars[entry_idx]["close"]
        if not entry_price:
            continue

        # Cap at MAX_HOLD_DAYS
        max_date = (date.fromisoformat(sig_date) + timedelta(days=MAX_HOLD_DAYS)).isoformat()
        end_idx  = next((i for i, b in enumerate(bars) if b["date"] > max_date), len(bars))
        window   = bars[:end_idx]

        # Trailing stop baseline
        te = trail_exit(window, entry_idx, entry_price)
        trail_exits.append(te)

        # Pattern exits
        pe = find_pattern_exit(window, entry_idx, entry_price)
        for name, exit_info in pe.items():
            if exit_info:
                pattern_exits[name].append({**exit_info, "trail_exit_ret": te["exit_ret"]})
                # Positive = pattern got you out better
                comparisons[name].append(exit_info["exit_ret"] - te["exit_ret"])

    # Baseline stats
    trail_rets = [r["exit_ret"] for r in trail_exits]
    print(f"BASELINE 20% trailing stop — {len(trail_exits)} trades")
    print(f"  Avg {statistics.mean(trail_rets):+.1f}%  "
          f"Median {statistics.median(trail_rets):+.1f}%  "
          f"Win {sum(1 for x in trail_rets if x>0)/len(trail_rets)*100:.0f}%\n")

    # Pattern results
    print(f"{'Pattern':>16}  {'Fired':>6}  {'Fire%':>6}  "
          f"{'AvgExit':>8}  {'MedExit':>8}  {'WinRate':>8}  "
          f"{'AvgDays':>8}  {'vs Trail':>9}  {'BetterThan':>11}")
    print("-" * 100)

    for name in PATTERNS:
        exits = pattern_exits[name]
        if not exits:
            print(f"  {name:>14}:   never fired")
            continue

        exit_rets = [e["exit_ret"] for e in exits]
        deltas    = comparisons[name]
        better    = sum(1 for d in deltas if d > 0)
        n         = len(exits)
        total     = len(trail_exits)

        print(f"  {name:>14}  {n:>6}  {n/total*100:>5.0f}%  "
              f"{statistics.mean(exit_rets):>+8.1f}%  "
              f"{statistics.median(exit_rets):>+8.1f}%  "
              f"{sum(1 for x in exit_rets if x>0)/n*100:>7.0f}%  "
              f"{statistics.mean(e['days'] for e in exits):>8.0f}  "
              f"{statistics.mean(deltas):>+9.1f}%  "
              f"{better}/{n} ({better/n*100:.0f}%)")

    # Detail: for best pattern, show what trades got out better
    best_pattern = max(PATTERNS, key=lambda p:
        statistics.mean(comparisons[p]) if comparisons[p] else -999)
    best_exits = pattern_exits[best_pattern]
    best_deltas = comparisons[best_pattern]

    if best_exits:
        print(f"\nBest pattern: {best_pattern}")
        better_trades = [(e, d) for e, d in zip(best_exits, best_deltas) if d > 0]
        worse_trades  = [(e, d) for e, d in zip(best_exits, best_deltas) if d <= 0]
        print(f"  Got out BETTER than trail: {len(better_trades)} trades")
        if better_trades:
            bt_rets = [e["exit_ret"] for e, _ in better_trades]
            tr_rets = [e["trail_exit_ret"] for e, _ in better_trades]
            print(f"    Pattern exit avg:  {statistics.mean(bt_rets):+.1f}%")
            print(f"    Trail exit avg:    {statistics.mean(tr_rets):+.1f}%")
            print(f"    Avg improvement:   {statistics.mean(d for _, d in better_trades):+.1f}%")
            print(f"    Avg days earlier:  {statistics.mean(e['days'] for e, _ in better_trades):.0f}d "
                  f"vs trail {statistics.mean(e['trail_exit_ret'] for e, _ in better_trades):+.1f}% avg")
        print(f"  Got out WORSE than trail: {len(worse_trades)} trades")
        if worse_trades:
            wt_rets = [e["exit_ret"] for e, _ in worse_trades]
            print(f"    Pattern exit avg:  {statistics.mean(wt_rets):+.1f}%")
            print(f"    Avg cost vs trail: {statistics.mean(d for _, d in worse_trades):+.1f}%")

    # Combined: what if you exited on whichever came first — pattern or trail?
    print("\nCOMBINED: exit on pattern OR trail — whichever fires first (pattern only while > entry)")
    for name in PATTERNS:
        exits = pattern_exits[name]
        if not exits:
            continue
        # For each trade: take pattern exit if it fired before trail, else trail
        combined_rets = []
        for te in trail_exits:
            # Find if this trade had a pattern exit earlier
            matched = next(
                (e for e in exits
                 if e.get("exit_date") and e["exit_date"] <= te["exit_date"]
                 and e["exit_ret"] > 0),
                None,
            )
            combined_rets.append(matched["exit_ret"] if matched else te["exit_ret"])

        avg_c = statistics.mean(combined_rets)
        med_c = statistics.median(combined_rets)
        win_c = sum(1 for x in combined_rets if x > 0) / len(combined_rets) * 100
        delta = avg_c - statistics.mean(trail_rets)
        print(f"  {name:>14}: avg {avg_c:>+.1f}%  median {med_c:>+.1f}%  "
              f"win {win_c:.0f}%  vs baseline {delta:>+.1f}%")


if __name__ == "__main__":
    main()
