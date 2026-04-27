"""
Near-entry peak analysis for stacked GC+RB trail-stopped trades.

Questions answered:
  1. What distinguishes trades that peaked near entry (0-15% peak gain)
     from big runners (30%+) — measurable at signal time?
  2. Does a tighter stop on the near-entry group help or hurt?
  3. How does the "sell half at 100%" rule affect the 100%+ trades?
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

TIGHT_STOPS   = [0.05, 0.08, 0.10, 0.12, 0.15]


def load_ohlcv(ticker: str) -> list[dict]:
    f = CACHE_DIR / f"{ticker}.json"
    if not f.exists():
        return []
    return json.loads(f.read_text())


def sma(bars: list[dict], idx: int, window: int) -> float | None:
    if idx < window - 1:
        return None
    return sum(b["close"] for b in bars[idx - window + 1: idx + 1]) / window


def avg_volume(bars: list[dict], idx: int, window: int = 20) -> float | None:
    if idx < window:
        return None
    return statistics.mean(b["volume"] for b in bars[idx - window: idx])


def simulate_trail(
    bars: list[dict], entry_idx: int, entry_price: float, trail_pct: float
) -> dict:
    peak = entry_price
    entry_date = date.fromisoformat(bars[entry_idx]["date"])
    for i in range(entry_idx + MIN_DAYS, len(bars)):
        b = bars[i]
        if b["close"] > peak:
            peak = b["close"]
        if b["close"] <= peak * (1 - trail_pct):
            return {
                "exit_ret":  (b["close"] - entry_price) / entry_price * 100,
                "peak_ret":  (peak - entry_price) / entry_price * 100,
                "days":      (date.fromisoformat(b["date"]) - entry_date).days,
                "stopped":   True,
            }
    lc = bars[-1]["close"]
    return {
        "exit_ret":  (lc - entry_price) / entry_price * 100,
        "peak_ret":  (peak - entry_price) / entry_price * 100,
        "days":      (date.fromisoformat(bars[-1]["date"]) - entry_date).days,
        "stopped":   False,
    }


def simulate_sell_half_at_100(
    bars: list[dict], entry_idx: int, entry_price: float
) -> dict:
    """
    Simulate: sell half at 100% gain, trail the rest with 20% stop.
    Returns blended exit return.
    """
    peak = entry_price
    half_sold = False
    half_sold_ret = None
    entry_date = date.fromisoformat(bars[entry_idx]["date"])

    for i in range(entry_idx + MIN_DAYS, len(bars)):
        b = bars[i]
        if b["close"] > peak:
            peak = b["close"]

        # Sell half at 100%
        if not half_sold and b["close"] >= entry_price * 2.0:
            half_sold = True
            half_sold_ret = (b["close"] - entry_price) / entry_price * 100  # = ~100%

        # Trail stop on full position (or remaining half)
        if b["close"] <= peak * (1 - TRAIL_PCT):
            trail_ret = (b["close"] - entry_price) / entry_price * 100
            if half_sold and half_sold_ret is not None:
                blended = (half_sold_ret + trail_ret) / 2
            else:
                blended = trail_ret
            return {
                "exit_ret":      blended,
                "half_ret":      half_sold_ret,
                "trail_ret":     trail_ret,
                "peak_ret":      (peak - entry_price) / entry_price * 100,
                "half_sold":     half_sold,
                "days":          (date.fromisoformat(b["date"]) - entry_date).days,
            }

    lc = bars[-1]["close"]
    trail_ret = (lc - entry_price) / entry_price * 100
    blended = (half_sold_ret + trail_ret) / 2 if (half_sold and half_sold_ret is not None) else trail_ret
    return {
        "exit_ret":  blended,
        "half_ret":  half_sold_ret,
        "trail_ret": trail_ret,
        "peak_ret":  (peak - entry_price) / entry_price * 100,
        "half_sold": half_sold,
        "days":      (date.fromisoformat(bars[-1]["date"]) - entry_date).days,
    }


def entry_context(bars: list[dict], entry_idx: int, entry_price: float) -> dict:
    """Measurable signals at entry time."""
    ctx = {}

    # Price vs MAs at entry
    ma20  = sma(bars, entry_idx, 20)
    ma50  = sma(bars, entry_idx, 50)
    ma200 = sma(bars, entry_idx, 200)
    ctx["above_ma20"]  = ma20  is not None and entry_price > ma20
    ctx["above_ma50"]  = ma50  is not None and entry_price > ma50
    ctx["above_ma200"] = ma200 is not None and entry_price > ma200

    # Distance above 50MA (how extended?)
    if ma50:
        ctx["pct_above_ma50"] = (entry_price - ma50) / ma50 * 100
    else:
        ctx["pct_above_ma50"] = None

    # Volume at entry vs 20-day average
    avg_vol = avg_volume(bars, entry_idx, 20)
    entry_vol = bars[entry_idx].get("volume", 0)
    ctx["vol_ratio"] = (entry_vol / avg_vol) if avg_vol else None

    # RSI-like: % of last 14 bars that were up-closes
    if entry_idx >= 14:
        recent = bars[entry_idx - 14: entry_idx]
        ups = sum(1 for b in recent if b["close"] > b["open"])
        ctx["up_pct_14d"] = ups / 14 * 100
    else:
        ctx["up_pct_14d"] = None

    # 20-day ATR as % of price (volatility proxy)
    if entry_idx >= 20:
        trs = [max(b["high"] - b["low"],
                   abs(b["high"] - bars[j]["close"]),
                   abs(b["low"]  - bars[j]["close"]))
               for j, b in enumerate(bars[entry_idx - 19: entry_idx], entry_idx - 20)]
        ctx["atr_pct"] = (statistics.mean(trs) / entry_price * 100) if trs else None
    else:
        ctx["atr_pct"] = None

    # 20-day return before entry (momentum)
    if entry_idx >= 20:
        ctx["mom_20d"] = (entry_price - bars[entry_idx - 20]["close"]) / bars[entry_idx - 20]["close"] * 100
    else:
        ctx["mom_20d"] = None

    return ctx


def build_entries(rows: list[dict]) -> list[tuple[str, str, dict]]:
    entries = []
    for r in rows:
        if r["gc_days"] in ("", "None") or r["rb_days"] in ("", "None"):
            continue
        td = r["trade_date"][:10]
        sd = min(int(float(r[s])) for s in ("gc_days", "rb_days"))
        sig = (date.fromisoformat(td) + timedelta(days=sd)).isoformat()
        entries.append((r["ticker"], sig, r))
    return entries


def fmt(vals: list[float], label: str = "") -> str:
    if not vals:
        return f"{label}: n/a"
    return (f"{label}: avg {statistics.mean(vals):+.1f}%  "
            f"median {statistics.median(vals):+.1f}%  n={len(vals)}")


def main() -> None:
    rows    = list(csv.DictReader(open("data/backtest_results.csv")))
    entries = build_entries(rows)
    print(f"Stacked GC+RB entries: {len(entries)}\n")

    # Collect per-trade data
    trades: list[dict] = []
    for ticker, sig_date, row in entries:
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

        base     = simulate_trail(window, entry_idx, entry_price, TRAIL_PCT)
        ctx      = entry_context(window, entry_idx, entry_price)
        sell_half = simulate_sell_half_at_100(window, entry_idx, entry_price)

        trades.append({
            "ticker":       ticker,
            "sig_date":     sig_date,
            "signal_days":  min(int(float(row[s])) for s in ("gc_days","rb_days")),
            "trade_value":  float(row["value"] or 0),
            **base,
            **{f"ctx_{k}": v for k, v in ctx.items()},
            "sell_half_ret":    sell_half["exit_ret"],
            "sell_half_sold":   sell_half["half_sold"],
            "sell_half_peak":   sell_half["peak_ret"],
        })

    # Split by peak bucket
    near   = [t for t in trades if t["peak_ret"] <  15]   # peaked near entry
    mid    = [t for t in trades if 15 <= t["peak_ret"] < 50]
    big    = [t for t in trades if t["peak_ret"] >= 50]
    huge   = [t for t in trades if t["peak_ret"] >= 100]

    print("Peak buckets:")
    print(f"  < 15% peak  : {len(near):>3} trades  "
          f"avg exit {statistics.mean(t['exit_ret'] for t in near):+.1f}%")
    print(f"  15–50% peak : {len(mid):>3} trades  "
          f"avg exit {statistics.mean(t['exit_ret'] for t in mid):+.1f}%")
    print(f"  50%+ peak   : {len(big):>3} trades  "
          f"avg exit {statistics.mean(t['exit_ret'] for t in big):+.1f}%")
    print(f"  100%+ peak  : {len(huge):>3} trades  "
          f"avg exit {statistics.mean(t['exit_ret'] for t in huge):+.1f}%")

    # -------------------------------------------------------------------------
    # 1. Entry context: near-entry-peak vs big runners
    # -------------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("SECTION 1: Entry characteristics — near-peak vs big runners")
    print(f"{'='*60}")

    ctx_fields = [
        ("ctx_pct_above_ma50", "% above 50MA at entry"),
        ("ctx_vol_ratio",      "Volume ratio (entry/20d avg)"),
        ("ctx_up_pct_14d",     "% up-close bars in prior 14d"),
        ("ctx_atr_pct",        "ATR as % of price (volatility)"),
        ("ctx_mom_20d",        "20-day momentum before entry"),
        ("signal_days",        "Days from trade to signal"),
    ]

    print(f"\n{'Metric':>34}  {'Near(<15%)':>12}  {'Big(50%+)':>12}  {'Diff':>8}")
    print("-" * 75)
    for field, label in ctx_fields:
        near_vals = [t[field] for t in near if t[field] is not None]
        big_vals  = [t[field] for t in big  if t[field] is not None]
        if not near_vals or not big_vals:
            continue
        na = statistics.mean(near_vals)
        ba = statistics.mean(big_vals)
        print(f"  {label:>32}  {na:>+12.1f}  {ba:>+12.1f}  {ba-na:>+8.1f}")

    # Boolean flags
    bool_fields = [
        ("ctx_above_ma20",  "Above 20MA at entry"),
        ("ctx_above_ma50",  "Above 50MA at entry"),
        ("ctx_above_ma200", "Above 200MA at entry"),
    ]
    print()
    for field, label in bool_fields:
        near_pct = sum(1 for t in near if t[field]) / len(near) * 100
        big_pct  = sum(1 for t in big  if t[field]) / len(big)  * 100
        print(f"  {label:>32}: near {near_pct:>5.0f}%  big {big_pct:>5.0f}%")

    # -------------------------------------------------------------------------
    # 2. Tighter stop on near-entry group
    # -------------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("SECTION 2: Tighter stops on near-entry-peak trades (<15% peak)")
    print(f"{'='*60}")
    print(f"  Baseline (20% trail): avg "
          f"{statistics.mean(t['exit_ret'] for t in near):+.1f}%  "
          f"median {statistics.median(t['exit_ret'] for t in near):+.1f}%\n")

    for ticker, sig_date, _ in [(t["ticker"], t["sig_date"], None) for t in near[:1]]:
        pass  # just warm up

    # Re-run with tighter stops for near group only
    near_entries = [(t["ticker"], t["sig_date"]) for t in near]
    print(f"  {'Stop':>6}  {'AvgRet':>8}  {'MedRet':>8}  {'WinRate':>8}  "
          f"{'AvgDays':>8}  vs 20% trail")
    print("  " + "-" * 60)

    for tight in TIGHT_STOPS:
        rets = []
        days_list = []
        for ticker, sig_date in near_entries:
            bars = load_ohlcv(ticker)
            if not bars:
                continue
            ei = next((i for i, b in enumerate(bars) if b["date"] >= sig_date), None)
            if ei is None or ei >= len(bars) - 5:
                continue
            ep = bars[ei]["close"]
            if not ep:
                continue
            md = (date.fromisoformat(sig_date) + timedelta(days=MAX_HOLD_DAYS)).isoformat()
            end = next((i for i, b in enumerate(bars) if b["date"] > md), len(bars))
            res = simulate_trail(bars[:end], ei, ep, tight)
            rets.append(res["exit_ret"])
            days_list.append(res["days"])

        if not rets:
            continue
        base_avg = statistics.mean(t["exit_ret"] for t in near)
        avg  = statistics.mean(rets)
        med  = statistics.median(rets)
        win  = sum(1 for x in rets if x > 0) / len(rets) * 100
        days = statistics.mean(days_list)
        delta = avg - base_avg
        print(f"  {tight*100:>5.0f}%  {avg:>+8.1f}%  {med:>+8.1f}%  "
              f"{win:>7.1f}%  {days:>8.0f}  {delta:>+.1f}%")

    # -------------------------------------------------------------------------
    # 3. Sell-half-at-100% analysis
    # -------------------------------------------------------------------------
    print(f"\n{'='*60}")
    print("SECTION 3: Sell half at 100% gain rule")
    print(f"{'='*60}")

    reached_100 = [t for t in trades if t["sell_half_sold"]]

    print(f"\n  Trades that reached 100% gain: {len(reached_100)}/{len(trades)} "
          f"({len(reached_100)/len(trades)*100:.0f}%)")

    if reached_100:
        flat_rets = [t["exit_ret"] for t in reached_100]
        half_rets = [t["sell_half_ret"] for t in reached_100]
        peak_rets = [t["sell_half_peak"] for t in reached_100]
        print(f"  Flat 20% trail exit avg:    {statistics.mean(flat_rets):>+.1f}%  "
              f"median {statistics.median(flat_rets):>+.1f}%")
        print(f"  Sell-half blended avg:      {statistics.mean(half_rets):>+.1f}%  "
              f"median {statistics.median(half_rets):>+.1f}%")
        print(f"  Delta (sell-half vs flat):  "
              f"{statistics.mean(h-f for h,f in zip(half_rets,flat_rets)):>+.1f}% avg")
        print(f"  Avg peak gain seen:         {statistics.mean(peak_rets):>+.1f}%")

        print("\n  What happened AFTER hitting 100%?")
        still_ran  = [t for t in reached_100 if t["sell_half_peak"] > 110]
        reversed_b = [t for t in reached_100 if t["exit_ret"] < 80]
        print(f"  Continued past 110% peak:   {len(still_ran)} trades  "
              f"avg peak {statistics.mean(t['sell_half_peak'] for t in still_ran):+.1f}%  "
              f"avg trail exit {statistics.mean(t['exit_ret'] for t in still_ran):+.1f}%")
        print(f"  Reversed (trail exit <80%): {len(reversed_b)} trades  "
              f"avg exit {statistics.mean(t['exit_ret'] for t in reversed_b):+.1f}%  "
              f"(sell-half would have locked in 100% on half)")

    # Overall sell-half impact across ALL trades
    all_flat = [t["exit_ret"] for t in trades]
    all_half = [t["sell_half_ret"] for t in trades]
    print(f"\n  Impact across ALL {len(trades)} trades:")
    print(f"  Flat trail avg:      {statistics.mean(all_flat):>+.1f}%  "
          f"median {statistics.median(all_flat):>+.1f}%  "
          f"win {sum(1 for x in all_flat if x>0)/len(all_flat)*100:.0f}%")
    print(f"  With sell-half avg:  {statistics.mean(all_half):>+.1f}%  "
          f"median {statistics.median(all_half):>+.1f}%  "
          f"win {sum(1 for x in all_half if x>0)/len(all_half)*100:.0f}%")
    print(f"  Delta:               {statistics.mean(h-f for h,f in zip(all_half,all_flat)):>+.1f}%")


if __name__ == "__main__":
    main()
