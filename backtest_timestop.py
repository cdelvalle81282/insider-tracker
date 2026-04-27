"""
Time stop backtest for stacked GC + RB signals.

Logic:
  - Enter at signal fire date
  - After TIME_STOP_DAYS calendar days, check if gain >= MIN_GAIN_PCT
      - If not: exit (stagnation stop)
      - If yes: continue holding with a 20% trailing stop

Tests a grid of (time_stop_days, min_gain_pct) combos.
Baseline: flat 20% trail with no time stop.
"""
from __future__ import annotations

import csv
import json
import statistics
from datetime import date, timedelta
from pathlib import Path

CACHE_DIR     = Path("data/polygon_cache")
MAX_HOLD_DAYS = 365
MIN_DAYS      = 3        # bars before any stop can trigger
TRAIL_PCT     = 0.20     # trailing stop once past time stop check

TIME_STOP_DAYS  = [15, 20, 25, 30]    # calendar days to check
MIN_GAIN_PCTS   = [0.00, 0.03, 0.05]  # minimum gain required to stay in


def load_bars(ticker: str) -> dict[str, float]:
    f = CACHE_DIR / f"{ticker}.json"
    if not f.exists():
        return {}
    return {b["date"]: b["close"]
            for b in json.loads(f.read_text()) if b.get("close")}


def simulate_timestop(
    closes: list[tuple[str, float]],
    time_stop_days: int,
    min_gain_pct: float,
) -> dict | None:
    if not closes:
        return None
    entry = closes[0][1]
    if not entry:
        return None

    entry_date    = date.fromisoformat(closes[0][0])
    check_date    = entry_date + timedelta(days=time_stop_days)
    time_checked  = False
    peak          = entry

    for i, (dt, close) in enumerate(closes):
        bar_date = date.fromisoformat(dt)

        if close > peak:
            peak = close

        # Time stop check — once we hit the check date, evaluate once
        if not time_checked and bar_date >= check_date:
            time_checked = True
            gain = (close - entry) / entry
            if gain < min_gain_pct:
                return {
                    "exit_ret":   gain * 100,
                    "peak_ret":   (peak - entry) / entry * 100,
                    "days":       (bar_date - entry_date).days,
                    "exit_reason": "time_stop",
                }
            # Passed check — continue with trailing stop

        # Trailing stop (only after min bars AND time check passed or bypassed)
        if i >= MIN_DAYS and time_checked:
            if close <= peak * (1 - TRAIL_PCT):
                return {
                    "exit_ret":   (close - entry) / entry * 100,
                    "peak_ret":   (peak - entry) / entry * 100,
                    "days":       (bar_date - entry_date).days,
                    "exit_reason": "trail_stop",
                }

    lc = closes[-1][1]
    return {
        "exit_ret":   (lc - entry) / entry * 100,
        "peak_ret":   (peak - entry) / entry * 100,
        "days":       (date.fromisoformat(closes[-1][0]) - entry_date).days,
        "exit_reason": "held",
    }


def simulate_flat_trail(closes: list[tuple[str, float]]) -> dict | None:
    if not closes:
        return None
    entry = closes[0][1]
    if not entry:
        return None
    peak = entry
    entry_date = date.fromisoformat(closes[0][0])
    for i, (dt, close) in enumerate(closes):
        if close > peak:
            peak = close
        if i >= MIN_DAYS and close <= peak * (1 - TRAIL_PCT):
            return {"exit_ret": (close-entry)/entry*100,
                    "peak_ret": (peak-entry)/entry*100,
                    "days": (date.fromisoformat(dt)-entry_date).days,
                    "exit_reason": "trail_stop"}
    lc = closes[-1][1]
    return {"exit_ret": (lc-entry)/entry*100,
            "peak_ret": (peak-entry)/entry*100,
            "days": (date.fromisoformat(closes[-1][0])-entry_date).days,
            "exit_reason": "held"}


def build_entries(rows: list[dict]) -> list[tuple[str, str]]:
    entries = []
    for r in rows:
        if r["gc_days"] in ("", "None") or r["rb_days"] in ("", "None"):
            continue
        td = r["trade_date"][:10]
        sd = min(int(float(r[s])) for s in ("gc_days", "rb_days"))
        sig_date = (date.fromisoformat(td) + timedelta(days=sd)).isoformat()
        entries.append((r["ticker"], sig_date))
    return entries


def run_all(
    entries: list[tuple[str, str]],
    bars_cache: dict[str, dict],
    simulate_fn,
) -> list[dict]:
    results = []
    for ticker, sig_date in entries:
        cm = bars_cache.get(ticker)
        if not cm:
            continue
        md = (date.fromisoformat(sig_date) + timedelta(days=MAX_HOLD_DAYS)).isoformat()
        fwd = sorted((d, c) for d, c in cm.items() if sig_date <= d <= md)
        if len(fwd) < 5:
            continue
        res = simulate_fn(fwd)
        if res:
            results.append(res)
    return results


def summarize(results: list[dict]) -> dict:
    exits = [r["exit_ret"] for r in results]
    return {
        "n":      len(results),
        "avg":    statistics.mean(exits),
        "median": statistics.median(exits),
        "win":    sum(1 for x in exits if x > 0) / len(exits) * 100,
        "peak":   statistics.mean(r["peak_ret"] for r in results),
        "days":   statistics.mean(r["days"] for r in results),
        "time_stopped": sum(1 for r in results if r["exit_reason"] == "time_stop"),
        "trail_stopped": sum(1 for r in results if r["exit_reason"] == "trail_stop"),
        "held":   sum(1 for r in results if r["exit_reason"] == "held"),
    }


def main() -> None:
    rows    = list(csv.DictReader(open("data/backtest_results.csv")))
    entries = build_entries(rows)
    tickers = {t for t, _ in entries}
    bars_cache = {t: load_bars(t) for t in tickers}
    print(f"Stacked GC+RB entries: {len(entries)}\n")

    # Baseline
    baseline = run_all(entries, bars_cache, simulate_flat_trail)
    b = summarize(baseline)
    print("BASELINE  flat 20% trail, no time stop")
    print(f"  Avg {b['avg']:+.1f}%  Median {b['median']:+.1f}%  "
          f"Win {b['win']:.0f}%  AvgDays {b['days']:.0f}  "
          f"TrailStopped {b['trail_stopped']}  Held {b['held']}\n")

    print(f"{'Days':>6} {'MinGain':>8} {'Trades':>7} {'AvgRet':>8} {'MedRet':>8} "
          f"{'WinRate':>8} {'AvgDays':>8} {'TimeStp':>8} {'TrlStp':>7} {'Held':>6}  vs base")
    print("-" * 100)

    best_avg = b["avg"]
    best_cfg = None
    best_res = None

    for tsd in TIME_STOP_DAYS:
        for mgp in MIN_GAIN_PCTS:
            def fn(fwd, t=tsd, m=mgp):
                return simulate_timestop(fwd, t, m)
            res = run_all(entries, bars_cache, fn)
            if not res:
                continue
            s = summarize(res)
            delta = s["avg"] - b["avg"]
            flag = " <-- best" if s["avg"] > best_avg else ""
            if s["avg"] > best_avg:
                best_avg = s["avg"]
                best_cfg = (tsd, mgp)
                best_res = res
            print(f"  {tsd:>4}  {mgp*100:>6.0f}%  {s['n']:>7}  "
                  f"{s['avg']:>+8.1f}%  {s['median']:>+8.1f}%  {s['win']:>7.1f}%  "
                  f"{s['days']:>8.0f}  {s['time_stopped']:>8}  {s['trail_stopped']:>7}  "
                  f"{s['held']:>6}  {delta:>+.1f}%{flag}")

    if best_cfg and best_res:
        tsd, mgp = best_cfg
        s = summarize(best_res)
        print(f"\n{'='*65}")
        print(f"BEST CONFIG: time stop at {tsd} days, min gain {mgp*100:.0f}%")
        print("  then 20% trailing stop on remainder")
        print(f"{'='*65}")
        print(f"  Trades:       {s['n']}")
        print(f"  Avg return:   {s['avg']:+.1f}%  (baseline {b['avg']:+.1f}%,  delta {s['avg']-b['avg']:+.1f}%)")
        print(f"  Median:       {s['median']:+.1f}%")
        print(f"  Win rate:     {s['win']:.0f}%")
        print(f"  Avg peak:     {s['peak']:+.1f}%")
        print(f"  Avg days:     {s['days']:.0f}")
        print(f"  Time stopped: {s['time_stopped']} ({s['time_stopped']/s['n']*100:.0f}%) — exited for stagnation")
        print(f"  Trail stopped:{s['trail_stopped']} ({s['trail_stopped']/s['n']*100:.0f}%) — trailed out")
        print(f"  Held to end:  {s['held']} ({s['held']/s['n']*100:.0f}%)")

        # What did time-stopped trades look like vs trail-stopped
        ts_group = [r for r in best_res if r["exit_reason"] == "time_stop"]
        tr_group = [r for r in best_res if r["exit_reason"] == "trail_stop"]
        hd_group = [r for r in best_res if r["exit_reason"] == "held"]
        print()
        for group, label in [(ts_group, "Time stopped"), (tr_group, "Trail stopped"), (hd_group, "Held to end")]:
            if not group:
                continue
            exits = [r["exit_ret"] for r in group]
            print(f"  {label} ({len(group)}):  "
                  f"avg {statistics.mean(exits):+.1f}%  "
                  f"median {statistics.median(exits):+.1f}%  "
                  f"win {sum(1 for x in exits if x>0)/len(exits)*100:.0f}%")

        print("\n  Return distribution (best config vs baseline):")
        buckets = [(-999,-20,"< -20%"),(-20,-10,"-20 to -10%"),(-10,0,"-10 to 0%"),
                   (0,10,"0 to +10%"),(10,20,"+10 to +20%"),(20,50,"+20 to +50%"),(50,999,"> +50%")]
        for lo, hi, lbl in buckets:
            n  = sum(1 for r in best_res if lo <= r["exit_ret"] < hi)
            nb = sum(1 for r in baseline  if lo <= r["exit_ret"] < hi)
            diff = n - nb
            sign = "+" if diff > 0 else ""
            print(f"  {lbl:>14}:  best {n:>3}  baseline {nb:>3}  ({sign}{diff})")


if __name__ == "__main__":
    main()
