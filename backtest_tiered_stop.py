"""
Tiered trailing stop backtest for stacked GC + RB signals.

Logic per trade:
  Phase 1 — entry to +prove_threshold: tight stop of entry_stop% below entry
  Phase 2 — once up prove_threshold%: floor rises to breakeven (+0%),
             trail widens to wide_trail%
  Phase 3 — once up lock_threshold%: floor locks in at lock_profit%,
             trail stays at wide_trail%

Tests a grid of (entry_stop, prove_threshold, wide_trail) combos.
Baseline comparison: flat 20% trail (from prior analysis).
"""
from __future__ import annotations

import csv
import json
import statistics
from datetime import date, timedelta
from itertools import product
from pathlib import Path

CACHE_DIR     = Path("data/polygon_cache")
MAX_HOLD_DAYS = 365
MIN_DAYS      = 3   # bars before any stop can trigger

# Grid to test
ENTRY_STOPS       = [0.06, 0.08, 0.10]          # tight initial stop
PROVE_THRESHOLDS  = [0.08, 0.12, 0.15]          # gain required to widen
WIDE_TRAILS       = [0.15, 0.20, 0.25]          # trailing % once proven
# Once up LOCK_THRESHOLD, floor moves up to LOCK_PROFIT
LOCK_THRESHOLD    = 0.25
LOCK_PROFIT       = 0.10


def load_bars(ticker: str) -> dict[str, float]:
    f = CACHE_DIR / f"{ticker}.json"
    if not f.exists():
        return {}
    return {b["date"]: b["close"]
            for b in json.loads(f.read_text()) if b.get("close")}


def simulate_tiered(
    closes: list[tuple[str, float]],
    entry_stop: float,
    prove_threshold: float,
    wide_trail: float,
) -> dict | None:
    if not closes:
        return None
    entry = closes[0][1]
    if not entry:
        return None

    peak        = entry
    proved      = False   # has trade hit prove_threshold
    locked      = False   # has trade hit lock_threshold

    for i, (dt, close) in enumerate(closes):
        if close > peak:
            peak = close

        gain_from_entry = (close - entry) / entry

        # Transition to wide trail once proved
        if not proved and gain_from_entry >= prove_threshold:
            proved = True
        if proved and not locked and gain_from_entry >= LOCK_THRESHOLD:
            locked = True

        # Compute stop level
        if not proved:
            # Phase 1: tight fixed stop below entry
            stop = entry * (1 - entry_stop)
        elif locked:
            # Phase 3: floor at lock_profit, wide trail from peak
            floor_price = entry * (1 + LOCK_PROFIT)
            trail_price = peak * (1 - wide_trail)
            stop = max(floor_price, trail_price)
        else:
            # Phase 2: floor at breakeven, wide trail from peak
            trail_price = peak * (1 - wide_trail)
            stop = max(entry, trail_price)

        if i >= MIN_DAYS and close <= stop:
            return {
                "exit_ret":  (close - entry) / entry * 100,
                "peak_ret":  (peak  - entry) / entry * 100,
                "days":      (date.fromisoformat(dt) - date.fromisoformat(closes[0][0])).days,
                "stopped":   True,
                "proved":    proved,
            }

    lc = closes[-1][1]
    return {
        "exit_ret":  (lc   - entry) / entry * 100,
        "peak_ret":  (peak - entry) / entry * 100,
        "days":      (date.fromisoformat(closes[-1][0]) - date.fromisoformat(closes[0][0])).days,
        "stopped":   False,
        "proved":    proved,
    }


def simulate_flat(closes: list[tuple[str, float]], trail: float = 0.20) -> dict | None:
    if not closes:
        return None
    entry = closes[0][1]
    if not entry:
        return None
    peak = entry
    for i, (dt, close) in enumerate(closes):
        if close > peak:
            peak = close
        if i >= MIN_DAYS and close <= peak * (1 - trail):
            return {"exit_ret": (close-entry)/entry*100,
                    "peak_ret": (peak-entry)/entry*100, "stopped": True}
    lc = closes[-1][1]
    return {"exit_ret": (lc-entry)/entry*100,
            "peak_ret": (peak-entry)/entry*100, "stopped": False}


def build_entries(rows: list[dict]) -> list[tuple[str, str]]:
    """Return (ticker, signal_date) for stacked GC+RB trades."""
    entries = []
    stacked = [r for r in rows
               if r["gc_days"] not in ("", "None") and r["rb_days"] not in ("", "None")]
    for r in stacked:
        td = r["trade_date"][:10]
        sd = min(int(float(r[s])) for s in ("gc_days", "rb_days")
                 if r[s] not in ("", "None"))
        sig_date = (date.fromisoformat(td) + timedelta(days=sd)).isoformat()
        entries.append((r["ticker"], sig_date))
    return entries


def run_entries(
    entries: list[tuple[str, str]],
    bars_cache: dict[str, dict[str, float]],
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


def stats(results: list[dict]) -> tuple:
    exits = [r["exit_ret"] for r in results]
    wins  = sum(1 for x in exits if x > 0)
    return (
        statistics.mean(exits),
        statistics.median(exits),
        wins / len(exits) * 100,
        statistics.mean(r["peak_ret"] for r in results),
    )


def main() -> None:
    rows = list(csv.DictReader(open("data/backtest_results.csv")))
    entries = build_entries(rows)
    print(f"Stacked GC+RB entries: {len(entries)}\n")

    # Pre-load all bars once
    tickers = {t for t, _ in entries}
    bars_cache = {t: load_bars(t) for t in tickers}

    # Baseline: flat 20% trail
    baseline = run_entries(entries, bars_cache, lambda fwd: simulate_flat(fwd, 0.20))
    ba, bm, bw, bp = stats(baseline)
    print(f"BASELINE  flat 20% trail  — {len(baseline)} trades")
    print(f"  Avg {ba:+.1f}%  Median {bm:+.1f}%  Win {bw:.0f}%  AvgPeak {bp:+.1f}%\n")

    # Grid search
    print(f"{'EntryStop':>10} {'ProveAt':>8} {'WideTrail':>10} "
          f"{'Trades':>7} {'AvgRet':>8} {'MedRet':>8} {'WinRate':>8} "
          f"{'AvgPeak':>8}  vs baseline")
    print("-" * 95)

    best_avg = ba
    best_cfg = None
    best_res = None

    for entry_stop, prove_thr, wide_trail in product(ENTRY_STOPS, PROVE_THRESHOLDS, WIDE_TRAILS):
        def fn(fwd, es=entry_stop, pt=prove_thr, wt=wide_trail):
            return simulate_tiered(fwd, es, pt, wt)
        res = run_entries(entries, bars_cache, fn)
        if not res:
            continue
        avg, med, win, peak = stats(res)
        delta = avg - ba
        flag = " <-- best" if avg > best_avg else ""
        if avg > best_avg:
            best_avg = avg
            best_cfg = (entry_stop, prove_thr, wide_trail)
            best_res = res
        print(f"  {entry_stop*100:>6.0f}%   {prove_thr*100:>5.0f}%   {wide_trail*100:>7.0f}%   "
              f"{len(res):>6}  {avg:>+8.1f}%  {med:>+8.1f}%  {win:>7.1f}%  "
              f"{peak:>+7.1f}%  {delta:>+.1f}%{flag}")

    if best_cfg and best_res:
        es, pt, wt = best_cfg
        print(f"\n{'='*60}")
        print(f"BEST CONFIG: entry stop {es*100:.0f}%  |  widen at +{pt*100:.0f}%  |  {wt*100:.0f}% trail after")
        print(f"Lock-in: once up {LOCK_THRESHOLD*100:.0f}%, floor at +{LOCK_PROFIT*100:.0f}%")
        print(f"{'='*60}")
        avg, med, win, peak = stats(best_res)
        stopped  = [r for r in best_res if r["stopped"]]
        held_out = [r for r in best_res if not r["stopped"]]
        proved   = [r for r in best_res if r["proved"]]
        print(f"  Trades:      {len(best_res)}")
        print(f"  Avg return:  {avg:+.1f}%  (vs baseline {ba:+.1f}%,  delta {avg-ba:+.1f}%)")
        print(f"  Median:      {med:+.1f}%")
        print(f"  Win rate:    {win:.0f}%")
        print(f"  Avg peak:    {peak:+.1f}%")
        print(f"  Stopped out: {len(stopped)} ({len(stopped)/len(best_res)*100:.0f}%)")
        print(f"  Held to end: {len(held_out)} ({len(held_out)/len(best_res)*100:.0f}%)")
        print(f"  Proved ({pt*100:.0f}%+ gained): {len(proved)} ({len(proved)/len(best_res)*100:.0f}%)")

        print("\n  Return distribution (best config):")
        buckets = [(-999,-20,"< -20%"),(-20,-10,"-20 to -10%"),(-10,0,"-10 to 0%"),
                   (0,10,"0 to +10%"),(10,20,"+10 to +20%"),(20,50,"+20 to +50%"),(50,999,"> +50%")]
        for lo, hi, lbl in buckets:
            n  = sum(1 for r in best_res   if lo <= r["exit_ret"] < hi)
            nb = sum(1 for r in baseline   if lo <= r["exit_ret"] < hi)
            print(f"  {lbl:>14}: {n:>3} (baseline {nb:>3})")


if __name__ == "__main__":
    main()
