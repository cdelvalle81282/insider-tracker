"""
Backtest: individual corporate insider open-market buys → forward excess return vs. SPY.

Entry date: transaction_date — the day the insider put their own money in. Form 4s must
  file within 2 business days so the public signal lag is minimal. This measures the
  *insider's edge* (do they buy before the stock rises?), not a replication strategy.

Individual filter: is_officer=1 OR is_director=1 gates out funds that typically file as
  10%-owner-only. Entity-named "directors" (PE board seats, fund nominees) are further
  excluded by word-boundary token matching on insider_name.

table_type = 'ND' is required — derivative rows store exercise/strike price in
  total_value, making value-based filtering meaningless for those rows.

Usage:
    python backtest_insiders.py [--min-value 100000] [--start 2022-01-01] [--output data/insider_backtest.csv]

Requires POLYGON_API_KEY in environment.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import re
import time
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import httpx

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from db import get_cli_db

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

WINDOWS             = [15, 30, 45, 60, 90]  # forward windows (calendar days)
TRADE_START_DEFAULT = "2022-01-01"
CACHE_DIR           = Path("data/polygon_cache")
RATE_LIMIT_SLEEP    = 0.25   # seconds between live Polygon calls (paid tier: ~4/sec)
CACHE_STALE_DAYS    = 3
PRICE_WARMUP_DAYS   = 60     # calendar days of bars before TRADE_START for snapping

CLUSTER_WINDOW_DAYS = 10     # ±days: another insider at same company counts as cluster

AMOUNT_TIERS = [
    (0,           100_000, "<100k"),
    (100_000,     500_000, "100k-500k"),
    (500_000,   2_000_000, "500k-2m"),
    (2_000_000,      None, ">2m"),
]

# ---------------------------------------------------------------------------
# Individual vs. entity filter
# ---------------------------------------------------------------------------

# Matched against whitespace/punctuation-split tokens to avoid false positives:
# "inc" → INC only, not Vince/Quincy;  "lp" → LP only, not Phelps;
# "asset" → ASSET only, not Bassett;   "co" → CO only, not Cochran.
ENTITY_TOKENS = frozenset({
    "llc", "lp", "ltd", "inc", "corp", "fund", "trust",
    "capital", "partners", "management", "advisors", "advisor",
    "holdings", "investments", "investment", "group", "asset",
    "equity", "ventures", "associates", "foundation", "partnership",
    "company", "co", "plc", "gmbh", "sa", "ag", "bv", "pty",
    "realty", "properties", "securities", "financial", "lending",
})

_SPLIT_RE = re.compile(r"[\s,./&()\[\]|]+")


def _is_likely_entity(name: str) -> bool:
    if not name:
        return False
    tokens = {t for t in _SPLIT_RE.split(name.lower()) if len(t) > 1}
    return bool(tokens & ENTITY_TOKENS)


# ---------------------------------------------------------------------------
# Role classification
# ---------------------------------------------------------------------------

def _classify_role(title: str | None, is_director: int, is_officer: int) -> str:
    if title:
        t = title.lower()
        if any(k in t for k in ("chief executive", "ceo", "president & ceo",
                                 "pres & ceo", "president/ceo")):
            return "CEO/President"
        if "president" in t and "vice" not in t:
            return "CEO/President"
        if any(k in t for k in ("chief financial", "cfo")):
            return "CFO"
        if any(k in t for k in ("chief operating", "coo")):
            return "COO"
        if any(k in t for k in ("general counsel", "chief legal", "clco")):
            return "GC/Secretary"
        if "secretary" in t:
            return "GC/Secretary"
        if any(k in t for k in ("chief", "evp", "executive vice", "svp",
                                 "senior vice", "group president")):
            return "Other C-Suite"
    if is_director and not is_officer:
        return "Director"
    if is_officer:
        return "Officer (other)"
    return "Director"


def _amount_tier(value: float | None) -> str:
    if value is None:
        return "unknown"
    for lo, hi, label in AMOUNT_TIERS:
        if value >= lo and (hi is None or value < hi):
            return label
    return ">2m"


# ---------------------------------------------------------------------------
# Price data  (shared cache with backtest.py + backtest_congress.py)
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
                print(f"    [429] rate-limited — waiting 60s (attempt {attempt+1}/3)...")
                time.sleep(60)
                continue
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"    [WARN] fetch error: {e}")
            return []

        bars = []
        for bar in (data.get("results") or []):
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
    """Return (bars, was_cached). Caches to CACHE_DIR/{ticker}.json; refreshes if stale."""
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file = CACHE_DIR / f"{ticker}.json"

    if cache_file.exists():
        try:
            bars = json.loads(cache_file.read_text())
            last_date = date.fromisoformat(bars[-1]["date"]) if bars else None
            if last_date and last_date >= date.fromisoformat(to_date) - timedelta(days=CACHE_STALE_DAYS):
                return bars, True
        except Exception as e:
            print(f"    [WARN] cache read for {ticker}: {e}")

    bars = _fetch_live(ticker, from_date, to_date, api_key)
    if bars:
        cache_file.write_text(json.dumps(bars))
    return bars, False


def forward_return(bars: list[dict], trade_idx: int, days: int) -> float | None:
    price = bars[trade_idx]["close"]
    if not price:
        return None
    target = date.fromisoformat(bars[trade_idx]["date"]) + timedelta(days=days)
    for i in range(trade_idx + 1, len(bars)):
        if date.fromisoformat(bars[i]["date"]) >= target:
            return round((bars[i]["close"] - price) / price * 100, 2)
    return None


# ---------------------------------------------------------------------------
# SPY benchmark
# ---------------------------------------------------------------------------

def build_spy_lookup(spy_bars: list[dict]) -> dict[str, dict[int, float | None]]:
    """Pre-compute SPY forward returns for every bar × every window."""
    lookup: dict[str, dict[int, float | None]] = {}
    for idx, bar in enumerate(spy_bars):
        lookup[bar["date"]] = {w: forward_return(spy_bars, idx, w) for w in WINDOWS}
    return lookup


def spy_return_on(spy_lookup: dict, entry_date: str, window: int) -> float | None:
    """SPY return from the first trading day on or after entry_date."""
    if entry_date in spy_lookup:
        return spy_lookup[entry_date].get(window)
    for candidate in sorted(spy_lookup):
        if candidate >= entry_date:
            return spy_lookup[candidate].get(window)
    return None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(description="Individual corporate insider buy backtest")
    parser.add_argument("--min-value", type=float, default=100_000,
                        help="Min total_value (default $100,000)")
    parser.add_argument("--start", default=TRADE_START_DEFAULT,
                        help=f"Earliest transaction_date (default {TRADE_START_DEFAULT})")
    parser.add_argument("--output", default="data/insider_backtest.csv")
    args = parser.parse_args()

    api_key = os.environ.get("POLYGON_API_KEY", "")
    if not api_key:
        raise SystemExit("POLYGON_API_KEY not set")

    trade_end = (date.today() - timedelta(days=max(WINDOWS))).isoformat()

    conn = get_cli_db()
    rows = conn.execute("""
        SELECT
            insider_cik, insider_name, insider_title,
            is_director, is_officer, is_ten_percent_owner,
            issuer_ticker, issuer_name, issuer_cik,
            transaction_date::text AS transaction_date,
            total_value, price_per_share,
            is_10b5_1, shares_owned_after
        FROM filings
        WHERE transaction_code = 'P'
          AND table_type = 'ND'
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND (is_officer = 1 OR is_director = 1)
          AND TRIM(issuer_ticker) IS NOT NULL
          AND TRIM(issuer_ticker) NOT IN ('NONE', 'N/A', '')
          AND total_value >= %s
          AND transaction_date::date >= %s::date
          AND transaction_date::date <= %s::date
        ORDER BY issuer_ticker, transaction_date
    """, [args.min_value, args.start, trade_end]).fetchall()
    conn.close()

    all_trades = [dict(r) for r in rows]

    # Python-side entity filter — catches PE board seats and fund nominees
    entity_names: list[str] = []
    trades: list[dict] = []
    for t in all_trades:
        if _is_likely_entity(t["insider_name"]):
            entity_names.append(t["insider_name"])
        else:
            trades.append(t)

    unique_entity_names = sorted(set(entity_names))

    # Cluster flag: build per-ticker list of (date, insider_cik) buy events
    company_dates: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for t in trades:
        company_dates[t["issuer_ticker"]].append((t["transaction_date"], t["insider_cik"]))

    def _is_clustered(ticker: str, trade_date: str, insider_cik: str) -> bool:
        td = date.fromisoformat(trade_date)
        lo = (td - timedelta(days=CLUSTER_WINDOW_DAYS)).isoformat()
        hi = (td + timedelta(days=CLUSTER_WINDOW_DAYS)).isoformat()
        return any(
            lo <= d <= hi and cik != insider_cik
            for d, cik in company_dates[ticker]
        )

    by_ticker: dict[str, list[dict]] = defaultdict(list)
    for t in trades:
        by_ticker[t["issuer_ticker"].strip()].append(t)

    ticker_list = sorted(by_ticker)

    print(f"Trades from DB:    {len(all_trades)}")
    print(f"Entity-filtered:   {len(entity_names)} rows removed ({len(unique_entity_names)} unique names)")
    if unique_entity_names:
        sample = unique_entity_names[:10]
        print(f"  Sample excluded: {', '.join(sample)}{'…' if len(unique_entity_names) > 10 else ''}")
    print(f"Individual trades: {len(trades)}")
    print(f"Tickers:           {len(ticker_list)}")
    print(f"Min value:         ${args.min_value:,.0f}")
    print(f"Trade window:      {args.start} → {trade_end}")
    print(f"Cache dir:         {CACHE_DIR}\n")

    fetch_start = (date.fromisoformat(args.start) - timedelta(days=PRICE_WARMUP_DAYS)).isoformat()
    fetch_end   = date.today().isoformat()

    # Fetch SPY benchmark first
    print("Fetching SPY benchmark...", end=" ", flush=True)
    spy_bars, spy_cached = fetch_bars("SPY", fetch_start, fetch_end, api_key)
    if not spy_bars:
        raise SystemExit("Could not fetch SPY bars — cannot compute excess returns.")
    spy_lookup = build_spy_lookup(spy_bars)
    print(f"ok ({len(spy_bars)} bars, {'cached' if spy_cached else 'live'})\n")
    if not spy_cached:
        time.sleep(RATE_LIMIT_SLEEP)

    results: list[dict] = []
    no_data: list[str]  = []

    for n, ticker in enumerate(ticker_list, 1):
        ticker_trades = by_ticker[ticker]
        print(
            f"[{n}/{len(ticker_list)}] {ticker}"
            f" ({len(ticker_trades)} trade{'s' if len(ticker_trades) != 1 else ''})...",
            end=" ", flush=True,
        )

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

            trade_idx = bar_by_date.get(td)
            if trade_idx is None:
                for i, b in enumerate(bars):
                    if b["date"] >= td:
                        trade_idx = i
                        break
            if trade_idx is None or trade_idx >= len(bars) - 5:
                continue

            role      = _classify_role(trade["insider_title"], trade["is_director"], trade["is_officer"])
            clustered = _is_clustered(ticker, td, trade["insider_cik"])
            tier      = _amount_tier(trade["total_value"])

            row: dict = {
                "ticker":             ticker,
                "issuer_name":        trade["issuer_name"],
                "insider_cik":        trade["insider_cik"],
                "insider_name":       trade["insider_name"],
                "title":              trade["insider_title"] or "",
                "role":               role,
                "is_director":        trade["is_director"],
                "is_officer":         trade["is_officer"],
                "trade_date":         td,
                "total_value":        trade["total_value"],
                "price_per_share":    trade["price_per_share"],
                "shares_owned_after": trade["shares_owned_after"],
                "is_10b5_1":          trade["is_10b5_1"],
                "same_co_cluster":    clustered,
                "amount_tier":        tier,
                "entry_price":        bars[trade_idx]["close"],
            }

            for w in WINDOWS:
                raw_ret = forward_return(bars, trade_idx, w)
                spy_ret = spy_return_on(spy_lookup, td, w)
                excess  = (
                    round(raw_ret - spy_ret, 2)
                    if raw_ret is not None and spy_ret is not None
                    else None
                )
                row[f"return_{w}d"]     = raw_ret
                row[f"spy_return_{w}d"] = spy_ret
                row[f"excess_{w}d"]     = excess

            results.append(row)

        print(f"ok ({len(bars)} bars)")
        if not was_cached:
            time.sleep(RATE_LIMIT_SLEEP)

    # Write CSV
    if not results:
        print("\nNo results — check --start date and --min-value against what's in the DB.")
        return

    with open(args.output, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(results[0].keys()))
        writer.writeheader()
        writer.writerows(results)
    print(f"\nWrote {len(results)} rows → {args.output}")

    # ---------------------------------------------------------------------------
    # Summary
    # ---------------------------------------------------------------------------

    def _stats(vals: list[float], label: str = "") -> str:
        if not vals:
            return "—"
        n    = len(vals)
        wins = sum(1 for v in vals if v > 0)
        srt  = sorted(vals)
        avg  = sum(vals) / n
        med  = srt[n // 2]
        pfx  = f"{label} " if label else ""
        return f"{pfx}n={n:>5}  win={wins/n*100:>4.0f}%  avg={avg:>+6.1f}%  med={med:>+6.1f}%"

    def _segment(label: str, sub: list[dict]) -> None:
        if not sub:
            return
        print(f"\n    {label}  [{len(sub)} trades]")
        for w in [30, 60, 90]:
            raw    = [r[f"return_{w}d"] for r in sub if r[f"return_{w}d"] is not None]
            excess = [r[f"excess_{w}d"] for r in sub if r[f"excess_{w}d"] is not None]
            print(f"      {w:>2}d  raw: {_stats(raw):<48}  vs SPY: {_stats(excess)}")

    sep = "=" * 80
    thin = "─" * 80

    print(f"\n{sep}")
    print(f"INSIDER BACKTEST  (min_value=${args.min_value:,.0f}  entry=transaction_date)")
    print(sep)
    print(f"  Trades scored:      {len(results)}")
    print(f"  Entity-filtered:    {len(entity_names)} rows removed")
    print(f"  Tickers w/ no data: {len(no_data)}")
    if no_data:
        sample = no_data[:30]
        print(f"  No-data:  {', '.join(sample)}{'…' if len(no_data) > 30 else ''}")

    print(f"\n{thin}")
    print("  OVERALL")
    _segment("All individual insider buys", results)

    print(f"\n{thin}")
    print("  CONVICTION: 10b5-1 plan vs. discretionary open-market buys")
    print("  Discretionary buys carry stronger signal — no pre-scheduled plan.")
    _segment("Discretionary (no 10b5-1)", [r for r in results if not r["is_10b5_1"]])
    _segment("10b5-1 plan",               [r for r in results if r["is_10b5_1"]])

    print(f"\n{thin}")
    print(f"  CLUSTER: ≥2 insiders at same company within ±{CLUSTER_WINDOW_DAYS} calendar days")
    _segment("Clustered (2+ insiders)", [r for r in results if r["same_co_cluster"]])
    _segment("Solo buy",                [r for r in results if not r["same_co_cluster"]])

    print(f"\n{thin}")
    print("  COMBINED: clustered + discretionary (highest-conviction composite)")
    _segment("Cluster + discretionary",
             [r for r in results if r["same_co_cluster"] and not r["is_10b5_1"]])

    print(f"\n{thin}")
    print("  ROLE")
    role_order = ["CEO/President", "CFO", "COO", "Other C-Suite", "GC/Secretary",
                  "Director", "Officer (other)"]
    for role in role_order:
        _segment(role, [r for r in results if r["role"] == role])

    print(f"\n{thin}")
    print("  TRADE SIZE")
    for _, _, label in AMOUNT_TIERS:
        _segment(label, [r for r in results if r["amount_tier"] == label])

    # Per-insider ranking
    MIN_TRADES = 5
    print(f"\n{thin}")
    print(f"  PER-INSIDER RANKING  (≥{MIN_TRADES} scored trades, sorted by median 90d excess vs SPY)")
    print(f"  Median is more robust than mean — a single outlier trade won't dominate.")
    print(f"  High N + consistent median = more trustworthy than high mean + low N.")
    print()
    hdr = (
        f"  {'Insider':<32} {'Role':<14} {'N':>4}  "
        f"{'30d exc':>8}  {'60d exc':>8}  {'90d exc':>8}  {'90d win':>7}  {'90d med':>8}"
    )
    print(hdr)
    print(f"  {'─'*32} {'─'*14} {'─'*4}  {'─'*8}  {'─'*8}  {'─'*8}  {'─'*7}  {'─'*8}")

    by_insider: dict[str, list[dict]] = defaultdict(list)
    for r in results:
        by_insider[r["insider_cik"]].append(r)

    ranked = []
    for cik, insider_trades in by_insider.items():
        if len(insider_trades) < MIN_TRADES:
            continue
        name = insider_trades[0]["insider_name"]
        role = insider_trades[0]["role"]
        e30  = [t["excess_30d"] for t in insider_trades if t.get("excess_30d") is not None]
        e60  = [t["excess_60d"] for t in insider_trades if t.get("excess_60d") is not None]
        e90  = [t["excess_90d"] for t in insider_trades if t.get("excess_90d") is not None]
        if not e90:
            continue
        srt90 = sorted(e90)
        med90 = srt90[len(e90) // 2]
        avg90 = sum(e90) / len(e90)
        avg30 = sum(e30) / len(e30) if e30 else None
        avg60 = sum(e60) / len(e60) if e60 else None
        w90   = sum(1 for v in e90 if v > 0) / len(e90)
        ranked.append({
            "name":  name,
            "role":  role,
            "n":     len(insider_trades),
            "avg30": avg30,
            "avg60": avg60,
            "avg90": avg90,
            "w90":   w90,
            "med90": med90,
        })

    ranked.sort(key=lambda x: x["med90"], reverse=True)

    for item in ranked:
        fmt  = lambda v: f"{v:>+7.1f}%" if v is not None else f"{'—':>8}"
        wfmt = f"{item['w90']*100:>6.0f}%"
        mfmt = f"{item['med90']:>+7.1f}%"
        print(
            f"  {item['name']:<32} {item['role']:<14} {item['n']:>4}  "
            f"{fmt(item['avg30'])}  {fmt(item['avg60'])}  {fmt(item['avg90'])}  "
            f"{wfmt}  {mfmt}"
        )

    print(f"\n{sep}")
    print("  NOTE: 'vs SPY' = raw return minus SPY return over the same window from the same")
    print("  entry date. Positive excess = outperformed the broad market. Entry =")
    print("  transaction_date (the day the insider traded, not the public filing date).")
    print(f"  Insider ranking sorted by median 90d excess.  Min trades threshold = {MIN_TRADES}.")


if __name__ == "__main__":
    main()
