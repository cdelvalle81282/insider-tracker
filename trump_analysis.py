import json, os, psycopg
from datetime import date, timedelta
from pathlib import Path
from collections import defaultdict

conn = psycopg.connect(os.environ['DATABASE_URL'], row_factory=psycopg.rows.dict_row)
CACHE = Path('/home/deploy/insider-tracker/data/polygon_cache')

def get_bars(ticker):
    f = CACHE / f"{ticker}.json"
    if not f.exists(): return {}
    return {b['date']: b['close'] for b in json.loads(f.read_text())}

def fwd_return(bars, entry, days):
    target = (date.fromisoformat(entry) + timedelta(days=days)).isoformat()
    ep = None
    for d in sorted(bars):
        if d >= entry: ep = bars[d]; break
    if not ep: return None
    for d in sorted(bars):
        if d >= target: return round((bars[d] - ep) / ep * 100, 2)
    return None

# Load all Trump purchases with tickers
rows = conn.execute("""
    SELECT ticker, asset_description, transaction_date::text AS tx_date,
           amount_min, amount_label
    FROM congress_trades
    WHERE politician_name = 'Donald J. Trump'
      AND LOWER(transaction_type) = 'purchase'
      AND ticker IS NOT NULL AND ticker ~ '^[A-Z]{1,5}$'
    ORDER BY tx_date, ticker
""").fetchall()

spy = get_bars('SPY')

corp_rows = conn.execute("""
    SELECT issuer_ticker, transaction_date::text AS tx_date,
           total_value, insider_title
    FROM filings
    WHERE transaction_code='P' AND table_type='ND'
      AND superseded_by IS NULL AND joint_filer_of IS NULL
      AND transaction_date >= '2025-01-01'
""").fetchall()

corp_by_ticker = defaultdict(list)
for r in corp_rows:
    corp_by_ticker[r['issuer_ticker']].append(
        (r['tx_date'], r['total_value'] or 0, r['insider_title'] or ''))

ticker_buy_count = defaultdict(int)
for r in rows:
    ticker_buy_count[r['ticker']] += 1

scored = []
for r in rows:
    t  = r['ticker']
    d  = r['tx_date']
    bars = get_bars(t)
    if not bars: continue
    r30 = fwd_return(bars, d, 30)
    r60 = fwd_return(bars, d, 60)
    r90 = fwd_return(bars, d, 90)
    if r30 is None and r60 is None and r90 is None: continue
    s30 = fwd_return(spy, d, 30)
    s60 = fwd_return(spy, d, 60)
    s90 = fwd_return(spy, d, 90)
    e30 = round(r30 - s30, 2) if r30 is not None and s30 is not None else None
    e60 = round(r60 - s60, 2) if r60 is not None and s60 is not None else None
    e90 = round(r90 - s90, 2) if r90 is not None and s90 is not None else None
    dobj = date.fromisoformat(d)
    lo = (dobj - timedelta(days=14)).isoformat()
    hi = (dobj + timedelta(days=14)).isoformat()
    cobuy_vals = [(cv, ct) for cb_d, cv, ct in corp_by_ticker.get(t, []) if lo <= cb_d <= hi]
    desc_upper = (r['asset_description'] or '').upper()
    is_etf = any(k in desc_upper for k in ['ETF', 'FUND', 'TRUST', 'MONEY', 'INDEX'])
    scored.append({
        'ticker': t, 'date': d,
        'amount_min': r['amount_min'] or 0,
        'amount_label': r['amount_label'],
        'r30': r30, 'r60': r60, 'r90': r90,
        'e30': e30, 'e60': e60, 'e90': e90,
        'cobuy': len(cobuy_vals) > 0,
        'cobuy_max_val': max((v for v, _ in cobuy_vals), default=0),
        'cobuy_is_ceo': any('CEO' in t2.upper() or 'CHIEF' in t2.upper() or 'PRESIDENT' in t2.upper()
                            for _, t2 in cobuy_vals),
        'is_etf': is_etf,
        'repeat_count': ticker_buy_count[t],
    })

W = 60
print(f"TRUMP TRADE ANALYSIS — {len(scored)} scored trades, primary window {W}d")
print("(raw return from transaction_date; exc = raw - SPY same window)")
print("=" * 72)

def show(label, subset, w):
    vals = [r[f'r{w}'] for r in subset if r[f'r{w}'] is not None]
    exc  = [r[f'e{w}'] for r in subset if r[f'e{w}'] is not None]
    if not vals: return
    wins = sum(1 for v in vals if v > 0)
    avg  = sum(vals)/len(vals)
    med  = sorted(vals)[len(vals)//2]
    eavg = sum(exc)/len(exc) if exc else float('nan')
    print(f"  {label:<35} n={len(vals):>4}  win={wins/len(vals)*100:.0f}%  avg={avg:+.1f}%  exc={eavg:+.1f}%")

print(f"\n  BY TRADE SIZE ({W}d):")
for label, fn in [
    ('$1K-$15K',    lambda r: r['amount_min'] < 15000),
    ('$15K-$50K',   lambda r: 15000  <= r['amount_min'] < 50000),
    ('$50K-$100K',  lambda r: 50000  <= r['amount_min'] < 100000),
    ('$100K-$250K', lambda r: 100000 <= r['amount_min'] < 250000),
    ('$250K-$500K', lambda r: 250000 <= r['amount_min'] < 500000),
    ('$500K-$1M',   lambda r: 500000 <= r['amount_min'] < 1000000),
    ('$1M+',        lambda r: r['amount_min'] >= 1000000),
]:
    show(label, [r for r in scored if fn(r)], W)

print(f"\n  ETF vs INDIVIDUAL STOCK ({W}d):")
show('Individual stocks', [r for r in scored if not r['is_etf']], W)
show('ETFs / Funds',      [r for r in scored if r['is_etf']],     W)

print(f"\n  BY BATCH DATE ({W}d):")
date_groups = defaultdict(list)
for r in scored: date_groups[r['date']].append(r)
for d in sorted(date_groups):
    if len(date_groups[d]) >= 5:
        show(f"{d}  ({len(date_groups[d])} trades)", date_groups[d], W)

print(f"\n  CONVICTION — REPEAT PURCHASE COUNT ({W}d):")
show('Bought once',   [r for r in scored if r['repeat_count'] == 1], W)
show('Bought 2x',     [r for r in scored if r['repeat_count'] == 2], W)
show('Bought 3x+',    [r for r in scored if r['repeat_count'] >= 3], W)

print(f"\n  CO-BUY BY CORPORATE INSIDER SIZE ({W}d):")
show('No co-buy',               [r for r in scored if not r['cobuy']], W)
show('Any co-buy',              [r for r in scored if r['cobuy']], W)
show('Co-buy insider $250K+',   [r for r in scored if r['cobuy'] and r['cobuy_max_val'] >= 250000], W)
show('Co-buy insider $1M+',     [r for r in scored if r['cobuy'] and r['cobuy_max_val'] >= 1_000_000], W)
show('Co-buy CEO/Pres title',   [r for r in scored if r['cobuy_is_ceo']], W)

print(f"\n  90d BREAKDOWNS (fewer datapoints — recent trades not mature):")
for label, fn in [
    ('$1M+',                    lambda r: r['amount_min'] >= 1_000_000),
    ('Individual stocks',       lambda r: not r['is_etf']),
    ('ETFs',                    lambda r: r['is_etf']),
    ('Bought 3x+',              lambda r: r['repeat_count'] >= 3),
    ('Co-buy insider $1M+',     lambda r: r['cobuy'] and r['cobuy_max_val'] >= 1_000_000),
    ('Co-buy CEO title',        lambda r: r['cobuy_is_ceo']),
    ('No co-buy',               lambda r: not r['cobuy']),
]:
    show(label, [r for r in scored if fn(r)], 90)

print(f"\n  TICKERS TRUMP BOUGHT 3+ TIMES — avg 60d return:")
multi = defaultdict(list)
for r in scored:
    if r['repeat_count'] >= 3 and r['r60'] is not None:
        multi[r['ticker']].append(r['r60'])
multi_stats = [(t, len(v), sum(v)/len(v), sum(1 for x in v if x > 0)/len(v))
               for t, v in multi.items() if len(v) >= 2]
multi_stats.sort(key=lambda x: -x[2])
print(f"  {'Ticker':<7} {'Trades':>6}  {'60d avg':>8}  {'Win%':>6}")
for t, n, avg, wr in multi_stats:
    print(f"  {t:<7} {n:>6}  {avg:>+7.1f}%  {wr*100:>5.0f}%")
