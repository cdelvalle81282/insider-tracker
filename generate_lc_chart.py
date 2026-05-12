"""
Generate a multi-ticker insider signal chart using TradingView Lightweight Charts v4.2.0.
Same library as the web app — no Plotly, no rendering issues.

Usage:
    .venv/bin/python generate_lc_chart.py
    .venv/bin/python generate_lc_chart.py --tickers AAT,KOS,PANW --days 180
    .venv/bin/python generate_lc_chart.py --output /tmp/signals.html
"""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import sys
from datetime import date, timedelta
from pathlib import Path

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

_env = _HERE / ".env"
if _env.exists():
    for _l in _env.read_text().splitlines():
        _l = _l.strip()
        if _l and not _l.startswith("#") and "=" in _l:
            _k, _v = _l.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

import polygon_client
from backtest import (
    PRICE_WARMUP_DAYS,
    detect_channel_break,
    detect_golden_cross,
    detect_hhl,
    detect_resistance_break,
)

_DB      = str(_HERE / "data" / "insider_tracker.db")
_API_KEY = os.getenv("POLYGON_API_KEY", "")

DEFAULT_TICKERS = [
    "AAT",  "KOS",  "PANW", "TTD",  "MESO", "ALKT",
    "LW",   "WASH", "GO",   "SOFI", "CPNG", "CAR",
]

SIGNAL_STYLES = {
    "gc":  {"color": "#f59e0b", "label": "GC",  "name": "Golden Cross"},
    "rb":  {"color": "#f97316", "label": "RB",  "name": "Resistance Break"},
    "hhl": {"color": "#22d3ee", "label": "HH",  "name": "Higher High+Low"},
    "cb":  {"color": "#a855f7", "label": "CB",  "name": "Channel Break"},
}
SIGNAL_DETECTORS = {
    "gc":  detect_golden_cross,
    "rb":  detect_resistance_break,
    "hhl": detect_hhl,
    "cb":  detect_channel_break,
}

MIN_VALUE = 500_000


def _sma(closes, period):
    out = [None] * len(closes)
    for i in range(period - 1, len(closes)):
        out[i] = sum(closes[i - period + 1: i + 1]) / period
    return out


def _rsi(closes, period=14):
    n = len(closes)
    if n < period + 1:
        return [None] * n
    result = [None] * period
    g = [max(closes[i] - closes[i-1], 0.0) for i in range(1, period+1)]
    l = [max(closes[i-1] - closes[i], 0.0) for i in range(1, period+1)]
    ag, al = sum(g)/period, sum(l)/period
    for i in range(period, n):
        if i > period:
            d = closes[i] - closes[i-1]
            ag = (ag*(period-1) + max(d, 0.0)) / period
            al = (al*(period-1) + max(-d, 0.0)) / period
        result.append(100.0 if al == 0 else round(100 - 100/(1 + ag/al), 2))
    return result


def _fmt(v):
    if not v: return ""
    if v >= 1e9: return f"${v/1e9:.1f}B"
    if v >= 1e6: return f"${v/1e6:.1f}M"
    return f"${v/1e3:.0f}K"


def _get_buys(conn, ticker, days):
    cutoff = (date.today() - timedelta(days=days)).isoformat()
    rows = conn.execute("""
        SELECT transaction_date, insider_name, insider_title, total_value
        FROM filings
        WHERE issuer_ticker = ?
          AND transaction_code = 'P'
          AND table_type = 'ND'
          AND is_10b5_1 = 0
          AND total_value >= ?
          AND transaction_date >= ?
          AND superseded_by IS NULL
          AND joint_filer_of IS NULL
          AND transaction_date != ''
        ORDER BY transaction_date
    """, [ticker.upper(), MIN_VALUE, cutoff]).fetchall()
    return [dict(r) for r in rows]


def _detect_signals(raw_bars, buys):
    if len(raw_bars) < 50 or not buys:
        return {k: [] for k in SIGNAL_DETECTORS}
    sb = [{**b, "date": b["time"]} for b in raw_bars]
    dates = [b["date"] for b in sb]
    result = {k: [] for k in SIGNAL_DETECTORS}
    seen = set()
    for buy in buys:
        td = buy["transaction_date"]
        idx = next((i for i, d in enumerate(dates) if d >= td), None)
        if idx is None or idx >= len(sb) - 1:
            continue
        for code, fn in SIGNAL_DETECTORS.items():
            _, dtf = fn(sb, idx)
            if dtf is None:
                continue
            fd = (date.fromisoformat(td) + timedelta(days=dtf)).isoformat()
            if (code, fd) not in seen:
                seen.add((code, fd))
                result[code].append(fd)
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", default=",".join(DEFAULT_TICKERS))
    ap.add_argument("--days",    type=int, default=180)
    ap.add_argument("--output",  default=str(_HERE / "data" / "insider_signals_chart.html"))
    args = ap.parse_args()

    tickers   = [t.strip().upper() for t in args.tickers.split(",")]
    today     = date.today()
    from_date = today - timedelta(days=args.days)
    full_from = from_date - timedelta(days=PRICE_WARMUP_DAYS)

    conn = sqlite3.connect(_DB)
    conn.row_factory = sqlite3.Row

    charts = []

    for ticker in tickers:
        print(f"  {ticker} ...", end="", flush=True)
        raw_bars = polygon_client.get_daily_bars(ticker, full_from, today, _API_KEY, limit=700)
        if not raw_bars:
            print(" no data"); continue

        display_bars = [b for b in raw_bars if b["time"] >= from_date.isoformat()]
        if not display_bars:
            print(" no display bars"); continue

        buys    = _get_buys(conn, ticker, args.days)
        signals = _detect_signals(raw_bars, buys)
        n_sig   = sum(len(v) for v in signals.values())
        print(f" {len(buys)} buys  {n_sig} signals")

        all_closes = [b["close"] for b in raw_bars]
        ds = len(raw_bars) - len(display_bars)
        sma50  = _sma(all_closes, 50)[ds:]
        sma200 = _sma(all_closes, 200)[ds:]
        rsi14  = _rsi(all_closes, 14)[ds:]

        dates = [b["time"] for b in display_bars]

        # Build data objects
        ohlcv = [
            {"time": b["time"], "open": b["open"], "high": b["high"],
             "low": b["low"], "close": b["close"]}
            for b in display_bars
        ]
        vol_data = [
            {"time": b["time"], "value": b["volume"],
             "color": "#26a69a" if b["close"] >= b["open"] else "#ef5350"}
            for b in display_bars
        ]
        ma50_data  = [{"time": d, "value": v} for d, v in zip(dates, sma50)  if v is not None]
        ma200_data = [{"time": d, "value": v} for d, v in zip(dates, sma200) if v is not None]
        rsi_data   = [{"time": d, "value": v} for d, v in zip(dates, rsi14)  if v is not None]

        # Buy markers (on candle chart)
        buy_markers = []
        d0, d1 = min(dates), max(dates)
        for buy in buys:
            bd = buy["transaction_date"]
            if not d0 <= bd <= d1:
                continue
            nm = (buy.get("insider_name") or "")[:25]
            tv = _fmt(buy.get("total_value"))
            buy_markers.append({
                "time": bd,
                "position": "belowBar",
                "color": "#22c55e",
                "shape": "arrowUp",
                "text": f"{nm} {tv}".strip(),
            })

        # Signal markers
        sig_markers = []
        for code, fire_dates in signals.items():
            s = SIGNAL_STYLES[code]
            for fd in fire_dates:
                if not d0 <= fd <= d1:
                    continue
                sig_markers.append({
                    "time": fd,
                    "position": "aboveBar",
                    "color": s["color"],
                    "shape": "circle",
                    "text": s["label"],
                })

        active_sigs = [SIGNAL_STYLES[c]["label"] for c, fds in signals.items() if fds]

        charts.append({
            "ticker":     ticker,
            "activeSigs": active_sigs,
            "ohlcv":      ohlcv,
            "volume":     vol_data,
            "ma50":       ma50_data,
            "ma200":      ma200_data,
            "rsi":        rsi_data,
            "buyMarkers": buy_markers,
            "sigMarkers": sig_markers,
            "from":       from_date.isoformat(),
            "to":         today.isoformat(),
        })

    conn.close()

    sig_legend = " ".join(
        f'<span><span style="color:{s["color"]}">━</span> {s["label"]} {s["name"]}</span>'
        for s in SIGNAL_STYLES.values()
    )

    charts_json = json.dumps(charts)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Insider Signals — {from_date} to {today}</title>
  <script src="https://unpkg.com/lightweight-charts@4.2.0/dist/lightweight-charts.standalone.production.js"></script>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body {{
      background: #0b1220;
      margin: 0;
      padding: 12px 16px 32px;
      font-family: 'JetBrains Mono', monospace;
      color: #6b7a8d;
    }}
    h1 {{ color: #c9d1d9; font-size: 11px; margin: 0 0 8px; letter-spacing: .1em; text-transform: uppercase; }}
    .legend {{ display: flex; flex-wrap: wrap; gap: 14px; margin-bottom: 12px; font-size: 10px; }}
    .ticker-block {{ margin-bottom: 8px; }}
    .ticker-title {{
      font-size: 11px; font-weight: 700; letter-spacing: .08em;
      padding: 4px 0 3px; display: flex; align-items: center; gap: 8px;
    }}
    .sig-badge {{ font-size: 9px; font-weight: 600; letter-spacing: .06em; }}
    .chart-wrap {{ position: relative; }}
    .candle-chart {{ width: 960px; height: 300px; }}
    .rsi-chart   {{ width: 960px; height: 80px;  margin-top: 1px; }}
    .vol-chart   {{ width: 960px; height: 60px;  margin-top: 1px; }}
    hr.sep {{ border: none; border-top: 1px solid #161f2e; margin: 6px 0; }}
  </style>
</head>
<body>
  <h1>Insider Buys + Technical Signal Triggers &nbsp;·&nbsp; {from_date} → {today}</h1>
  <div class="legend">
    <span><span style="color:#22c55e">▲</span> Insider Buy</span>
    {sig_legend}
    <span><span style="color:#f59e0b">─</span> 50 MA</span>
    <span><span style="color:#818cf8">─</span> 200 MA</span>
  </div>
  <div id="root"></div>

  <script>
  const CHARTS = {charts_json};
  const CHART_BG  = '#0b1220';
  const PANEL_BG  = '#0d1b2e';
  const GRID      = '#161f2e';
  const TEXT      = '#5a6a7e';
  const SIG_COLORS = {{
    GC: '#f59e0b', RB: '#f97316', HH: '#22d3ee', CB: '#a855f7'
  }};

  const root = document.getElementById('root');

  function initBlock(block, data) {{
    if (block._lc_done) return;
    block._lc_done = true;

    const OPTS = {{
      layout:    {{ background: {{ color: PANEL_BG }}, textColor: TEXT }},
      grid:      {{ vertLines: {{ color: GRID }}, horzLines: {{ color: GRID }} }},
      crosshair: {{ mode: LightweightCharts.CrosshairMode.Normal }},
    }};

    // ── Candle chart ─────────────────────────────────────────────────────
    const candleEl = block.querySelector('.candle-chart');
    const candleChart = LightweightCharts.createChart(candleEl, Object.assign({{}}, OPTS, {{
      autoSize: true,
      rightPriceScale: {{ borderColor: GRID }},
      timeScale: {{ borderColor: GRID, timeVisible: false }},
    }}));
    const candles = candleChart.addCandlestickSeries({{
      upColor:'#26a69a', downColor:'#ef5350',
      borderUpColor:'#26a69a', borderDownColor:'#ef5350',
      wickUpColor:'#26a69a', wickDownColor:'#ef5350',
    }});
    candles.setData(data.ohlcv);
    candleChart.addLineSeries({{ color:'#f59e0b', lineWidth:1.3, priceLineVisible:false }}).setData(data.ma50);
    candleChart.addLineSeries({{ color:'#818cf8', lineWidth:1.3, priceLineVisible:false }}).setData(data.ma200);
    const allM = data.buyMarkers.concat(data.sigMarkers)
      .sort(function(a,b){{return a.time<b.time?-1:a.time>b.time?1:0;}});
    if (allM.length) candles.setMarkers(allM);
    candleChart.timeScale().setVisibleRange({{from:data.from, to:data.to}});

    // ── RSI chart ────────────────────────────────────────────────────────
    const rsiEl = block.querySelector('.rsi-chart');
    const rsiChart = LightweightCharts.createChart(rsiEl, Object.assign({{}}, OPTS, {{
      autoSize: true,
      rightPriceScale: {{ borderColor: GRID, scaleMargins: {{top:0.05,bottom:0.05}} }},
      timeScale: {{ borderColor: GRID, timeVisible: false }},
    }}));
    const rsiLine = rsiChart.addLineSeries({{
      color:'#22d3ee', lineWidth:1.2, priceLineVisible:false,
      autoscaleInfoProvider: function(){{return{{priceRange:{{minValue:0,maxValue:100}}}};}}
    }});
    rsiLine.setData(data.rsi);
    [30,50,70].forEach(function(y){{
      rsiLine.createPriceLine({{price:y, color:y===50?'rgba(90,100,120,0.3)':'rgba(90,100,120,0.4)',
        lineWidth:1, lineStyle:2, axisLabelVisible:false}});
    }});
    rsiChart.timeScale().setVisibleRange({{from:data.from, to:data.to}});

    // ── Volume chart ─────────────────────────────────────────────────────
    const volEl = block.querySelector('.vol-chart');
    const volChart = LightweightCharts.createChart(volEl, Object.assign({{}}, OPTS, {{
      autoSize: true,
      rightPriceScale: {{ borderColor: GRID, scaleMargins: {{top:0.1,bottom:0}} }},
      timeScale: {{ borderColor: GRID, timeVisible: true }},
    }}));
    volChart.addHistogramSeries({{priceLineVisible:false}}).setData(data.volume);
    volChart.timeScale().setVisibleRange({{from:data.from, to:data.to}});

    // Crosshair sync
    function sync(src, others, p) {{
      if (!p.time) return;
      var x = src.timeScale().timeToCoordinate(p.time);
      others.forEach(function(c){{ try{{c.setCrosshairPosition(0, p.time, c.series);}}catch(e){{}} }});
    }}
    candleChart.subscribeCrosshairMove(function(p){{sync(candleChart,[rsiChart,volChart],p);}});
    rsiChart.subscribeCrosshairMove(function(p){{sync(rsiChart,[candleChart,volChart],p);}});
    volChart.subscribeCrosshairMove(function(p){{sync(volChart,[candleChart,rsiChart],p);}});

    // Double-RAF: with autoSize, the ResizeObserver fires after layout —
    // a second RAF ensures the canvas is fully composited before any screenshot
    requestAnimationFrame(function() {{
      requestAnimationFrame(function() {{
        candleChart.timeScale().setVisibleRange({{from:data.from, to:data.to}});
        rsiChart.timeScale().setVisibleRange({{from:data.from, to:data.to}});
        volChart.timeScale().setVisibleRange({{from:data.from, to:data.to}});
      }});
    }});
  }}

  // Build DOM skeletons first (fast), then init charts on scroll-into-view
  CHARTS.forEach(function(data) {{
    const block = document.createElement('div');
    block.className = 'ticker-block';
    block._lcData = data;

    const title = document.createElement('div');
    title.className = 'ticker-title';
    title.innerHTML = '<span style="color:#e2e8f0">$' + data.ticker + '</span>' +
      data.activeSigs.map(function(s){{
        return '<span class="sig-badge" style="color:'+(SIG_COLORS[s]||'#fff')+'">'+s+'</span>';
      }}).join(' ');
    block.appendChild(title);

    ['candle-chart','rsi-chart','vol-chart'].forEach(function(cls){{
      const el = document.createElement('div');
      el.className = cls;
      block.appendChild(el);
    }});

    const hr = document.createElement('hr');
    hr.className = 'sep';
    block.appendChild(hr);
    root.appendChild(block);
  }});

  // IntersectionObserver: init each chart 300px before it enters viewport
  const obs = new IntersectionObserver(function(entries){{
    entries.forEach(function(e){{
      if (e.isIntersecting) {{ initBlock(e.target, e.target._lcData); }}
    }});
  }}, {{ rootMargin: '300px' }});

  document.querySelectorAll('.ticker-block').forEach(function(b){{ obs.observe(b); }});
  </script>
</body>
</html>"""

    Path(args.output).write_text(html, encoding="utf-8")
    print(f"\n✓  {args.output}  ({len(charts)} tickers)")


if __name__ == "__main__":
    main()
