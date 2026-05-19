"""
Standalone chart generator — insider buys + technical signal triggers.

Usage:
    .venv/bin/python generate_signals_chart.py
    .venv/bin/python generate_signals_chart.py --tickers AAT,KOS,PANW --days 180
    .venv/bin/python generate_signals_chart.py --output /tmp/my_chart.html

Panels per ticker:
  1. Candlestick + 50MA + 200MA  (insider buy dashes + signal fire lines)
  2. RSI(14)
  3. Volume
"""
from __future__ import annotations

import argparse
import os
import sys
from datetime import date, timedelta
from pathlib import Path

# ── Path / env bootstrap ──────────────────────────────────────────────────────

_HERE = Path(__file__).parent
sys.path.insert(0, str(_HERE))

_env_file = _HERE / ".env"
if _env_file.exists():
    for _line in _env_file.read_text().splitlines():
        _line = _line.strip()
        if _line and not _line.startswith("#") and "=" in _line:
            _k, _v = _line.split("=", 1)
            os.environ.setdefault(_k.strip(), _v.strip())

# ── Project imports ───────────────────────────────────────────────────────────

import plotly.graph_objects as go
from plotly.subplots import make_subplots

import polygon_client
from backtest import (
    PRICE_WARMUP_DAYS,
    detect_channel_break,
    detect_golden_cross,
    detect_hhl,
    detect_resistance_break,
)
from db import get_db
from queries import get_chart_buys

# ── Config ────────────────────────────────────────────────────────────────────

_API_KEY = os.getenv("POLYGON_API_KEY", "")

DEFAULT_TICKERS = [
    "AAT",   # exec chairman, all 4 signals
    "KOS",   # dual C-suite + golden cross 3d
    "PANW",  # CEO Arora $10M, RB+HH
    "TTD",   # CEO Green $35-58M cluster
    "MESO",  # repeat buys, fast RB
    "ALKT",  # institutional $9-19M, RB 1d
    "LW",    # activist Jana, RB 1-6d
    "WASH",  # most recent (Apr 23)
    "GO",    # CEO + 3 directors cluster
    "SOFI",  # CEO Noto two buys
    "CPNG",  # $37-56M buys, RB+HH
    "CAR",   # Pentwater $40M, GC+RB+HH (3-signal)
]

SIGNAL_STYLES: dict[str, dict] = {
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

# Colours
CHART_BG   = "#0b1220"
PANEL_BG   = "#0d1b2e"
GRID       = "#161f2e"
TEXT       = "#6b7a8d"
UP         = "#26a69a"
DOWN       = "#ef5350"
BUY_LINE   = "rgba(34,197,94,0.55)"
BUY_TEXT   = "#22c55e"
MA50       = "#f59e0b"
MA200      = "#818cf8"

MIN_VALUE  = 500_000

# ── Helpers ───────────────────────────────────────────────────────────────────

def _sma(closes: list[float], period: int) -> list[float | None]:
    out: list[float | None] = [None] * len(closes)
    for i in range(period - 1, len(closes)):
        out[i] = sum(closes[i - period + 1 : i + 1]) / period
    return out


def _rsi(closes: list[float], period: int = 14) -> list[float | None]:
    n = len(closes)
    if n < period + 1:
        return [None] * n
    result: list[float | None] = [None] * period
    g = [max(closes[i] - closes[i - 1], 0.0) for i in range(1, period + 1)]
    l = [max(closes[i - 1] - closes[i], 0.0) for i in range(1, period + 1)]
    ag, al = sum(g) / period, sum(l) / period
    for i in range(period, n):
        if i > period:
            d = closes[i] - closes[i - 1]
            ag = (ag * (period - 1) + max(d, 0.0)) / period
            al = (al * (period - 1) + max(-d, 0.0)) / period
        result.append(100.0 if al == 0 else round(100 - 100 / (1 + ag / al), 2))
    return result


def _fmt(v: float | None) -> str:
    if not v:
        return ""
    if v >= 1e9:
        return f"${v/1e9:.1f}B"
    if v >= 1e6:
        return f"${v/1e6:.1f}M"
    return f"${v/1e3:.0f}K"



def _detect_signals(raw_bars: list[dict], buys: list[dict]) -> dict[str, list[str]]:
    if len(raw_bars) < 50 or not buys:
        return {k: [] for k in SIGNAL_DETECTORS}
    sb = [{**b, "date": b["time"]} for b in raw_bars]
    dates = [b["date"] for b in sb]
    result: dict[str, list[str]] = {k: [] for k in SIGNAL_DETECTORS}
    seen: set[tuple[str, str]] = set()
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
            key = (code, fd)
            if key not in seen:
                seen.add(key)
                result[code].append(fd)
    return result


# ── Figure builder ────────────────────────────────────────────────────────────

def _build_figure(
    ticker: str,
    raw_bars: list[dict],
    display_bars: list[dict],
    buys: list[dict],
    signals: dict[str, list[str]],
) -> go.Figure:

    dates  = [b["time"]   for b in display_bars]
    opens  = [b["open"]   for b in display_bars]
    highs  = [b["high"]   for b in display_bars]
    lows   = [b["low"]    for b in display_bars]
    closes = [b["close"]  for b in display_bars]
    vols   = [b["volume"] for b in display_bars]

    all_c = [b["close"] for b in raw_bars]
    ds    = len(raw_bars) - len(display_bars)
    sma50  = _sma(all_c, 50)[ds:]
    sma200 = _sma(all_c, 200)[ds:]
    rsi    = _rsi(all_c, 14)[ds:]

    fig = make_subplots(
        rows=3, cols=1,
        row_heights=[0.65, 0.20, 0.15],
        shared_xaxes=True,
        vertical_spacing=0.018,
    )

    # ── Candles ───────────────────────────────────────────────────────────
    fig.add_trace(go.Candlestick(
        x=dates, open=opens, high=highs, low=lows, close=closes,
        name=ticker,
        increasing=dict(fillcolor=UP,   line=dict(color=UP,   width=1)),
        decreasing=dict(fillcolor=DOWN, line=dict(color=DOWN, width=1)),
        showlegend=False, hoverinfo="x+y",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=dates, y=sma50, mode="lines",
        line=dict(color=MA50, width=1.3),
        name="50MA", showlegend=False, hoverinfo="skip",
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=dates, y=sma200, mode="lines",
        line=dict(color=MA200, width=1.3),
        name="200MA", showlegend=False, hoverinfo="skip",
    ), row=1, col=1)

    # ── RSI ───────────────────────────────────────────────────────────────
    fig.add_trace(go.Scatter(
        x=dates, y=rsi, mode="lines",
        line=dict(color="#22d3ee", width=1.2),
        fill="tozeroy", fillcolor="rgba(34,211,238,0.04)",
        name="RSI(14)", showlegend=False,
    ), row=2, col=1)

    # ── Volume ────────────────────────────────────────────────────────────
    vc = [UP if c >= o else DOWN for c, o in zip(closes, opens)]
    fig.add_trace(go.Bar(
        x=dates, y=vols, marker_color=vc, marker_line_width=0,
        showlegend=False,
    ), row=3, col=1)

    # add_vline with annotations is broken for string dates in Plotly 6;
    # use add_shape + add_annotation instead.
    def _vshape(x: str, color: str, width: float, dash: str, opacity: float = 0.65) -> None:
        fig.add_shape(
            type="line", x0=x, x1=x, y0=0, y1=1,
            xref="x", yref="paper",
            line=dict(color=color, width=width, dash=dash),
            opacity=opacity,
        )

    def _vann(x: str, text: str, color: str, y: float, anchor: str) -> None:
        fig.add_annotation(
            x=x, y=y, xref="x", yref="paper",
            text=text, showarrow=False,
            font=dict(size=7, color=color, family="JetBrains Mono, monospace"),
            bgcolor="rgba(11,18,32,0.82)",
            bordercolor=color, borderwidth=1,
            xanchor=anchor, yanchor="top",
        )

    # ── Insider buy vertical lines (green dashed) ─────────────────────────
    d0, d1 = (min(dates), max(dates)) if dates else ("", "")
    for buy in buys:
        bd = buy["transaction_date"]
        if not d0 <= bd <= d1:
            continue
        name_s  = (buy.get("insider_name") or "Unknown")[:28]
        title_s = (buy.get("insider_title") or "")[:22]
        tv      = _fmt(buy.get("total_value"))
        label   = f"▲ {name_s}"
        if tv:
            label += f"  {tv}"
        if title_s:
            label += f"  {title_s}"
        _vshape(bd, BUY_LINE, width=1.1, dash="dash")
        _vann(bd, label, BUY_TEXT, y=0.985, anchor="left")

    # ── Signal fire vertical lines (solid, coloured) ──────────────────────
    for code, fire_dates in signals.items():
        s = SIGNAL_STYLES[code]
        for fd in fire_dates:
            if not d0 <= fd <= d1:
                continue
            _vshape(fd, s["color"], width=2.0, dash="solid", opacity=0.75)
            _vann(fd, s["label"], s["color"], y=0.92, anchor="right")

    # ── RSI guides (add_shape to avoid Plotly 6 row= issues) ─────────────
    for rsi_y, rsi_col in [(70, "rgba(239,83,80,0.4)"), (50, "rgba(110,120,140,0.3)"), (30, "rgba(38,166,154,0.4)")]:
        fig.add_shape(
            type="line", x0=0, x1=1, y0=rsi_y, y1=rsi_y,
            xref="paper", yref="y2",
            line=dict(color=rsi_col, width=0.7, dash="dot"),
        )

    # ── Layout ────────────────────────────────────────────────────────────
    active_sigs = [
        f'<span style="color:{SIGNAL_STYLES[c]["color"]}">{SIGNAL_STYLES[c]["label"]}</span>'
        for c, fds in signals.items() if fds
    ]
    sig_badge = "  ".join(active_sigs)

    fig.update_layout(
        width=960,
        height=440,
        margin=dict(l=10, r=75, t=38, b=8),
        paper_bgcolor=CHART_BG,
        plot_bgcolor=PANEL_BG,
        font=dict(color=TEXT, size=9, family="JetBrains Mono, monospace"),
        showlegend=False,
        title=dict(
            text=(
                f"<b style='color:#e2e8f0;font-size:14px'>${ticker}</b>"
                f"{'&nbsp;&nbsp;' + sig_badge if sig_badge else ''}"
            ),
            x=0.01, y=0.99,
            font=dict(size=12),
        ),
        xaxis=dict(
            gridcolor=GRID, showgrid=True,
            rangeslider=dict(visible=False),
            tickfont=dict(size=8, color=TEXT),
            tickformat="%b %d '%y",
            showticklabels=False,
        ),
        xaxis2=dict(
            gridcolor=GRID, showgrid=True,
            tickfont=dict(size=8, color=TEXT),
            showticklabels=False,
        ),
        xaxis3=dict(
            gridcolor=GRID, showgrid=True,
            tickfont=dict(size=8, color=TEXT),
            tickformat="%b %d '%y",
        ),
        yaxis=dict(
            gridcolor=GRID, showgrid=True, side="right",
            tickfont=dict(size=8, color=TEXT),
        ),
        yaxis2=dict(
            gridcolor=GRID, showgrid=True, side="right",
            tickfont=dict(size=8, color=TEXT),
            range=[0, 100], tickvals=[30, 50, 70],
            title=dict(text="RSI", standoff=2, font=dict(size=7, color=TEXT)),
        ),
        yaxis3=dict(
            gridcolor=GRID, showgrid=True, side="right",
            tickfont=dict(size=8, color=TEXT),
            title=dict(text="Vol", standoff=2, font=dict(size=7, color=TEXT)),
        ),
    )

    return fig


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate insider signals chart")
    ap.add_argument("--tickers", default=",".join(DEFAULT_TICKERS),
                    help="Comma-separated ticker list")
    ap.add_argument("--days",    type=int, default=180,
                    help="Display window in calendar days (default 180)")
    ap.add_argument("--output",  default=str(_HERE / "data" / "insider_signals_chart.html"),
                    help="Output HTML path")
    args = ap.parse_args()

    tickers    = [t.strip().upper() for t in args.tickers.split(",")]
    today      = date.today()
    from_date  = today - timedelta(days=args.days)
    full_from  = from_date - timedelta(days=PRICE_WARMUP_DAYS)

    conn = get_db()

    html_parts: list[str] = []

    for ticker in tickers:
        print(f"  {ticker} ...", end="", flush=True)
        raw_bars = polygon_client.get_daily_bars(ticker, full_from, today, _API_KEY, limit=700)
        if not raw_bars:
            print(" no data")
            continue
        display_bars = [b for b in raw_bars if b["time"] >= from_date.isoformat()]
        if not display_bars:
            print(" no display bars")
            continue
        buys    = get_chart_buys(conn, ticker, args.days, MIN_VALUE)
        signals = _detect_signals(raw_bars, buys)
        n_sig   = sum(len(v) for v in signals.values())
        print(f" {len(buys)} buys  {n_sig} signal fires")
        fig  = _build_figure(ticker, raw_bars, display_bars, buys, signals)
        inner = fig.to_html(
            include_plotlyjs=False, full_html=False,
            div_id=f"c_{ticker}",
            config={"responsive": False, "staticPlot": False},
        )
        html_parts.append(inner)

    conn.close()

    if not html_parts:
        print("No charts generated.")
        return

    legend_items = [
        f'<span><span style="color:{BUY_TEXT}">▲ ┊ ┊</span>&nbsp;Insider Buy</span>',
        *[
            f'<span><span style="color:{s["color"]}">━</span>&nbsp;{s["label"]}&nbsp;{s["name"]}</span>'
            for s in SIGNAL_STYLES.values()
        ],
        f'<span><span style="color:{MA50}">─</span>&nbsp;50 MA</span>',
        f'<span><span style="color:{MA200}">─</span>&nbsp;200 MA</span>',
    ]

    sep = "\n<div style='height:6px'></div>\n"
    full_html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Insider Signals — {from_date} to {today}</title>
  <style>
    *, *::before, *::after {{ box-sizing: border-box; }}
    body  {{ background:{CHART_BG}; margin:0; padding:14px 16px 32px;
             font-family:'JetBrains Mono',monospace; }}
    h1   {{ color:#c9d1d9; font-size:12px; margin:0 0 10px;
             letter-spacing:.1em; text-transform:uppercase; }}
    .leg {{ display:flex; flex-wrap:wrap; gap:14px; margin-bottom:14px;
             font-size:10px; color:{TEXT}; }}
    .leg span {{ display:flex; align-items:center; gap:3px; }}
  </style>
</head>
<body>
  <script src="https://cdn.plot.ly/plotly-3.3.1.min.js"
          integrity="sha256-4rD3fugVb/nVJYUv5Ky3v+fYXoouHaBSP20WIJuEiWg=" crossorigin="anonymous">
  </script>
  <h1>Insider Buys + Technical Signal Triggers &nbsp;·&nbsp; {from_date} → {today}</h1>
  <div class="leg">{''.join(legend_items)}</div>
  {sep.join(html_parts)}
  <script>
    // Force all Plotly figures to render — Plotly 6 lazy-skips off-screen charts
    window.addEventListener('load', function() {{
      setTimeout(function() {{
        document.querySelectorAll('.plotly-graph-div').forEach(function(el) {{
          try {{ Plotly.Plots.resize(el); }} catch(e) {{}}
        }});
      }}, 400);
    }});
  </script>
</body>
</html>"""

    Path(args.output).write_text(full_html, encoding="utf-8")
    print(f"\n✓  Saved → {args.output}  ({len(html_parts)} tickers)")


if __name__ == "__main__":
    main()
