"""
Load backtest results into insider_perf_profile table and add winning insiders to watchlist.

Run after backtest_insiders.py has produced data/insider_backtest.csv:

    python load_insider_profiles.py [--csv data/insider_backtest.csv] [--min-trades 5]
                                    [--min-win90 0.50] [--min-med90 1.0]
"""
from __future__ import annotations

import argparse
import csv
from collections import defaultdict
from statistics import median

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

from db import get_cli_db


def _stats(vals: list[float]) -> tuple[float, float, float] | tuple[None, None, None]:
    if not vals:
        return None, None, None
    n   = len(vals)
    win = sum(1 for v in vals if v > 0) / n
    avg = sum(vals) / n
    med = sorted(vals)[n // 2]
    return win, avg, med


def _peak_window(m30, m60, m90) -> int:
    candidates = [(m30 or -9999, 30), (m60 or -9999, 60), (m90 or -9999, 90)]
    return max(candidates, key=lambda x: x[0])[1]


def _profile_label(w30, w60, w90, m30, m60, m90) -> str:
    w30 = w30 or 0; w60 = w60 or 0; w90 = w90 or 0
    m30 = m30 or 0; m60 = m60 or 0; m90 = m90 or 0
    if w30 >= 0.70 and w60 >= 0.70 and w90 >= 0.70 and m90 > 0:
        return "Consistent"
    if w30 >= 0.65 and w90 < 0.50 and m30 > m90:
        return "30d peak"
    if w30 < 0.55 and w90 >= 0.65 and m90 > m30:
        return "Late mover"
    if w60 >= 0.65 and w60 >= w30 and w60 >= w90 and m60 > 0:
        return "60d peak"
    if m90 > 0 and w90 >= 0.50:
        return "Positive"
    return "Mixed"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv",        default="data/insider_backtest.csv")
    parser.add_argument("--min-trades", type=int,   default=5)
    parser.add_argument("--min-win90",  type=float, default=0.50,
                        help="Min 90d win rate to add to watchlist (default 0.50)")
    parser.add_argument("--min-med90",  type=float, default=1.0,
                        help="Min 90d median excess vs SPY to add to watchlist (default 1.0%%)")
    args = parser.parse_args()

    # Read CSV
    with open(args.csv) as f:
        rows = list(csv.DictReader(f))
    print(f"Read {len(rows)} rows from {args.csv}")

    # Group by insider_cik
    by_cik: dict[str, list[dict]] = defaultdict(list)
    for r in rows:
        by_cik[r["insider_cik"]].append(r)

    def _floats(trades, col):
        return [float(t[col]) for t in trades if t.get(col) not in ("", "None", None)]

    profiles = []
    for cik, trades in by_cik.items():
        if len(trades) < args.min_trades:
            continue
        name = trades[0]["insider_name"]
        role = trades[0]["role"]

        w30, a30, m30 = _stats(_floats(trades, "excess_30d"))
        w60, a60, m60 = _stats(_floats(trades, "excess_60d"))
        w90, a90, m90 = _stats(_floats(trades, "excess_90d"))

        if m90 is None:
            continue

        peak  = _peak_window(m30, m60, m90)
        label = _profile_label(w30, w60, w90, m30, m60, m90)

        profiles.append({
            "insider_cik":   cik,
            "insider_name":  name,
            "role":          role,
            "n_trades":      len(trades),
            "win_30": w30, "avg_30": a30, "med_30": m30,
            "win_60": w60, "avg_60": a60, "med_60": m60,
            "win_90": w90, "avg_90": a90, "med_90": m90,
            "peak_window":   peak,
            "profile_label": label,
        })

    print(f"Profiles computed: {len(profiles)}")

    conn = get_cli_db()

    # Create table (research table — not in Alembic migrations intentionally)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS insider_perf_profile (
            insider_cik   TEXT PRIMARY KEY,
            insider_name  TEXT,
            role          TEXT,
            n_trades      INT,
            win_30  REAL, avg_30 REAL, med_30 REAL,
            win_60  REAL, avg_60 REAL, med_60 REAL,
            win_90  REAL, avg_90 REAL, med_90 REAL,
            peak_window   INT,
            profile_label TEXT,
            updated_at    TEXT DEFAULT (NOW()::text)
        )
    """)

    # Upsert all profiles
    upserted = 0
    for p in profiles:
        conn.execute("""
            INSERT INTO insider_perf_profile
                (insider_cik, insider_name, role, n_trades,
                 win_30, avg_30, med_30,
                 win_60, avg_60, med_60,
                 win_90, avg_90, med_90,
                 peak_window, profile_label, updated_at)
            VALUES (%s,%s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s,%s, %s,%s, NOW()::text)
            ON CONFLICT (insider_cik) DO UPDATE SET
                insider_name  = EXCLUDED.insider_name,
                role          = EXCLUDED.role,
                n_trades      = EXCLUDED.n_trades,
                win_30 = EXCLUDED.win_30, avg_30 = EXCLUDED.avg_30, med_30 = EXCLUDED.med_30,
                win_60 = EXCLUDED.win_60, avg_60 = EXCLUDED.avg_60, med_60 = EXCLUDED.med_60,
                win_90 = EXCLUDED.win_90, avg_90 = EXCLUDED.avg_90, med_90 = EXCLUDED.med_90,
                peak_window   = EXCLUDED.peak_window,
                profile_label = EXCLUDED.profile_label,
                updated_at    = NOW()::text
        """, [
            p["insider_cik"], p["insider_name"], p["role"], p["n_trades"],
            p["win_30"], p["avg_30"], p["med_30"],
            p["win_60"], p["avg_60"], p["med_60"],
            p["win_90"], p["avg_90"], p["med_90"],
            p["peak_window"], p["profile_label"],
        ])
        upserted += 1

    print(f"Upserted {upserted} profiles → insider_perf_profile")

    # Add winning insiders to watchlist
    winners = [
        p for p in profiles
        if (p["win_90"] or 0) >= args.min_win90
        and (p["med_90"] or 0) >= args.min_med90
    ]
    print(f"\nWinners (win90≥{args.min_win90:.0%}, med90≥{args.min_med90}%): {len(winners)}")

    added = 0
    already = 0
    for w in winners:
        existing = conn.execute(
            "SELECT id FROM watchlist WHERE type='insider' AND value=%s",
            [w["insider_cik"]],
        ).fetchone()
        if existing:
            already += 1
        else:
            conn.execute(
                "INSERT INTO watchlist (type, value, label) VALUES ('insider', %s, %s)"
                " ON CONFLICT DO NOTHING",
                [w["insider_cik"], w["insider_name"]],
            )
            added += 1
            print(f"  + {w['insider_name']:<32} peak={w['peak_window']}d  label={w['profile_label']}"
                  f"  win90={w['win_90']:.0%}  med90={w['med_90']:+.1f}%")

    print(f"\nWatchlist: {added} added, {already} already tracked")

    conn.commit()
    conn.close()
    print("\nDone.")


if __name__ == "__main__":
    main()
