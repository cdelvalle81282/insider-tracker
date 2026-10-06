"""
Schedule-aware supervisor for the 2021-2024 historical Form 4 backfill.

The gap: `filings` has real, continuous data from 2025 onward but is
effectively empty from 2021 through 2024 (see the "filings table has a
~4-year data gap" entry in private/gotchas.md, added 2026-08-21). This script
fills it via `ingest.py --backfill`, which already does the real EDGAR
fetch/parse/insert work -- this file only adds scheduling around it.

Runs ONE CALENDAR DAY per `ingest.py --backfill <date> <date>` subprocess,
not one process per quarter. That's deliberate: ingest.py's own --backfill
loop processes a whole date range in a single uninterrupted process, and
pausing that mid-request (e.g. via SIGSTOP) would risk corrupting whatever
HTTP call was in flight. A single day takes a few minutes, so checking the
clock between day-subprocesses is a fine enough grain to respect the
scheduled ingest windows without ever touching a live request in progress.

Chunked and reported by quarter (2021Q1 .. 2024Q4) since that's the natural
unit for tracking a multi-day job, but the pause/resume and resume-after-
interruption granularity is per day. Progress is persisted to --state-file
after every completed day, so re-running this script after ANY interruption
(crash, manual stop, server reboot) picks up exactly where it left off --
already-completed days are never redone. ingest.py's own `ON CONFLICT DO
NOTHING` makes redoing a day harmless anyway, but tracking state avoids
wasting real time re-fetching thousands of already-ingested filings.

Pause windows mirror the live systemd timer schedule, checked directly via
`systemctl list-timers` on 2026-08-22:
  - insider-ingest-nightly.timer: Mon-Sat 03:00 UTC (`--since-last-run`,
    the heavy nightly catch-up)
  - insider-ingest.timer: Mon-Fri 10:30 / 14:00 / 19:00 UTC (`--date today`,
    usually near-empty per the EDGAR daily-index-not-ready gotcha, but still
    real requests against the same rate budget)
If that timer schedule ever changes, update PAUSE_WINDOWS below to match --
this script does not read the systemd units itself.

Usage:
    python backfill_2021_2024.py [--start 2021-01-01] [--end 2024-12-31]
        [--state-file data/backfill_2021_2024_state.json]
        [--log-file data/backfill_2021_2024.log]

Run under nohup -- expected wall-clock is on the order of several days,
bounded by SEC_RATE_LIMIT (8 req/sec, config.py) in ingest.py itself.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent

STATE_FILE_DEFAULT = "data/backfill_2021_2024_state.json"
LOG_FILE_DEFAULT   = "data/backfill_2021_2024.log"

MAX_ATTEMPTS        = 3     # per day, before stopping for manual investigation
RETRY_SLEEP_SECONDS = 120

# (weekdays as {0=Mon .. 6=Sun}, "HH:MM" start, "HH:MM" end), all UTC.
# Generous buffers around each scheduled ingest.py fire time -- see docstring.
PAUSE_WINDOWS = [
    ({0, 1, 2, 3, 4, 5}, "02:55", "03:25"),   # nightly catch-up, Mon-Sat 03:00 UTC
    ({0, 1, 2, 3, 4},    "10:25", "10:40"),   # daytime run 1,   Mon-Fri 10:30 UTC
    ({0, 1, 2, 3, 4},    "13:55", "14:10"),   # daytime run 2,   Mon-Fri 14:00 UTC
    ({0, 1, 2, 3, 4},    "18:55", "19:10"),   # daytime run 3,   Mon-Fri 19:00 UTC
]


def _in_pause_window(now: datetime) -> bool:
    hm = now.strftime("%H:%M")
    return any(now.weekday() in weekdays and start <= hm <= end
               for weekdays, start, end in PAUSE_WINDOWS)


def _wait_out_pause_window(log) -> None:
    announced = False
    while True:
        now = datetime.now(timezone.utc)
        if not _in_pause_window(now):
            return
        if not announced:
            log(f"pausing for scheduled ingest window ({now.strftime('%H:%M UTC')})")
            announced = True
        time.sleep(60)


def _quarter_bounds(y: int, q: int) -> tuple[date, date]:
    start_month = (q - 1) * 3 + 1
    q_start = date(y, start_month, 1)
    end_month = start_month + 2
    next_month_first = date(y + 1, 1, 1) if end_month == 12 else date(y, end_month + 1, 1)
    return q_start, next_month_first - timedelta(days=1)


def _quarters(start: date, end: date) -> list[tuple[str, date, date]]:
    quarters = []
    y, q = start.year, (start.month - 1) // 3 + 1
    while True:
        q_start, q_end = _quarter_bounds(y, q)
        window_start = max(q_start, start)
        window_end = min(q_end, end)
        quarters.append((f"{y}Q{q}", window_start, window_end))
        if q_end >= end:
            break
        q += 1
        if q > 4:
            q = 1
            y += 1
    return quarters


def _daterange(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def _load_state(path: Path) -> dict:
    if path.exists():
        return json.loads(path.read_text())
    return {"completed_days": []}


def _save_state(path: Path, state: dict) -> None:
    path.write_text(json.dumps(state))


def main() -> None:
    parser = argparse.ArgumentParser(description="Schedule-aware historical backfill, chunked by quarter")
    parser.add_argument("--start", default="2021-01-01")
    parser.add_argument("--end", default="2024-12-31")
    parser.add_argument("--state-file", default=STATE_FILE_DEFAULT)
    parser.add_argument("--log-file", default=LOG_FILE_DEFAULT)
    args = parser.parse_args()

    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    state_path = REPO_ROOT / args.state_file
    log_path = REPO_ROOT / args.log_file
    state_path.parent.mkdir(parents=True, exist_ok=True)

    state = _load_state(state_path)
    completed = set(state.get("completed_days", []))

    log_f = open(log_path, "a", buffering=1)

    def log(msg: str) -> None:
        line = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC] {msg}"
        print(line, flush=True)
        log_f.write(line + "\n")

    quarters = _quarters(start, end)
    log(f"=== backfill supervisor starting: {start} -> {end}, {len(quarters)} quarters, "
        f"{len(completed)} day(s) already completed from a prior run ===")

    for label, q_start, q_end in quarters:
        days = [d for d in _daterange(q_start, q_end) if d.weekday() < 5]
        remaining = [d for d in days if d.isoformat() not in completed]
        if not remaining:
            log(f"{label}: already complete ({len(days)} weekdays), skipping")
            continue
        log(f"{label}: {len(remaining)}/{len(days)} weekday(s) remaining")

        for d in remaining:
            _wait_out_pause_window(log)
            iso = d.isoformat()
            # A day is safe to redo (ON CONFLICT DO NOTHING), so retry transient
            # failures (e.g. a dropped DB connection) before giving up.
            for attempt in range(1, MAX_ATTEMPTS + 1):
                result = subprocess.run(
                    [sys.executable, "ingest.py", "--backfill", iso, iso],
                    capture_output=True, text=True, cwd=REPO_ROOT,
                )
                if result.returncode == 0 or attempt == MAX_ATTEMPTS:
                    break
                err_tail = (result.stderr or result.stdout).strip()[-200:]
                log(f"{iso}: attempt {attempt}/{MAX_ATTEMPTS} failed (exit {result.returncode}), "
                    f"retrying in {RETRY_SLEEP_SECONDS}s -- {err_tail}")
                time.sleep(RETRY_SLEEP_SECONDS)
                _wait_out_pause_window(log)
            if result.returncode != 0:
                err_tail = (result.stderr or result.stdout).strip()[-500:]
                log(f"{iso}: FAILED (exit {result.returncode}) -- {err_tail}")
                log("stopping for manual investigation -- re-run this script to resume once fixed")
                log_f.close()
                sys.exit(1)
            out_lines = result.stdout.strip().splitlines()
            tail = out_lines[-1] if out_lines else ""
            log(f"{iso}: ok -- {tail}")
            completed.add(iso)
            state["completed_days"] = sorted(completed)
            _save_state(state_path, state)

        log(f"{label}: COMPLETE ({len(days)} weekday(s))")

    log("=== BACKFILL COMPLETE -- all quarters done ===")
    log_f.close()


if __name__ == "__main__":
    main()
