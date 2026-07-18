"""
Autonomous diagnostic agent for Insider Scanner.

Called by the /webhook/alert endpoint when Healthchecks.io or BetterStack
fires an alert. Collects system state, asks Claude to analyze it, applies
safe fixes automatically, and posts a rich report to Slack.

Safe auto-fixes (no human approval needed):
  - service_restart: restart insider-tracker.service if it's dead/failed
  - cache_clear: touch the ingest sentinel to invalidate all in-process caches

Everything else goes into the Slack report as "needs your attention".

Run manually: python auto_diagnose.py [alert_name]
"""
from __future__ import annotations

import json
import logging
import os
import re
import subprocess
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

from alerts import _post_to_slack
from ingest import INGEST_SENTINEL

SERVICE = "insider-tracker.service"
_LOG = logging.getLogger(__name__)

# Seconds to wait after restarting the service before checking its status.
_RESTART_SETTLE = 3


# ---------------------------------------------------------------------------
# Diagnostics collection
# ---------------------------------------------------------------------------

def _cmd(cmd: str, timeout: int = 20) -> str:
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=timeout)
        return (r.stdout + r.stderr).strip()
    except subprocess.TimeoutExpired:
        return f"[timed out after {timeout}s]"
    except Exception as e:
        return f"[error: {e}]"


def _sentinel_age_hours() -> float | None:
    try:
        return (datetime.now().timestamp() - INGEST_SENTINEL.stat().st_mtime) / 3600
    except FileNotFoundError:
        return None


def _recent_run_log() -> str:
    try:
        from db import get_cli_db
        conn = get_cli_db()
        try:
            rows = conn.execute(
                """SELECT run_kind, date_processed, filings_found, errors,
                          LEFT(error_detail, 200) AS error_detail, started_at::text
                   FROM run_log ORDER BY started_at DESC LIMIT 5"""
            ).fetchall()
            return json.dumps([dict(r) for r in rows], default=str, indent=2)
        finally:
            conn.close()
    except Exception as e:
        return f"[DB unavailable: {e}]"


def _pg_connection_states() -> str:
    try:
        from db import get_cli_db
        conn = get_cli_db()
        try:
            rows = conn.execute(
                "SELECT state, count(*) AS n FROM pg_stat_activity "
                "WHERE datname = current_database() GROUP BY state"
            ).fetchall()
            if not rows:
                return "(no rows)"
            return "\n".join(f"{r['state']} | {r['n']}" for r in rows)
        finally:
            conn.close()
    except Exception as e:
        return f"[DB unavailable: {e}]"


def collect_diagnostics() -> dict:
    # Run independent subprocess calls in parallel to cut worst-case from ~160s to ~20s
    cmds = {
        "service_status": f"sudo systemctl status {SERVICE} --no-pager -l",
        "recent_logs":    f"sudo journalctl -u {SERVICE} -n 60 --no-pager",
        "disk":           "df -h / /home",
        "memory":         "free -h",
        "nightly_timer":  "sudo systemctl status insider-ingest-nightly.timer --no-pager",
        "prices_timer":   "sudo systemctl status insider-prices.timer --no-pager",
    }
    results: dict[str, str] = {}
    with ThreadPoolExecutor(max_workers=len(cmds)) as pool:
        futures = {pool.submit(_cmd, cmd): key for key, cmd in cmds.items()}
        for future in as_completed(futures):
            results[futures[future]] = future.result()

    return {
        "timestamp_utc": datetime.now(timezone.utc).isoformat(),
        "sentinel_age_hours": _sentinel_age_hours(),
        "recent_run_log": _recent_run_log(),
        "pg_connections": _pg_connection_states(),
        **results,
    }


# ---------------------------------------------------------------------------
# Claude analysis
# ---------------------------------------------------------------------------

def analyze(diagnostics: dict, alert_info: dict) -> dict:
    api_key = os.getenv("ANTHROPIC_API_KEY")
    if not api_key:
        return {
            "root_cause": "ANTHROPIC_API_KEY not set — cannot auto-diagnose",
            "severity": "high",
            "safe_auto_fixes": [],
            "manual_actions": ["Set ANTHROPIC_API_KEY in /home/deploy/insider-tracker/.env and restart the service"],
            "diagnosis_summary": "Auto-diagnosis was triggered but ANTHROPIC_API_KEY is missing. Manual investigation required.",
        }

    import anthropic  # deferred — only installed on server
    client = anthropic.Anthropic(api_key=api_key, timeout=60.0)

    prompt = f"""You are an autonomous ops agent for the Insider Scanner application.
Stack: FastAPI + PostgreSQL 16 + uvicorn (2 workers) on a DigitalOcean droplet.
Key services: insider-tracker.service (uvicorn), insider-ingest-nightly.timer (03:00 UTC Mon-Sat), insider-prices.timer (01:00 UTC Mon-Fri ONLY — no weekend runs; a stale prices sentinel Sat/Sun/Mon-before-02:00 UTC is expected and not a failure).

An alert fired: {json.dumps(alert_info, indent=2)}

Current system diagnostics:
{json.dumps(diagnostics, indent=2, default=str)}

Respond with ONLY a JSON object in this exact format (no markdown, no explanation outside the JSON):
{{
  "root_cause": "one clear sentence",
  "severity": "critical|high|medium|low",
  "safe_auto_fixes": [],
  "manual_actions": [],
  "diagnosis_summary": "2-4 sentence explanation suitable for a Slack alert"
}}

Rules for safe_auto_fixes — only include these exact strings if clearly warranted:
  "service_restart" — only if service is failed/dead/crashed
  "cache_clear"     — only if there is evidence of stale cache causing errors

manual_actions should be specific, actionable steps a developer can take.
Be conservative: prefer doing nothing over taking risky auto-actions."""

    msg = client.messages.create(
        model="claude-opus-4-7",
        max_tokens=800,
        messages=[{"role": "user", "content": prompt}],
    )
    raw = msg.content[0].text.strip()
    # Strip markdown code fences if the model wrapped the JSON
    raw = re.sub(r"^```\w*\n?", "", raw)
    raw = re.sub(r"\n?```$", "", raw).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return {
            "root_cause": "Claude returned unparseable response",
            "severity": "unknown",
            "safe_auto_fixes": [],
            "manual_actions": ["Check server logs manually — auto-diagnosis produced malformed output"],
            "diagnosis_summary": raw[:600],
        }


# ---------------------------------------------------------------------------
# Auto-fix execution
# ---------------------------------------------------------------------------

_VALID_FIXES = {"service_restart", "cache_clear"}


def apply_fixes(fixes: list[str]) -> list[str]:
    done = []
    for fix in fixes:
        if fix not in _VALID_FIXES:
            _LOG.warning("Ignoring unrecognised fix action: %r", fix)

    if "service_restart" in fixes:
        _cmd(f"sudo systemctl restart {SERVICE}")
        # Allow systemd time to settle before checking active state
        time.sleep(_RESTART_SETTLE)
        status = _cmd(f"systemctl is-active {SERVICE}")
        done.append(f"Restarted {SERVICE} → now: {status}")

    if "cache_clear" in fixes:
        try:
            INGEST_SENTINEL.touch()
            done.append("Touched ingest sentinel — all worker caches will invalidate on next request")
        except Exception as e:
            done.append(f"Cache clear failed: {e}")

    return done


# ---------------------------------------------------------------------------
# Slack reporting
# ---------------------------------------------------------------------------

def post_slack(analysis: dict, auto_fixed: list[str], alert_info: dict, webhook_url: str) -> None:
    emoji = {"critical": ":red_circle:", "high": ":large_orange_circle:",
             "medium": ":large_yellow_circle:", "low": ":large_green_circle:"}.get(
        analysis.get("severity", ""), ":white_circle:"
    )
    check = alert_info.get("check_name", alert_info.get("monitor_url", "unknown"))
    fixed_lines = "\n".join(f"  :white_check_mark: {f}" for f in auto_fixed) or "  _none_"
    manual_lines = "\n".join(f"  :small_red_triangle: {a}" for a in analysis.get("manual_actions", [])) or "  _none — all clear_"

    text = (
        f"{emoji} *Insider Scanner — Auto-Diagnosis*\n"
        f"*Alert:* {check}\n"
        f"*Root cause:* {analysis.get('root_cause', 'unknown')}\n\n"
        f"{analysis.get('diagnosis_summary', '')}\n\n"
        f"*Auto-fixed:*\n{fixed_lines}\n\n"
        f"*Needs your attention:*\n{manual_lines}"
    )
    _post_to_slack(webhook_url, {"text": text})


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def _prices_weekend_gap(check_name: str) -> bool:
    """True during the Fri→Mon gap when insider-prices.timer (Mon-Fri 01:00 UTC) doesn't run.

    Covers Sat all day, Sun all day, and Mon before 02:00 UTC (giving the 01:00 run
    a one-hour window to complete and ping the heartbeat before we'd suppress anything).
    """
    if "price" not in check_name.lower():
        return False
    now = datetime.now(timezone.utc)
    wd = now.weekday()  # 0=Mon … 6=Sun
    return wd in (5, 6) or (wd == 0 and now.hour < 2)


def run_diagnostic(alert_info: dict) -> None:
    slack_url = os.getenv("SLACK_WEBHOOK_URL")
    check_name = alert_info.get("check_name", "")

    # Short-circuit known weekend false positive for the prices timer (Mon-Fri only).
    if _prices_weekend_gap(check_name):
        msg = (
            ":white_check_mark: *Insider Scanner — Expected Weekend Gap*\n"
            f"*Alert:* {check_name}\n"
            "insider-prices.timer runs Mon–Fri 01:00 UTC only. "
            "This staleness is expected over the weekend — no action needed."
        )
        if slack_url:
            try:
                _post_to_slack(slack_url, {"text": msg})
            except Exception as e:
                _LOG.error("Slack post failed: %s", e)
        else:
            print(msg)
        return

    diagnostics = collect_diagnostics()
    analysis = analyze(diagnostics, alert_info)
    auto_fixed = apply_fixes(analysis.get("safe_auto_fixes", []))

    if slack_url:
        try:
            post_slack(analysis, auto_fixed, alert_info, slack_url)
        except Exception as e:
            _LOG.error("Slack post failed: %s", e)
    else:
        print("No SLACK_WEBHOOK_URL — printing report:")
        print(json.dumps({"analysis": analysis, "auto_fixed": auto_fixed}, indent=2))


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()
    name = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "manual_trigger"
    run_diagnostic({"check_name": name, "source": "cli"})
