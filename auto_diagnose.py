"""
Autonomous diagnostic agent for Insider Tracker.

Called by the /webhook/alert endpoint when Healthchecks.io or BetterStack
fires an alert. Collects system state, asks Claude to analyze it, applies
safe fixes automatically, and posts a rich report to Slack.

Safe auto-fixes (no human approval needed):
  - service_restart: restart insider-tracker.service if it's dead/failed
  - cache_clear: touch the ingest sentinel to invalidate all in-process caches

Everything else (code changes, DB ops, config edits) goes into the Slack
report as "needs your attention" for a human to handle.

Run manually: python auto_diagnose.py [alert_name]
"""
from __future__ import annotations

import json
import os
import subprocess
import urllib.request
from datetime import datetime
from pathlib import Path

SENTINEL = Path(__file__).parent / "data" / ".last_ingest"
SERVICE = "insider-tracker.service"


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
        mtime = SENTINEL.stat().st_mtime
        return (datetime.now().timestamp() - mtime) / 3600
    except FileNotFoundError:
        return None


def _recent_run_log() -> str:
    try:
        from db import get_cli_db
        conn = get_cli_db()
        rows = conn.execute(
            """SELECT run_kind, date_processed, filings_found, errors,
                      LEFT(error_detail, 200) AS error_detail, started_at::text
               FROM run_log ORDER BY started_at DESC LIMIT 5"""
        ).fetchall()
        conn.close()
        return json.dumps([dict(r) for r in rows], default=str, indent=2)
    except Exception as e:
        return f"[DB unavailable: {e}]"


def collect_diagnostics() -> dict:
    db_url = os.getenv("DATABASE_URL", "")
    pg_cmd = f'psql "{db_url}" -c "SELECT state, count(*) FROM pg_stat_activity WHERE datname=\'insider_tracker\' GROUP BY state;" 2>&1' if db_url else "DATABASE_URL not set"

    age = _sentinel_age_hours()
    return {
        "timestamp_utc": datetime.utcnow().isoformat(),
        "service_status": _cmd(f"sudo systemctl status {SERVICE} --no-pager -l"),
        "recent_logs": _cmd(f"sudo journalctl -u {SERVICE} -n 60 --no-pager"),
        "pg_connections": _cmd(pg_cmd),
        "disk": _cmd("df -h / /home"),
        "memory": _cmd("free -h"),
        "sentinel_age_hours": age,
        "nightly_timer": _cmd("sudo systemctl status insider-ingest-nightly.timer --no-pager"),
        "prices_timer": _cmd("sudo systemctl status insider-prices.timer --no-pager"),
        "recent_run_log": _recent_run_log(),
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
            "diagnosis_summary": "Auto-diagnosis was triggered but ANTHROPIC_API_KEY is missing from the environment. Manual investigation required.",
        }

    import anthropic  # deferred — only installed on server
    client = anthropic.Anthropic(api_key=api_key)

    prompt = f"""You are an autonomous ops agent for the Insider Tracker application.
Stack: FastAPI + PostgreSQL 16 + uvicorn (2 workers) on a DigitalOcean droplet.
Key services: insider-tracker.service (uvicorn), insider-ingest-nightly.timer (03:00 UTC Mon-Sat), insider-prices.timer (01:00 UTC Mon-Fri).

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
    # Strip markdown fences if model wrapped it
    if raw.startswith("```"):
        raw = raw.split("```")[1]
        if raw.startswith("json"):
            raw = raw[4:]
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

def apply_fixes(fixes: list[str]) -> list[str]:
    done = []
    if "service_restart" in fixes:
        _cmd(f"sudo systemctl restart {SERVICE}")
        import time; time.sleep(3)
        status = _cmd(f"systemctl is-active {SERVICE}")
        done.append(f"Restarted {SERVICE} → now: {status}")
    if "cache_clear" in fixes:
        try:
            SENTINEL.touch()
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
        f"{emoji} *Insider Tracker — Auto-Diagnosis*\n"
        f"*Alert:* {check}\n"
        f"*Root cause:* {analysis.get('root_cause', 'unknown')}\n\n"
        f"{analysis.get('diagnosis_summary', '')}\n\n"
        f"*Auto-fixed:*\n{fixed_lines}\n\n"
        f"*Needs your attention:*\n{manual_lines}"
    )

    payload = json.dumps({"text": text}).encode()
    req = urllib.request.Request(webhook_url, data=payload,
                                 headers={"Content-Type": "application/json"})
    urllib.request.urlopen(req, timeout=10)


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def run_diagnostic(alert_info: dict) -> None:
    slack_url = os.getenv("SLACK_WEBHOOK_URL")

    diagnostics = collect_diagnostics()
    analysis = analyze(diagnostics, alert_info)
    auto_fixed = apply_fixes(analysis.get("safe_auto_fixes", []))

    if slack_url:
        try:
            post_slack(analysis, auto_fixed, alert_info, slack_url)
        except Exception as e:
            print(f"Slack post failed: {e}")
    else:
        print("No SLACK_WEBHOOK_URL — printing report:")
        print(json.dumps({"analysis": analysis, "auto_fixed": auto_fixed}, indent=2))


if __name__ == "__main__":
    import sys
    from dotenv import load_dotenv
    load_dotenv()
    name = " ".join(sys.argv[1:]) if len(sys.argv) > 1 else "manual_trigger"
    run_diagnostic({"check_name": name, "source": "cli"})
