"""Static checks on the deployment bootstrap.

A fresh rebuild could not work: boto3 was missing from requirements.txt so the
nightly backup could not run, .env.example omitted DATABASE_URL so the setup
path died at the backfill, deploy.sh never ran `alembic upgrade head` so the
schema was never created, and it installed 3 of the 12 units in schedule/ so a
rebuilt server silently had no backups, no price updates, no backtest and no
perf-profile refresh.

Testing that properly means rebuilding a server, so these assert the invariants
that made it broken instead.
"""
from __future__ import annotations

import re
import shutil
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent

ENV_PATTERN = re.compile(
    r"os\.(?:getenv\(|environ\.get\(|environ\[)\s*[\"']([A-Z_][A-Z0-9_]*)[\"']"
)
SKIP_DIRS = {".venv", "__pycache__", "tests", ".git", "migrations"}


def _python_sources() -> list[Path]:
    return [
        p for p in ROOT.rglob("*.py")
        if not any(part in SKIP_DIRS for part in p.relative_to(ROOT).parts)
    ]


def _env_vars_referenced() -> dict[str, set[str]]:
    found: dict[str, set[str]] = {}
    for path in _python_sources():
        text = path.read_text(encoding="utf-8", errors="replace")
        for match in ENV_PATTERN.finditer(text):
            found.setdefault(match.group(1), set()).add(path.name)
    return found


def _env_example_keys() -> set[str]:
    text = (ROOT / ".env.example").read_text(encoding="utf-8")
    return {
        line.split("=", 1)[0].strip()
        for line in text.splitlines()
        if "=" in line and not line.lstrip().startswith("#")
    }


class TestEnvExample:
    def test_every_env_var_the_code_reads_is_documented(self):
        referenced = _env_vars_referenced()
        missing = {k: sorted(v) for k, v in referenced.items() if k not in _env_example_keys()}
        assert not missing, f"env vars read by code but absent from .env.example: {missing}"

    @pytest.mark.parametrize("key", [
        "DATABASE_URL",       # absent before; setup died at the backfill without it
        "WEBHOOK_SECRET",     # absent before; /webhook/alert 503s without it
        "BACKUP_S3_ACCESS_KEY_ID",
        "BACKUP_S3_SECRET_ACCESS_KEY",
        "PRICES_HEARTBEAT_URL",
        "ANTHROPIC_API_KEY",
    ])
    def test_previously_missing_keys_are_present(self, key):
        assert key in _env_example_keys()

    def test_no_real_secret_values_committed(self):
        """The template must ship empty or placeholder values only."""
        text = (ROOT / ".env.example").read_text(encoding="utf-8")
        for line in text.splitlines():
            if line.lstrip().startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip().endswith(("_KEY", "_SECRET", "_PASSWORD", "SECRET_KEY")):
                assert value.strip() == "", f"{key} ships a non-empty value"


class TestRequirements:
    def _requirements(self) -> set[str]:
        text = (ROOT / "requirements.txt").read_text(encoding="utf-8")
        names = set()
        for line in text.splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            names.add(re.split(r"[<>=\[]", line, 1)[0].strip().lower())
        return names

    def test_boto3_is_declared(self):
        """scripts/backup_postgres.py imports it at module level."""
        assert "boto3" in self._requirements()

    def test_scripts_third_party_imports_are_declared(self):
        """Anything under scripts/ runs on the server from the same venv."""
        declared = self._requirements()
        stdlib_ok = {
            "os", "sys", "re", "json", "subprocess", "tempfile", "datetime",
            "pathlib", "urllib", "argparse", "logging", "time", "hashlib",
            "contextlib", "dataclasses", "typing", "collections", "csv", "io",
            "statistics", "math", "itertools", "functools", "shutil", "hmac",
            "secrets", "base64", "textwrap", "random", "string", "sqlite3",
            "__future__",
        }
        local = {p.stem for p in ROOT.glob("*.py")}
        pattern = re.compile(r"^\s*(?:import|from)\s+([a-zA-Z0-9_]+)", re.MULTILINE)
        undeclared = set()
        for path in (ROOT / "scripts").glob("*.py"):
            for match in pattern.finditer(path.read_text(encoding="utf-8")):
                mod = match.group(1).lower()
                if mod in stdlib_ok or mod in local or mod in declared:
                    continue
                undeclared.add(f"{path.name}:{mod}")
        assert not undeclared, f"undeclared third-party imports: {sorted(undeclared)}"


class TestDeployScript:
    @pytest.fixture(scope="class")
    def deploy_text(self) -> str:
        return (ROOT / "deploy.sh").read_text(encoding="utf-8")

    def test_runs_migrations(self, deploy_text):
        """Absent entirely before, on both paths."""
        assert deploy_text.count("alembic upgrade head") >= 2, (
            "both the --setup and the normal deploy path must migrate"
        )

    def test_migrates_before_ingesting(self, deploy_text):
        """A backfill against a schemaless database is a confusing failure."""
        assert deploy_text.index("alembic upgrade head") < deploy_text.index("--backfill-days")

    def test_gates_on_database_url(self, deploy_text):
        assert "DATABASE_URL=.+" in deploy_text

    def test_exports_env_before_using_it(self, deploy_text):
        """ingest.py and migrations/env.py do not call load_dotenv."""
        assert "set -a" in deploy_text

    def test_installs_every_systemd_unit(self, deploy_text):
        """It installed 3 of 12, so backups and price updates never ran."""
        units = sorted(
            p.name for p in (ROOT / "schedule").iterdir()
            if p.suffix in {".service", ".timer"}
        )
        assert len(units) >= 10, "sanity: schedule/ should hold many units"
        assert "schedule/*.service" in deploy_text and "schedule/*.timer" in deploy_text, (
            f"deploy.sh must install all of: {units}"
        )

    def test_generates_the_nginx_htpasswd(self, deploy_text):
        assert "htpasswd-insider" in deploy_text
        assert "openssl passwd" in deploy_text

    @pytest.mark.skipif(shutil.which("bash") is None, reason="bash not available")
    def test_script_parses(self):
        result = subprocess.run(
            ["bash", "-n", str(ROOT / "deploy.sh")],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, result.stderr
