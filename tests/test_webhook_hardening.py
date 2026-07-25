"""Tests for /webhook/alert replay and blast-radius guards.

The endpoint is authenticated by a static shared header and is deliberately
exempt from Basic Auth and rate limiting, because external monitors call it. The
providers send no timestamp and no nonce, so a captured request is replayable
indefinitely and a flapping check fires repeatedly. Each replay used to queue a
fresh Claude API call whose output could restart the service.

Signature-based replay protection is not available here, so the guards bound the
damage instead: a per-check cooldown, a single-flight lock, a body cap, and
truncation of the attacker-controlled payload before it reaches the prompt.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import app as app_module
import auto_diagnose
import cache as cache_module
import config as cfg
from db import get_request_db

SECRET = "webhook-secret-value"


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("WEBHOOK_SECRET", SECRET)
    monkeypatch.setenv("BASIC_AUTH_USER", "u")
    monkeypatch.setenv("BASIC_AUTH_PASSWORD", "p")
    monkeypatch.setenv("SECRET_KEY", "0" * 64)


@pytest.fixture(autouse=True)
def stub_db():
    app_module.app.dependency_overrides[get_request_db] = lambda: MagicMock()
    yield
    app_module.app.dependency_overrides.pop(get_request_db, None)


@pytest.fixture(autouse=True)
def no_real_diagnostics(monkeypatch):
    """Never actually shell out or call the Claude API from a test."""
    calls = []
    monkeypatch.setattr(app_module, "_run_diagnostic_bg", lambda info: calls.append(info))
    return calls


@pytest.fixture
def allow_cooldown(monkeypatch):
    monkeypatch.setattr(cache_module, "acquire_cooldown", lambda name, ttl: True)


@pytest.fixture
def client() -> TestClient:
    return TestClient(app_module.app)


HEADERS = {"X-Webhook-Secret": SECRET}


class TestSecretStillRequired:
    def test_wrong_secret_is_rejected(self, client, allow_cooldown):
        resp = client.post("/webhook/alert", json={}, headers={"X-Webhook-Secret": "no"})
        assert resp.status_code == 403

    def test_missing_secret_config_is_503(self, client, monkeypatch, allow_cooldown):
        monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
        assert client.post("/webhook/alert", json={}, headers=HEADERS).status_code == 503

    def test_route_stays_exempt_from_basic_auth(self, client, allow_cooldown):
        """No Authorization header at all, and it must still be reachable."""
        resp = client.post("/webhook/alert", json={"check_name": "x"}, headers=HEADERS)
        assert resp.status_code == 200


class TestBodyCap:
    def test_oversized_body_is_rejected(self, client, allow_cooldown):
        payload = "x" * (cfg.WEBHOOK_MAX_BODY_BYTES + 100)
        resp = client.post(
            "/webhook/alert", content=payload.encode(),
            headers={**HEADERS, "Content-Type": "application/json"},
        )
        assert resp.status_code == 413

    def test_oversized_body_queues_nothing(self, client, allow_cooldown, no_real_diagnostics):
        payload = "x" * (cfg.WEBHOOK_MAX_BODY_BYTES + 100)
        client.post(
            "/webhook/alert", content=payload.encode(),
            headers={**HEADERS, "Content-Type": "application/json"},
        )
        assert no_real_diagnostics == []

    def test_unparseable_body_is_tolerated(self, client, allow_cooldown):
        resp = client.post(
            "/webhook/alert", content=b"not json",
            headers={**HEADERS, "Content-Type": "application/json"},
        )
        assert resp.status_code == 200

    def test_non_object_json_is_tolerated(self, client, allow_cooldown):
        """A bare list would break body.get(...)."""
        resp = client.post("/webhook/alert", json=[1, 2, 3], headers=HEADERS)
        assert resp.status_code == 200


class TestCooldown:
    def test_suppressed_request_returns_200_not_429(self, client, monkeypatch, no_real_diagnostics):
        """A non-2xx would make the provider retry, defeating the cooldown."""
        monkeypatch.setattr(cache_module, "acquire_cooldown", lambda name, ttl: False)
        resp = client.post("/webhook/alert", json={"check_name": "ingest"}, headers=HEADERS)
        assert resp.status_code == 200
        assert resp.json()["suppressed"] == "cooldown"

    def test_suppressed_request_queues_no_diagnostic(self, client, monkeypatch, no_real_diagnostics):
        monkeypatch.setattr(cache_module, "acquire_cooldown", lambda name, ttl: False)
        client.post("/webhook/alert", json={"check_name": "ingest"}, headers=HEADERS)
        assert no_real_diagnostics == []

    def test_allowed_request_queues_one(self, client, allow_cooldown, no_real_diagnostics):
        client.post("/webhook/alert", json={"check_name": "ingest"}, headers=HEADERS)
        assert len(no_real_diagnostics) == 1

    def test_cooldown_is_keyed_per_check_name(self, client, monkeypatch, no_real_diagnostics):
        seen = []
        monkeypatch.setattr(
            cache_module, "acquire_cooldown",
            lambda name, ttl: seen.append((name, ttl)) or True,
        )
        client.post("/webhook/alert", json={"check_name": "nightly ingest"}, headers=HEADERS)
        assert seen[0][1] == cfg.WEBHOOK_DIAG_COOLDOWN_SECONDS
        assert "nightly" in seen[0][0]

    def test_check_name_is_sanitised_for_the_key(self, client, monkeypatch, no_real_diagnostics):
        """A hostile check_name must not shape the Redis key."""
        seen = []
        monkeypatch.setattr(
            cache_module, "acquire_cooldown",
            lambda name, ttl: seen.append(name) or True,
        )
        client.post(
            "/webhook/alert",
            json={"check_name": "a b/../*\n\r{weird}" + "z" * 300},
            headers=HEADERS,
        )
        key = seen[0]
        assert len(key) <= 120
        assert all(c.isalnum() or c in "_.:-" for c in key)


class TestSingleFlight:
    def test_concurrent_diagnostic_is_skipped(self, monkeypatch):
        """Two overlapping diagnostics could each decide to restart the service."""
        ran = []
        monkeypatch.setattr(
            auto_diagnose, "run_diagnostic", lambda info: ran.append(info)
        )
        app_module._diagnostic_lock.acquire()
        try:
            app_module._run_diagnostic_bg({"check_name": "x"})
            assert ran == [], "must not run while another diagnostic holds the lock"
        finally:
            app_module._diagnostic_lock.release()

    def test_lock_is_released_after_a_run(self, monkeypatch):
        monkeypatch.setattr(auto_diagnose, "run_diagnostic", lambda info: None)
        app_module._run_diagnostic_bg({"check_name": "x"})
        assert app_module._diagnostic_lock.acquire(blocking=False), "lock leaked"
        app_module._diagnostic_lock.release()

    def test_lock_is_released_even_when_the_diagnostic_raises(self, monkeypatch):
        def boom(info):
            raise RuntimeError("probe failed")

        monkeypatch.setattr(auto_diagnose, "run_diagnostic", boom)
        app_module._run_diagnostic_bg({"check_name": "x"})
        assert app_module._diagnostic_lock.acquire(blocking=False), "lock leaked on error"
        app_module._diagnostic_lock.release()


class TestPromptTruncation:
    def test_long_payload_is_truncated(self):
        info = {"check_name": "x", "payload": {"note": "A" * 50_000}}
        out = auto_diagnose._truncate_for_prompt(info)
        assert len(out) < 10_000
        assert "truncated" in out

    def test_short_payload_is_preserved(self):
        info = {"check_name": "x", "payload": {"note": "brief"}}
        out = auto_diagnose._truncate_for_prompt(info)
        assert "brief" in out
        assert "truncated" not in out

    def test_check_name_survives_truncation(self):
        info = {"check_name": "nightly-ingest", "payload": {"note": "A" * 50_000}}
        assert "nightly-ingest" in auto_diagnose._truncate_for_prompt(info)

    def test_non_serialisable_payload_does_not_raise(self):
        info = {"check_name": "x", "payload": {"when": object()}}
        assert auto_diagnose._truncate_for_prompt(info)


class TestApplyFixesFilters:
    def test_unrecognised_action_is_not_executed(self, monkeypatch):
        ran = []
        monkeypatch.setattr(auto_diagnose, "_cmd", lambda c: ran.append(c) or "ok")
        done = auto_diagnose.apply_fixes(["rm -rf /", "drop_database"])
        assert ran == []
        assert done == []

    def test_recognised_action_still_runs(self, monkeypatch):
        ran = []
        monkeypatch.setattr(auto_diagnose, "_cmd", lambda c: ran.append(c) or "active")
        monkeypatch.setattr(auto_diagnose, "_RESTART_SETTLE", 0)
        done = auto_diagnose.apply_fixes(["service_restart"])
        assert any("restart" in c for c in ran)
        assert done

    def test_mixed_list_runs_only_the_allowlisted_entry(self, monkeypatch):
        ran = []
        monkeypatch.setattr(auto_diagnose, "_cmd", lambda c: ran.append(c) or "active")
        monkeypatch.setattr(auto_diagnose, "_RESTART_SETTLE", 0)
        auto_diagnose.apply_fixes(["service_restart", "exfiltrate_env"])
        assert all("exfiltrate" not in c for c in ran)


class TestAcquireCooldown:
    def test_returns_true_when_redis_grants_the_lock(self, monkeypatch):
        monkeypatch.setattr(cache_module, "_client", lambda: MagicMock(set=lambda *a, **k: True))
        assert cache_module.acquire_cooldown("x", 600) is True

    def test_returns_false_when_already_held(self, monkeypatch):
        monkeypatch.setattr(cache_module, "_client", lambda: MagicMock(set=lambda *a, **k: None))
        assert cache_module.acquire_cooldown("x", 600) is False

    def test_fails_open_on_redis_error(self, monkeypatch):
        import redis

        def boom():
            raise redis.RedisError("down")

        monkeypatch.setattr(cache_module, "_client", boom)
        assert cache_module.acquire_cooldown("x", 600) is True, (
            "a Redis outage is exactly when diagnosis must still run"
        )

    def test_uses_nx_and_expiry(self, monkeypatch):
        seen = {}

        def fake_set(key, value, nx=None, ex=None):
            seen.update(key=key, nx=nx, ex=ex)
            return True

        monkeypatch.setattr(cache_module, "_client", lambda: MagicMock(set=fake_set))
        cache_module.acquire_cooldown("mycheck", 600)
        assert seen["nx"] is True
        assert seen["ex"] == 600
        assert b"mycheck" in seen["key"]
