"""Route-level tests for authentication, authorization, and CSRF.

Before this, the app had none of the three: the only mention of Basic Auth in
app.py was a docstring claiming a route was exempt from auth that did not exist,
and the real gate was an nginx block added by hand on the live server and absent
from the committed config.

The dashboard is also embedded in a cross-site iframe on vip.optionpit.com, so
subscribers arrive through /sso holding a wp_sso session and no Basic Auth
credentials. Authenticating them is not authorizing them: the mutating endpoints
require staff, so a subscriber gets a read-only view.

No live PostgreSQL: get_request_db is overridden with a MagicMock.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import app as app_module
import security
from db import get_request_db

USER = "tester"
PASSWORD = "s3cret"
KEY = "0" * 64


@pytest.fixture(autouse=True)
def auth_env(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH_USER", USER)
    monkeypatch.setenv("BASIC_AUTH_PASSWORD", PASSWORD)
    monkeypatch.setenv("SECRET_KEY", KEY)


@pytest.fixture(autouse=True)
def stub_db():
    app_module.app.dependency_overrides[get_request_db] = lambda: MagicMock()
    yield
    app_module.app.dependency_overrides.pop(get_request_db, None)


@pytest.fixture
def anon() -> TestClient:
    return TestClient(app_module.app)


@pytest.fixture
def client() -> TestClient:
    """Pre-authenticated as staff. TestClient is httpx-based, so auth= works."""
    c = TestClient(app_module.app)
    c.auth = (USER, PASSWORD)
    return c


@pytest.fixture
def csrf() -> dict:
    return {"X-CSRF-Token": security.make_csrf_token()}


class TestAuthentication:
    def test_dashboard_requires_credentials(self, anon):
        assert anon.get("/watchlist").status_code == 401

    def test_challenge_header_is_sent(self, anon):
        resp = anon.get("/watchlist")
        assert "basic" in resp.headers.get("www-authenticate", "").lower()

    def test_wrong_password_is_rejected(self):
        c = TestClient(app_module.app)
        c.auth = (USER, "wrong")
        assert c.get("/watchlist").status_code == 401

    def test_wrong_username_is_rejected(self):
        c = TestClient(app_module.app)
        c.auth = ("nobody", PASSWORD)
        assert c.get("/watchlist").status_code == 401

    def test_correct_credentials_pass_the_gate(self, client):
        assert client.get("/watchlist").status_code == 200

    def test_malformed_authorization_header_is_rejected(self, anon):
        resp = anon.get("/watchlist", headers={"Authorization": "Basic !!!not-base64"})
        assert resp.status_code == 401


class TestExemptPaths:
    @pytest.mark.parametrize("path", ["/healthz", "/robots.txt"])
    def test_exempt_paths_never_challenge(self, anon, path):
        assert anon.get(path).status_code != 401

    def test_healthz_works_without_credentials(self, anon):
        """The uptime monitor must keep working when auth is misconfigured."""
        body = anon.get("/healthz").json()
        assert body["ok"] is True

    def test_webhook_is_exempt_from_basic_auth(self, anon, monkeypatch):
        """It authenticates itself with WEBHOOK_SECRET instead."""
        monkeypatch.delenv("WEBHOOK_SECRET", raising=False)
        resp = anon.post("/webhook/alert", json={})
        # 503 (secret unconfigured), never 401: it got past the auth middleware.
        assert resp.status_code != 401

    def test_exempt_set_is_pinned(self):
        """Widening this has to be a deliberate test edit, and must be mirrored
        in schedule/nginx-insider-tracker.conf."""
        assert security.EXEMPT_PATHS == frozenset({
            "/healthz", "/robots.txt", "/webhook/alert",
            "/sso", "/internal/sso-authz",
        })

    def test_csrf_exempt_set_is_pinned(self):
        assert security.CSRF_EXEMPT_PATHS == frozenset({"/webhook/alert"})


class TestFailClosed:
    def test_unset_credentials_return_503_not_open_access(self, anon, monkeypatch):
        monkeypatch.delenv("BASIC_AUTH_USER", raising=False)
        monkeypatch.delenv("BASIC_AUTH_PASSWORD", raising=False)
        resp = anon.get("/watchlist")
        assert resp.status_code == 503, "must never serve unauthenticated"

    def test_503_names_the_missing_variables(self, anon, monkeypatch):
        monkeypatch.delenv("BASIC_AUTH_USER", raising=False)
        assert "BASIC_AUTH_USER" in anon.get("/watchlist").text

    def test_healthz_survives_missing_config(self, anon, monkeypatch):
        """Fail-closed must not blind the uptime monitor."""
        monkeypatch.delenv("BASIC_AUTH_USER", raising=False)
        monkeypatch.delenv("SECRET_KEY", raising=False)
        assert anon.get("/healthz").status_code == 200

    def test_missing_config_lists_every_unset_var(self, monkeypatch):
        monkeypatch.delenv("BASIC_AUTH_USER", raising=False)
        monkeypatch.delenv("BASIC_AUTH_PASSWORD", raising=False)
        monkeypatch.delenv("SECRET_KEY", raising=False)
        assert set(security.missing_config()) == {
            "BASIC_AUTH_USER", "BASIC_AUTH_PASSWORD", "SECRET_KEY",
        }


class TestCsrf:
    def test_post_without_a_token_is_rejected(self, client):
        resp = client.post("/watchlist/toggle", data={"ticker": "AAPL"})
        assert resp.status_code == 403

    def test_post_with_a_header_token_passes_csrf(self, client, csrf):
        """A garbage ticker returning 400 proves CSRF and auth both passed."""
        resp = client.post(
            "/performance/add",
            data={"ticker": "!!!", "signal_code": "gc", "trigger_date": "2026-07-24"},
            headers=csrf,
        )
        assert resp.status_code == 400

    def test_post_with_a_form_field_token_passes_csrf(self, client):
        resp = client.post(
            "/performance/add",
            data={
                "ticker": "!!!", "signal_code": "gc", "trigger_date": "2026-07-24",
                "csrf_token": security.make_csrf_token(),
            },
        )
        assert resp.status_code == 400, "form-field tokens must work, not just headers"

    def test_tampered_token_is_rejected(self, client):
        token = security.make_csrf_token()
        expiry, _, sig = token.partition(".")
        bad = f"{expiry}.{'f' * len(sig)}"
        resp = client.post(
            "/watchlist/toggle", data={"ticker": "AAPL"},
            headers={"X-CSRF-Token": bad},
        )
        assert resp.status_code == 403

    def test_get_requests_need_no_token(self, client):
        assert client.get("/watchlist").status_code == 200


class TestCsrfTokenUnit:
    def test_round_trip(self):
        assert security.verify_csrf_token(security.make_csrf_token()) is True

    def test_expired_token_fails(self):
        stale = security.make_csrf_token(now=0)
        assert security.verify_csrf_token(stale) is False

    def test_future_expiry_is_honoured(self):
        token = security.make_csrf_token()
        assert security.verify_csrf_token(token, now=0) is True

    @pytest.mark.parametrize("bad", [None, "", "nodot", "abc.def", "9999999999.x"])
    def test_malformed_tokens_fail(self, bad):
        assert security.verify_csrf_token(bad) is False

    def test_no_secret_key_mints_nothing(self, monkeypatch):
        monkeypatch.delenv("SECRET_KEY", raising=False)
        assert security.make_csrf_token() == ""
        assert security.verify_csrf_token("anything") is False

    def test_token_changes_with_the_key(self, monkeypatch):
        a = security.make_csrf_token(now=1000)
        monkeypatch.setenv("SECRET_KEY", "f" * 64)
        b = security.make_csrf_token(now=1000)
        assert a != b


class TestStaffAuthorization:
    """CLAUDE.md: SSO must not confer app-level authorization."""

    def _session(self, monkeypatch, staff: bool):
        monkeypatch.setattr(
            security, "sso_session",
            lambda _req: {"email": "sub@example.com", "staff": staff},
        )

    def test_subscriber_session_authenticates(self, anon, monkeypatch):
        self._session(monkeypatch, staff=False)
        assert anon.get("/watchlist").status_code == 200

    def test_subscriber_cannot_mutate(self, anon, monkeypatch, csrf):
        """Read-only: a valid CSRF token is not authorization."""
        self._session(monkeypatch, staff=False)
        resp = anon.post("/watchlist/toggle", data={"ticker": "AAPL"}, headers=csrf)
        assert resp.status_code == 403

    def test_staff_session_may_mutate(self, anon, monkeypatch):
        self._session(monkeypatch, staff=True)
        resp = anon.post(
            "/performance/add",
            data={"ticker": "!!!", "signal_code": "gc", "trigger_date": "2026-07-24"},
            headers={"X-CSRF-Token": security.make_csrf_token()},
        )
        assert resp.status_code == 400, "reached the handler, so it was authorized"

    def test_basic_auth_counts_as_staff(self, monkeypatch):
        req = MagicMock()
        req.headers = {"authorization": _basic(USER, PASSWORD)}
        monkeypatch.setattr(security, "sso_session", lambda _req: None)
        assert security.is_staff(req) is True

    def test_no_credentials_is_not_staff(self, monkeypatch):
        req = MagicMock()
        req.headers = {}
        monkeypatch.setattr(security, "sso_session", lambda _req: None)
        assert security.is_staff(req) is False


class TestSsoSoftDependency:
    def test_missing_module_yields_no_session(self, monkeypatch):
        """security.py must import and work in a checkout without wisepub_sso."""
        monkeypatch.setattr(security, "wisepub_sso", None)
        assert security.sso_session(MagicMock()) is None

    def test_missing_secret_yields_no_session(self, monkeypatch):
        monkeypatch.delenv("WISEPUB_SSO_SECRET", raising=False)
        assert security.sso_session(MagicMock()) is None


class TestTokenDelivery:
    def test_csrf_token_is_a_jinja_global(self):
        assert callable(app_module.templates.env.globals.get("csrf_token"))

    def test_rendered_page_carries_the_meta_tag(self, client):
        html = client.get("/watchlist").text
        assert 'name="csrf-token"' in html
        assert "htmx:configRequest" in html

    def test_rendered_forms_carry_a_hidden_token(self, client):
        html = client.get("/watchlist").text
        assert 'name="csrf_token"' in html


def _basic(user: str, password: str) -> str:
    import base64
    return "Basic " + base64.b64encode(f"{user}:{password}".encode()).decode()
