"""Tests for security response headers and the self-hosted front-end assets.

The app previously sent no security headers at all, and loaded three scripts from
two third-party CDNs, one of them (bare cdn.tailwindcss.com) unpinned so a
Tailwind release could silently restyle the whole dashboard.

frame-ancestors is the subtle one: the dashboard is deliberately embedded in an
iframe on vip.optionpit.com, so 'none' or X-Frame-Options: DENY would break the
product. That is a real risk when adding headers as a "hardening win".
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import app as app_module
import security
from db import get_request_db

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(autouse=True)
def env(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH_USER", "u")
    monkeypatch.setenv("BASIC_AUTH_PASSWORD", "p")
    monkeypatch.setenv("SECRET_KEY", "0" * 64)


@pytest.fixture(autouse=True)
def stub_db():
    app_module.app.dependency_overrides[get_request_db] = lambda: MagicMock()
    yield
    app_module.app.dependency_overrides.pop(get_request_db, None)


@pytest.fixture
def client() -> TestClient:
    c = TestClient(app_module.app)
    c.auth = ("u", "p")
    return c


class TestHeadersPresent:
    @pytest.mark.parametrize("header", [
        "content-security-policy",
        "x-content-type-options",
        "referrer-policy",
        "permissions-policy",
    ])
    def test_header_is_sent(self, client, header):
        assert header in client.get("/watchlist").headers

    def test_nosniff_value(self, client):
        assert client.get("/watchlist").headers["x-content-type-options"] == "nosniff"

    def test_headers_reach_exempt_endpoints(self, client):
        assert "content-security-policy" in client.get("/healthz").headers

    def test_headers_reach_401_responses(self):
        """The middleware is outermost so auth failures are covered too."""
        anon = TestClient(app_module.app)
        resp = anon.get("/watchlist")
        assert resp.status_code == 401
        assert "content-security-policy" in resp.headers

    def test_headers_reach_503_responses(self, monkeypatch):
        monkeypatch.delenv("BASIC_AUTH_USER", raising=False)
        anon = TestClient(app_module.app)
        resp = anon.get("/watchlist")
        assert resp.status_code == 503
        assert "content-security-policy" in resp.headers


class TestFraming:
    def test_frame_ancestors_allows_the_vip_embed(self, client):
        csp = client.get("/watchlist").headers["content-security-policy"]
        assert "frame-ancestors 'self' https://vip.optionpit.com" in csp

    def test_frame_ancestors_is_not_none(self, client):
        """'none' would break the whole reason the SSO work exists."""
        csp = client.get("/watchlist").headers["content-security-policy"]
        assert "frame-ancestors 'none'" not in csp

    def test_frame_ancestors_matches_the_shared_nginx_snippet(self):
        """nginx's snippets/frame-vip.conf sends its own CSP with only
        frame-ancestors. Two CSP headers are enforced as an intersection, so a
        narrower value here would silently tighten framing past the house policy
        that every app on the droplet shares."""
        assert security.FRAME_ANCESTORS == "'self' https://vip.optionpit.com"

    def test_x_frame_options_is_not_sent(self, client):
        """It has no multi-origin form, and DENY would override frame-ancestors."""
        assert "x-frame-options" not in client.get("/watchlist").headers


class TestHsts:
    def test_absent_on_plain_http(self, client):
        assert "strict-transport-security" not in client.get("/watchlist").headers

    def test_present_when_forwarded_proto_is_https(self, client):
        resp = client.get("/watchlist", headers={"X-Forwarded-Proto": "https"})
        assert "strict-transport-security" in resp.headers

    def test_handles_a_proto_list(self, client):
        resp = client.get("/watchlist", headers={"X-Forwarded-Proto": "https, http"})
        assert "strict-transport-security" in resp.headers


class TestCspContents:
    @pytest.mark.parametrize("directive", [
        "default-src 'self'",
        "object-src 'none'",
        "base-uri 'self'",
        "form-action 'self'",
        "connect-src 'self'",
    ])
    def test_directive_present(self, directive):
        assert directive in security.CONTENT_SECURITY_POLICY

    def test_allows_the_pinned_tailwind_host(self):
        assert "https://cdn.tailwindcss.com" in security.CONTENT_SECURITY_POLICY

    def test_does_not_allow_unpkg(self):
        """Both unpkg scripts are self-hosted now, so the host is not needed."""
        assert "unpkg.com" not in security.CONTENT_SECURITY_POLICY

    def test_allows_google_fonts(self):
        csp = security.CONTENT_SECURITY_POLICY
        assert "https://fonts.googleapis.com" in csp
        assert "https://fonts.gstatic.com" in csp


class TestVendoredAssets:
    VENDOR = ROOT / "static" / "vendor"

    @pytest.mark.parametrize("name,marker", [
        ("htmx-1.9.12.min.js", "htmx"),
        ("lightweight-charts-4.2.0.standalone.production.js", "LightweightCharts"),
    ])
    def test_asset_exists_and_looks_right(self, name, marker):
        path = self.VENDOR / name
        assert path.exists(), f"{name} must be committed, not fetched at runtime"
        text = path.read_text(encoding="utf-8", errors="replace")
        assert len(text) > 10_000
        assert marker in text

    def test_templates_reference_the_local_copies(self):
        base = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
        chart = (ROOT / "templates" / "chart.html").read_text(encoding="utf-8")
        assert "/static/vendor/htmx-1.9.12.min.js" in base
        assert "/static/vendor/lightweight-charts-4.2.0" in chart

    def test_no_template_still_loads_scripts_from_unpkg(self):
        """Match actual src/href attributes, not prose: the comments explaining
        the change legitimately mention the host."""
        import re

        pattern = re.compile(r"""(?:src|href)\s*=\s*["'][^"']*unpkg\.com""", re.I)
        for path in (ROOT / "templates").glob("*.html"):
            text = path.read_text(encoding="utf-8")
            assert not pattern.search(text), f"{path.name} still loads from unpkg"

    def test_tailwind_cdn_is_version_pinned(self):
        """The bare URL floats to the next major release."""
        base = (ROOT / "templates" / "base.html").read_text(encoding="utf-8")
        assert 'src="https://cdn.tailwindcss.com/3.4.16"' in base
        assert 'src="https://cdn.tailwindcss.com"' not in base
