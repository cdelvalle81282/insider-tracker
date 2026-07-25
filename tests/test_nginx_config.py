"""Static checks on the committed nginx front door.

The committed config had no auth_basic at all, while the live server had it added
by hand. deploy.sh installs this file, so any rebuild or drift served the whole
dashboard and every mutating endpoint to the internet.

The exempt locations here must mirror security.EXEMPT_PATHS. That set has an
equality assert in test_security.py; this file is the other half of the pair.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import security

CONF = (Path(__file__).resolve().parent.parent
        / "schedule" / "nginx-insider-tracker.conf").read_text(encoding="utf-8")


def _location_blocks() -> dict[str, str]:
    """Map each `location <match>` to its block body (non-nested, which is all
    this config uses)."""
    blocks: dict[str, str] = {}
    for match in re.finditer(r"location\s+(=\s*)?(\S+)\s*\{", CONF):
        name = match.group(2)
        start = match.end()
        depth = 1
        i = start
        while i < len(CONF) and depth:
            if CONF[i] == "{":
                depth += 1
            elif CONF[i] == "}":
                depth -= 1
            i += 1
        blocks[name] = CONF[start:i]
    return blocks


BLOCKS = _location_blocks()


class TestClosedByDefault:
    def test_catch_all_requires_a_credential(self):
        body = BLOCKS["/"]
        assert "auth_basic" in body
        assert "auth_basic_user_file" in body

    def test_static_requires_a_credential(self):
        body = BLOCKS["/static/"]
        assert "auth_basic" in body
        assert "auth_basic_user_file" in body

    @pytest.mark.parametrize("path", ["/", "/static/"])
    def test_both_credentials_are_accepted(self, path):
        """satisfy any is what lets a subscriber in without Basic Auth."""
        body = BLOCKS[path]
        assert "satisfy any" in body
        assert "auth_request /internal/sso-authz" in body

    def test_htpasswd_path_is_consistent(self):
        """Count directives, not mentions: the header comment names it too."""
        directives = re.findall(r"^\s*auth_basic_user_file\s+(\S+?);", CONF, re.M)
        assert len(directives) == 2, "one per protected location"
        assert set(directives) == {"/etc/nginx/.htpasswd-insider"}, (
            "deploy.sh generates exactly this path"
        )


class TestExemptions:
    EXPECTED_EXEMPT = {"/healthz", "/robots.txt", "/webhook/alert", "/sso"}

    @pytest.mark.parametrize("path", sorted(EXPECTED_EXEMPT))
    def test_exempt_paths_disable_basic_auth(self, path):
        assert "auth_basic off" in BLOCKS[path]

    @pytest.mark.parametrize("path", sorted(EXPECTED_EXEMPT))
    def test_exempt_paths_are_exact_matches(self, path):
        """A prefix match would also expose anything sharing the prefix, e.g.
        /healthz-admin."""
        assert re.search(rf"location\s+=\s*{re.escape(path)}\s*\{{", CONF), (
            f"{path} must use an exact-match location"
        )

    def test_nginx_exemptions_match_the_application(self):
        """The two layers must agree, or a path is open in one and closed in the
        other. /internal/sso-authz is app-exempt but nginx-internal, so it is
        excluded from the comparison."""
        app_exempt = set(security.EXEMPT_PATHS) - {"/internal/sso-authz"}
        assert app_exempt == self.EXPECTED_EXEMPT

    def test_auth_request_target_is_internal(self):
        """Otherwise a client could call the authorization endpoint directly."""
        assert "internal;" in BLOCKS["/internal/sso-authz"]

    def test_auth_request_target_drops_the_body(self):
        body = BLOCKS["/internal/sso-authz"]
        assert "proxy_pass_request_body off" in body


class TestNoRegressions:
    def test_every_location_proxies_upstream(self):
        for name, body in BLOCKS.items():
            assert "proxy_pass" in body, f"location {name} does not proxy"

    def test_port_is_unchanged(self):
        assert CONF.count("127.0.0.1:8002") == len(BLOCKS)

    def test_gzip_and_static_caching_survived(self):
        assert "gzip on" in CONF
        assert "immutable" in BLOCKS["/static/"]

    def test_braces_balance(self):
        assert CONF.count("{") == CONF.count("}")
