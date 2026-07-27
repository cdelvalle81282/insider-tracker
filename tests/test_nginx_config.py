"""Static checks on the committed nginx front door.

Two separate problems this guards against.

First, the config originally committed here carried no auth_basic at all, while
the live server had it added by hand. deploy.sh installs this file, so a rebuild
or any drift would have served the whole dashboard to the internet.

Second, the replacement written during the security pass was itself a regression
against production: no TLS, no frame snippet, no AI-crawler block, no staff-only
rule for the mutating endpoints, and a per-app htpasswd that is not the house
convention. This file was reconciled against the live server on 2026-07-26; these
tests pin the parts that matter so it cannot silently drift again.

Note this is the PRE-certbot template. certbot adds `listen 443 ssl`, the
certificate paths and the :80 redirect block on first run, so their absence here
is correct and deliberate.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

import security

CONF = (Path(__file__).resolve().parent.parent
        / "schedule" / "nginx-insider-tracker.conf").read_text(encoding="utf-8")

# Matches `location <modifier?> <pattern> {`, where the modifier is one of
# = ~ ~* ^~ or absent. The earlier version of this helper used a single \S+ and
# so captured "~" as the location name for regex blocks, silently skipping them.
_LOCATION_RE = re.compile(r"location\s+(?:(=|~\*|~|\^~)\s*)?(\S+)\s*\{")


def _location_blocks() -> dict[str, str]:
    blocks: dict[str, str] = {}
    for match in _LOCATION_RE.finditer(CONF):
        name = match.group(2)
        depth, i = 1, match.end()
        while i < len(CONF) and depth:
            if CONF[i] == "{":
                depth += 1
            elif CONF[i] == "}":
                depth -= 1
            i += 1
        blocks[name] = CONF[match.end():i]
    return blocks


BLOCKS = _location_blocks()
REGEX_LOCATION = next(n for n in BLOCKS if n.startswith("^/(logic"))

# Exempt in nginx. /internal/sso-authz is app-exempt too but `internal` here, so
# a client can never reach it; it is excluded from the comparison below.
EXPECTED_EXEMPT = {"/healthz", "/robots.txt", "/webhook/alert", "/sso"}

# Writes nginx itself must keep away from a subscriber session. Editing
# thresholds or firing a test alert is editorial-only under any identity.
STAFF_ONLY_ROUTES = [
    "/logic/save", "/logic/test-alert",
    "/performance/add", "/performance/remove",
]

# Writes a subscriber legitimately performs on their own rows, so nginx must NOT
# gate them behind Basic Auth. They were in the staff-only regex until
# 2026-07-27, when per-subscriber watchlists landed and enforcement moved into
# the app (security.SUBSCRIBER_WRITABLE_PATHS plus an owner predicate on every
# statement). Leaving them here would make the feature fail in production while
# passing every test, since the app never sees the request.
SUBSCRIBER_WRITABLE_ROUTES = [
    "/watchlist/add", "/watchlist/remove", "/watchlist/toggle",
]


class TestClosedByDefault:
    @pytest.mark.parametrize("path", ["/", "/static/"])
    def test_catch_all_locations_require_a_credential(self, path):
        body = BLOCKS[path]
        assert "auth_basic" in body
        assert "auth_basic_user_file" in body

    @pytest.mark.parametrize("path", ["/", "/static/"])
    def test_both_credentials_are_accepted(self, path):
        """satisfy any is what lets a subscriber in without Basic Auth."""
        body = BLOCKS[path]
        assert "satisfy any" in body
        assert "auth_request /internal/sso-authz" in body

    def test_htpasswd_is_the_shared_house_file(self):
        """Count directives, not mentions: the header comment names it too.

        /etc/nginx/.htpasswd is shared by every app on the droplet, per
        _shared/deploy.md, which is also why deploy.sh must never overwrite it.
        """
        directives = re.findall(r"^\s*auth_basic_user_file\s+(\S+?);", CONF, re.M)
        assert len(directives) == 3, "/, /static/, and the staff-only regex"
        assert set(directives) == {"/etc/nginx/.htpasswd"}


class TestStaffOnlyWrites:
    def test_mutating_endpoints_do_not_accept_an_sso_session(self):
        """Without this, a paying subscriber could edit thresholds or fire test
        alerts. The application enforces the same rule independently in
        security.verify_mutation."""
        body = BLOCKS[REGEX_LOCATION]
        assert "auth_basic" in body
        assert "satisfy any" not in body
        assert "auth_request" not in body

    @pytest.mark.parametrize("path", STAFF_ONLY_ROUTES)
    def test_every_staff_only_route_matches_the_regex(self, path):
        assert re.match(REGEX_LOCATION + r"\Z", path), (
            f"{path} is not covered by the staff-only nginx location"
        )

    @pytest.mark.parametrize("path", SUBSCRIBER_WRITABLE_ROUTES)
    def test_subscriber_writable_routes_are_not_gated_by_nginx(self, path):
        """These must reach the app, which scopes the write to rows the caller
        owns. Putting them back in the regex would break subscriber watchlists
        in production while every application-level test still passed."""
        assert not re.match(REGEX_LOCATION + r"\Z", path), (
            f"{path} is gated by nginx, so a subscriber can never reach it"
        )

    def test_the_two_sets_agree_with_the_application(self):
        """The nginx regex and security.SUBSCRIBER_WRITABLE_PATHS describe the
        same boundary from two sides, so they must not drift apart."""
        import security
        assert set(SUBSCRIBER_WRITABLE_ROUTES) == set(security.SUBSCRIBER_WRITABLE_PATHS)
        assert not (set(STAFF_ONLY_ROUTES) & set(security.SUBSCRIBER_WRITABLE_PATHS))

    @pytest.mark.parametrize("path", ["/", "/watchlist", "/logic", "/performance"])
    def test_read_only_paths_are_not_caught_by_the_regex(self, path):
        """The regex must not accidentally lock the viewing pages to staff."""
        assert not re.match(REGEX_LOCATION + r"\Z", path)


class TestExemptions:
    @pytest.mark.parametrize("path", sorted(EXPECTED_EXEMPT))
    def test_exempt_paths_carry_no_auth_directive(self, path):
        """auth_basic is not set at server level, so a location that simply omits
        it is open. That is how the live config does it; an explicit
        `auth_basic off` would be equivalent but is not what production runs."""
        body = BLOCKS[path]
        assert "auth_basic" not in body
        assert "satisfy" not in body
        assert "auth_request" not in body

    @pytest.mark.parametrize("path", sorted(EXPECTED_EXEMPT))
    def test_exempt_paths_are_exact_matches(self, path):
        """A prefix match would also expose anything sharing the prefix, e.g.
        /healthz-admin."""
        assert re.search(rf"location\s+=\s*{re.escape(path)}\s*\{{", CONF), (
            f"{path} must use an exact-match location"
        )

    def test_nginx_exemptions_match_the_application(self):
        """A path exempt in one layer and protected in the other is either a
        surprise 401 or a hole."""
        app_exempt = set(security.EXEMPT_PATHS) - {"/internal/sso-authz"}
        assert app_exempt == EXPECTED_EXEMPT

    def test_auth_request_target_is_internal(self):
        """Otherwise a client could call the authorization endpoint directly."""
        assert "internal;" in BLOCKS["/internal/sso-authz"]

    def test_auth_request_target_drops_the_body(self):
        assert "proxy_pass_request_body off" in BLOCKS["/internal/sso-authz"]


class TestLiveParity:
    """Things the live server has that the committed file must not lose again."""

    def test_framing_is_delegated_to_the_shared_snippet(self):
        assert "include snippets/frame-vip.conf;" in CONF

    @pytest.mark.parametrize("path", ["/", "/static/"])
    def test_ai_crawlers_are_blocked(self, path):
        assert "GPTBot" in BLOCKS[path]
        assert "return 403" in BLOCKS[path]

    def test_robots_is_served_from_disk(self):
        """So crawler rules survive an application outage."""
        body = BLOCKS["/robots.txt"]
        assert "alias /var/www/insider/robots.txt" in body
        assert "proxy_pass" not in body

    def test_subrequest_passes_the_original_uri(self):
        assert "X-Original-URI" in BLOCKS["/internal/sso-authz"]


class TestNoRegressions:
    def test_every_proxying_location_targets_the_right_port(self):
        for name, body in BLOCKS.items():
            if "proxy_pass" in body:
                assert "127.0.0.1:8002" in body, f"location {name} proxies elsewhere"

    def test_only_robots_avoids_the_proxy(self):
        non_proxying = {n for n, b in BLOCKS.items() if "proxy_pass" not in b}
        assert non_proxying == {"/robots.txt"}

    def test_gzip_and_static_caching_survived(self):
        assert "gzip on" in CONF
        assert "immutable" in BLOCKS["/static/"]

    def test_is_the_pre_certbot_template(self):
        """certbot adds TLS on first run; hand-adding it here would duplicate.

        Matches directives at the start of a line, not prose: the header comment
        legitimately explains what certbot will add.
        """
        assert re.search(r"^\s*listen\s+80;", CONF, re.M)
        assert not re.search(r"^\s*listen\s+443", CONF, re.M)
        assert not re.search(r"^\s*ssl_certificate", CONF, re.M)

    def test_braces_balance(self):
        assert CONF.count("{") == CONF.count("}")
