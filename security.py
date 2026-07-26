"""Application-level authentication, authorization, and CSRF.

Before this module the app had none of the three. The only mention of Basic Auth
in app.py was a docstring claiming one route was "exempt" from auth that did not
exist; the real gate was an nginx `auth_basic` block added by hand on the live
server, absent from the committed config, so any rebuild or config drift exposed
the whole dashboard plus every mutating endpoint.

Two principals reach the app:

  staff       HTTP Basic Auth, or a Wisepub SSO session with staff=True.
  subscriber  A valid Wisepub SSO session with staff=False. Read-only.

That split matters because the dashboard is embedded in a cross-site iframe on
vip.optionpit.com and subscribers arrive through /sso with no Basic Auth
credentials at all. Authenticating them is not the same as authorizing them:
CLAUDE.md is explicit that SSO must not confer app-level authorization, so the
mutating endpoints require staff specifically.

Design notes worth keeping:

  * Both middlewares are pure ASGI classes, not BaseHTTPMiddleware. /export.csv
    streams its response and slowapi installs its own exception handler;
    BaseHTTPMiddleware re-wraps responses and is a well-known source of bugs
    with both.
  * Missing credentials fail closed with a per-request 503 rather than raising at
    import time. Crashing at startup would make insider-tracker.service flap and
    fight auto_diagnose's restart action, and it would take /healthz down with
    it, blinding the uptime monitor at the worst moment.
  * The CSRF token is a site-wide HMAC, not bound to the principal. It only has
    to be unguessable by a third-party site, and an attacker cannot read our HTML
    cross-origin. A subscriber CAN read a valid token, which is exactly why
    authorization is enforced separately rather than being implied by CSRF.
"""
from __future__ import annotations

import base64
import binascii
import hashlib
import hmac
import logging
import os
import time

from fastapi import HTTPException, Request

# Soft dependency. The Wisepub SSO module may not be present in every checkout,
# and this module must import cleanly without it: Basic Auth alone is then the
# only way in, which is the correct degraded behaviour rather than a crash.
try:
    import wisepub_sso
except ImportError:  # pragma: no cover - depends on checkout contents
    wisepub_sso = None

_LOG = logging.getLogger("security")

# Paths that bypass authentication entirely. Exact matches only: a prefix rule
# here would be a hole (/healthz-admin would sail through a startswith check).
#
# Kept in lockstep with the `auth_basic off` locations in
# schedule/nginx-insider-tracker.conf. If you add one here, add it there.
EXEMPT_PATHS = frozenset({
    "/healthz",            # uptime monitor; must stay reachable when auth is broken
    "/robots.txt",
    "/webhook/alert",      # authenticated by its own WEBHOOK_SECRET header
    "/sso",                # authenticates itself with a signed handover token
    "/internal/sso-authz", # nginx auth_request target; called before any credential
})

# Mutating requests that skip the CSRF and staff checks. /webhook/alert is called
# by Healthchecks/BetterStack, which cannot carry a CSRF token.
CSRF_EXEMPT_PATHS = frozenset({"/webhook/alert"})

SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS", "TRACE"})

# 24h: the dashboard is a tab that sits open all day, and a shorter TTL would
# turn a stale tab's first action into a confusing 403.
CSRF_TTL_SECONDS = 24 * 3600

_CSRF_FORM_FIELD = "csrf_token"
_CSRF_HEADER = "x-csrf-token"


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

def basic_auth_credentials() -> tuple[str, str]:
    """Read credentials per request so tests can monkeypatch and so rotating
    them needs only a service restart, not a code change."""
    return os.getenv("BASIC_AUTH_USER", ""), os.getenv("BASIC_AUTH_PASSWORD", "")


def signing_key() -> str:
    return os.getenv("SECRET_KEY", "")


def missing_config() -> list[str]:
    """Names of the env vars required to serve authenticated traffic."""
    user, password = basic_auth_credentials()
    missing = []
    if not user:
        missing.append("BASIC_AUTH_USER")
    if not password:
        missing.append("BASIC_AUTH_PASSWORD")
    if not signing_key():
        missing.append("SECRET_KEY")
    return missing


# ---------------------------------------------------------------------------
# Basic Auth
# ---------------------------------------------------------------------------

def check_basic_auth(header_value: str | None) -> bool:
    """Validate an Authorization header against the configured credentials."""
    user, password = basic_auth_credentials()
    if not user or not password:
        return False
    if not header_value or not header_value.lower().startswith("basic "):
        return False
    try:
        decoded = base64.b64decode(header_value.split(" ", 1)[1], validate=True)
        supplied_user, _, supplied_password = decoded.decode("utf-8").partition(":")
    except (binascii.Error, UnicodeDecodeError, IndexError):
        return False
    # Compare both halves unconditionally: short-circuiting on a wrong username
    # would leak which half was wrong through timing.
    user_ok = hmac.compare_digest(supplied_user, user)
    password_ok = hmac.compare_digest(supplied_password, password)
    return user_ok and password_ok


# ---------------------------------------------------------------------------
# Identity
# ---------------------------------------------------------------------------

def sso_session(request_or_scope) -> dict | None:
    """The Wisepub identity carried by this request, or None.

    Accepts a Starlette Request or a raw ASGI scope so both the middleware and
    the route dependencies can use it.
    """
    if wisepub_sso is None:
        return None
    secret = os.getenv("WISEPUB_SSO_SECRET", "")
    if not secret:
        return None
    cookie = _cookie_from(request_or_scope, wisepub_sso.SESSION_COOKIE)
    if not cookie:
        return None
    try:
        return wisepub_sso.read_session(cookie, secret)
    except Exception:  # pragma: no cover - read_session is defensive already
        return None


def _cookie_from(request_or_scope, name: str) -> str | None:
    if isinstance(request_or_scope, Request):
        return request_or_scope.cookies.get(name)
    for key, value in request_or_scope.get("headers") or []:
        if key == b"cookie":
            for part in value.decode("latin-1").split(";"):
                cookie_name, _, cookie_value = part.strip().partition("=")
                if cookie_name == name:
                    return cookie_value
    return None


def is_staff(request: Request) -> bool:
    """True when this request may mutate state.

    Basic Auth is the staff credential. An SSO session only counts as staff when
    the token said so, so a paying subscriber viewing the embedded dashboard
    cannot edit the config or the watchlist.
    """
    if check_basic_auth(request.headers.get("authorization")):
        return True
    session = sso_session(request)
    return bool(session and session.get("staff"))


# ---------------------------------------------------------------------------
# CSRF
# ---------------------------------------------------------------------------

def make_csrf_token(now: float | None = None) -> str:
    """Mint a token of the form "<expiry>.<hmac>".

    Registered as a Jinja global so templates can call it with no per-route
    context plumbing. Returns "" when SECRET_KEY is unset: the auth middleware
    is already returning 503 in that case, so there is nothing to protect.
    """
    key = signing_key()
    if not key:
        return ""
    expiry = int((time.time() if now is None else now) + CSRF_TTL_SECONDS)
    return f"{expiry}.{_csrf_signature(key, expiry)}"


def _csrf_signature(key: str, expiry: int) -> str:
    return hmac.new(key.encode(), str(expiry).encode(), hashlib.sha256).hexdigest()


def verify_csrf_token(token: str | None, now: float | None = None) -> bool:
    key = signing_key()
    if not key or not token or "." not in token:
        return False
    expiry_raw, _, signature = token.partition(".")
    try:
        expiry = int(expiry_raw)
    except ValueError:
        return False
    if expiry < (time.time() if now is None else now):
        return False
    return hmac.compare_digest(signature, _csrf_signature(key, expiry))


async def verify_mutation(request: Request) -> None:
    """App-level dependency: CSRF plus staff authorization on mutating requests.

    Registered on the FastAPI app itself rather than per route, so a POST added
    later is covered without anyone remembering to decorate it.

    Two implementation details that are easy to get wrong:
      * `await request.form()` is safe here even though the endpoint also
        declares Form(...) params, because Starlette memoizes the parsed form on
        the request. Reading the body in pure ASGI middleware instead would
        consume the stream and hang the endpoint.
      * App-level dependencies resolve before per-route ones, so a rejected
        request never reaches Depends(get_request_db) and never checks out a
        database connection.
    """
    if request.method in SAFE_METHODS:
        return
    if request.url.path in CSRF_EXEMPT_PATHS:
        return

    token = request.headers.get(_CSRF_HEADER)
    if not token:
        try:
            form = await request.form()
            token = form.get(_CSRF_FORM_FIELD)
        except Exception:
            token = None

    if not verify_csrf_token(token):
        _LOG.warning("CSRF rejection on %s %s", request.method, request.url.path)
        raise HTTPException(status_code=403, detail="Invalid or missing CSRF token")

    if not is_staff(request):
        # A subscriber can legitimately hold a valid CSRF token, since they can
        # load the page. Authorization is therefore a separate check, not
        # something CSRF validity implies.
        _LOG.warning("Non-staff mutation attempt on %s", request.url.path)
        raise HTTPException(status_code=403, detail="Staff credentials required")


# ---------------------------------------------------------------------------
# Middleware
# ---------------------------------------------------------------------------

async def _send_plain(send, status: int, body: bytes, extra_headers=()) -> None:
    headers = [
        (b"content-type", b"text/plain; charset=utf-8"),
        (b"content-length", str(len(body)).encode()),
    ]
    headers.extend(extra_headers)
    await send({"type": "http.response.start", "status": status, "headers": headers})
    await send({"type": "http.response.body", "body": body})


# Origins allowed to frame the dashboard. The whole point of the Wisepub SSO work
# is the embed on the paid site, so 'none' would break the product. X-Frame-Options
# is deliberately NOT sent: it has no multi-origin form, and DENY would override
# this in browsers that support both.
#
# This value must match nginx's snippets/frame-vip.conf exactly. That snippet
# emits its own Content-Security-Policy containing only frame-ancestors, and when
# a response carries two CSP headers the browser enforces BOTH, so the effective
# policy is their intersection. Listing a narrower set here (dropping 'self',
# say) would silently tighten framing beyond what the shared house policy
# intends. See _shared/deploy.md.
FRAME_ANCESTORS = "'self' https://vip.optionpit.com"

CONTENT_SECURITY_POLICY = "; ".join([
    "default-src 'self'",
    # 'unsafe-inline' and 'unsafe-eval' are both required today: the templates are
    # full of inline onclick/onmouseover handlers that a nonce cannot cover, and
    # the Tailwind Play CDN is a runtime compiler that evals. Removing either
    # means a Tailwind build step plus a handler refactor.
    "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.tailwindcss.com",
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com",
    "font-src 'self' https://fonts.gstatic.com",
    "img-src 'self' data:",
    "connect-src 'self'",
    f"frame-ancestors {FRAME_ANCESTORS}",
    "base-uri 'self'",
    "form-action 'self'",
    "object-src 'none'",
])


class SecurityHeadersMiddleware:
    """Attach response headers to every response, including error responses.

    Added outermost so 401 and 503 bodies from AuthMiddleware carry them too.

    Even with the unsafe-inline/unsafe-eval concessions this is worth having: it
    still blocks script and connect to foreign origins, restricts form targets,
    pins the frame parent to the one site that is meant to embed us, and kills
    plugin content. The app previously sent no security headers whatsoever.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        # HSTS only when the request actually arrived over TLS. Sending it on
        # plain HTTP is meaningless, and during the pre-certbot window in
        # deploy.sh it would pin a host to HTTPS that cannot serve it yet.
        is_https = False
        for key, value in scope.get("headers") or []:
            if key == b"x-forwarded-proto":
                is_https = value.decode("latin-1").split(",")[0].strip() == "https"
                break

        async def send_with_headers(message):
            if message["type"] == "http.response.start":
                headers = message.setdefault("headers", [])
                existing = {k.lower() for k, _ in headers}
                additions = [
                    (b"content-security-policy", CONTENT_SECURITY_POLICY.encode()),
                    (b"x-content-type-options", b"nosniff"),
                    (b"referrer-policy", b"same-origin"),
                    (b"permissions-policy", b"geolocation=(), microphone=(), camera=()"),
                ]
                if is_https:
                    additions.append(
                        (b"strict-transport-security", b"max-age=31536000; includeSubDomains")
                    )
                for name, value in additions:
                    # Never clobber a header a route set deliberately.
                    if name not in existing:
                        headers.append((name, value))
            await send(message)

        return await self.app(scope, receive, send_with_headers)


class AuthMiddleware:
    """Require staff Basic Auth or a valid Wisepub session for every request.

    Pure ASGI on purpose; see the module docstring.
    """

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        path = scope.get("path", "")
        if path in EXEMPT_PATHS:
            return await self.app(scope, receive, send)

        missing = missing_config()
        if missing:
            # Fail closed. Serving unauthenticated is the exact bug being fixed,
            # and a 503 naming the variables is far easier to diagnose than a
            # blanket 401 when nobody can get in.
            _LOG.error("Refusing traffic: unset %s", ", ".join(missing))
            return await _send_plain(
                send, 503,
                ("Server auth is not configured. Set "
                 + ", ".join(missing)
                 + " in the environment and restart.").encode(),
            )

        header = None
        for key, value in scope.get("headers") or []:
            if key == b"authorization":
                header = value.decode("latin-1")
                break

        if check_basic_auth(header) or sso_session(scope) is not None:
            return await self.app(scope, receive, send)

        return await _send_plain(
            send, 401, b"Authentication required",
            [(b"www-authenticate", b'Basic realm="Insider Scanner"')],
        )
