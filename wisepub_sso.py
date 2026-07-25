"""Wisepub SSO Sites receiver — token verification and session handling.

Wisepub (the platform behind vip.optionpit.com) has an admin resource called
"SSO Sites" (/admin/resources/s-s-o-sites). Each record points at one of our
apps and holds a shared JWT secret. A logged-in Wisepub subscriber who hits
that record's outbound URL (https://vip.optionpit.com/sso-redirect/{id}) is
302'd to:

    GET {our_base_url}{sso_endpoint}?token=<jwt>       (default endpoint /sso)

The JWT is HS256-signed with the shared secret. Its payload, confirmed against
a real captured token:

    {"email": "<base64>", "first_name": "...", "last_name": "...",
     "allow_create_account": false, "allow_staff": true,
     "iat": 1784962617, "phone": "<base64>"}

Two consequences drive the code below:

1. There is **no `exp` claim**, so tokens never self-expire. Freshness is
   enforced here from `iat` (MAX_TOKEN_AGE), and each token is accepted only
   once (ReplayGuard) so a leaked URL cannot be reused.
2. `email`/`phone` are base64-*encoded*, not encrypted. The signature is what
   proves authenticity; the values themselves are readable in the URL.

The token proves "this is a logged-in Wisepub user" and nothing more (there is
no subscription or product claim), which is exactly the access rule these apps
apply: a valid token grants access, Wisepub's own login is the paywall.

This file is duplicated verbatim across the apps that receive Wisepub SSO
(portfolio-tracker, insider-tracker, income-value-tracker). Keep the copies in
sync; the Next.js equivalent is MacroB's src/lib/auth/wisepubSso.ts.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import time

# Freshness policy. Wisepub sends the token on a live redirect, so the gap
# between iat and our receiving it is normally under a second. 120s leaves
# room for a slow hop without leaving a usefully replayable window, and
# CLOCK_SKEW tolerates our clock running slightly behind Wisepub's.
MAX_TOKEN_AGE = 120
CLOCK_SKEW = 60

SESSION_COOKIE = "wp_sso"
SESSION_MAX_AGE = 8 * 3600


class SsoError(Exception):
    """Token was absent, malformed, unsigned, stale, or already used."""


def _b64url_decode(raw: str) -> bytes:
    return base64.urlsafe_b64decode(raw + "=" * (-len(raw) % 4))


def _b64url_encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def decode_claim(value):
    """Decode a base64-encoded Wisepub claim, tolerating plain values.

    Wisepub base64-encodes email and phone. A plain email is not valid base64
    (it has '@' and '.'), so a failed decode means the value already arrived in
    the clear, and we return it as-is rather than dropping the identity.
    """
    if not isinstance(value, str) or not value:
        return ""
    try:
        decoded = base64.b64decode(value, validate=True).decode("utf-8")
    except Exception:
        return value
    if not decoded.isprintable():
        return value
    return decoded


def verify_wisepub_token(token: str, secret: str, *, max_age: int = MAX_TOKEN_AGE,
                         clock_skew: int = CLOCK_SKEW, now: float | None = None) -> dict:
    """Verify a Wisepub SSO JWT and return the identity it carries.

    Raises SsoError on any failure. Never returns a partially trusted result.
    """
    if not secret:
        raise SsoError("no SSO secret configured")
    if not token:
        raise SsoError("no token supplied")

    parts = token.split(".")
    if len(parts) != 3:
        raise SsoError("malformed token")
    header_b64, payload_b64, signature_b64 = parts

    try:
        header = json.loads(_b64url_decode(header_b64))
    except Exception:
        raise SsoError("unreadable header")
    # Pin the algorithm: accepting whatever the header claims is how "alg":"none"
    # and RS256-key-confusion forgeries get in.
    if header.get("alg") != "HS256":
        raise SsoError(f"unexpected alg {header.get('alg')!r}")

    expected = hmac.new(
        secret.encode(), f"{header_b64}.{payload_b64}".encode(), hashlib.sha256
    ).digest()
    try:
        supplied = _b64url_decode(signature_b64)
    except Exception:
        raise SsoError("unreadable signature")
    if not hmac.compare_digest(expected, supplied):
        raise SsoError("bad signature")

    try:
        payload = json.loads(_b64url_decode(payload_b64))
    except Exception:
        raise SsoError("unreadable payload")
    if not isinstance(payload, dict):
        raise SsoError("unreadable payload")

    iat = payload.get("iat")
    if not isinstance(iat, (int, float)) or isinstance(iat, bool):
        raise SsoError("missing iat")
    current = time.time() if now is None else now
    if iat > current + clock_skew:
        raise SsoError("token issued in the future")
    if iat < current - max_age:
        raise SsoError("token too old")

    email = decode_claim(payload.get("email")).strip().lower()
    if not email:
        raise SsoError("no email in token")

    return {
        "email": email,
        "first_name": payload.get("first_name") or "",
        "last_name": payload.get("last_name") or "",
        "phone": decode_claim(payload.get("phone")),
        "staff": bool(payload.get("allow_staff")),
        "iat": int(iat),
        # Fingerprint of the signature, for one-time-use tracking. Hashed so a
        # replay-guard store never holds a usable token.
        "fingerprint": hashlib.sha256(signature_b64.encode()).hexdigest()[:32],
    }


class ReplayGuard:
    """One-time-use tracking for SSO tokens.

    Backed by Redis when the app has it (survives restarts, shared across
    workers); otherwise an in-process dict, which is sufficient for a
    single-worker app. Entries only need to outlive the freshness window,
    since verify_wisepub_token rejects anything older anyway.
    """

    def __init__(self, redis_client=None, prefix: str = "wpsso:used:"):
        self._redis = redis_client
        self._prefix = prefix
        self._seen: dict[str, float] = {}

    def claim(self, fingerprint: str, ttl: int = MAX_TOKEN_AGE + CLOCK_SKEW) -> bool:
        """Return True the first time a fingerprint is seen, False after."""
        if self._redis is not None:
            try:
                return bool(self._redis.set(self._prefix + fingerprint, 1, nx=True, ex=ttl))
            except Exception:
                # A Redis outage must not lock subscribers out; fall through to
                # the in-process guard rather than failing the login.
                pass
        now = time.time()
        for key, expiry in list(self._seen.items()):
            if expiry <= now:
                del self._seen[key]
        if fingerprint in self._seen:
            return False
        self._seen[fingerprint] = now + ttl
        return True


def _session_key(secret: str, session_secret: str | None) -> bytes:
    """Key used to sign our own session cookie.

    Prefer the app's existing secret. Falling back to a value derived from the
    Wisepub secret keeps this drop-in for apps that have no app-level secret.
    """
    base = session_secret or secret
    return hashlib.sha256(b"wisepub-sso-session|" + base.encode()).digest()


def issue_session(identity: dict, secret: str, *, session_secret: str | None = None,
                  max_age: int = SESSION_MAX_AGE, now: float | None = None) -> str:
    """Mint a compact signed session token for a verified identity."""
    current = int(time.time() if now is None else now)
    body = {
        "email": identity["email"],
        "name": " ".join(x for x in (identity.get("first_name"), identity.get("last_name")) if x),
        "staff": bool(identity.get("staff")),
        "iat": current,
        "exp": current + max_age,
    }
    payload = _b64url_encode(json.dumps(body, separators=(",", ":")).encode())
    signature = hmac.new(_session_key(secret, session_secret), payload.encode(), hashlib.sha256).digest()
    return f"{payload}.{_b64url_encode(signature)}"


def read_session(cookie_value: str | None, secret: str, *, session_secret: str | None = None,
                 now: float | None = None) -> dict | None:
    """Return the identity in a session cookie, or None if absent/invalid/expired."""
    if not cookie_value or "." not in cookie_value:
        return None
    payload, _, signature_b64 = cookie_value.rpartition(".")
    expected = hmac.new(_session_key(secret, session_secret), payload.encode(), hashlib.sha256).digest()
    try:
        supplied = _b64url_decode(signature_b64)
    except Exception:
        return None
    if not hmac.compare_digest(expected, supplied):
        return None
    try:
        body = json.loads(_b64url_decode(payload))
    except Exception:
        return None
    if not isinstance(body, dict):
        return None
    exp = body.get("exp")
    if not isinstance(exp, (int, float)) or exp < (time.time() if now is None else now):
        return None
    return body


def set_session_cookie(response, value: str, *, name: str = SESSION_COOKIE,
                       max_age: int = SESSION_MAX_AGE, path: str = "/") -> None:
    """Attach the session cookie with the attributes an iframe needs.

    SameSite=None is mandatory: the app runs in a cross-site iframe on
    vip.optionpit.com, and a Lax cookie is not sent there. Partitioned (CHIPS)
    keeps that acceptable by scoping the cookie to the embedding site, and is
    what Chrome requires as third-party cookies wind down. Set manually because
    Starlette's set_cookie() has no Partitioned parameter.
    """
    attributes = [
        f"{name}={value}",
        f"Path={path}",
        f"Max-Age={max_age}",
        "HttpOnly",
        "Secure",
        "SameSite=None",
        "Partitioned",
    ]
    response.headers.append("set-cookie", "; ".join(attributes))


def clear_session_cookie(response, *, name: str = SESSION_COOKIE, path: str = "/") -> None:
    response.headers.append(
        "set-cookie",
        f"{name}=; Path={path}; Max-Age=0; HttpOnly; Secure; SameSite=None; Partitioned",
    )


def safe_next_path(candidate: str | None, default: str) -> str:
    """Constrain a ?next= value to a path on this site.

    Blocks open redirects: anything not starting with a single '/' (so no
    scheme, no '//host', no backslash trick) falls back to the default.
    """
    if not candidate or not candidate.startswith("/"):
        return default
    if candidate.startswith("//") or candidate.startswith("/\\"):
        return default
    return candidate
