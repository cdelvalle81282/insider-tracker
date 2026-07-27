from __future__ import annotations

import hashlib
import hmac
import logging
import os
import pickle

import redis

logger = logging.getLogger(__name__)

_SENTINEL = os.path.join(os.path.dirname(__file__), "data", ".last_ingest")

_SIG_LEN = 32  # SHA-256 digest size

_redis: redis.Redis | None = None


def _client() -> redis.Redis:
    global _redis
    if _redis is None:
        _redis = redis.Redis(
            host="localhost",
            port=6379,
            db=3,
            max_connections=20,
            decode_responses=False,
            socket_connect_timeout=0.5,
            socket_timeout=0.5,
        )
    return _redis


def _sentinel_mtime() -> float:
    try:
        return os.path.getmtime(_SENTINEL)
    except OSError:
        return 0.0


def _signing_key() -> bytes:
    """Stable secret shared across both uvicorn workers/restarts — must NOT be
    per-process-random or cross-worker cache reads would always fail signature
    verification. Falls back to DATABASE_URL (already a required env var) so no
    new env var is needed to deploy this; set CACHE_SIGNING_KEY for a dedicated
    secret instead of reusing the DB one."""
    secret = os.getenv("CACHE_SIGNING_KEY") or os.getenv("DATABASE_URL") or ""
    return hashlib.sha256(secret.encode("utf-8")).digest()


def _serialize(payload) -> bytes:
    """Pickle + HMAC-sign so a co-tenant process that can write to this Redis
    db (unauthenticated, shared droplet) can't forge a payload we'll unpickle —
    pickle.loads on attacker-controlled bytes is an RCE primitive."""
    blob = pickle.dumps(payload)
    sig = hmac.new(_signing_key(), blob, hashlib.sha256).digest()
    return sig + blob


def _deserialize(raw: bytes):
    if len(raw) < _SIG_LEN:
        raise ValueError("cached blob too short to contain a signature")
    sig, blob = raw[:_SIG_LEN], raw[_SIG_LEN:]
    expected = hmac.new(_signing_key(), blob, hashlib.sha256).digest()
    if not hmac.compare_digest(sig, expected):
        raise ValueError("cache signature mismatch (tampered, forged, or a pre-signing entry)")
    return pickle.loads(blob)


def _read(key: str):
    """Return (stored_mtime, value), or None on miss, Redis error, or bad blob."""
    try:
        raw = _client().get(key)
    except redis.RedisError as exc:
        logger.debug("Redis get %r failed: %s", key, exc)
        return None
    if raw is None:
        return None
    try:
        return _deserialize(raw)
    except Exception as exc:
        logger.debug("Redis deserialize failed for key %r: %s", key, exc)
        return None


def cache_get(key: str):
    """Return cached value, or None on miss, stale entry, or Redis error."""
    entry = _read(key)
    if entry is None:
        return None
    stored_mtime, value = entry
    if stored_mtime < _sentinel_mtime():
        return None  # stale since last ingest
    return value


def cache_get_single_flight(key: str, lock_ttl: int = 15):
    """cache_get, but only ONE caller is sent to the database on a stale entry.

    Every entry goes stale at the same instant, because staleness is measured
    against one global sentinel mtime that the ingest bumps. With a handful of
    editorial users that is invisible. With a few hundred subscribers it is a
    thundering herd: every in-flight request misses together and every one of
    them runs the same expensive query against the same connection pool.

    So the first caller to arrive after an ingest claims a short Redis lock and
    recomputes (returns None, meaning "you do the work"); everyone else is handed
    the previous value until that finishes. The cost is that a page can show
    pre-ingest data for a few seconds after the ingest, which is a fair trade for
    a tool whose data arrives in daily batches anyway.

    A cold key has no previous value to serve, so all callers compute. That is
    unavoidable rather than a gap: there is nothing to serve.
    """
    entry = _read(key)
    if entry is None:
        return None  # cold: nothing to serve, everyone computes
    stored_mtime, value = entry
    if stored_mtime >= _sentinel_mtime():
        return value  # fresh
    if _claim_refresh(key, lock_ttl):
        return None  # we won the race; recompute and cache_set
    return value  # someone else is recomputing; serve the previous value


def _claim_refresh(key: str, ttl_seconds: int) -> bool:
    """True when this caller should perform the refresh for `key`.

    Fails OPEN, like acquire_cooldown: if Redis is unreachable there is no cache
    to stampede in the first place, and every request has to hit the DB anyway.
    """
    try:
        acquired = _client().set(
            f"it:refresh:{key}".encode(), b"1", nx=True, ex=ttl_seconds
        )
    except redis.RedisError as exc:
        logger.debug("Redis _claim_refresh %r failed, proceeding: %s", key, exc)
        return True
    return bool(acquired)


def cache_set(key: str, pre_mtime: float, value, ttl: int = 86400) -> None:
    """Store (pre_mtime, value) in Redis. Silent on error."""
    try:
        _client().set(key, _serialize((pre_mtime, value)), ex=ttl)
    except redis.RedisError as exc:
        logger.debug("Redis set %r failed: %s", key, exc)


def acquire_cooldown(name: str, ttl_seconds: int) -> bool:
    """Return True if the caller may proceed, False while a cooldown is active.

    Backed by Redis SET NX EX rather than an in-process guard because the service
    runs two uvicorn workers, and a per-process cooldown would allow one firing
    per worker.

    Fails OPEN: if Redis is unreachable the caller proceeds. This gates
    auto-diagnosis, which exists precisely to investigate outages, and a Redis
    outage is exactly when you most want it to run.
    """
    try:
        acquired = _client().set(
            f"it:cooldown:{name}".encode(), b"1", nx=True, ex=ttl_seconds
        )
    except redis.RedisError as exc:
        logger.debug("Redis acquire_cooldown %r failed, proceeding: %s", name, exc)
        return True
    return bool(acquired)


def invalidate_query_cache() -> None:
    """Delete all cached query results. Call on watchlist changes."""
    try:
        cl = _client()
        keys = list(cl.scan_iter("it:query:*"))
        cl.delete(*keys)
    except redis.RedisError as exc:
        logger.debug("Redis invalidate_query_cache failed: %s", exc)
