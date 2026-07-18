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


def cache_get(key: str):
    """Return cached value, or None on miss, stale entry, or Redis error."""
    try:
        raw = _client().get(key)
    except redis.RedisError as exc:
        logger.debug("Redis get %r failed: %s", key, exc)
        return None
    if raw is None:
        return None
    try:
        stored_mtime, value = _deserialize(raw)
    except Exception as exc:
        logger.debug("Redis deserialize failed for key %r: %s", key, exc)
        return None
    if stored_mtime < _sentinel_mtime():
        return None  # stale since last ingest
    return value


def cache_set(key: str, pre_mtime: float, value, ttl: int = 86400) -> None:
    """Store (pre_mtime, value) in Redis. Silent on error."""
    try:
        _client().set(key, _serialize((pre_mtime, value)), ex=ttl)
    except redis.RedisError as exc:
        logger.debug("Redis set %r failed: %s", key, exc)


def invalidate_query_cache() -> None:
    """Delete all cached query results. Call on watchlist changes."""
    try:
        cl = _client()
        keys = list(cl.scan_iter("it:query:*"))
        cl.delete(*keys)
    except redis.RedisError as exc:
        logger.debug("Redis invalidate_query_cache failed: %s", exc)
