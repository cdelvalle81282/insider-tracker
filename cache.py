from __future__ import annotations

import logging
import os
import pickle

import redis

logger = logging.getLogger(__name__)

_SENTINEL = os.path.join(os.path.dirname(__file__), "data", ".last_ingest")

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
        stored_mtime, value = pickle.loads(raw)
    except Exception as exc:
        logger.debug("Redis unpickle failed for key %r: %s", key, exc)
        return None
    if stored_mtime < _sentinel_mtime():
        return None  # stale since last ingest
    return value


def cache_set(key: str, pre_mtime: float, value, ttl: int = 86400) -> None:
    """Store (pre_mtime, value) in Redis. Silent on error."""
    try:
        _client().set(key, pickle.dumps((pre_mtime, value)), ex=ttl)
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
