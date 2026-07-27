"""Unit tests for cache.py (Redis-backed cache with sentinel mtime invalidation
and HMAC-signed pickle serialization).

All Redis interaction is mocked — no real network calls are made.
"""
from __future__ import annotations

import os
from datetime import date
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest
import redis

import cache


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_redis_global():
    """Reset the module-level _redis singleton before every test."""
    original = cache._redis
    cache._redis = None
    yield
    cache._redis = original


@pytest.fixture()
def mock_client():
    """Inject a MagicMock as the active Redis client."""
    client = MagicMock(spec=redis.Redis)
    cache._redis = client
    return client


# ---------------------------------------------------------------------------
# _sentinel_mtime
# ---------------------------------------------------------------------------


class TestSentinelMtime:
    def test_returns_mtime_when_file_exists(self, tmp_path):
        sentinel = tmp_path / ".last_ingest"
        sentinel.touch()
        original = cache._SENTINEL
        cache._SENTINEL = str(sentinel)
        try:
            result = cache._sentinel_mtime()
            assert result == pytest.approx(os.path.getmtime(str(sentinel)))
        finally:
            cache._SENTINEL = original

    def test_returns_zero_when_file_missing(self, tmp_path):
        original = cache._SENTINEL
        cache._SENTINEL = str(tmp_path / "nonexistent")
        try:
            result = cache._sentinel_mtime()
            assert result == 0.0
        finally:
            cache._SENTINEL = original


# ---------------------------------------------------------------------------
# cache_get
# ---------------------------------------------------------------------------


class TestCacheGet:
    def test_cache_miss_returns_none(self, mock_client):
        mock_client.get.return_value = None

        result = cache.cache_get("it:query:missing")

        assert result is None

    def test_cache_hit_fresh_returns_value(self, mock_client):
        sentinel_mtime = 100.0
        stored_mtime = 200.0  # stored_mtime > sentinel_mtime → fresh
        value = [{"ticker": "AAPL", "total_value": 500_000}]
        raw = cache._serialize((stored_mtime, value))
        mock_client.get.return_value = raw

        with patch("cache._sentinel_mtime", return_value=sentinel_mtime):
            result = cache.cache_get("it:query:filings")

        assert result == value

    def test_cache_hit_stale_returns_none(self, mock_client):
        sentinel_mtime = 200.0
        stored_mtime = 100.0  # stored_mtime < sentinel_mtime → stale
        value = [{"ticker": "MSFT"}]
        raw = cache._serialize((stored_mtime, value))
        mock_client.get.return_value = raw

        with patch("cache._sentinel_mtime", return_value=sentinel_mtime):
            result = cache.cache_get("it:query:filings")

        assert result is None

    def test_tampered_signature_returns_none(self, mock_client):
        raw = cache._serialize((0.0, "some value"))
        tampered = raw[: cache._SIG_LEN] + b"x" + raw[cache._SIG_LEN + 1:]
        mock_client.get.return_value = tampered

        result = cache.cache_get("it:query:filings")

        assert result is None

    def test_unsigned_legacy_pickle_returns_none(self, mock_client):
        """A pre-migration entry (raw pickle, no HMAC prefix) must be treated as
        a miss, not crash — cold-cache repopulate is the expected behavior."""
        import pickle
        legacy_raw = pickle.dumps((0.0, "old value"))
        mock_client.get.return_value = legacy_raw

        result = cache.cache_get("it:query:filings")

        assert result is None

    def test_redis_connection_error_returns_none(self, mock_client):
        mock_client.get.side_effect = redis.ConnectionError("refused")

        result = cache.cache_get("it:query:filings")

        assert result is None

    def test_corrupt_pickle_returns_none(self, mock_client):
        mock_client.get.return_value = b"this is not valid pickle data \x00\xff"

        result = cache.cache_get("it:query:filings")

        assert result is None


# ---------------------------------------------------------------------------
# cache_set
# ---------------------------------------------------------------------------


class TestCacheSet:
    def test_stores_tuple_with_correct_ttl(self, mock_client):
        pre_mtime = 123.456
        value = "<html>fragment</html>"
        ttl = 3600

        cache.cache_set("it:query:stats", pre_mtime, value, ttl=ttl)

        mock_client.set.assert_called_once()
        call_args = mock_client.set.call_args
        key, raw = call_args.args
        assert key == "it:query:stats"
        stored_mtime, stored_value = cache._deserialize(raw)
        assert stored_mtime == pre_mtime
        assert stored_value == value
        assert call_args.kwargs["ex"] == ttl

    def test_default_ttl_is_86400(self, mock_client):
        cache.cache_set("it:query:x", 1.0, "val")

        call_args = mock_client.set.call_args
        assert call_args.kwargs["ex"] == 86400

    def test_stores_list_of_dicts(self, mock_client):
        value = [{"a": 1}, {"b": 2}]
        cache.cache_set("it:query:list", 1.0, value)

        raw = mock_client.set.call_args.args[1]
        _, stored_value = cache._deserialize(raw)
        assert stored_value == value

    def test_stores_none_value(self, mock_client):
        cache.cache_set("it:query:empty", 1.0, None)

        raw = mock_client.set.call_args.args[1]
        _, stored_value = cache._deserialize(raw)
        assert stored_value is None

    def test_date_and_decimal_round_trip(self, mock_client):
        """it:query:* entries cache enriched row dicts containing real date/
        Decimal values (from PG rows) — HMAC-signing must not change pickle's
        type-preserving round-trip behavior."""
        value = [{"transaction_date": date(2026, 7, 18), "total_value": Decimal("1234.56")}]
        cache.cache_set("it:query:typed", 1.0, value)

        raw = mock_client.set.call_args.args[1]
        _, stored_value = cache._deserialize(raw)
        assert stored_value == value
        assert isinstance(stored_value[0]["transaction_date"], date)
        assert isinstance(stored_value[0]["total_value"], Decimal)

    def test_redis_connection_error_does_not_raise(self, mock_client):
        mock_client.set.side_effect = redis.ConnectionError("refused")

        # Must not propagate
        cache.cache_set("it:query:x", 1.0, "value")


# ---------------------------------------------------------------------------
# invalidate_query_cache
# ---------------------------------------------------------------------------


class TestInvalidateQueryCache:
    def test_deletes_all_matching_keys(self, mock_client):
        keys = [b"it:query:filings", b"it:query:stats", b"it:query:clusters"]
        mock_client.scan_iter.return_value = iter(keys)

        cache.invalidate_query_cache()

        mock_client.delete.assert_called_once_with(*keys)

    def test_empty_scan_result_still_calls_delete(self, mock_client):
        mock_client.scan_iter.return_value = iter([])

        # Should not raise; delete is called with zero args
        cache.invalidate_query_cache()

        mock_client.delete.assert_called_once_with()

    def test_redis_error_does_not_raise(self, mock_client):
        mock_client.scan_iter.side_effect = redis.RedisError("scan failed")

        # Must not propagate
        cache.invalidate_query_cache()


# ---------------------------------------------------------------------------
# cache_get_single_flight
# ---------------------------------------------------------------------------


class TestSingleFlight:
    """Staleness is measured against one global sentinel the ingest bumps, so
    every cached entry expires at the same instant. Without single-flighting,
    every request in flight at that moment runs the same expensive query against
    the same pool."""

    def _store(self, mock_client, mtime, value):
        mock_client.get.return_value = cache._serialize((mtime, value))

    def test_fresh_entry_is_returned_without_taking_a_lock(self, mock_client, monkeypatch):
        monkeypatch.setattr(cache, "_sentinel_mtime", lambda: 100.0)
        self._store(mock_client, 200.0, "fresh")

        assert cache.cache_get_single_flight("it:query:x") == "fresh"
        mock_client.set.assert_not_called()

    def test_winner_is_told_to_recompute(self, mock_client, monkeypatch):
        monkeypatch.setattr(cache, "_sentinel_mtime", lambda: 300.0)
        self._store(mock_client, 200.0, "stale")
        mock_client.set.return_value = True  # won SET NX

        assert cache.cache_get_single_flight("it:query:x") is None

    def test_loser_is_served_the_previous_value(self, mock_client, monkeypatch):
        """The whole point: one caller hits the DB, everyone else gets served."""
        monkeypatch.setattr(cache, "_sentinel_mtime", lambda: 300.0)
        self._store(mock_client, 200.0, "stale")
        mock_client.set.return_value = None  # lost SET NX

        assert cache.cache_get_single_flight("it:query:x") == "stale"

    def test_cold_key_computes(self, mock_client, monkeypatch):
        """No previous value exists, so there is nothing to serve and every
        caller must compute. Not a gap in the design."""
        monkeypatch.setattr(cache, "_sentinel_mtime", lambda: 300.0)
        mock_client.get.return_value = None

        assert cache.cache_get_single_flight("it:query:x") is None

    def test_redis_error_falls_through_to_compute(self, mock_client, monkeypatch):
        """Redis is a performance dependency, never an availability one."""
        monkeypatch.setattr(cache, "_sentinel_mtime", lambda: 300.0)
        mock_client.get.side_effect = redis.RedisError("down")

        assert cache.cache_get_single_flight("it:query:x") is None

    def test_lock_failure_fails_open(self, mock_client, monkeypatch):
        """If the lock cannot be taken there is no cache to stampede anyway, so
        the caller proceeds rather than serving something it cannot verify."""
        monkeypatch.setattr(cache, "_sentinel_mtime", lambda: 300.0)
        self._store(mock_client, 200.0, "stale")
        mock_client.set.side_effect = redis.RedisError("down")

        assert cache.cache_get_single_flight("it:query:x") is None

    def test_lock_is_scoped_to_the_key_and_expires(self, mock_client, monkeypatch):
        """A stuck lock must not freeze a key on stale data forever."""
        monkeypatch.setattr(cache, "_sentinel_mtime", lambda: 300.0)
        self._store(mock_client, 200.0, "stale")
        mock_client.set.return_value = True

        cache.cache_get_single_flight("it:query:abc", lock_ttl=15)

        args, kwargs = mock_client.set.call_args
        assert args[0] == b"it:refresh:it:query:abc"
        assert kwargs["nx"] is True
        assert kwargs["ex"] == 15


# ---------------------------------------------------------------------------
# invalidate_owner_cache
# ---------------------------------------------------------------------------


class TestInvalidateOwnerCache:
    """Watchlist edits used to call invalidate_query_cache(), which deletes
    every cached query for everyone. With one editorial user that was fine. With
    subscribers it would mean one person adding a ticker flushes the shared
    dashboard for all of them, so the cache would never survive."""

    def test_scan_is_scoped_to_one_owner(self, mock_client):
        mock_client.scan_iter.return_value = iter([b"it:query:o=abc:watchsets"])

        cache.invalidate_owner_cache("sub:alice@example.com")

        pattern = mock_client.scan_iter.call_args.args[0]
        assert pattern.startswith("it:query:o=")
        assert pattern.endswith(":*")
        assert pattern != "it:query:*", "that would evict every other viewer"

    def test_two_owners_scan_different_patterns(self, mock_client):
        mock_client.scan_iter.return_value = iter([])
        cache.invalidate_owner_cache("sub:alice@example.com")
        first = mock_client.scan_iter.call_args.args[0]
        mock_client.scan_iter.return_value = iter([])
        cache.invalidate_owner_cache("sub:bob@example.com")
        assert first != mock_client.scan_iter.call_args.args[0]

    def test_empty_scan_does_not_call_delete(self, mock_client):
        """redis-py errors on DEL with no keys, and unlike the global helper
        this runs on every watchlist write."""
        mock_client.scan_iter.return_value = iter([])

        cache.invalidate_owner_cache("sub:alice@example.com")

        mock_client.delete.assert_not_called()

    def test_redis_error_does_not_raise(self, mock_client):
        mock_client.scan_iter.side_effect = redis.RedisError("down")

        cache.invalidate_owner_cache("sub:alice@example.com")

    def test_prefix_is_stable_and_hides_the_address(self):
        owner = "sub:alice@example.com"
        assert cache.owner_key_prefix(owner) == cache.owner_key_prefix(owner)
        assert "alice@example.com" not in cache.owner_key_prefix(owner)
