"""Unit tests for cache.py (Redis-backed cache with sentinel mtime invalidation).

All Redis interaction is mocked — no real network calls are made.
"""
from __future__ import annotations

import os
import pickle
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
        raw = pickle.dumps((stored_mtime, value))
        mock_client.get.return_value = raw

        with patch("cache._sentinel_mtime", return_value=sentinel_mtime):
            result = cache.cache_get("it:query:filings")

        assert result == value

    def test_cache_hit_stale_returns_none(self, mock_client):
        sentinel_mtime = 200.0
        stored_mtime = 100.0  # stored_mtime < sentinel_mtime → stale
        value = [{"ticker": "MSFT"}]
        raw = pickle.dumps((stored_mtime, value))
        mock_client.get.return_value = raw

        with patch("cache._sentinel_mtime", return_value=sentinel_mtime):
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
        stored_mtime, stored_value = pickle.loads(raw)
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
        _, stored_value = pickle.loads(raw)
        assert stored_value == value

    def test_stores_none_value(self, mock_client):
        cache.cache_set("it:query:empty", 1.0, None)

        raw = mock_client.set.call_args.args[1]
        _, stored_value = pickle.loads(raw)
        assert stored_value is None

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
