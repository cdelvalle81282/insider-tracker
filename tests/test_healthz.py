"""Unit tests for /healthz ingest-freshness reporting.

The route used to compute last_ingest and then return only {"ok": true}, so a
caller could not see ingest staleness at all. These tests pin the reported shape
and the deliberate always-200 behaviour.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone

import pytest

import app
import cache as cache_module


def _call():
    return asyncio.run(app.healthz())


class TestHealthz:
    def test_reports_last_ingest_and_age(self, monkeypatch):
        six_hours_ago = datetime.now(timezone.utc) - timedelta(hours=6)
        monkeypatch.setattr(
            cache_module, "_sentinel_mtime", lambda: six_hours_ago.timestamp()
        )
        body = _call()
        assert body["ok"] is True
        assert body["last_ingest"].endswith("Z"), "UTC marker, not a +00:00 offset"
        assert body["ingest_age_hours"] == pytest.approx(6.0, abs=0.05)

    def test_stale_ingest_still_returns_ok(self, monkeypatch):
        """Staleness is reported, not enforced: the uptime monitor owns the
        threshold, and a non-200 here would mask a real outage."""
        ancient = datetime.now(timezone.utc) - timedelta(days=30)
        monkeypatch.setattr(
            cache_module, "_sentinel_mtime", lambda: ancient.timestamp()
        )
        body = _call()
        assert body["ok"] is True
        assert body["ingest_age_hours"] > 700

    def test_missing_sentinel_reports_nulls(self, monkeypatch):
        monkeypatch.setattr(cache_module, "_sentinel_mtime", lambda: 0)
        body = _call()
        assert body == {"ok": True, "last_ingest": None, "ingest_age_hours": None}

    def test_sentinel_error_does_not_500(self, monkeypatch):
        """The health endpoint must never fail on its own bookkeeping."""
        def boom():
            raise OSError("stat failed")

        monkeypatch.setattr(cache_module, "_sentinel_mtime", boom)
        body = _call()
        assert body["ok"] is True
        assert body["last_ingest"] is None


def _bounds(fn, name: str) -> dict:
    """Read ge/le off a FastAPI Query default.

    Constraints are not attributes on the Query object; FastAPI stores them in
    `.metadata` as annotated_types Ge/Le instances.
    """
    import inspect

    default = inspect.signature(fn).parameters[name].default
    found = {}
    for item in getattr(default, "metadata", []):
        for key in ("ge", "le"):
            if hasattr(item, key):
                found[key] = getattr(item, key)
    return found


class TestDaysBounds:
    def test_issuer_days_is_bounded(self):
        """An unbounded days param reached timedelta(days=...) in
        get_issuer_filings and raised OverflowError, surfacing as a 500."""
        assert _bounds(app.issuer_view, "days") == {"ge": 1, "le": 3650}

    def test_congress_days_allows_zero_sentinel(self):
        """0 means 'all time' in the congress views, so the floor must be 0."""
        for fn in (app.congress_view, app.htmx_congress_trades):
            assert _bounds(fn, "days") == {"ge": 0, "le": 3650}, fn.__name__
