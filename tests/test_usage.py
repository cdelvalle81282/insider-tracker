"""Buffered usage tracking.

Nothing measured usage before this. The constraint that shapes the whole design
is that analytics must never compete with page rendering for a database
connection: the documented way this app falls over under load is pool
exhaustion, so an inline INSERT per page view would make that failure arrive
sooner in exchange for numbers nobody reads in real time.

These tests therefore care as much about what the recording path does NOT do
(touch the database, raise, grow without bound) as about what it records.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import app as app_module
import security
import usage
from db import get_request_db

USER = "tester"
PASSWORD = "s3cret"


@pytest.fixture(autouse=True)
def clean_buffer():
    usage._buffer.clear()
    yield
    usage._buffer.clear()


@pytest.fixture(autouse=True)
def auth_env(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH_USER", USER)
    monkeypatch.setenv("BASIC_AUTH_PASSWORD", PASSWORD)
    monkeypatch.setenv("SECRET_KEY", "0" * 64)


@pytest.fixture(autouse=True)
def stub_redis():
    import cache
    original = cache._redis
    cache._redis = MagicMock()
    yield
    cache._redis = original


@pytest.fixture
def db():
    conn = MagicMock()
    conn.autocommit = True
    conn.execute.return_value.fetchall.return_value = []
    conn.execute.return_value.fetchone.return_value = {"n": 0}
    conn.execute.return_value.rowcount = 1
    app_module.app.dependency_overrides[get_request_db] = lambda: conn
    yield conn
    app_module.app.dependency_overrides.pop(get_request_db, None)


class TestRecording:
    def test_record_never_touches_the_database(self, db):
        """The entire point of buffering. A connection checkout here would put
        analytics on the hot path for every viewer."""
        usage.record("a@example.com", "page_view", "/")
        db.execute.assert_not_called()

    def test_events_accumulate(self):
        usage.record("a@example.com", "page_view", "/")
        usage.record("b@example.com", "page_view", "/congress")
        assert usage.pending() == 2

    def test_buffer_is_bounded(self):
        """A database outage must not turn this into a memory leak."""
        for i in range(usage.MAX_BUFFERED + 500):
            usage.record("a@example.com", "page_view", f"/{i}")
        assert usage.pending() == usage.MAX_BUFFERED

    def test_meta_is_serialised_to_json(self):
        usage.record("a@example.com", "search", "/", {"q": "NVDA"})
        assert usage._buffer[0][3] == '{"q": "NVDA"}'

    def test_absent_meta_stays_null(self):
        usage.record("a@example.com", "page_view", "/")
        assert usage._buffer[0][3] is None


class TestFlush:
    def test_writes_one_batch_and_empties_the_buffer(self, db):
        usage.record("a@example.com", "page_view", "/")
        usage.record("a@example.com", "page_view", "/congress")

        assert usage.flush(db) == 2
        assert usage.pending() == 0
        db.cursor.return_value.__enter__.return_value.executemany.assert_called_once()

    def test_empty_buffer_issues_no_statement(self, db):
        assert usage.flush(db) == 0
        db.cursor.assert_not_called()

    def test_failure_requeues_rather_than_dropping(self, db):
        """A transient database blip should delay events, not lose them."""
        usage.record("a@example.com", "page_view", "/")
        db.cursor.side_effect = RuntimeError("connection reset")

        assert usage.flush(db) == 0
        assert usage.pending() == 1, "the event was dropped on the floor"

    def test_failure_does_not_propagate(self, db):
        """A broken analytics write must never surface as a failed page."""
        usage.record("a@example.com", "page_view", "/")
        db.cursor.side_effect = RuntimeError("boom")
        usage.flush(db)  # must not raise

    def test_requeued_events_stay_bounded(self, db):
        """Re-queueing on failure must not defeat the memory ceiling."""
        for i in range(usage.MAX_BUFFERED):
            usage.record("a@example.com", "page_view", f"/{i}")
        db.cursor.side_effect = RuntimeError("still down")

        usage.flush(db)

        assert usage.pending() <= usage.MAX_BUFFERED


class TestMiddleware:
    def _subscriber(self, monkeypatch, email):
        monkeypatch.setattr(
            security, "sso_session", lambda _req: {"email": email, "staff": False}
        )

    def test_page_view_is_attributed_to_the_subscriber(self, db, monkeypatch):
        self._subscriber(monkeypatch, "sub@example.com")
        TestClient(app_module.app).get("/guide")
        assert [e[0] for e in usage._buffer] == ["sub@example.com"]
        assert usage._buffer[0][1] == "page_view"

    def test_staff_collapse_to_one_identity(self, db, monkeypatch):
        """Editorial traffic would otherwise dominate every count."""
        monkeypatch.setattr(security, "sso_session", lambda _req: None)
        c = TestClient(app_module.app)
        c.auth = (USER, PASSWORD)
        c.get("/guide")
        assert usage._buffer[0][0] == usage.STAFF_EMAIL

    def test_htmx_fragments_are_not_counted(self, db, monkeypatch):
        """One dashboard view fires five of them, so counting them would inflate
        every figure by a factor that varies per page.

        raise_server_exceptions=False because this route uses the acquire-late
        pattern (a bare get_db(), not Depends), so the dependency override does
        not reach it and it fails on the absent pool. That is fine here: the
        middleware runs before the route either way, so whether it recorded is
        exactly what is being asserted."""
        self._subscriber(monkeypatch, "sub@example.com")
        c = TestClient(app_module.app, raise_server_exceptions=False)
        c.get("/htmx/watchlist-activity")
        assert usage.pending() == 0

    def test_unauthenticated_requests_record_nothing(self, db, monkeypatch):
        monkeypatch.setattr(security, "sso_session", lambda _req: None)
        TestClient(app_module.app).get("/guide")
        assert usage.pending() == 0

    @pytest.mark.parametrize("path", ["/healthz", "/robots.txt"])
    def test_exempt_paths_record_nothing(self, db, path):
        TestClient(app_module.app).get(path)
        assert usage.pending() == 0

    def test_post_requests_are_not_page_views(self, db, monkeypatch):
        """Writes record their own richer events from inside the route."""
        self._subscriber(monkeypatch, "sub@example.com")
        TestClient(app_module.app).post(
            "/watchlist/toggle", data={}, headers={"X-CSRF-Token": security.make_csrf_token()}
        )
        assert not any(e[1] == "page_view" for e in usage._buffer)


class TestStaffOnly:
    def test_the_usage_page_is_staff_only(self, db, monkeypatch):
        """It shows one named subscriber per row, so it is the last page that
        should be readable by a subscriber."""
        assert "/admin/usage" in security.STAFF_ONLY_PATHS
        monkeypatch.setattr(
            security, "sso_session", lambda _req: {"email": "s@example.com", "staff": False}
        )
        assert TestClient(app_module.app).get("/admin/usage").status_code == 403


class TestRetention:
    def test_prune_deletes_by_age(self, db):
        usage.prune(db, days=90)
        sql, params = db.execute.call_args.args
        assert "DELETE FROM usage_event" in sql
        assert "ts <" in sql
        assert params == [90]

    def test_default_retention_is_declared_once(self):
        """ingest.py and the staff page both read this, so it cannot drift."""
        assert usage.RETENTION_DAYS == 90
