"""Unit tests for db.py (PostgreSQL connection pool) and queries.add/remove_watch.

All psycopg and psycopg_pool interaction is mocked — no real network calls.
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

import db
import queries


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def reset_pool_global():
    """Reset the module-level _pool singleton before every test."""
    original = db._pool
    db._pool = None
    yield
    db._pool = original


# ---------------------------------------------------------------------------
# _configure_connection — ordering matters
# ---------------------------------------------------------------------------


class TestConfigureConnection:
    def test_sets_prepare_threshold_before_execute_calls(self):
        """prepare_threshold = None must be set before any execute() call."""
        events: list[tuple] = []

        class FakeConn:
            @property
            def prepare_threshold(self):
                return self._pt

            @prepare_threshold.setter
            def prepare_threshold(self, value):
                events.append(("prepare_threshold", value))
                self._pt = value

            def execute(self, sql):
                events.append(("execute", sql))

        conn = FakeConn()
        db._configure_connection(conn)

        assert events == [
            ("prepare_threshold", None),
            ("execute", "SET timezone = 'UTC'"),
            ("execute", "SET statement_timeout = 8000"),
        ]


# ---------------------------------------------------------------------------
# _get_pool — URL selection and pool kwargs
# ---------------------------------------------------------------------------


class TestGetPool:
    def test_uses_pgbouncer_url_when_set(self, monkeypatch):
        monkeypatch.setenv("PGBOUNCER_URL", "postgresql://bouncer/db")
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with patch("db.ConnectionPool") as MockPool:
            MockPool.return_value = MagicMock()
            db._get_pool()

        url_arg = MockPool.call_args.args[0]
        assert url_arg == "postgresql://bouncer/db"

    def test_falls_back_to_database_url_when_no_pgbouncer(self, monkeypatch):
        monkeypatch.delenv("PGBOUNCER_URL", raising=False)
        monkeypatch.setenv("DATABASE_URL", "postgresql://direct/db")

        with patch("db.ConnectionPool") as MockPool:
            MockPool.return_value = MagicMock()
            db._get_pool()

        url_arg = MockPool.call_args.args[0]
        assert url_arg == "postgresql://direct/db"

    def test_raises_runtime_error_when_neither_set(self, monkeypatch):
        monkeypatch.delenv("PGBOUNCER_URL", raising=False)
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(RuntimeError) as exc_info:
            db._get_pool()

        msg = str(exc_info.value)
        assert "PGBOUNCER_URL" in msg
        assert "DATABASE_URL" in msg

    def test_pool_created_with_autocommit_true(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://x/db")
        monkeypatch.delenv("PGBOUNCER_URL", raising=False)

        with patch("db.ConnectionPool") as MockPool:
            MockPool.return_value = MagicMock()
            db._get_pool()

        kwargs_param = MockPool.call_args.kwargs["kwargs"]
        assert kwargs_param["autocommit"] is True

    def test_pool_created_with_configure_connection(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://x/db")
        monkeypatch.delenv("PGBOUNCER_URL", raising=False)

        with patch("db.ConnectionPool") as MockPool:
            MockPool.return_value = MagicMock()
            db._get_pool()

        configure_arg = MockPool.call_args.kwargs["configure"]
        assert configure_arg is db._configure_connection

    def test_prefers_pgbouncer_url_when_both_set(self, monkeypatch):
        monkeypatch.setenv("PGBOUNCER_URL", "postgresql://bouncer/db")
        monkeypatch.setenv("DATABASE_URL", "postgresql://direct/db")

        with patch("db.ConnectionPool") as MockPool:
            MockPool.return_value = MagicMock()
            db._get_pool()

        assert MockPool.call_args.args[0] == "postgresql://bouncer/db"

    def test_returns_existing_pool_without_creating_new_one(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://x/db")
        monkeypatch.delenv("PGBOUNCER_URL", raising=False)
        existing_pool = MagicMock()
        db._pool = existing_pool

        with patch("db.ConnectionPool") as MockPool:
            result = db._get_pool()

        MockPool.assert_not_called()
        assert result is existing_pool


# ---------------------------------------------------------------------------
# get_cli_db — direct connection, not pooled
# ---------------------------------------------------------------------------


class TestGetCliDb:
    def test_uses_database_url(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://direct/db")

        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            db.get_cli_db()

        url_arg = mock_connect.call_args.args[0]
        assert url_arg == "postgresql://direct/db"

    def test_passes_timezone_option(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://direct/db")

        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            db.get_cli_db()

        options = mock_connect.call_args.kwargs.get("options")
        assert options == "-c timezone=UTC"

    def test_does_not_pass_autocommit(self, monkeypatch):
        monkeypatch.setenv("DATABASE_URL", "postgresql://direct/db")

        with patch("psycopg.connect") as mock_connect:
            mock_connect.return_value = MagicMock()
            db.get_cli_db()

        kwargs = mock_connect.call_args.kwargs
        assert "autocommit" not in kwargs

    def test_raises_runtime_error_when_database_url_missing(self, monkeypatch):
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(RuntimeError) as exc_info:
            db.get_cli_db()

        assert "DATABASE_URL" in str(exc_info.value)

    def test_does_not_use_pgbouncer_url(self, monkeypatch):
        monkeypatch.setenv("PGBOUNCER_URL", "postgresql://bouncer/db")
        monkeypatch.delenv("DATABASE_URL", raising=False)

        with pytest.raises(RuntimeError):
            db.get_cli_db()


# ---------------------------------------------------------------------------
# add_watch / remove_watch — autocommit assertion
# ---------------------------------------------------------------------------


class TestAddWatch:
    def test_raises_when_connection_not_autocommit(self):
        conn = MagicMock()
        conn.autocommit = False

        with pytest.raises(AssertionError):
            queries.add_watch(conn, "ticker", "AAPL", "Apple Inc")

    def test_does_not_raise_when_connection_is_autocommit(self):
        conn = MagicMock()
        conn.autocommit = True

        # Should not raise
        queries.add_watch(conn, "ticker", "AAPL", "Apple Inc")

        conn.execute.assert_called_once()

    def test_strips_whitespace_from_value_and_label(self):
        conn = MagicMock()
        conn.autocommit = True

        queries.add_watch(conn, "ticker", "  AAPL  ", "  Apple Inc  ")

        args = conn.execute.call_args.args
        # Second positional arg is the params list: [watch_type, value, label]
        params = args[1]
        assert params[1] == "AAPL"
        assert params[2] == "Apple Inc"


class TestRemoveWatch:
    def test_raises_when_connection_not_autocommit(self):
        conn = MagicMock()
        conn.autocommit = False

        with pytest.raises(AssertionError):
            queries.remove_watch(conn, 42)

    def test_does_not_raise_when_connection_is_autocommit(self):
        conn = MagicMock()
        conn.autocommit = True

        # Should not raise
        queries.remove_watch(conn, 42)

        conn.execute.assert_called_once()
