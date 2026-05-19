"""
PostgreSQL connection pool for Insider Tracker.

Replaces the sqlite3 connection pattern from ingest.get_db().
The pool is lazily initialized on first use.

Usage:
  - app.py uses get_request_db() as a FastAPI dependency (a plain generator
    function — FastAPI handles teardown via the generator's try/finally).
  - ingest.py and other scripts use get_db() directly and must close()
    the connection when done (which returns it to the pool, thanks to
    psycopg_pool's PoolConnection.close() override).

DATABASE_URL must be set in the environment, e.g.:
  postgresql://user:pass@localhost:5432/insider_tracker
"""
from __future__ import annotations

import os
from typing import Generator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None


def _get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        url = os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("DATABASE_URL environment variable not set")
        _pool = ConnectionPool(
            url,
            min_size=2,
            max_size=8,
            kwargs={"row_factory": dict_row, "options": "-c timezone=UTC"},
            open=True,
        )
    return _pool


def get_db() -> psycopg.Connection:
    """
    Get a connection from the pool. Caller must close() it when done.
    psycopg_pool's PoolConnection.close() returns the connection to the pool
    rather than tearing it down — drop-in replacement for the old
    `sqlite3.Connection.close()` pattern.
    """
    return _get_pool().getconn()


def get_request_db() -> Generator[psycopg.Connection, None, None]:
    """
    FastAPI dependency: yields a connection from the pool, returns it on exit.

    This is a plain generator (NOT a @contextmanager) so FastAPI's `Depends()`
    treats it as a request-scoped dependency and handles teardown.
    """
    pool = _get_pool()
    conn = pool.getconn()
    try:
        yield conn
    finally:
        pool.putconn(conn)


def close_pool() -> None:
    global _pool
    if _pool is not None:
        _pool.close()
        _pool = None
