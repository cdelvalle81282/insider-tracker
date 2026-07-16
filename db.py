"""
PostgreSQL connection pool for Insider Scanner.

The pool is lazily initialized on first use.

Usage:
  - app.py routes use get_request_db() as a FastAPI dependency, or get_db()
    for the deferred HTMX endpoints (htmx_stats, htmx_clusters).
  - CLI scripts (ingest.py, congress_ingest.py, etc.) use get_cli_db() --
    a plain psycopg connection with no pool. CLI scripts are sequential and
    don't need pooling; creating a pool in a CLI process competes with the
    web app's pool under PostgreSQL load.

DATABASE_URL   -- direct PostgreSQL connection (used by get_cli_db and as
                  fallback when PGBOUNCER_URL is not set).
PGBOUNCER_URL  -- PgBouncer connection (transaction mode, port 6432).
                  When set, the pool connects here instead of directly to PG.
                  CLI scripts always use DATABASE_URL (direct PG).
"""
from __future__ import annotations

import os
from typing import Generator

import psycopg
from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool

_pool: ConnectionPool | None = None


def _configure_connection(conn: psycopg.Connection) -> None:
    """Set backend session state once; persists across PgBouncer transaction boundaries."""
    conn.prepare_threshold = None  # None = never prepare; 0 = prepare immediately (wrong)
    conn.execute("SET timezone = 'UTC'")
    conn.execute("SET statement_timeout = 8000")


def _get_pool() -> ConnectionPool:
    global _pool
    if _pool is None:
        url = os.environ.get("PGBOUNCER_URL") or os.environ.get("DATABASE_URL")
        if not url:
            raise RuntimeError("PGBOUNCER_URL or DATABASE_URL environment variable not set")
        _pool = ConnectionPool(
            url,
            min_size=2,
            max_size=16,
            kwargs={"row_factory": dict_row, "autocommit": True},
            configure=_configure_connection,
            open=True,
        )
    return _pool


def get_db() -> psycopg.Connection:
    """
    Get a connection from the pool. Caller must release it via put_db() when done.
    Calling conn.close() instead only kills the physical connection -- it does NOT
    notify the pool, so the pool's in-use slot is never freed. That silently depletes
    max_size over time (each leaked call permanently loses one slot for that worker).
    """
    return _get_pool().getconn()


def put_db(conn: psycopg.Connection) -> None:
    """Return a connection acquired via get_db() to the pool."""
    _get_pool().putconn(conn)


def get_cli_db() -> psycopg.Connection:
    """Direct connection (not pooled) for CLI scripts like ingest.py.

    Always connects to DATABASE_URL (direct PG, not PgBouncer) so ingest
    scripts can use explicit transactions and conn.commit()/rollback().
    """
    url = os.environ.get("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL environment variable not set")
    return psycopg.connect(url, row_factory=dict_row, options="-c timezone=UTC")


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
