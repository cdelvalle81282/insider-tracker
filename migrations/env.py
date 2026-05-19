"""
Alembic environment for Insider Tracker.

Uses psycopg3 directly (no SQLAlchemy) — matches the rest of the codebase.
Reads DATABASE_URL from the environment.
"""
from __future__ import annotations

import os
from logging.config import fileConfig

import psycopg
from alembic import context

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

_db_url = os.environ.get("DATABASE_URL")
if not _db_url:
    raise RuntimeError(
        "DATABASE_URL environment variable not set — required for Alembic migrations"
    )

target_metadata = None


def run_migrations_offline() -> None:
    """Emit SQL to stdout without a live connection."""
    context.configure(
        url=_db_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
        dialect_name="postgresql",
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    """Run migrations against a live database via psycopg3."""
    with psycopg.connect(_db_url) as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
