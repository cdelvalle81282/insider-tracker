"""
Alembic environment for Insider Scanner.

Uses SQLAlchemy 2 with the psycopg3 dialect (postgresql+psycopg).
Reads DATABASE_URL from the environment.
"""
from __future__ import annotations

import os
from logging.config import fileConfig

from alembic import context
from sqlalchemy import create_engine, pool

config = context.config

if config.config_file_name is not None:
    fileConfig(config.config_file_name)

_db_url = os.environ.get("DATABASE_URL")
if not _db_url:
    raise RuntimeError(
        "DATABASE_URL environment variable not set — required for Alembic migrations"
    )

# Alembic needs SQLAlchemy; swap scheme to psycopg3 dialect.
# The app uses psycopg3 directly (no SQLAlchemy), so this only affects migrations.
if _db_url.startswith("postgresql://"):
    _sa_url = _db_url.replace("postgresql://", "postgresql+psycopg://", 1)
else:
    _sa_url = _db_url

target_metadata = None


def run_migrations_offline() -> None:
    context.configure(
        url=_sa_url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )
    with context.begin_transaction():
        context.run_migrations()


def run_migrations_online() -> None:
    engine = create_engine(_sa_url, poolclass=pool.NullPool)
    with engine.connect() as connection:
        context.configure(connection=connection, target_metadata=target_metadata)
        with context.begin_transaction():
            context.run_migrations()


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
