"""Add signal_triggers (auto-logged technical signal fires) and
tracked_signals (user opt-in performance tracking) tables.

Revision ID: 0003
Revises: 0002
Create Date: 2026-07-18
"""
from __future__ import annotations

from alembic import op

revision = "0003"
down_revision = "0002"
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ---------------- signal_triggers ----------------
    op.execute("""
        CREATE TABLE signal_triggers (
            id                    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            issuer_ticker         TEXT NOT NULL,
            issuer_cik            TEXT NOT NULL,
            issuer_name           TEXT,
            signal_code           TEXT NOT NULL CHECK (signal_code IN ('gc', 'rb', 'hhl', 'cb')),
            trigger_date          DATE NOT NULL,
            trade_transaction_id  TEXT,
            insider_name          TEXT,
            insider_title         TEXT,
            trade_date            DATE,
            trade_value           REAL,
            days_to_fire          INTEGER,
            detected_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (issuer_ticker, signal_code, trigger_date)
        )
    """)
    op.execute("CREATE INDEX idx_st_trigger_date ON signal_triggers(trigger_date)")
    op.execute("CREATE INDEX idx_st_ticker       ON signal_triggers(issuer_ticker)")

    # ---------------- tracked_signals ----------------
    op.execute("""
        CREATE TABLE tracked_signals (
            id             BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            issuer_ticker  TEXT NOT NULL,
            issuer_name    TEXT,
            signal_code    TEXT NOT NULL CHECK (signal_code IN ('gc', 'rb', 'hhl', 'cb')),
            trigger_date   DATE NOT NULL,
            added_at       TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (issuer_ticker, signal_code, trigger_date)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS tracked_signals")
    op.execute("DROP TABLE IF EXISTS signal_triggers")
