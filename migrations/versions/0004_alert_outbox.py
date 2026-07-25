"""Turn alerts_sent into a delivery outbox.

The table recorded only that an alert slot was claimed, and the claim was
committed before the Slack POST was attempted. A failed POST therefore left a
row that read as "already sent" forever, so one transient Slack outage silently
dropped those alerts permanently.

Adding state to this table rather than introducing a separate outbox is
deliberate: the UNIQUE(alert_key) insert IS the concurrency primitive that stops
two overlapping ingest runs double-firing. A second table would either duplicate
that key or need a two-table transaction at claim time.

status DEFAULT 'sent' means every pre-existing row reads as delivered with no
backfill, and any code path that still does a bare INSERT degrades to exactly
today's dedup-only behaviour rather than queueing a surprise replay.

Revision ID: 0004
Revises: 0003
Create Date: 2026-07-25
"""
from __future__ import annotations

from alembic import op

revision = "0004"
down_revision = "0003"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE alerts_sent
            ADD COLUMN status          TEXT NOT NULL DEFAULT 'sent'
                CHECK (status IN ('pending', 'sent', 'failed')),
            ADD COLUMN attempts        INTEGER NOT NULL DEFAULT 0,
            ADD COLUMN last_attempt_at TIMESTAMP,
            ADD COLUMN last_error      TEXT,
            ADD COLUMN payload         JSONB
    """)
    # Partial index: the drain only ever scans for pending rows, and in steady
    # state there are none, so a full index on status would be mostly dead weight.
    op.execute("""
        CREATE INDEX idx_alerts_sent_pending
            ON alerts_sent (id)
            WHERE status = 'pending'
    """)


def downgrade() -> None:
    op.execute("DROP INDEX IF EXISTS idx_alerts_sent_pending")
    op.execute("""
        ALTER TABLE alerts_sent
            DROP COLUMN IF EXISTS status,
            DROP COLUMN IF EXISTS attempts,
            DROP COLUMN IF EXISTS last_attempt_at,
            DROP COLUMN IF EXISTS last_error,
            DROP COLUMN IF EXISTS payload
    """)
