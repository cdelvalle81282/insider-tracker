"""Add 'congress_member' to watchlist type check constraint.

Revision ID: 0002
Revises: 0001
Create Date: 2026-06-04
"""
from __future__ import annotations

from alembic import op

revision = "0002"
down_revision = "0001"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("ALTER TABLE watchlist DROP CONSTRAINT watchlist_type_check")
    op.execute(
        "ALTER TABLE watchlist ADD CONSTRAINT watchlist_type_check "
        "CHECK (type = ANY (ARRAY['ticker'::text, 'insider'::text, 'congress_member'::text]))"
    )


def downgrade() -> None:
    op.execute("ALTER TABLE watchlist DROP CONSTRAINT watchlist_type_check")
    op.execute(
        "ALTER TABLE watchlist ADD CONSTRAINT watchlist_type_check "
        "CHECK (type = ANY (ARRAY['ticker'::text, 'insider'::text]))"
    )
