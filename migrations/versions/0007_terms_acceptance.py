"""Track Terms of Service acceptance per subscriber.

Subscriber launch needs a legal acceptance record before opening the
dashboard to paying users (see the ToS line item in CLAUDE.md's "Subscriber
launch" section). email is the natural key: it's the identity Wisepub SSO
carries, and the one already used to key watchlist ownership (0005) and
usage events (0006).

Composite-keyed on (email, version) rather than a single row per email: a
future Terms revision needs re-acceptance from someone who already agreed to
an older version, and the old row stays as an audit record instead of being
overwritten. Unlike usage_event, this is a legal record, not analytics — it
is never pruned.

Revision ID: 0007
Revises: 0006
Create Date: 2026-08-17
"""
from __future__ import annotations

from alembic import op

revision = "0007"
down_revision = "0006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE terms_acceptance (
            email       TEXT NOT NULL,
            version     TEXT NOT NULL,
            accepted_at TIMESTAMPTZ NOT NULL DEFAULT now(),
            ip_address  TEXT,
            PRIMARY KEY (email, version)
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS terms_acceptance")
