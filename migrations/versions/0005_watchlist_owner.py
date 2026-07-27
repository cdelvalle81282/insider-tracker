"""Give the watchlist an owner, so subscribers can have their own.

The table was built for a single-tenant editorial tool: one row per watched
thing, UNIQUE (type, value), no notion of who was watching. That is fine while
the only users are staff. It stops working the moment paying subscribers get a
login and are told they can build a watchlist, because they would all be editing
the same rows and deleting each other's.

'house' rather than NULL for the editorial list, for two reasons. Postgres
treats NULLs as distinct in a unique constraint, so a nullable owner would
silently allow duplicate house rows for the same ticker. And NOT NULL DEFAULT
'house' means every existing row becomes the house list with no backfill step
and no window where a row has no owner.

The unique constraint has to be replaced rather than added to: UNIQUE
(type, value) would stop two different subscribers from watching the same
ticker, which is the entire point of the feature.

Revision ID: 0005
Revises: 0004
Create Date: 2026-07-27
"""
from __future__ import annotations

from alembic import op

revision = "0005"
down_revision = "0004"
branch_labels = None
depends_on = None

# The editorial list. Alerts, the dashboard's staff view, and the auto-add in
# load_insider_profiles.py all pin to this value; a subscriber can never write
# it, because the owner is derived from the session and never from form input.
HOUSE = "house"


def upgrade() -> None:
    op.execute(f"""
        ALTER TABLE watchlist
            ADD COLUMN owner TEXT NOT NULL DEFAULT '{HOUSE}'
    """)
    op.execute("ALTER TABLE watchlist DROP CONSTRAINT watchlist_type_value_key")
    op.execute("""
        ALTER TABLE watchlist
            ADD CONSTRAINT watchlist_owner_type_value_key UNIQUE (owner, type, value)
    """)
    # Every per-subscriber read filters on owner, so this is the access path,
    # not a nicety.
    op.execute("CREATE INDEX idx_watchlist_owner ON watchlist(owner)")


def downgrade() -> None:
    # Subscriber rows have to go before the old constraint can come back: two
    # subscribers watching the same ticker is legal now and would violate
    # UNIQUE (type, value). Deleting them is the only honest downgrade, and it
    # is why this migration is worth not reversing casually.
    op.execute(f"DELETE FROM watchlist WHERE owner <> '{HOUSE}'")
    op.execute("DROP INDEX IF EXISTS idx_watchlist_owner")
    op.execute("ALTER TABLE watchlist DROP CONSTRAINT watchlist_owner_type_value_key")
    op.execute("ALTER TABLE watchlist ADD CONSTRAINT watchlist_type_value_key UNIQUE (type, value)")
    op.execute("ALTER TABLE watchlist DROP COLUMN owner")
