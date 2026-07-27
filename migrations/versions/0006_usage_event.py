"""Record what subscribers actually do with the app.

Nothing measured usage before this. With the dashboard about to go to paying
subscribers, "is anyone logging in, which tabs do they use, what are they
searching for that we do not cover" are all unanswerable, and they are the
questions that decide what to build next and whether a subscriber is getting
enough value to renew.

email is NOT NULL and staff requests are stored as the literal 'staff'. Staff
traffic would otherwise dominate every count during the editorial working day,
and a nullable column would make every query say "AND email IS NOT NULL" to mean
"subscribers".

meta is JSONB rather than columns because the interesting payload differs per
event: a search carries a query string, a watchlist add carries a ticker, a page
view carries nothing. Indexing it is deliberately skipped; these are read by a
staff dashboard a few times a week, not on a hot path.

Revision ID: 0006
Revises: 0005
Create Date: 2026-07-27
"""
from __future__ import annotations

from alembic import op

revision = "0006"
down_revision = "0005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE usage_event (
            id    BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            email TEXT NOT NULL,
            kind  TEXT NOT NULL,
            path  TEXT NOT NULL,
            ts    TIMESTAMPTZ NOT NULL DEFAULT now(),
            meta  JSONB
        )
    """)
    # "who is active lately" and "which paths are popular lately" are the two
    # questions the staff page asks, and the retention prune deletes by ts.
    op.execute("CREATE INDEX idx_usage_email_ts ON usage_event(email, ts DESC)")
    op.execute("CREATE INDEX idx_usage_path_ts ON usage_event(path, ts DESC)")
    op.execute("CREATE INDEX idx_usage_ts ON usage_event(ts)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS usage_event")
