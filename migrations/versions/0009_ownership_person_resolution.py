"""Resolve 13D reporting persons that carry no CIK.

About a third of 13D reporting-person rows have no CIK (entities, and some
individuals), so a pure CIK join to Form 4 insiders misses them. The raw
`person_cik` is left exactly as filed; the resolver writes its best match into
a separate `resolved_cik`, with `resolved_via` recording which rule produced it.
Keeping them apart means a bad rule can be rolled back with one UPDATE and the
filing data is never rewritten.

`person_alias` is the manual override: exact `person_name` as it appears in the
filing, mapped to a CIK. It is how entity chains (a holding company that files
without a CIK on behalf of a known individual) get attached by hand.

Revision ID: 0009
Revises: 0008
Create Date: 2026-10-15
"""
from __future__ import annotations

from alembic import op

revision = "0009"
down_revision = "0008"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        ALTER TABLE ownership_filing_person
            ADD COLUMN resolved_cik TEXT,
            ADD COLUMN resolved_via TEXT
    """)
    op.execute(
        "CREATE INDEX idx_ofp_resolved_cik ON ownership_filing_person(resolved_cik)"
        " WHERE resolved_cik IS NOT NULL"
    )
    op.execute("""
        CREATE TABLE person_alias (
            person_name TEXT PRIMARY KEY,
            person_cik  TEXT NOT NULL,
            note        TEXT,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS person_alias")
    op.execute("DROP INDEX IF EXISTS idx_ofp_resolved_cik")
    op.execute("""
        ALTER TABLE ownership_filing_person
            DROP COLUMN IF EXISTS resolved_via,
            DROP COLUMN IF EXISTS resolved_cik
    """)
