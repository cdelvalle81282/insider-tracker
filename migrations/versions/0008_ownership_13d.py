"""Schedule 13D beneficial-ownership filings.

Form 4 only fires when someone is an officer, director or 10% owner of the
issuer, so an outside investor's accumulation of a stake (5% to 10%) is
invisible to the `filings` table. Schedule 13D is where that shows up.

Two tables rather than one because a 13D routinely lists several reporting
persons (a holding company plus the individual who controls it), each with
their own share count. One row per person is what makes the CIK join to
`filings.insider_cik` possible.

13D only for now. 13G is passive and high volume; it can reuse these tables
(`schedule` column) if it is ever added.

accession_no is the primary key of `ownership_filing`. EDGAR lists every 13D
twice in its index (once under the subject company, once under each filer), so
the ingester dedupes on accession and this key is the backstop.

Revision ID: 0008
Revises: 0007
Create Date: 2026-10-14
"""
from __future__ import annotations

from alembic import op

revision = "0008"
down_revision = "0007"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE ownership_filing (
            accession_no        TEXT PRIMARY KEY,
            schedule            TEXT NOT NULL DEFAULT '13D',
            form_type           TEXT NOT NULL,
            is_amendment        BOOLEAN NOT NULL DEFAULT FALSE,
            amendment_no        INTEGER,
            previous_accession  TEXT,
            filed_at            DATE NOT NULL,
            event_date          DATE,
            filer_cik           TEXT,
            issuer_cik          TEXT NOT NULL,
            issuer_name         TEXT,
            issuer_cusip        TEXT,
            issuer_ticker       TEXT,
            class_title         TEXT,
            funds_source        TEXT,
            purpose_text        TEXT,
            raw_xml_url         TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE TABLE ownership_filing_person (
            accession_no        TEXT NOT NULL
                REFERENCES ownership_filing(accession_no) ON DELETE CASCADE,
            seq                 INTEGER NOT NULL,
            person_cik          TEXT,
            person_name         TEXT NOT NULL,
            no_cik              BOOLEAN NOT NULL DEFAULT FALSE,
            member_of_group     TEXT,
            fund_type           TEXT,
            person_type         TEXT,
            sole_voting         NUMERIC,
            shared_voting       NUMERIC,
            sole_dispositive    NUMERIC,
            shared_dispositive  NUMERIC,
            aggregate_owned     NUMERIC,
            pct_class           NUMERIC,
            comment_text        TEXT,
            PRIMARY KEY (accession_no, seq)
        )
    """)
    # The two access paths: "what does this person hold" and "who holds this issuer".
    op.execute("CREATE INDEX idx_ofp_person_cik ON ownership_filing_person(person_cik)")
    op.execute("CREATE INDEX idx_of_issuer_event ON ownership_filing(issuer_cik, event_date)")
    op.execute("CREATE INDEX idx_of_filed_at ON ownership_filing(filed_at)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS ownership_filing_person")
    op.execute("DROP TABLE IF EXISTS ownership_filing")
