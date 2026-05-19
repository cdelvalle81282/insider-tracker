"""initial schema — full PostgreSQL translation of the SQLite tables.

Includes everything that the legacy ingest._migrate() added incrementally
over time (superseded_by, sector, joint_filer_of, run_kind, ticker_metadata
last_close columns, etc.). This is the authoritative schema for PG.

Revision ID: 0001
Revises:
Create Date: 2026-05-19

"""
from __future__ import annotations

from alembic import op

# revision identifiers, used by Alembic.
revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # ---------------- filings ----------------
    op.execute("""
        CREATE TABLE filings (
            transaction_id        TEXT PRIMARY KEY,
            accession_no          TEXT NOT NULL,
            filed_at              TIMESTAMP NOT NULL,
            form_type             TEXT NOT NULL,

            issuer_cik            TEXT NOT NULL,
            issuer_name           TEXT NOT NULL,
            issuer_ticker         TEXT,

            insider_cik           TEXT NOT NULL,
            insider_name          TEXT NOT NULL,
            insider_title         TEXT,
            is_director           INTEGER NOT NULL DEFAULT 0,
            is_officer            INTEGER NOT NULL DEFAULT 0,
            is_ten_percent_owner  INTEGER NOT NULL DEFAULT 0,
            is_other              INTEGER NOT NULL DEFAULT 0,

            transaction_date      DATE NOT NULL,
            transaction_code      TEXT NOT NULL,
            equity_swap           INTEGER NOT NULL DEFAULT 0,
            table_type            TEXT NOT NULL,

            shares                REAL NOT NULL DEFAULT 0,
            price_per_share       REAL,
            total_value           REAL,

            shares_owned_after    REAL,
            ownership_type        TEXT,

            is_10b5_1             INTEGER NOT NULL DEFAULT 0,
            footnote_text         TEXT,
            raw_xml_url           TEXT NOT NULL,
            ingested_at           TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,

            -- added later by _migrate() in the SQLite era
            superseded_by         TEXT,
            sector                TEXT,
            joint_filer_of        TEXT
        )
    """)
    op.execute("CREATE INDEX idx_filed_at      ON filings(filed_at)")
    op.execute("CREATE INDEX idx_accession     ON filings(accession_no)")
    op.execute("CREATE INDEX idx_ticker        ON filings(issuer_ticker)")
    op.execute("CREATE INDEX idx_insider       ON filings(insider_cik)")
    op.execute("CREATE INDEX idx_tx_code       ON filings(transaction_code)")
    op.execute("CREATE INDEX idx_issuer_cik    ON filings(issuer_cik)")
    op.execute("CREATE INDEX idx_tx_date       ON filings(transaction_date)")
    op.execute("CREATE INDEX idx_superseded    ON filings(superseded_by)")
    op.execute("CREATE INDEX idx_sector        ON filings(sector)")
    op.execute("CREATE INDEX idx_joint_filer   ON filings(joint_filer_of)")
    # functional index on DATE(filed_at) — PG equivalent of the SQLite expression index
    op.execute("CREATE INDEX idx_filed_date    ON filings((filed_at::date))")

    # ---------------- run_log ----------------
    op.execute("""
        CREATE TABLE run_log (
            id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            started_at      TIMESTAMP NOT NULL,
            finished_at     TIMESTAMP,
            date_processed  TEXT NOT NULL,
            filings_found   INTEGER DEFAULT 0,
            rows_inserted   INTEGER DEFAULT 0,
            errors          INTEGER DEFAULT 0,
            error_detail    TEXT,
            run_kind        TEXT DEFAULT 'unknown'
        )
    """)

    # ---------------- alerts_sent ----------------
    op.execute("""
        CREATE TABLE alerts_sent (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            alert_key   TEXT NOT NULL UNIQUE,
            alert_type  TEXT NOT NULL,
            sent_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
    """)

    # ---------------- sectors ----------------
    op.execute("""
        CREATE TABLE sectors (
            issuer_cik  TEXT PRIMARY KEY,
            sic_code    TEXT,
            sic_desc    TEXT,
            sector      TEXT,
            fetched_at  TEXT
        )
    """)
    op.execute("CREATE INDEX idx_sectors_sic ON sectors(sic_code)")

    # ---------------- watchlist ----------------
    op.execute("""
        CREATE TABLE watchlist (
            id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            type        TEXT NOT NULL CHECK (type IN ('ticker', 'insider')),
            value       TEXT NOT NULL,
            label       TEXT,
            created_at  TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE (type, value)
        )
    """)

    # ---------------- ticker_metadata ----------------
    op.execute("""
        CREATE TABLE ticker_metadata (
            ticker        TEXT PRIMARY KEY,
            has_options   INTEGER,
            market_cap    REAL,
            fetched_at    TEXT,
            last_close    REAL,
            last_close_at TEXT
        )
    """)
    op.execute("CREATE INDEX idx_tm_market_cap ON ticker_metadata(market_cap)")

    # ---------------- congress_trades ----------------
    op.execute("""
        CREATE TABLE congress_trades (
            id                BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            source            TEXT NOT NULL,
            transaction_id    TEXT NOT NULL UNIQUE,
            politician_name   TEXT NOT NULL,
            chamber           TEXT NOT NULL,
            party             TEXT,
            state             TEXT,
            ticker            TEXT,
            asset_description TEXT,
            transaction_type  TEXT,
            transaction_date  TEXT,
            disclosure_date   TEXT,
            amount_min        REAL,
            amount_max        REAL,
            amount_label      TEXT,
            raw_url           TEXT,
            ingested_at       TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    """)
    op.execute("CREATE INDEX idx_ct_ticker           ON congress_trades(ticker)")
    op.execute("CREATE INDEX idx_ct_disclosure_date  ON congress_trades(disclosure_date)")
    op.execute("CREATE INDEX idx_ct_politician       ON congress_trades(politician_name)")
    op.execute("CREATE INDEX idx_ct_source           ON congress_trades(source)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS congress_trades")
    op.execute("DROP TABLE IF EXISTS ticker_metadata")
    op.execute("DROP TABLE IF EXISTS watchlist")
    op.execute("DROP TABLE IF EXISTS sectors")
    op.execute("DROP TABLE IF EXISTS alerts_sent")
    op.execute("DROP TABLE IF EXISTS run_log")
    op.execute("DROP TABLE IF EXISTS filings")
