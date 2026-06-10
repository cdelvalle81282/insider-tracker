"""
fix_corrupt_prices.py — Correct price_per_share corruption in the filings table.

Bug: for certain transactions, the parser stored (shares × actual_price) in
price_per_share instead of actual_price.  This makes:
  - price_per_share  = total transaction value  (should be per-share price)
  - total_value      = shares²  × actual_price  (completely wrong)

Fix:
  corrected_price_per_share  = stored_price_per_share / shares
  corrected_total_value      = stored_price_per_share   (it WAS the total)

Rows where the corrected price is implausible (shares=0 or still >$100K) are
NULLed out rather than written with a bad number.

Usage:
  python fix_corrupt_prices.py            # dry-run, prints summary
  python fix_corrupt_prices.py --dry-run  # explicit dry-run
  python fix_corrupt_prices.py --apply    # execute both UPDATEs
"""

import argparse
import sys

from dotenv import load_dotenv

from db import get_cli_db

load_dotenv()

# ---------------------------------------------------------------------------
# Stocks whose high share prices are legitimate — never touch these rows.
# ---------------------------------------------------------------------------
LEGITIMATE_HIGH_PRICE = {
    "BRK.A",   # ~$730K/share
    "BRK.B",   # ~$490/share
    "NVR",     # ~$7-8K/share
    "BKNG",    # ~$4-6K/share
    "AZO",     # ~$3-4K/share
    "FICO",    # ~$1.2-2.2K/share
    "MELI",    # ~$1.7-2K/share
    "FIX",     # ~$1-2K/share
    "MKL",     # ~$1.8-2.1K/share
    "FCNCA",   # ~$1-2K/share
    "WTM",     # ~$2K/share
    "MNTR",    # uncertain — exclude to be safe
    "ECDA",    # uncertain — exclude to be safe
    "FROG",    # uncertain — exclude to be safe
    # Legitimate high-priced stocks discovered after initial run
    "TPL",     # Texas Pacific Land ~$1,000-1,400/share
    "EQIX",    # Equinix ~$900-1,100/share
    "GWW",     # W.W. Grainger ~$1,000-1,100/share
    "TDG",     # TransDigm Group ~$1,200-1,400/share
}

# SQL placeholder list for the exclusion set.
# psycopg3 uses %s placeholders; we build one per ticker.
_EXCLUDE_PLACEHOLDERS = ", ".join(["%s"] * len(LEGITIMATE_HIGH_PRICE))
_EXCLUDE_PARAMS = tuple(sorted(LEGITIMATE_HIGH_PRICE))

# ---------------------------------------------------------------------------
# SQL — shared WHERE predicate parts (excluding the phase-specific condition)
# ---------------------------------------------------------------------------
_COMMON_WHERE = f"""
    price_per_share > 1000
    AND issuer_ticker NOT IN ({_EXCLUDE_PLACEHOLDERS})
    AND superseded_by IS NULL
    AND transaction_code IN ('P', 'S')
    AND table_type = 'ND'
"""

# Phase 1: rows where we CAN compute a plausible corrected price
_PHASE1_EXTRA = """
    AND shares > 0
    AND shares IS NOT NULL
    AND price_per_share / shares BETWEEN 0.001 AND 100000
"""

# Phase 2: rows where we CANNOT — zero/null shares, or corrected price still absurd
_PHASE2_EXTRA = """
    AND (
        shares = 0
        OR shares IS NULL
        OR price_per_share / shares > 100000
    )
"""

# ---------------------------------------------------------------------------
# SELECT queries for dry-run preview
# ---------------------------------------------------------------------------
_DRY_RUN_COUNT_SQL = "SELECT COUNT(*) AS n FROM filings WHERE" + _COMMON_WHERE + _PHASE1_EXTRA
_DRY_RUN_SAMPLE_SQL = """
    SELECT
        issuer_ticker,
        insider_name,
        transaction_date,
        shares,
        price_per_share,
        price_per_share / shares AS corrected_price
    FROM filings
    WHERE
""" + _COMMON_WHERE + _PHASE1_EXTRA + """
    ORDER BY price_per_share DESC
    LIMIT 10
"""

_DRY_RUN_NULL_COUNT_SQL = "SELECT COUNT(*) AS n FROM filings WHERE" + _COMMON_WHERE + _PHASE2_EXTRA
_DRY_RUN_NULL_SAMPLE_SQL = """
    SELECT
        issuer_ticker,
        insider_name,
        transaction_date,
        shares,
        price_per_share
    FROM filings
    WHERE
""" + _COMMON_WHERE + _PHASE2_EXTRA + """
    ORDER BY price_per_share DESC
    LIMIT 10
"""

# ---------------------------------------------------------------------------
# UPDATE queries for --apply mode
# ---------------------------------------------------------------------------
_PHASE1_UPDATE_SQL = """
    UPDATE filings
    SET
        total_value     = price_per_share,
        price_per_share = price_per_share / shares
    WHERE
""" + _COMMON_WHERE + _PHASE1_EXTRA

_PHASE2_UPDATE_SQL = """
    UPDATE filings
    SET
        price_per_share = NULL,
        total_value     = NULL
    WHERE
""" + _COMMON_WHERE + _PHASE2_EXTRA

# Verification query: how many >$1000 rows remain outside the exclusion list?
_VERIFY_SQL = f"""
    SELECT COUNT(*) AS n FROM filings
    WHERE price_per_share > 1000
      AND issuer_ticker NOT IN ({_EXCLUDE_PLACEHOLDERS})
      AND superseded_by IS NULL
      AND transaction_code IN ('P', 'S')
      AND table_type = 'ND'
"""


def _fmt_price(val: float | None) -> str:
    if val is None:
        return "NULL"
    return f"${val:,.2f}"


def _fmt_shares(val: float | None) -> str:
    if val is None:
        return "NULL"
    return f"{val:,.0f}"


def run_dry_run(conn) -> None:
    print("DRY RUN — no changes applied. Pass --apply to execute.\n")

    # ----- Phase 1 -----
    row = conn.execute(_DRY_RUN_COUNT_SQL, _EXCLUDE_PARAMS).fetchone()
    phase1_count = row["n"]
    print(f"Phase 1 (would correct): {phase1_count:,} rows")

    if phase1_count > 0:
        print("  Sample corrections:")
        rows = conn.execute(_DRY_RUN_SAMPLE_SQL, _EXCLUDE_PARAMS).fetchall()
        for r in rows:
            print(
                f"    {r['issuer_ticker'] or 'N/A':8s} | {(r['insider_name'] or '')[:35]:35s} | "
                f"{str(r['transaction_date']):10s} | "
                f"shares={_fmt_shares(r['shares']):>15s} | "
                f"old_price={_fmt_price(r['price_per_share']):>18s} | "
                f"new_price={_fmt_price(r['corrected_price']):>12s} | "
                f"new_total={_fmt_price(r['price_per_share']):>18s}"
            )
    print()

    # ----- Phase 2 -----
    row = conn.execute(_DRY_RUN_NULL_COUNT_SQL, _EXCLUDE_PARAMS).fetchone()
    phase2_count = row["n"]
    print(f"Phase 2 (would nullify): {phase2_count:,} rows")

    if phase2_count > 0:
        print("  Sample:")
        rows = conn.execute(_DRY_RUN_NULL_SAMPLE_SQL, _EXCLUDE_PARAMS).fetchall()
        for r in rows:
            print(
                f"    {r['issuer_ticker'] or 'N/A':8s} | {(r['insider_name'] or '')[:35]:35s} | "
                f"{str(r['transaction_date']):10s} | "
                f"shares={_fmt_shares(r['shares']):>15s} | "
                f"old_price={_fmt_price(r['price_per_share']):>18s}"
            )
    print()

    total = phase1_count + phase2_count
    print(f"Total rows affected (dry-run): {total:,}")


def run_apply(conn) -> None:
    print("Applying corrections in a single transaction...\n")

    with conn.transaction():
        cur1 = conn.execute(_PHASE1_UPDATE_SQL, _EXCLUDE_PARAMS)
        phase1_updated = cur1.rowcount

        cur2 = conn.execute(_PHASE2_UPDATE_SQL, _EXCLUDE_PARAMS)
        phase2_nulled = cur2.rowcount

    print(f"Phase 1 (corrected):  {phase1_updated:,} rows updated")
    print(f"Phase 2 (nullified):  {phase2_nulled:,} rows nulled")
    print(f"Total rows modified:  {phase1_updated + phase2_nulled:,}")
    print()

    # Verify no corrupt rows remain outside the exclusion list
    row = conn.execute(_VERIFY_SQL, _EXCLUDE_PARAMS).fetchone()
    remaining = row["n"]
    if remaining == 0:
        print("Verification passed: 0 rows with price_per_share > 1000 remain outside exclusion list.")
    else:
        print(
            f"WARNING: {remaining:,} rows with price_per_share > 1000 still remain outside "
            "the exclusion list. Manual review required."
        )
        sys.exit(1)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fix corrupt price_per_share values in the filings table."
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument(
        "--dry-run",
        action="store_true",
        default=True,
        help="Preview changes without modifying the database (default).",
    )
    mode.add_argument(
        "--apply",
        action="store_true",
        default=False,
        help="Execute both UPDATE phases and commit.",
    )
    args = parser.parse_args()

    # --apply overrides the dry-run default
    apply_mode = args.apply

    conn = get_cli_db()
    try:
        if apply_mode:
            run_apply(conn)
        else:
            run_dry_run(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
