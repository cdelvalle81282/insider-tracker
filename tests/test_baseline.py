"""Unit tests for queries._batch_insider_baseline keying and prior-history window.

Two bugs are pinned here:
  1. Flags were keyed by (insider_cik, transaction_date), so an insider with
     several buys on one day had all but the last silently overwritten.
  2. `prior = trades[:i]` walked an index over date-ordered rows, so same-day
     trades counted as each other's prior history and produced gap_days == 0.

No PostgreSQL: the connection is a stub returning canned history rows.
"""
from __future__ import annotations

from datetime import date

import queries

CFG = {"min_prior_trades": 2, "size_multiplier": 3.0, "silence_days": 365}


class StubConn:
    """Returns the given history rows for the single history query."""

    def __init__(self, history: list[dict]):
        self.history = history
        self.queries: list[str] = []

    def execute(self, sql, params=None):
        self.queries.append(sql)
        rows = self.history
        return type("Cur", (), {"fetchall": staticmethod(lambda: rows)})()


def _hist(txid: str, day: str, value: float, cik: str = "C1") -> dict:
    return {
        "transaction_id": txid,
        "insider_cik": cik,
        "transaction_date": day,
        "total_value": value,
    }


def _row(txid: str, cik: str = "C1") -> dict:
    return {"transaction_id": txid, "insider_cik": cik, "transaction_code": "P"}


# Two small early buys, then two large buys on the SAME later day.
HISTORY = [
    _hist("t1", "2024-01-01", 100.0),
    _hist("t2", "2024-01-02", 100.0),
    _hist("t3", "2025-06-01", 1000.0),
    _hist("t4", "2025-06-01", 1000.0),
]
EXPECTED_GAP = (date(2025, 6, 1) - date(2024, 1, 2)).days


class TestSameDayTrades:
    def test_both_same_day_trades_get_their_own_flag(self):
        conn = StubConn(HISTORY)
        flags = queries._batch_insider_baseline(conn, [_row("t3"), _row("t4")], CFG)
        assert set(flags) == {"t3", "t4"}, "neither same-day buy may overwrite the other"

    def test_same_day_sibling_is_not_prior_history(self):
        """t4's prior window must stop before 2025-06-01, so it sees t1/t2 and a
        real gap, not t3 and a gap of zero."""
        conn = StubConn(HISTORY)
        flags = queries._batch_insider_baseline(conn, [_row("t3"), _row("t4")], CFG)
        assert flags["t4"]["gap_days"] == EXPECTED_GAP
        assert flags["t3"]["gap_days"] == EXPECTED_GAP
        assert flags["t4"]["gap_days"] > 0, "a same-day sibling produced gap_days == 0"

    def test_median_excludes_same_day_sibling(self):
        conn = StubConn(HISTORY)
        flags = queries._batch_insider_baseline(conn, [_row("t4")], CFG)
        # Prior is [100, 100]; including t3 (1000) would move the median.
        assert flags["t4"]["median_value"] == 100.0
        assert flags["t4"]["multiple"] == 10.0


class TestFlagContent:
    def test_size_and_silence_outliers_both_detected(self):
        conn = StubConn(HISTORY)
        flags = queries._batch_insider_baseline(conn, [_row("t3")], CFG)
        assert flags["t3"]["size_outlier"] is True
        assert flags["t3"]["silence_outlier"] is True

    def test_min_prior_trades_is_respected(self):
        """t2 has only one strictly-earlier trade, below min_prior_trades=2."""
        conn = StubConn(HISTORY)
        flags = queries._batch_insider_baseline(conn, [_row("t2")], CFG)
        assert "t2" not in flags

    def test_unremarkable_trade_is_not_flagged(self):
        history = [
            _hist("a", "2024-01-01", 100.0),
            _hist("b", "2024-02-01", 100.0),
            _hist("c", "2024-03-01", 110.0),
        ]
        conn = StubConn(history)
        flags = queries._batch_insider_baseline(conn, [_row("c")], CFG)
        assert flags == {}, "neither 3x size nor 365d silence"

    def test_rows_outside_result_set_get_no_flag(self):
        """History covers the insider's whole timeline, but only rows the caller
        actually rendered should come back."""
        conn = StubConn(HISTORY)
        flags = queries._batch_insider_baseline(conn, [_row("t3")], CFG)
        assert set(flags) == {"t3"}

    def test_no_p_code_rows_short_circuits(self):
        conn = StubConn(HISTORY)
        sells = [{"transaction_id": "s1", "insider_cik": "C1", "transaction_code": "S"}]
        assert queries._batch_insider_baseline(conn, sells, CFG) == {}
        assert conn.queries == [], "must not query when there is nothing to flag"


class TestHistoryQuery:
    def test_history_selects_and_orders_by_transaction_id(self):
        """transaction_id is now the flag key, and it is in ORDER BY so the
        same-day walk-back is deterministic."""
        conn = StubConn(HISTORY)
        queries._batch_insider_baseline(conn, [_row("t3")], CFG)
        sql = conn.queries[0]
        assert "SELECT transaction_id" in sql
        assert "ORDER BY insider_cik, transaction_date, transaction_id" in sql

    def test_history_excludes_superseded_and_joint_filers(self):
        conn = StubConn(HISTORY)
        queries._batch_insider_baseline(conn, [_row("t3")], CFG)
        sql = conn.queries[0]
        assert "superseded_by IS NULL" in sql
        assert "joint_filer_of IS NULL" in sql
