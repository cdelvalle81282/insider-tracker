"""Unit tests for queries.get_cluster_activity predicate consistency.

The aggregate query excluded superseded rows and joint-filer duplicates; the
per-transaction follow-up did not, and correlated on the ticker string rather
than issuer_cik. A card could therefore display transactions that were excluded
from its own insider_count / tx_count / total_value, and
enrich_clusters_with_quality rated the cluster off that inflated insider set.

Both queries now share one WHERE fragment. No PostgreSQL: the connection records
the SQL it is handed.
"""
from __future__ import annotations

from datetime import date

import queries


class RecordingConn:
    """Records each query, returning aggregate rows first then transaction rows."""

    def __init__(self, agg_rows: list[dict], tx_rows: list[dict]):
        self._responses = [agg_rows, tx_rows]
        self.calls: list[tuple[str, list]] = []

    def execute(self, sql, params=None):
        self.calls.append((sql, list(params or [])))
        rows = self._responses.pop(0) if self._responses else []
        return type("Cur", (), {"fetchall": staticmethod(lambda: rows)})()


AGG = [{
    "issuer_cik": "0000320193",
    "issuer_ticker": "AAPL",
    "issuer_name": "Apple Inc",
    "sector": "Tech",
    "direction": "buy",
    "insider_count": 3,
    "tx_count": 3,
    "total_value": 3000.0,
    "insider_names": "A, B, C",
    "insider_titles": "CEO, CFO, Dir",
}]
TX = [
    {"transaction_id": "x1", "insider_cik": "I1", "insider_name": "A",
     "insider_title": "CEO", "transaction_code": "P", "shares": 10,
     "price_per_share": 100.0, "total_value": 1000.0, "is_10b5_1": 0,
     "issuer_cik": "0000320193"},
]


def _run(conn):
    return queries.get_cluster_activity(conn, date(2026, 7, 24), min_insiders=2)


class TestSharedPredicate:
    def test_both_queries_exclude_superseded_and_joint_filers(self):
        conn = RecordingConn(AGG, TX)
        _run(conn)
        assert len(conn.calls) == 2, "aggregate plus follow-up"
        for sql, _ in conn.calls:
            assert "superseded_by IS NULL" in sql
            assert "joint_filer_of IS NULL" in sql

    def test_both_queries_apply_the_same_code_filter(self):
        conn = RecordingConn(AGG, TX)
        _run(conn)
        agg_sql, tx_sql = conn.calls[0][0], conn.calls[1][0]
        assert "transaction_code IN" in agg_sql
        assert "transaction_code IN" in tx_sql

    def test_followup_correlates_on_issuer_cik_not_ticker(self):
        """Two issuers can share a ticker string after a rename or reuse, which
        merged their filings into one card."""
        conn = RecordingConn(AGG, TX)
        _run(conn)
        tx_sql = conn.calls[1][0]
        assert "issuer_cik IN" in tx_sql
        assert "issuer_ticker IN" not in tx_sql

    def test_aggregate_groups_by_issuer_cik(self):
        conn = RecordingConn(AGG, TX)
        _run(conn)
        agg_sql = conn.calls[0][0]
        assert "GROUP BY issuer_cik" in agg_sql
        # Ticker and name still surface for the template, via aggregates.
        assert "MAX(issuer_ticker)" in agg_sql
        assert "MAX(issuer_name)" in agg_sql


class TestParameterOrder:
    def test_followup_params_end_with_the_cluster_ciks(self):
        """base_params come first, then the IN list, matching the SQL order."""
        conn = RecordingConn(AGG, TX)
        _run(conn)
        _, tx_params = conn.calls[1]
        assert tx_params[-1] == "0000320193"

    def test_aggregate_params_end_with_min_insiders(self):
        conn = RecordingConn(AGG, TX)
        _run(conn)
        _, agg_params = conn.calls[0]
        assert agg_params[-1] == 2


class TestResultShape:
    def test_transactions_are_bucketed_by_cik(self):
        conn = RecordingConn(AGG, TX)
        result = _run(conn)
        assert len(result) == 1
        assert [t["transaction_id"] for t in result[0]["transactions"]] == ["x1"]

    def test_template_fields_survive_the_regroup(self):
        conn = RecordingConn(AGG, TX)
        result = _run(conn)
        card = result[0]
        # _clusters_partial.html reads these three.
        assert card["issuer_ticker"] == "AAPL"
        assert card["issuer_name"] == "Apple Inc"
        assert "transactions" in card
        assert card["total_value_fmt"]

    def test_empty_aggregate_skips_the_followup_query(self):
        conn = RecordingConn([], TX)
        assert _run(conn) == []
        assert len(conn.calls) == 1, "no point fetching transactions for zero clusters"
