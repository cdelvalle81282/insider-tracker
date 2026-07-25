"""Unit tests for the SQL conviction pre-score used to order the candidate pool.

A conviction sort cannot be done in SQL alone (the cluster bonus needs a batched
per-row distinct count, and the displayed reasons are built in Python), so the
pool is ordered by a SQL pre-score and then scored exactly in Python. The pool is
capped, and it used to be ordered by total_value, which is close to independent
of conviction: a small high-conviction CEO purchase could fall outside the cap
while large low-conviction rows were kept, so page one could be wrong on any
range matching more than the cap.

These tests pin the expression's shape and the per-side wiring. No PostgreSQL.
"""
from __future__ import annotations

from datetime import date

import config as cfg
import queries
from queries import EnrichContext

CONF = cfg.load_config()
KEYWORDS = CONF["alert_rules"]["insider_title_keywords"]


def _prescore(tiers=None, flags=None, keywords=None, max_score=10):
    return queries._conviction_prescore_sql(
        tiers if tiers is not None else CONF["conviction_tiers"],
        flags if flags is not None else CONF["conviction_flags"],
        keywords if keywords is not None else KEYWORDS,
        max_score,
    )


class TestPrescoreExpression:
    def test_placeholders_match_parameter_count(self):
        """A mismatch here is a runtime psycopg error on every conviction sort."""
        sql, params = _prescore()
        assert sql.count("%s") == len(params)

    def test_omits_the_cluster_bonus(self):
        """cluster_bonus is intentionally absent: duplicating the windowed
        distinct-count in SQL would mean maintaining the rule twice."""
        flags = dict(CONF["conviction_flags"])
        flags["cluster_bonus"] = 999
        sql, params = _prescore(flags=flags)
        assert 999 not in params

    def test_is_capped_like_the_python_scorer(self):
        sql, params = _prescore(max_score=7)
        assert sql.startswith("LEAST(")
        assert params[-1] == 7

    def test_value_tiers_collapse_to_one_case(self):
        """Only the highest matching tier may contribute, mirroring the Python
        loop's break. Separate additive CASEs would stack all three."""
        sql, _ = _prescore()
        assert sql.count("WHEN total_value >=") == 3
        value_case = sql.split("CASE", 1)[1]
        assert value_case.count("ELSE 0 END") >= 1

    def test_weights_come_from_config_not_hardcoded(self):
        flags = dict(CONF["conviction_flags"])
        flags["base_open_market_buy"] = 42
        _, params = _prescore(flags=flags)
        assert params[0] == 42

    def test_tier_points_come_from_config(self):
        tiers = {"value": [(5_000_000, 8, "value_over_5m")], "pct_holdings": []}
        _, params = _prescore(tiers=tiers)
        assert 8 in params

    def test_keyword_patterns_are_wrapped_for_ilike(self):
        sql, params = _prescore(keywords=["CEO"])
        assert "ILIKE ANY(%s)" in sql
        assert ["%CEO%"] in params

    def test_no_keywords_omits_the_ceo_term(self):
        sql, _ = _prescore(keywords=[])
        assert "ILIKE" not in sql

    def test_zero_weight_flags_are_omitted(self):
        """A disabled bonus should not add a dead CASE to every row."""
        flags = {k: 0 for k in CONF["conviction_flags"]}
        sql, _ = _prescore(flags=flags, keywords=[])
        assert "is_director" not in sql
        assert "is_ten_percent_owner" not in sql
        assert "COALESCE(is_10b5_1" not in sql

    def test_pct_expression_mirrors_the_python_helper(self):
        """_pct_holdings returns None for derivatives and for positions that did
        not already exist, so the SQL must exclude the same rows."""
        sql, _ = _prescore()
        assert "table_type <> 'D'" in sql
        assert "shares_owned_after > shares" in sql


class TestSortWiring:
    """get_filings_for_date must apply the pre-score to the P side only."""

    class Rec:
        def __init__(self):
            self.calls: list[tuple[str, list]] = []

        def execute(self, sql, params=None):
            self.calls.append((sql, list(params or [])))
            return type("Cur", (), {"fetchall": staticmethod(lambda: [])})()

    def _ctx(self, conn):
        return EnrichContext(
            conn=conn,
            conviction_flags=CONF["conviction_flags"],
            conviction_tiers=CONF["conviction_tiers"],
            conviction_max=CONF["conviction_max"],
            ceo_cfo_keywords=KEYWORDS,
            compute_conviction=True,
        )

    def _run(self, conn, sort_by):
        return queries.get_filings_for_date(
            conn, date(2026, 7, 24), transaction_codes=["P", "S"],
            sort_by=sort_by, ctx=self._ctx(conn), page_size=25,
        )

    def test_conviction_sort_orders_p_side_by_prescore(self):
        conn = self.Rec()
        self._run(conn, "conviction")
        p_sql = conn.calls[0][0]
        assert "ORDER BY LEAST(" in p_sql, "P side must rank by pre-score"
        assert "total_value DESC NULLS LAST" in p_sql, "value is the tiebreak"

    def test_conviction_sort_leaves_s_side_on_value(self):
        """Every non-P code scores 0, so a pre-score on sells is wasted work."""
        conn = self.Rec()
        self._run(conn, "conviction")
        s_sql = conn.calls[1][0]
        assert "LEAST(" not in s_sql
        assert s_sql.rstrip().endswith("LIMIT %s") or "ORDER BY total_value" in s_sql

    def test_value_sort_uses_no_prescore(self):
        conn = self.Rec()
        self._run(conn, "value")
        for sql, _ in conn.calls:
            assert "LEAST(" not in sql

    def test_prescore_params_precede_the_limit(self):
        """SQL order is WHERE, then ORDER BY, then LIMIT, so the bound params
        must be in that order or psycopg binds them to the wrong slots."""
        conn = self.Rec()
        self._run(conn, "conviction")
        p_sql, p_params = conn.calls[0]
        assert p_params[-1] == queries.CONVICTION_POOL_CAP
        # The cap param is last; the pre-score's own max_score sits just before
        # it, since LEAST(..., max_score) closes the ORDER BY expression.
        assert p_params[-2] == CONF["conviction_max"]

    def test_conviction_sort_uses_the_pool_cap_not_page_size(self):
        conn = self.Rec()
        self._run(conn, "conviction")
        assert queries.CONVICTION_POOL_CAP in conn.calls[0][1]

    def test_missing_ctx_falls_back_without_crashing(self):
        """Export and backtest paths call this with ctx=None."""
        conn = self.Rec()
        queries.get_filings_for_date(
            conn, date(2026, 7, 24), transaction_codes=["P"],
            sort_by="conviction", ctx=None, page_size=25,
        )
        assert "LEAST(" not in conn.calls[0][0]
        assert "ORDER BY total_value" in conn.calls[0][0]
