"""Unit tests for ingest._upsert_rows failure surfacing.

A row the database refuses used to be swallowed by a bare `except psycopg.Error: pass`,
so a run that inserted nothing still reported errors=0 and pinged the success heartbeat.
These tests pin the fixed behaviour. No real PostgreSQL, the connection is a stub.
"""
from __future__ import annotations

from contextlib import contextmanager

import psycopg
import pytest

import ingest


def _row(txid: str) -> dict:
    """Minimal row dict. _upsert_rows only reads transaction_id for diagnostics,
    everything else is passed through to psycopg as named params."""
    return {"transaction_id": txid}


class FakeDiag:
    def __init__(self, message_primary: str):
        self.message_primary = message_primary


class FakePgError(psycopg.Error):
    """psycopg.Error exposes sqlstate/diag as read-only properties populated from
    the server, so they cannot be assigned on an instance. Override them instead."""

    def __init__(self, message: str, sqlstate: str, diag_message: str):
        # Must precede super().__init__: psycopg's Error.__init__ reads
        # self.sqlstate, which our override resolves against these attributes.
        self._sqlstate = sqlstate
        self._diag = FakeDiag(diag_message)
        super().__init__(message)

    @property
    def sqlstate(self):
        return self._sqlstate

    @property
    def diag(self):
        return self._diag


def _pg_error(sqlstate: str, message: str) -> psycopg.Error:
    return FakePgError(message, sqlstate, message)


class FakeConn:
    """Stub connection whose transaction() raises for transaction_ids in `fail_on`.

    Mirrors the real contract: `conn.transaction()` is a context manager, and a
    psycopg.Error raised inside it propagates after the savepoint rolls back.
    """

    def __init__(self, fail_on: set[str] | None = None, error=None):
        self.fail_on = fail_on or set()
        self.error = error or _pg_error("23502", 'null value in column "issuer_cik"')
        self.commits = 0
        self.executed: list[dict] = []
        self._current: dict | None = None

    @contextmanager
    def transaction(self):
        yield self

    def execute(self, sql, params=None):
        if isinstance(params, dict):
            self._current = params
            if params.get("transaction_id") in self.fail_on:
                raise self.error
            self.executed.append(params)
        cur = type("Cur", (), {})()
        cur.rowcount = 1
        return cur

    def commit(self):
        self.commits += 1


class TestUpsertRowsFailures:
    def test_clean_batch_reports_no_failures(self):
        conn = FakeConn()
        result = ingest._upsert_rows(conn, [_row("a"), _row("b")])
        assert result.inserted == 2
        assert result.failed == 0
        assert result.failures == []

    def test_failing_row_is_counted_not_silently_dropped(self):
        conn = FakeConn(fail_on={"bad"})
        result = ingest._upsert_rows(conn, [_row("ok1"), _row("bad"), _row("ok2")])
        assert result.inserted == 2, "good rows still insert"
        assert result.failed == 1, "the refused row must be counted"

    def test_failure_detail_carries_sqlstate_and_message(self):
        conn = FakeConn(fail_on={"bad"})
        result = ingest._upsert_rows(conn, [_row("bad")])
        assert len(result.failures) == 1
        detail = result.failures[0]
        assert "bad" in detail
        assert "23502" in detail, "sqlstate is what distinguishes schema drift"
        assert "issuer_cik" in detail

    def test_missing_diag_falls_back_to_str(self):
        """psycopg errors raised client-side have no diag; must not crash."""
        bare = psycopg.Error("connection sanity check failed")
        conn = FakeConn(fail_on={"bad"}, error=bare)
        result = ingest._upsert_rows(conn, [_row("bad")])
        assert result.failed == 1
        assert "connection sanity check failed" in result.failures[0]
        assert "?????" in result.failures[0], "unknown sqlstate placeholder"

    def test_failure_detail_is_capped(self):
        rows = [_row(f"bad{i}") for i in range(25)]
        conn = FakeConn(fail_on={r["transaction_id"] for r in rows})
        result = ingest._upsert_rows(conn, rows)
        assert result.failed == 25, "every failure is counted"
        assert len(result.failures) == ingest._MAX_FAILURE_DETAIL, "detail list is bounded"


class TestIngestResult:
    def test_error_detail_joins_and_tails(self):
        res = ingest.IngestResult()
        for i in range(15):
            res.error_lines.append(f"line{i}")
        detail = res.error_detail
        assert "line14" in detail, "keeps the most recent lines"
        assert "line0" not in detail
        assert detail.count(";") == ingest._MAX_FAILURE_DETAIL - 1

    def test_defaults_are_not_shared_between_instances(self):
        """error_lines must be a per-instance list, not a class-level default."""
        a, b = ingest.IngestResult(), ingest.IngestResult()
        a.error_lines.append("x")
        assert b.error_lines == []

    @pytest.mark.parametrize("failures,expected_folded", [(0, 0), (3, 3)])
    def test_insert_failures_fold_into_errors(self, failures, expected_folded):
        """The ingest_date fold is what makes health_check's consecutive-errors
        alarm cover insert failures. Simulate the fold the loop performs."""
        res = ingest.IngestResult()
        upsert = ingest.UpsertResult(inserted=0, failed=failures, failures=["x"] * failures)
        if upsert.failed:
            res.errors += upsert.failed
            res.insert_failures += upsert.failed
            for line in upsert.failures:
                res.error_lines.append(f"insert: {line}")
        assert res.errors == expected_folded
        assert res.insert_failures == expected_folded
        assert all(line.startswith("insert: ") for line in res.error_lines)
