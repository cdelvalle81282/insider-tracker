"""Ownership rules for per-subscriber watchlists.

The watchlist was a single global table until 2026-07-27: UNIQUE (type, value),
no owner. Opening the app to paying subscribers means many people editing what
used to be one editorial list, so every read and write now carries an owner.

The interesting failures here are all silent ones. A missing owner filter does
not raise; it just serves or edits the wrong person's data, and every test
written from one user's point of view still passes. So these tests are written
from two users' point of view.

No live PostgreSQL: connections are MagicMocks and the assertions are on the SQL
and parameters actually issued.
"""
from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from fastapi.testclient import TestClient

import app as app_module
import queries
import security
from db import get_request_db

USER = "tester"
PASSWORD = "s3cret"
ALICE = "alice@example.com"
BOB = "bob@example.com"


@pytest.fixture(autouse=True)
def auth_env(monkeypatch):
    monkeypatch.setenv("BASIC_AUTH_USER", USER)
    monkeypatch.setenv("BASIC_AUTH_PASSWORD", PASSWORD)
    monkeypatch.setenv("SECRET_KEY", "0" * 64)


@pytest.fixture(autouse=True)
def stub_redis():
    """Keep invalidate_owner_cache on its real code path without real Redis.

    A watchlist write ends in a cache invalidation, and on a machine with no
    Redis each of those paid a ~15s connection timeout, which alone made this
    module take 45 seconds. Mocking the client rather than the function means
    the invalidation logic still runs and is still asserted on.
    """
    import cache
    original = cache._redis
    cache._redis = MagicMock()
    yield cache._redis
    cache._redis = original


@pytest.fixture
def db():
    conn = MagicMock()
    conn.autocommit = True
    conn.execute.return_value.fetchall.return_value = []
    conn.execute.return_value.fetchone.return_value = {"n": 0}
    conn.execute.return_value.rowcount = 1
    return conn


def _subscriber(monkeypatch, email: str):
    monkeypatch.setattr(
        security, "sso_session", lambda _req: {"email": email, "staff": False}
    )


def _client(db_conn) -> TestClient:
    app_module.app.dependency_overrides[get_request_db] = lambda: db_conn
    return TestClient(app_module.app)


@pytest.fixture(autouse=True)
def _clear_overrides():
    yield
    app_module.app.dependency_overrides.pop(get_request_db, None)


class TestOwnerDerivation:
    """The owner comes from the session and nothing else."""

    def test_staff_owns_the_house_list(self):
        req = MagicMock()
        req.state.is_staff = True
        req.state.subscriber_email = None
        assert app_module.watch_owner(req) == queries.HOUSE

    def test_subscriber_owns_their_own_namespace(self):
        req = MagicMock()
        req.state.is_staff = False
        req.state.subscriber_email = ALICE
        assert app_module.watch_owner(req) == f"sub:{ALICE}"

    def test_two_subscribers_never_collide(self):
        a, b = MagicMock(), MagicMock()
        a.state.is_staff = b.state.is_staff = False
        a.state.subscriber_email, b.state.subscriber_email = ALICE, BOB
        assert app_module.watch_owner(a) != app_module.watch_owner(b)

    def test_unidentifiable_subscriber_is_not_given_the_house_list(self):
        """The dangerous default. Falling back to HOUSE for a session with no
        email would hand the editorial list to whoever held it."""
        req = MagicMock()
        req.state.is_staff = False
        req.state.subscriber_email = None
        assert app_module.watch_owner(req) != queries.HOUSE

    def test_unidentifiable_subscriber_cannot_write_at_all(self):
        from fastapi import HTTPException
        req = MagicMock()
        req.state.is_staff = False
        req.state.subscriber_email = None
        with pytest.raises(HTTPException) as exc:
            app_module.require_watch_owner(req)
        assert exc.value.status_code == 403


class TestOwnerScopedQueries:
    @pytest.mark.parametrize("fn", [
        queries.watched_tickers, queries.watched_insiders,
        queries.watched_congress_members, queries.list_watchlist,
        queries.count_watch,
    ])
    def test_owner_has_no_default(self, fn, db):
        """A default would let a forgotten call site silently read the editorial
        list instead of failing."""
        with pytest.raises(TypeError):
            fn(db)

    @pytest.mark.parametrize("fn", [
        queries.watched_tickers, queries.watched_insiders,
        queries.watched_congress_members,
    ])
    def test_every_read_filters_on_owner(self, fn, db):
        fn(db, owner=f"sub:{ALICE}")
        sql, params = db.execute.call_args.args
        assert "owner = %s" in sql
        assert f"sub:{ALICE}" in params

    def test_add_stores_the_owner(self, db):
        queries.add_watch(db, "ticker", "AAPL", "Apple", owner=f"sub:{ALICE}")
        sql, params = db.execute.call_args.args
        assert "owner" in sql
        assert params[-1] == f"sub:{ALICE}"

    def test_toggle_looks_up_within_the_owner(self, db):
        db.execute.return_value.fetchone.return_value = None
        queries.toggle_watch(db, "ticker", "AAPL", "Apple", owner=f"sub:{ALICE}")
        first_sql, first_params = db.execute.call_args_list[0].args
        assert "owner = %s" in first_sql
        assert f"sub:{ALICE}" in first_params


class TestWatchedOnlyFilter:
    """The one query whose ROWS differ per viewer, because the filter runs in
    SQL rather than as a post-hoc flag."""

    def test_subquery_is_scoped_to_the_owner(self):
        from datetime import date
        sql, params = queries._build_filings_where(
            date(2026, 7, 27),
            transaction_codes=["P"],
            watched_only=True,
            watched_owner=f"sub:{ALICE}",
        )
        assert sql.count("owner=%s") == 2, "one per watchlist subquery"
        assert params.count(f"sub:{ALICE}") == 2

    def test_owner_params_land_in_placeholder_order(self):
        """queries.py warns that param order must mirror placeholder order
        exactly. The watched fragment sits between sector and market cap."""
        from datetime import date
        sql, params = queries._build_filings_where(
            date(2026, 7, 27),
            transaction_codes=["P"],
            sector="Technology",
            watched_only=True,
            watched_owner=f"sub:{ALICE}",
        )
        assert params.index("Technology") < params.index(f"sub:{ALICE}")

    def test_no_owner_predicate_when_the_filter_is_off(self):
        from datetime import date
        sql, params = queries._build_filings_where(
            date(2026, 7, 27), transaction_codes=["P"], watched_only=False,
        )
        assert "owner" not in sql
        assert f"sub:{ALICE}" not in params


class TestCacheIsolation:
    """The failure this class exists for: one subscriber's watch flags being
    served to another out of a cache entry they share."""

    def test_enrich_never_bakes_in_a_watch_flag(self):
        rows = [{"issuer_ticker": "AAPL", "insider_cik": "1", "transaction_id": "t1"}]
        out = queries._enrich(rows, None)
        assert out[0]["is_watched"] is False, (
            "cached under a key shared by every viewer, so it must be user-independent"
        )

    def test_enrich_context_cannot_carry_watch_sets(self):
        """Putting the sets back on the dataclass is what would reintroduce the
        leak, so the field must not exist to be set."""
        with pytest.raises(TypeError):
            queries.EnrichContext(conn=None, watched_tickers={"AAPL"})

    def test_decoration_is_per_viewer(self):
        rows = [
            {"issuer_ticker": "AAPL", "insider_cik": "1"},
            {"issuer_ticker": "MSFT", "insider_cik": "2"},
        ]
        queries.decorate_watched(rows, tickers={"AAPL"}, insiders=set())
        assert [r["is_watched"] for r in rows] == [True, False]

        queries.decorate_watched(rows, tickers={"MSFT"}, insiders=set())
        assert [r["is_watched"] for r in rows] == [False, True], (
            "a second viewer must not inherit the first viewer's flags"
        )

    def test_shared_key_has_no_owner_segment(self):
        """Fragmenting the shared dashboard cache per subscriber would throw
        away the thing that makes subscriber load cheap."""
        key = app_module._query_cache_key({"d": "2026-07-27"})
        assert "o=" not in key

    def test_per_viewer_key_carries_an_owner_segment(self):
        a = app_module._query_cache_key({"d": "2026-07-27"}, owner=f"sub:{ALICE}")
        b = app_module._query_cache_key({"d": "2026-07-27"}, owner=f"sub:{BOB}")
        assert a != b
        assert a.startswith("it:query:o=")

    def test_owner_prefix_does_not_leak_the_email(self):
        import cache
        assert ALICE not in cache.owner_key_prefix(f"sub:{ALICE}")


class TestWriteAuthorization:
    def test_owner_cannot_be_supplied_by_the_client(self, db, monkeypatch):
        """The privilege escalation this design exists to prevent: posting
        owner=house to edit the editorial list."""
        _subscriber(monkeypatch, ALICE)
        c = _client(db)
        # follow_redirects=False so the assertion sees only this request's SQL.
        # The redirect target legitimately reads the house list, to render the
        # read-only Editorial Picks panel.
        c.post("/watchlist/add", data={
            "watch_type": "ticker", "value": "AAPL",
            "owner": queries.HOUSE, "csrf_token": security.make_csrf_token(),
        }, follow_redirects=False)
        inserts = [
            call for call in db.execute.call_args_list
            if "INSERT INTO watchlist" in str(call.args[0])
        ]
        assert inserts, "no insert was issued"
        assert queries.HOUSE not in list(inserts[-1].args[1]), (
            "a form field reached the owner column"
        )
        assert f"sub:{ALICE}" in list(inserts[-1].args[1])

    def test_subscriber_add_is_stored_under_their_own_owner(self, db, monkeypatch):
        _subscriber(monkeypatch, ALICE)
        c = _client(db)
        c.post("/watchlist/add", data={
            "watch_type": "ticker", "value": "AAPL",
            "csrf_token": security.make_csrf_token(),
        }, follow_redirects=False)
        inserts = [
            call for call in db.execute.call_args_list
            if "INSERT INTO watchlist" in str(call.args[0])
        ]
        assert inserts, "no insert was issued"
        assert f"sub:{ALICE}" in list(inserts[-1].args[1])

    def test_quota_blocks_a_subscriber_over_the_cap(self, db, monkeypatch):
        db.execute.return_value.fetchone.return_value = {
            "n": queries.SUBSCRIBER_WATCH_LIMIT
        }
        _subscriber(monkeypatch, ALICE)
        c = _client(db)
        resp = c.post("/watchlist/add", data={
            "watch_type": "ticker", "value": "AAPL",
            "csrf_token": security.make_csrf_token(),
        })
        assert resp.status_code == 400

    def test_staff_are_not_capped(self, db):
        db.execute.return_value.fetchone.return_value = {
            "n": queries.SUBSCRIBER_WATCH_LIMIT * 10
        }
        c = _client(db)
        c.auth = (USER, PASSWORD)
        resp = c.post("/watchlist/add", data={
            "watch_type": "ticker", "value": "AAPL",
            "csrf_token": security.make_csrf_token(),
        }, follow_redirects=False)
        assert resp.status_code == 303

    def test_removing_a_row_you_do_not_own_is_a_404(self, db, monkeypatch):
        db.execute.return_value.rowcount = 0  # owner predicate matched nothing
        _subscriber(monkeypatch, ALICE)
        c = _client(db)
        resp = c.post("/watchlist/remove", data={
            "watch_id": "1", "csrf_token": security.make_csrf_token(),
        })
        assert resp.status_code == 404
