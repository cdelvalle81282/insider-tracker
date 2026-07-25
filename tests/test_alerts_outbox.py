"""Unit tests for the Slack alert outbox.

alerts_sent used to record only that a dedup slot was claimed, and the claim was
committed before the HTTP POST was attempted. Every caller then left that row in
place when _post_to_slack returned False, so a row that meant "claimed" was read
forever after as "sent": one transient Slack outage silently and permanently
dropped those alerts. health_check.py had its own copy of the same bug and
additionally never checked the response status.

The commit-before-HTTP ordering is deliberate and must stay: it is what stops two
overlapping ingest runs double-firing. What changed is that a claim is now
'pending' and only a 2xx promotes it to 'sent'.

No PostgreSQL: the connection is a recording fake.
"""
from __future__ import annotations

import pytest

import alerts


class FakeConn:
    """Records executes and commits in order.

    rowcount_rules maps a SQL substring to the rowcount that call should report,
    which is how these tests simulate "someone else already claimed this".
    """

    def __init__(self, rowcount_rules: dict[str, int] | None = None,
                 select_rows: list[dict] | None = None):
        self.events: list[tuple] = []
        self.rowcount_rules = rowcount_rules or {}
        self.select_rows = select_rows or []

    def execute(self, sql, params=None):
        self.events.append(("execute", " ".join(sql.split()), list(params or [])))
        rowcount = 1
        for needle, value in self.rowcount_rules.items():
            if needle in sql:
                rowcount = value
                break
        rows = self.select_rows if sql.lstrip().upper().startswith("SELECT") else []
        return type("Cur", (), {
            "rowcount": rowcount,
            "fetchall": staticmethod(lambda: rows),
        })()

    def commit(self):
        self.events.append(("commit",))

    # helpers
    def sqls(self) -> list[str]:
        return [e[1] for e in self.events if e[0] == "execute"]

    def kinds(self) -> list[str]:
        return [e[0] for e in self.events]


PAYLOAD = {"blocks": [{"type": "section"}]}


@pytest.fixture
def ok_post(monkeypatch):
    calls = []

    def fake(url, payload, timeout=5.0):
        calls.append((url, payload))
        return True, None

    monkeypatch.setattr(alerts, "_post_to_slack", fake)
    return calls


@pytest.fixture
def failing_post(monkeypatch):
    calls = []

    def fake(url, payload, timeout=5.0):
        calls.append((url, payload))
        return False, "HTTP 503"

    monkeypatch.setattr(alerts, "_post_to_slack", fake)
    return calls


class TestClaimAndSend:
    def test_claim_is_committed_before_the_http_call(self, monkeypatch):
        """The no-double-fire guarantee depends on this ordering."""
        conn = FakeConn()
        seen_at_post: list[list[str]] = []

        def fake(url, payload, timeout=5.0):
            seen_at_post.append(conn.kinds().copy())
            return True, None

        monkeypatch.setattr(alerts, "_post_to_slack", fake)
        alerts.claim_and_send(conn, "k1", "big_buy", PAYLOAD, "https://hook")

        assert seen_at_post, "_post_to_slack was never called"
        assert "commit" in seen_at_post[0], "claim must be committed before posting"

    def test_successful_send_marks_sent_and_clears_payload(self, ok_post):
        conn = FakeConn()
        assert alerts.claim_and_send(conn, "k1", "big_buy", PAYLOAD, "https://hook") is True
        update = [s for s in conn.sqls() if s.startswith("UPDATE")][0]
        assert "status = 'sent'" in update
        assert "payload = NULL" in update, "stop storing the body once delivered"

    def test_failed_send_leaves_the_row_retryable(self, failing_post):
        conn = FakeConn()
        assert alerts.claim_and_send(conn, "k1", "big_buy", PAYLOAD, "https://hook") is False
        update = [s for s in conn.sqls() if s.startswith("UPDATE")][0]
        assert "status" not in update, "must NOT mark sent or failed; stays pending"
        assert "attempts = attempts + 1" in update
        assert "last_error" in update

    def test_failed_send_records_the_error(self, failing_post):
        conn = FakeConn()
        alerts.claim_and_send(conn, "k1", "big_buy", PAYLOAD, "https://hook")
        params = [e[2] for e in conn.events if e[0] == "execute" and "last_error" in e[1]][0]
        assert "HTTP 503" in params

    def test_duplicate_claim_does_not_post(self, ok_post):
        """A key already owned by another run must not be delivered twice."""
        conn = FakeConn(rowcount_rules={"INSERT INTO alerts_sent": 0})
        assert alerts.claim_and_send(conn, "k1", "big_buy", PAYLOAD, "https://hook") is False
        assert ok_post == [], "no HTTP call for an already-claimed key"

    def test_claim_stores_the_rendered_payload(self, failing_post):
        """A retry must not re-derive the body from the matchers, whose since_ts
        window will have moved on."""
        conn = FakeConn()
        alerts.claim_and_send(conn, "k1", "big_buy", PAYLOAD, "https://hook")
        insert = [e for e in conn.events if e[0] == "execute" and "INSERT" in e[1]][0]
        assert "payload" in insert[1]
        assert insert[2][2] is not None, "payload param must be bound, not None"

    def test_claim_inserts_as_pending(self, ok_post):
        conn = FakeConn()
        alerts.claim_and_send(conn, "k1", "big_buy", PAYLOAD, "https://hook")
        insert = [s for s in conn.sqls() if "INSERT" in s][0]
        assert "'pending'" in insert


class TestDrain:
    def _pending(self, **over):
        row = {"id": 1, "alert_key": "k1", "attempts": 1, "payload": PAYLOAD}
        row.update(over)
        return row

    def test_no_webhook_is_a_noop(self):
        conn = FakeConn()
        assert alerts.drain_pending_alerts(conn, "") == 0
        assert conn.events == []

    def test_expires_exhausted_and_stale_rows(self, ok_post):
        conn = FakeConn(select_rows=[])
        alerts.drain_pending_alerts(conn, "https://hook")
        expire = conn.sqls()[0]
        assert "status = 'failed'" in expire
        assert "attempts >= %s" in expire
        assert "payload IS NULL" in expire, "pre-outbox rows can never be retried"
        assert str(alerts.GIVE_UP_AFTER_HOURS) in expire

    def test_successful_retry_marks_sent(self, ok_post):
        conn = FakeConn(select_rows=[self._pending()])
        assert alerts.drain_pending_alerts(conn, "https://hook") == 1
        assert len(ok_post) == 1
        assert any("status = 'sent'" in s for s in conn.sqls())

    def test_retry_bumps_attempts_before_posting(self, ok_post):
        conn = FakeConn(select_rows=[self._pending()])
        alerts.drain_pending_alerts(conn, "https://hook")
        claim = [s for s in conn.sqls() if "attempts = attempts + 1" in s][0]
        assert "AND status = 'pending' AND attempts = %s" in claim, (
            "optimistic claim must be conditional or concurrent drains double-post"
        )

    def test_concurrent_drain_skips_the_row(self, ok_post):
        """rowcount 0 on the optimistic claim means another drain took it."""
        conn = FakeConn(
            rowcount_rules={"attempts = attempts + 1": 0},
            select_rows=[self._pending()],
        )
        assert alerts.drain_pending_alerts(conn, "https://hook") == 0
        assert ok_post == [], "must not post a row another run already claimed"

    def test_still_failing_retry_stays_pending(self, failing_post):
        conn = FakeConn(select_rows=[self._pending()])
        assert alerts.drain_pending_alerts(conn, "https://hook") == 0
        assert not any("status = 'sent'" in s for s in conn.sqls())
        assert any("last_error" in s for s in conn.sqls())

    def test_posts_the_stored_payload(self, ok_post):
        conn = FakeConn(select_rows=[self._pending(payload={"text": "stored"})])
        alerts.drain_pending_alerts(conn, "https://hook")
        assert ok_post[0][1] == {"text": "stored"}


class TestPostToSlack:
    def test_returns_error_string_on_http_error(self, monkeypatch):
        import urllib.error

        def boom(req, timeout=None):
            raise urllib.error.HTTPError("u", 429, "Too Many", {}, None)

        monkeypatch.setattr("urllib.request.urlopen", boom)
        ok, error = alerts._post_to_slack("https://hook", PAYLOAD)
        assert ok is False
        assert "429" in error

    def test_returns_error_string_on_urlerror(self, monkeypatch):
        import urllib.error

        def boom(req, timeout=None):
            raise urllib.error.URLError("dns went away")

        monkeypatch.setattr("urllib.request.urlopen", boom)
        ok, error = alerts._post_to_slack("https://hook", PAYLOAD)
        assert ok is False
        assert "dns went away" in error

    def test_non_2xx_status_is_a_failure(self, monkeypatch):
        class Resp:
            status = 302

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        monkeypatch.setattr("urllib.request.urlopen", lambda req, timeout=None: Resp())
        ok, error = alerts._post_to_slack("https://hook", PAYLOAD)
        assert ok is False
        assert "302" in error


class TestHealthCheckRoutesThroughOutbox:
    def test_uses_claim_and_send(self, monkeypatch):
        """The private copy in health_check had the same permanent-loss bug and
        never checked the response status."""
        import health_check

        calls = []
        monkeypatch.setattr(
            alerts, "claim_and_send",
            lambda conn, key, kind, payload, url: calls.append((key, kind)) or True,
        )
        monkeypatch.setattr(
            health_check, "check_ingest_health",
            lambda conn: [{"kind": "no_runs", "message": "nothing ran"}],
        )
        sent = health_check.send_health_alerts(FakeConn(), "https://hook")
        assert sent == 1
        assert calls[0][1] == "ingest_health"

    def test_no_webhook_short_circuits(self):
        import health_check

        assert health_check.send_health_alerts(FakeConn(), None) == 0
