"""Unit tests for alerts.py's Slack mrkdwn escaping (_slack_escape).

Covers the escape helper itself plus the two message builders where the same
external string is used both in mrkdwn text (must be escaped) and in a URL via
urllib.parse.quote (must stay raw) — _format_cobuy_message and
_format_congress_message.
"""
from __future__ import annotations

import urllib.parse

import alerts


class TestSlackEscape:
    def test_escapes_ampersand_lt_gt(self):
        assert alerts._slack_escape("A<b>&c") == "A&lt;b&gt;&amp;c"

    def test_ampersand_escaped_before_lt_gt_no_double_escape(self):
        # If & were escaped after < and >, "&lt;" would itself become "&amp;lt;".
        assert alerts._slack_escape("&lt;") == "&amp;lt;"

    def test_plain_text_unchanged(self):
        assert alerts._slack_escape("AT&T") == "AT&amp;T"

    def test_channel_mention_neutralized(self):
        escaped = alerts._slack_escape("<!channel>")
        assert "<!channel>" not in escaped
        assert escaped == "&lt;!channel&gt;"


class TestCobuyMessageEscaping:
    def _base_row(self, **overrides):
        row = {
            "ticker": "ACME",
            "politician_name": "Jane <!channel> & Co",
            "amount_label": "$1M-$5M",
            "disclosure_date": "2026-07-01",
            "source": "ainvest",
        }
        row.update(overrides)
        return row

    def test_name_escaped_in_mrkdwn_text(self):
        payload = alerts._format_cobuy_message(self._base_row(), [], "https://x.test")
        section_text = payload["blocks"][1]["text"]["text"]
        assert "<!channel>" not in section_text
        assert "&lt;!channel&gt;" in section_text

    def test_congress_url_stays_percent_encoded_not_escaped(self):
        raw_name = self._base_row()["politician_name"]
        payload = alerts._format_cobuy_message(self._base_row(), [], "https://x.test")
        context_text = payload["blocks"][-1]["elements"][0]["text"]
        expected_url = f"https://x.test/congress?politician={urllib.parse.quote(raw_name)}"
        assert expected_url in context_text

    def test_link_syntax_preserved(self):
        payload = alerts._format_cobuy_message(self._base_row(), [], "https://x.test")
        context_text = payload["blocks"][-1]["elements"][0]["text"]
        assert context_text.startswith("<https://x.test/congress?politician=")
        assert "|View" in context_text

    def test_corp_buy_names_escaped(self):
        corp_buys = [{
            "insider_name": "Evil <b>CEO</b>",
            "insider_title": "CEO & Chair",
            "total_value": 1_000_000,
            "transaction_date": "2026-07-02",
        }]
        payload = alerts._format_cobuy_message(self._base_row(), corp_buys, "https://x.test")
        section_text = payload["blocks"][1]["text"]["text"]
        assert "<b>" not in section_text
        assert "&lt;b&gt;" in section_text
        assert "&amp;" in section_text


class TestCongressMessageEscaping:
    def _base_row(self, **overrides):
        row = {
            "politician_name": "Rep. <script>&Smith",
            "ticker": "ACME",
            "amount_label": "$50K-$100K",
            "transaction_date": "2026-07-01",
            "disclosure_date": "2026-07-05",
            "party": "Democrat",
        }
        row.update(overrides)
        return row

    def test_name_escaped_in_mrkdwn_but_raw_in_url(self):
        row = self._base_row()
        payload = alerts._format_congress_message(row, "https://x.test")
        section_text = payload["blocks"][1]["text"]["text"]
        button_url = payload["blocks"][1]["accessory"]["url"]

        assert "<script>" not in section_text
        assert "&lt;script&gt;" in section_text
        assert button_url == f"https://x.test/congress?politician={urllib.parse.quote(row['politician_name'])}"
