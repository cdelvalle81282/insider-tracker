"""Unit tests for parser.py's CIK cleaning (_clean_cik)."""
from __future__ import annotations

import parser


class TestCleanCik:
    def test_pads_valid_numeric_cik(self):
        assert parser._clean_cik("320193") == "0000320193"

    def test_already_full_length(self):
        assert parser._clean_cik("0000320193") == "0000320193"

    def test_empty_string_falls_back_to_zero_sentinel(self):
        assert parser._clean_cik("") == "0000000000"

    def test_none_falls_back_to_zero_sentinel(self):
        assert parser._clean_cik(None) == "0000000000"

    def test_non_numeric_falls_back_to_zero_sentinel(self):
        # Previously this zfill'd into "0000000ABC" — must not happen anymore.
        assert parser._clean_cik("ABC") == "0000000000"

    def test_mixed_alnum_falls_back_to_zero_sentinel(self):
        assert parser._clean_cik("12A") == "0000000000"

    def test_strips_whitespace_before_validating(self):
        assert parser._clean_cik("  320193  ") == "0000320193"
