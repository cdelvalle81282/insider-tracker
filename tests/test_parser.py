"""Unit tests for parser.py's CIK cleaning (_clean_cik)."""
from __future__ import annotations

import pytest

import parser


class TestCleanCik:
    def test_pads_valid_numeric_cik(self):
        assert parser._clean_cik("320193") == "0000320193"

    def test_already_full_length(self):
        assert parser._clean_cik("0000320193") == "0000320193"

    def test_strips_whitespace_before_validating(self):
        assert parser._clean_cik("  320193  ") == "0000320193"


class TestCleanCikRejects:
    """A malformed CIK rejects the filing rather than returning a placeholder.

    These four cases previously returned the all-zero sentinel "0000000000",
    which merged every malformed filer in the database into one fake entity:
    unrelated issuers and unrelated insiders collapsed onto a single CIK and the
    dashboard rendered them as one company or one person.
    """

    @pytest.mark.parametrize("raw", ["", None, "ABC", "12A"])
    def test_malformed_values_raise(self, raw):
        with pytest.raises(parser.Form4ParseError):
            parser._clean_cik(raw)

    def test_over_long_cik_raises(self):
        """zfill passed these through unchanged, yielding an over-long key."""
        with pytest.raises(parser.Form4ParseError):
            parser._clean_cik("123456789012")

    def test_error_names_the_offending_field(self):
        """ingest records this message in run_log.error_detail, so it has to say
        which of the two CIK fields was bad."""
        with pytest.raises(parser.Form4ParseError, match="issuerCik"):
            parser._clean_cik("nope", "issuerCik")

    def test_error_is_a_valueerror(self):
        """ingest_date's per-filing handler catches broad Exception, but keeping
        this a ValueError means callers can be narrower if they want."""
        assert issubclass(parser.Form4ParseError, ValueError)
