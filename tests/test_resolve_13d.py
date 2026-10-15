"""Unit tests for the 13D no-CIK person resolver (pure logic, no database)."""
from __future__ import annotations

from resolve_13d_persons import Maps, key_core, key_full, resolve

ISSUER = "0000000111"
OTHER = "0000000222"


def test_key_ignores_order_initials_and_suffixes():
    assert key_core("Kenneth R. Lehman") == key_core("LEHMAN KENNETH R")
    assert key_core("Curtis F. Bradbury, Jr.") == key_core("Bradbury Curtis F")
    assert key_core("Saba Capital Management, L.P.") == key_core("SABA CAPITAL MANAGEMENT LLC")


def test_single_token_names_are_unkeyable():
    assert key_core("Madonna") is None
    assert key_full("Doug Fallon") is None  # only 2 tokens: too common for cross-issuer use


def test_key_full_keeps_initials():
    assert key_full("Andrew R. Heyer") == key_full("HEYER ANDREW R")
    assert key_full("Andrew R. Heyer") != key_full("Andrew S. Heyer")


def test_manual_alias_wins_over_everything():
    m = Maps(manual={"Drew Holdings Ltd.": "0000000999"})
    m.f4_issuer[(ISSUER, key_core("Drew Holdings Ltd."))].add("0000000001")
    assert resolve("Drew Holdings Ltd.", ISSUER, "CO", m) == ("0000000999", "manual")


def test_same_issuer_form4_match_is_unique_only():
    m = Maps()
    m.f4_issuer[(ISSUER, key_core("Mark Getty"))].add("0001070896")
    assert resolve("GETTY MARK", ISSUER, "IN", m) == ("0001070896", "form4_same_issuer")

    m.f4_issuer[(ISSUER, key_core("Mark Getty"))].add("0000000042")  # namesake at same issuer
    assert resolve("GETTY MARK", ISSUER, "IN", m) is None


def test_two_token_individual_never_matches_across_issuers():
    m = Maps()
    m.own_full[key_full("John A Smith")].add("0000000005")
    m.f4_full[key_full("John A Smith")].add("0000000006")
    # "John Smith" has no middle initial, so no cross-issuer rule may fire.
    assert resolve("John Smith", OTHER, "IN", m) is None


def test_three_token_individual_matches_across_issuers():
    m = Maps()
    m.f4_full[key_full("Andrew R. Heyer")].add("0001259062")
    assert resolve("HEYER ANDREW R", OTHER, "IN", m) == ("0001259062", "form4_name")


def test_ownership_name_preferred_over_form4_name():
    m = Maps()
    m.own_full[key_full("Andrew R. Heyer")].add("0000000007")
    m.f4_full[key_full("Andrew R. Heyer")].add("0000000008")
    assert resolve("Andrew R. Heyer", OTHER, "IN", m) == ("0000000007", "ownership_name")


def test_entity_matches_by_core_name_across_issuers():
    m = Maps()
    m.own_core[key_core("Saba Capital Management, L.P.")].add("0001510281")
    assert resolve("SABA CAPITAL MANAGEMENT LP", OTHER, "PN", m) == ("0001510281", "ownership_name")


def test_ambiguous_entity_name_is_skipped():
    m = Maps()
    m.own_core[key_core("Acme Capital Partners")].update({"0000000010", "0000000011"})
    assert resolve("Acme Capital Partners", OTHER, "PN", m) is None
