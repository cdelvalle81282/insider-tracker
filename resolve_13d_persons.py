"""
Attach a CIK to 13D reporting persons that were filed without one.

Usage:
  python resolve_13d_persons.py            # resolve unresolved rows
  python resolve_13d_persons.py --dry-run  # report what would match, write nothing

Rules, strongest first. The first rule that yields exactly one CIK wins; an
ambiguous match (two or more CIKs) is skipped, never guessed.

  manual               exact person_name in `person_alias`
  form4_same_issuer    name matches a Form 4 insider of the SAME issuer the 13D is on
  ownership_same_issuer  name matches a CIK-bearing 13D person on the same issuer
  ownership_name       same normalized name seen with a CIK in another 13D
                       (individuals need a middle initial/name, i.e. 3+ tokens,
                       because two-token personal names collide too often)
  form4_name           individual whose 3+ token name matches exactly one Form 4
                       insider at any issuer. Weakest rule; the UI marks it.

`resolved_via` records the rule so a bad one can be undone with a single UPDATE.
"""
from __future__ import annotations

import re
from collections import Counter, defaultdict
from dataclasses import dataclass, field

import click
import psycopg

from db import get_cli_db

_DROP = frozenset({
    "JR", "SR", "II", "III", "IV", "MR", "MRS", "MS", "DR", "ESQ", "PHD", "MD", "CPA",
    "LLC", "LP", "INC", "LTD", "CORP", "THE",
})

WEAK_RULES = frozenset({"ownership_name", "form4_name"})


def _tokens(name: str) -> list[str]:
    cleaned = re.sub(r"[^A-Z0-9 ]", " ", (name or "").upper())
    return [t for t in cleaned.split() if t not in _DROP]


def key_core(name: str) -> frozenset[str] | None:
    """Name as a set of tokens, ignoring order, initials and suffixes.

    "Kenneth R. Lehman" and "LEHMAN KENNETH R" collapse to the same key.
    """
    toks = [t for t in _tokens(name) if len(t) > 1]
    return frozenset(toks) if len(toks) >= 2 else None


def key_full(name: str) -> frozenset[str] | None:
    """Like key_core but keeps initials and requires 3+ tokens (more specific)."""
    toks = _tokens(name)
    return frozenset(toks) if len(toks) >= 3 else None


@dataclass
class Maps:
    manual: dict[str, str] = field(default_factory=dict)
    f4_issuer: dict[tuple[str, frozenset], set[str]] = field(default_factory=lambda: defaultdict(set))
    own_issuer: dict[tuple[str, frozenset], set[str]] = field(default_factory=lambda: defaultdict(set))
    own_core: dict[frozenset, set[str]] = field(default_factory=lambda: defaultdict(set))
    own_full: dict[frozenset, set[str]] = field(default_factory=lambda: defaultdict(set))
    f4_full: dict[frozenset, set[str]] = field(default_factory=lambda: defaultdict(set))


def _unique(ciks: set[str] | None) -> str | None:
    return next(iter(ciks)) if ciks and len(ciks) == 1 else None


def resolve(name: str, issuer_cik: str, person_type: str | None, m: Maps) -> tuple[str, str] | None:
    """Return (cik, rule) or None."""
    if name in m.manual:
        return m.manual[name], "manual"

    core = key_core(name)
    full = key_full(name)
    individual = person_type == "IN"

    if core:
        hit = _unique(m.f4_issuer.get((issuer_cik, core)))
        if hit:
            return hit, "form4_same_issuer"
        hit = _unique(m.own_issuer.get((issuer_cik, core)))
        if hit:
            return hit, "ownership_same_issuer"

    if individual:
        if full:
            hit = _unique(m.own_full.get(full))
            if hit:
                return hit, "ownership_name"
            hit = _unique(m.f4_full.get(full))
            if hit:
                return hit, "form4_name"
    elif core:
        hit = _unique(m.own_core.get(core))
        if hit:
            return hit, "ownership_name"
    return None


def load_maps(conn: psycopg.Connection) -> Maps:
    m = Maps()
    for r in conn.execute("SELECT person_name, person_cik FROM person_alias").fetchall():
        m.manual[r["person_name"]] = r["person_cik"]

    for r in conn.execute(
        """SELECT DISTINCT insider_cik, insider_name, issuer_cik FROM filings
           WHERE is_officer = 1 OR is_director = 1 OR is_ten_percent_owner = 1"""
    ).fetchall():
        core, full = key_core(r["insider_name"] or ""), key_full(r["insider_name"] or "")
        if core:
            m.f4_issuer[(r["issuer_cik"], core)].add(r["insider_cik"])
        if full:
            m.f4_full[full].add(r["insider_cik"])

    for r in conn.execute(
        """SELECT DISTINCT p.person_cik, p.person_name, p.person_type, f.issuer_cik
           FROM ownership_filing_person p JOIN ownership_filing f USING (accession_no)
           WHERE p.person_cik IS NOT NULL"""
    ).fetchall():
        core, full = key_core(r["person_name"]), key_full(r["person_name"])
        if core:
            m.own_issuer[(r["issuer_cik"], core)].add(r["person_cik"])
            if r["person_type"] != "IN":
                m.own_core[core].add(r["person_cik"])
        if full and r["person_type"] == "IN":
            m.own_full[full].add(r["person_cik"])
    return m


def resolve_persons(conn: psycopg.Connection, dry_run: bool = False) -> Counter:
    m = load_maps(conn)
    todo = conn.execute(
        """SELECT p.accession_no, p.seq, p.person_name, p.person_type, f.issuer_cik
           FROM ownership_filing_person p JOIN ownership_filing f USING (accession_no)
           WHERE p.person_cik IS NULL AND p.resolved_cik IS NULL"""
    ).fetchall()

    counts: Counter = Counter()
    updates: list[tuple[str, str, str, int]] = []
    for r in todo:
        hit = resolve(r["person_name"], r["issuer_cik"], r["person_type"], m)
        if hit:
            counts[hit[1]] += 1
            updates.append((hit[0], hit[1], r["accession_no"], r["seq"]))
        else:
            counts["unresolved"] += 1

    if updates and not dry_run:
        with conn.transaction():
            conn.cursor().executemany(
                """UPDATE ownership_filing_person
                   SET resolved_cik = %s, resolved_via = %s
                   WHERE accession_no = %s AND seq = %s""",
                updates,
            )
    return counts


@click.command()
@click.option("--dry-run", is_flag=True, help="Report matches without writing")
def main(dry_run: bool) -> None:
    conn = get_cli_db()
    try:
        counts = resolve_persons(conn, dry_run=dry_run)
        conn.commit()
        for rule, n in counts.most_common():
            click.echo(f"{rule:<24}{n:>8}")
        if dry_run:
            click.echo("(dry run, nothing written)")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
