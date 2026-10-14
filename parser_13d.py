"""
Schedule 13D / 13D/A XML parser.

EDGAR serves structured `primary_doc.xml` for Schedule 13D filings made since
2024-12-18. The namespace URI is the same across filers but the prefix is not
(`sch:` for Workiva, a default namespace for others), so every lookup here
matches on local name only.

One filing -> one `filing` dict plus a list of `persons` dicts (one per
reporting person; joint filings have several).
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Any

from lxml import etree


class OwnershipParseError(ValueError):
    """The filing cannot be turned into trustworthy rows, so reject it."""


def _local(el: etree._Element) -> str:
    tag = el.tag
    return tag.rsplit("}", 1)[-1] if isinstance(tag, str) else ""


def _first(root: etree._Element, name: str) -> etree._Element | None:
    for el in root.iter():
        if _local(el) == name:
            return el
    return None


def _child(el: etree._Element | None, name: str) -> etree._Element | None:
    if el is None:
        return None
    for c in el:
        if _local(c) == name:
            return c
    return None


def _txt(el: etree._Element | None, name: str) -> str | None:
    c = _child(el, name)
    if c is None or c.text is None:
        return None
    t = c.text.strip()
    return t or None


def _deep_txt(root: etree._Element, name: str) -> str | None:
    el = _first(root, name)
    if el is None or el.text is None:
        return None
    t = el.text.strip()
    return t or None


def _num(raw: str | None) -> float | None:
    if raw is None:
        return None
    cleaned = raw.replace(",", "").replace("%", "").strip()
    try:
        return float(cleaned)
    except ValueError:
        return None


def _date(raw: str | None) -> str | None:
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(raw.strip(), fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _cik(raw: str | None) -> str | None:
    cleaned = (raw or "").strip()
    if not cleaned.isdigit() or len(cleaned) > 10:
        return None
    return cleaned.zfill(10)


def parse_13d(
    xml_bytes: bytes, accession_no: str, filed_at: str, xml_url: str
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as exc:
        raise OwnershipParseError(f"bad XML: {exc}") from exc

    form_type = _deep_txt(root, "submissionType")
    if form_type not in ("SCHEDULE 13D", "SCHEDULE 13D/A"):
        raise OwnershipParseError(f"unexpected submissionType {form_type!r}")

    cover = _first(root, "coverPageHeader")
    issuer = _child(cover, "issuerInfo")
    issuer_cik = _cik(_txt(issuer, "issuerCIK"))
    if issuer_cik is None:
        # Same rule as Form 4: a malformed CIK must reject the filing, not merge
        # unrelated issuers onto one all-zero entity.
        raise OwnershipParseError("missing or malformed issuerCIK")

    cusip_el = _child(issuer, "issuerCusips")
    cusip = _txt(cusip_el, "issuerCusipNumber")

    amend_raw = _txt(cover, "amendmentNo")
    filer_el = _first(root, "filerCredentials")

    items = _first(root, "items1To7")
    item3 = _child(items, "item3")
    item4 = _child(items, "item4")

    filing: dict[str, Any] = {
        "accession_no": accession_no,
        "schedule": "13D",
        "form_type": form_type,
        "is_amendment": form_type.endswith("/A"),
        "amendment_no": int(amend_raw) if amend_raw and amend_raw.isdigit() else None,
        "previous_accession": _deep_txt(root, "previousAccessionNumber"),
        "filed_at": filed_at,
        "event_date": _date(_txt(cover, "dateOfEvent")),
        "filer_cik": _cik(_txt(filer_el, "cik")),
        "issuer_cik": issuer_cik,
        "issuer_name": _txt(issuer, "issuerName"),
        "issuer_cusip": cusip,
        "issuer_ticker": None,  # filled by the ingester from the CIK map
        "class_title": _txt(cover, "securitiesClassTitle"),
        "funds_source": _txt(item3, "fundsSource"),
        "purpose_text": _txt(item4, "transactionPurpose"),
        "raw_xml_url": xml_url,
    }

    persons: list[dict[str, Any]] = []
    rp_root = _first(root, "reportingPersons")
    if rp_root is not None:
        for seq, info in enumerate(c for c in rp_root if _local(c) == "reportingPersonInfo"):
            name = _txt(info, "reportingPersonName")
            if not name:
                continue
            persons.append({
                "accession_no": accession_no,
                "seq": seq,
                "person_cik": _cik(_txt(info, "reportingPersonCIK")),
                "person_name": name,
                "no_cik": (_txt(info, "reportingPersonNoCIK") or "N").upper() == "Y",
                "member_of_group": _txt(info, "memberOfGroup"),
                "fund_type": _txt(info, "fundType"),
                "person_type": _txt(info, "typeOfReportingPerson"),
                "sole_voting": _num(_txt(info, "soleVotingPower")),
                "shared_voting": _num(_txt(info, "sharedVotingPower")),
                "sole_dispositive": _num(_txt(info, "soleDispositivePower")),
                "shared_dispositive": _num(_txt(info, "sharedDispositivePower")),
                "aggregate_owned": _num(_txt(info, "aggregateAmountOwned")),
                "pct_class": _num(_txt(info, "percentOfClass")),
                "comment_text": _txt(info, "commentContent"),
            })

    if not persons:
        raise OwnershipParseError("no reporting persons")

    # Fail loudly on an unparseable date rather than store a filing that looks real.
    date.fromisoformat(filed_at)
    return filing, persons
