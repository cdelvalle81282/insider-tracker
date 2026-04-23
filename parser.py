"""
Parse a Form 4 XML document into a list of transaction row dicts.

Each dict maps directly to a column in the `filings` table.
A single filing can produce multiple rows (one per transaction line item
across the nonDerivativeTable and derivativeTable).
"""
from __future__ import annotations

from typing import Any

from lxml import etree


def _text(el: etree._Element | None, tag: str, default: str | None = None) -> str | None:
    if el is None:
        return default
    child = el.find(tag)
    if child is None:
        return default
    # Values are often wrapped in a <value> child element
    value_el = child.find("value")
    text = (value_el.text if value_el is not None else child.text)
    if text is None:
        return default
    return text.strip()


def _float(el: etree._Element | None, tag: str) -> float | None:
    val = _text(el, tag)
    if val is None:
        return None
    try:
        return float(val.replace(",", ""))
    except ValueError:
        return None


def _int_flag(el: etree._Element | None, tag: str) -> int:
    val = _text(el, tag, "0")
    return 1 if val and val.upper() in ("1", "TRUE", "YES", "Y") else 0


def _collect_footnotes(root: etree._Element) -> str:
    """Concatenate all footnote text nodes."""
    notes = []
    for fn in root.findall(".//footnote"):
        if fn.text:
            notes.append(fn.text.strip())
    return " | ".join(notes) if notes else ""


def _detect_10b5_1(root: etree._Element, footnote_text: str) -> int:
    """Return 1 if this filing signals a 10b5-1 plan."""
    indicator = root.find(".//rule10b5-1Indicator")
    if indicator is not None and indicator.text:
        return 1 if indicator.text.strip().upper() == "Y" else 0
    # Older filings only mention it in footnotes
    return 1 if "10b5-1" in footnote_text.lower() else 0


def _parse_reporting_owner(root: etree._Element) -> dict[str, Any]:
    owner: dict[str, Any] = {}
    ro = root.find(".//reportingOwner")
    if ro is None:
        return owner

    owner_id = ro.find("reportingOwnerId")
    owner["insider_cik"] = (_text(owner_id, "rptOwnerCik") or "").zfill(10)
    owner["insider_name"] = _text(owner_id, "rptOwnerName") or ""

    rel = ro.find("reportingOwnerRelationship")
    owner["is_director"] = _int_flag(rel, "isDirector")
    owner["is_officer"] = _int_flag(rel, "isOfficer")
    owner["is_ten_percent_owner"] = _int_flag(rel, "isTenPercentOwner")
    owner["is_other"] = _int_flag(rel, "isOther")
    owner["insider_title"] = _text(rel, "officerTitle")

    return owner


def _parse_issuer(root: etree._Element) -> dict[str, Any]:
    issuer = root.find(".//issuer")
    return {
        "issuer_cik": (_text(issuer, "issuerCik") or "").zfill(10),
        "issuer_name": _text(issuer, "issuerName") or "",
        "issuer_ticker": _text(issuer, "issuerTradingSymbol"),
    }


def _build_row(
    accession_no: str,
    filed_at: str,
    form_type: str,
    issuer: dict,
    owner: dict,
    table_type: str,
    row_idx: int,
    tx: etree._Element,
    footnote_text: str,
    is_10b5_1: int,
    raw_xml_url: str,
) -> dict[str, Any]:
    transaction_id = f"{accession_no}-{table_type}-{row_idx}"

    amounts = tx.find("transactionAmounts")
    post_tx = tx.find("postTransactionAmounts")
    ownership_nature = tx.find("ownershipNature")

    shares = _float(amounts, "transactionShares")
    price = _float(amounts, "transactionPricePerShare")
    total_value = (shares * price) if (shares is not None and price is not None) else None

    return {
        "transaction_id": transaction_id,
        "accession_no": accession_no,
        "filed_at": filed_at,
        "form_type": form_type,
        **issuer,
        **owner,
        "transaction_date": _text(tx, "transactionDate") or "",
        "transaction_code": _text(tx.find("transactionCoding"), "transactionCode") or "",
        "equity_swap": _int_flag(tx.find("transactionCoding"), "equitySwapInvolved"),
        "table_type": table_type,
        "shares": shares or 0.0,
        "price_per_share": price,
        "total_value": total_value,
        "shares_owned_after": _float(post_tx, "sharesOwnedFollowingTransaction"),
        "ownership_type": _text(ownership_nature, "directOrIndirectOwnership"),
        "is_10b5_1": is_10b5_1,
        "footnote_text": footnote_text or None,
        "raw_xml_url": raw_xml_url,
    }


def parse_form4(xml_bytes: bytes, accession_no: str, filed_at: str, raw_xml_url: str) -> list[dict[str, Any]]:
    """Parse Form 4 XML and return list of transaction row dicts."""
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError:
        # Some filings wrap XML in a <ownershipDocument> inside a larger doc
        # Try stripping to first occurrence
        start = xml_bytes.find(b"<ownershipDocument")
        if start == -1:
            raise
        root = etree.fromstring(xml_bytes[start:])

    form_type = (_text(root, "documentType") or "4").strip()
    footnote_text = _collect_footnotes(root)
    is_10b5_1 = _detect_10b5_1(root, footnote_text)
    issuer = _parse_issuer(root)
    owner = _parse_reporting_owner(root)

    rows: list[dict] = []

    # Non-derivative transactions
    nd_table = root.find(".//nonDerivativeTable")
    if nd_table is not None:
        for idx, tx in enumerate(nd_table.findall("nonDerivativeTransaction")):
            rows.append(_build_row(
                accession_no, filed_at, form_type, issuer, owner,
                "ND", idx, tx, footnote_text, is_10b5_1, raw_xml_url
            ))

    # Derivative transactions
    d_table = root.find(".//derivativeTable")
    if d_table is not None:
        for idx, tx in enumerate(d_table.findall("derivativeTransaction")):
            rows.append(_build_row(
                accession_no, filed_at, form_type, issuer, owner,
                "D", idx, tx, footnote_text, is_10b5_1, raw_xml_url
            ))

    return rows
