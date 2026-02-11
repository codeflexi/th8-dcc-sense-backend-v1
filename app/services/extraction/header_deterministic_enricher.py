from __future__ import annotations
import re
from datetime import datetime, timedelta
from typing import Dict, Any, List, Optional, Tuple


DATE_PATTERNS = [
    "%d %b %Y",
    "%d %B %Y",
    "%Y-%m-%d",
    "%d/%m/%Y",
    "%m/%d/%Y",
]


def _parse_date_safe(raw: str) -> Optional[datetime.date]:
    raw = raw.strip().replace(",", "")
    for fmt in DATE_PATTERNS:
        try:
            return datetime.strptime(raw, fmt).date()
        except Exception:
            continue
    return None


class HeaderDeterministicEnricher:
    """
    ENTERPRISE CONSOLIDATED VERSION

    Covers:
    - Validity Period range
    - Effective from ... to ...
    - Single effective date
    - Until further notice
    - Term derivation
    - Title extraction
    - Contract number extraction
    - Party extraction
    - Page trace
    """

    # ============================================================
    # PUBLIC
    # ============================================================

    def enrich(self, pages: List[Dict[str, Any]], header: Dict[str, Any]) -> Dict[str, Any]:
        header = dict(header or {})
        header.setdefault("extracted_fields", {})

        blocks = [
            (p.get("page_number"), p.get("text", "") or "")
            for p in pages[:3]
        ]

        # ---------------------------------------------------------
        # TITLE
        # ---------------------------------------------------------
        if not header.get("doc_title"):
            result = self._extract_title(blocks)
            if result:
                title, page_no, raw = result
                header["doc_title"] = title
                header["extracted_fields"]["title_trace"] = {
                    "page_number": page_no,
                    "raw_text": raw,
                    "method": "TITLE_PATTERN",
                }

        # ---------------------------------------------------------
        # CONTRACT NUMBER
        # ---------------------------------------------------------
        if not header.get("doc_number"):
            result = self._extract_contract_number(blocks)
            if result:
                number, page_no, raw = result
                header["doc_number"] = number
                header["extracted_fields"]["doc_number_trace"] = {
                    "page_number": page_no,
                    "raw_text": raw,
                    "method": "CONTRACT_NO_PATTERN",
                }

        # ---------------------------------------------------------
        # PARTY EXTRACTION
        # ---------------------------------------------------------
        if not header.get("parties"):
            result = self._extract_parties(blocks)
            if result:
                parties, trace = result
                header["parties"] = parties
                header["extracted_fields"]["party_trace"] = trace

        # ---------------------------------------------------------
        # VALIDITY RANGE
        # ---------------------------------------------------------
        if not header.get("effective_from") or not header.get("effective_to"):
            result = self._extract_validity_range(blocks)
            if result:
                start, end, page_no, raw = result
                if not header.get("effective_from"):
                    header["effective_from"] = start
                if not header.get("effective_to"):
                    header["effective_to"] = end
                header["extracted_fields"]["validity_range_trace"] = {
                    "page_number": page_no,
                    "raw_text": raw,
                    "method": "VALIDITY_RANGE_PATTERN",
                }

        # ---------------------------------------------------------
        # EFFECTIVE FROM ... TO ...
        # ---------------------------------------------------------
        if not header.get("effective_from") or not header.get("effective_to"):
            result = self._extract_effective_from_to(blocks)
            if result:
                start, end, page_no, raw = result
                if not header.get("effective_from"):
                    header["effective_from"] = start
                if not header.get("effective_to"):
                    header["effective_to"] = end
                header["extracted_fields"]["effective_from_to_trace"] = {
                    "page_number": page_no,
                    "raw_text": raw,
                    "method": "EFFECTIVE_FROM_TO_PATTERN",
                }

        # ---------------------------------------------------------
        # SINGLE EFFECTIVE DATE
        # ---------------------------------------------------------
        if not header.get("effective_from"):
            result = self._extract_single_effective(blocks)
            if result:
                date_val, page_no, raw = result
                header["effective_from"] = date_val
                header["extracted_fields"]["effective_single_trace"] = {
                    "page_number": page_no,
                    "raw_text": raw,
                    "method": "EFFECTIVE_SINGLE_PATTERN",
                }

        # ---------------------------------------------------------
        # UNTIL FURTHER NOTICE
        # ---------------------------------------------------------
        if not header.get("effective_to"):
            result = self._extract_until_further_notice(blocks)
            if result:
                page_no, raw = result
                header["extracted_fields"]["open_end_trace"] = {
                    "page_number": page_no,
                    "raw_text": raw,
                    "method": "UNTIL_FURTHER_NOTICE_PATTERN",
                }

        # ---------------------------------------------------------
        # TERM DERIVATION
        # ---------------------------------------------------------
        if header.get("effective_from") and not header.get("effective_to"):
            term_years = self._extract_term_years(blocks)
            if term_years:
                header["effective_to"] = header["effective_from"] + timedelta(days=365 * term_years)
                header["extracted_fields"]["term_derivation_trace"] = {
                    "method": "TERM_DERIVED",
                    "years": term_years,
                }
                
        # =========================================================
        # FIX: CONTRACT DETECTION (deterministic override)
        # =========================================================
        full_text = "\n".join([b[1] for b in blocks]).lower()

        if not header.get("doc_type") or header.get("doc_type") == "OTHER":
            if (
                "master service agreement" in full_text
                or "this agreement" in full_text
                or "contract id" in full_text
                or "agreement no" in full_text
                or "governing law" in full_text
            ):
                header["doc_type"] = "CONTRACT"
                header.setdefault("extracted_fields", {})
                header["extracted_fields"]["doc_type_trace"] = {
                    "method": "DETERMINISTIC_CONTRACT_PATTERN",
                    "confidence": 0.95,
                }


        return header

    # ============================================================
    # EXTRACTION METHODS
    # ============================================================

    def _extract_title(self, blocks):
        for page_no, text in blocks:
            lines = text.splitlines()
            for line in lines[:10]:
                line_clean = line.strip()
                if 10 < len(line_clean) < 200:
                    if "agreement" in line_clean.lower() or "contract" in line_clean.lower():
                        return line_clean, page_no, line_clean
        return None

    def _extract_contract_number(self, blocks):
        patterns = [
            r"(contract\s*(no|number)?\s*[:\-]?\s*)([A-Za-z0-9\-/]+)",
            r"(agreement\s*(no|number)?\s*[:\-]?\s*)([A-Za-z0-9\-/]+)",
            r"(ctr[-\s]?\d{4}[-\s]?\d+)",
        ]

        for page_no, text in blocks:
            for pat in patterns:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    number = m.group(len(m.groups()))
                    return number.strip(), page_no, m.group(0)
        return None

    def _extract_parties(self, blocks):
        pattern = re.compile(
            r"(between|by and between)\s+(.+?)\s+and\s+(.+?)(\.|\n)",
            re.IGNORECASE | re.DOTALL,
        )

        for page_no, text in blocks:
            m = pattern.search(text)
            if m:
                p1 = m.group(2).strip()
                p2 = m.group(3).strip()
                parties = [
                    {"role": "party_1", "name": p1},
                    {"role": "party_2", "name": p2},
                ]
                trace = {
                    "page_number": page_no,
                    "raw_text": m.group(0),
                    "method": "PARTY_PATTERN",
                }
                return parties, trace
        return None

    def _extract_validity_range(self, blocks) -> Optional[Tuple]:
        dash = r"(?:-|–|—|to|until)"
        pattern = re.compile(
            r"(validity\s*(?:period)?|period)\s*[:\-]?\s*"
            r"([A-Za-z0-9/\-\s]+?)\s*" + dash + r"\s*([A-Za-z0-9/\-\s]+)",
            re.IGNORECASE,
        )

        for page_no, text in blocks:
            m = pattern.search(text)
            if m:
                raw_start = m.group(2).strip()
                raw_end = m.group(3).strip()
                d1 = _parse_date_safe(raw_start)
                d2 = _parse_date_safe(raw_end)
                if d1 and d2:
                    return d1, d2, page_no, m.group(0)
        return None

    def _extract_effective_from_to(self, blocks) -> Optional[Tuple]:
        dash = r"(?:-|–|—|to|until)"
        pattern = re.compile(
            r"(effective\s+(from|date)\s*)"
            r"([A-Za-z0-9/\-\s]+?)\s*" + dash + r"\s*([A-Za-z0-9/\-\s]+)",
            re.IGNORECASE,
        )

        for page_no, text in blocks:
            m = pattern.search(text)
            if m:
                raw_start = m.group(3).strip()
                raw_end = m.group(4).strip()
                d1 = _parse_date_safe(raw_start)
                d2 = _parse_date_safe(raw_end)
                if d1 and d2:
                    return d1, d2, page_no, m.group(0)
        return None

    def _extract_single_effective(self, blocks) -> Optional[Tuple]:
        patterns = [
            r"(effective date|commencement date)\s*[:\-]?\s*([A-Za-z0-9/\-\s]+)",
            r"this agreement is made on\s+([A-Za-z0-9/\-\s]+)",
        ]

        for page_no, text in blocks:
            for pat in patterns:
                m = re.search(pat, text, re.IGNORECASE)
                if m:
                    raw = m.group(2)
                    d = _parse_date_safe(raw.strip())
                    if d:
                        return d, page_no, m.group(0)
        return None

    def _extract_until_further_notice(self, blocks) -> Optional[Tuple]:
        pattern = re.compile(r"(until further notice)", re.IGNORECASE)
        for page_no, text in blocks:
            m = pattern.search(text)
            if m:
                return page_no, m.group(0)
        return None

    def _extract_term_years(self, blocks) -> Optional[int]:
        pattern = re.compile(r"(term|period)\s+(of\s+)?(\d+)\s+year", re.IGNORECASE)
        for _, text in blocks:
            m = pattern.search(text)
            if m:
                try:
                    return int(m.group(3))
                except Exception:
                    return None
        return None
