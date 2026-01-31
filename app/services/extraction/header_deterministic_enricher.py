from __future__ import annotations
import re
from typing import Dict, Any, List, Optional
from datetime import date, timedelta
from dateutil import parser as date_parser


class HeaderDeterministicEnricher:
    """
    Deterministic enrichment for document headers.
    - Regex + positional only
    - No inference
    - Never override non-null LLM fields
    """

    # -------------------------
    # regex patterns (explicit only)
    # -------------------------
    _EFFECTIVE_DATE_PATTERNS = [
        r"(effective\s+date|commencement\s+date)\s*[:\-]?\s*(.+)",
        r"this\s+agreement\s+is\s+made\s+on\s+(.+)",
    ]

    _TERM_PATTERNS = [
        r"term\s+of\s+(\d+)\s+(year|years|month|months)",
        r"for\s+a\s+period\s+of\s+(\d+)\s+(year|years|month|months)",
    ]

    _PARTIES_PATTERN = r"between\s+(.+?)\s+and\s+(.+?)(?:\.|\n|$)"

    # -------------------------
    # public API
    # -------------------------
    def enrich(
        self,
        *,
        pages: List[Dict[str, Any]],
        header: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Returns header with deterministic fields filled if found.
        """
        text = self._build_text(pages)

        # effective_from
        if header.get("effective_from") is None:
            eff = self._extract_effective_date(text)
            if eff:
                header["effective_from"] = eff

        # effective_to
        if header.get("effective_to") is None:
            eff_to = self._extract_effective_to(
                text, header.get("effective_from")
            )
            if eff_to:
                header["effective_to"] = eff_to

        # parties
        if not header.get("parties"):
            parties = self._extract_parties(text)
            if parties:
                header["parties"] = parties

        return header

    # -------------------------
    # helpers
    # -------------------------
    def _build_text(self, pages: List[Dict[str, Any]]) -> str:
        # deterministic = only first pages
        chunks = []
        for p in pages[:3]:
            chunks.append(p.get("text", ""))
        return "\n".join(chunks)

    def _extract_effective_date(self, text: str) -> Optional[date]:
        for pat in self._EFFECTIVE_DATE_PATTERNS:
            m = re.search(pat, text, re.IGNORECASE)
            if not m:
                continue
            try:
                return date_parser.parse(
                    m.group(m.lastindex)
                ).date()
            except Exception:
                continue
        return None

    def _extract_effective_to(
        self, text: str, effective_from: Optional[date]
    ) -> Optional[date]:
        if not effective_from:
            return None

        for pat in self._TERM_PATTERNS:
            m = re.search(pat, text, re.IGNORECASE)
            if not m:
                continue

            value = int(m.group(1))
            unit = m.group(2).lower()

            if "year" in unit:
                return effective_from.replace(
                    year=effective_from.year + value
                )
            if "month" in unit:
                return effective_from + timedelta(days=30 * value)

        return None

    def _extract_parties(self, text: str) -> Optional[Dict[str, str]]:
        m = re.search(self._PARTIES_PATTERN, text, re.IGNORECASE | re.DOTALL)
        if not m:
            return None

        return {
            "party_a": m.group(1).strip()[:300],
            "party_b": m.group(2).strip()[:300],
        }
