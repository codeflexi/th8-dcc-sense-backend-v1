from __future__ import annotations

import re
from datetime import date
from typing import Dict, Any, List, Optional


# ============================================================
# DOCUMENT TYPE NORMALIZATION
# ============================================================

_DOC_TYPE_MAP = {
    "CONTRACT": "CONTRACT",
    "AMENDMENT": "AMENDMENT",
    "SLA": "SLA",
    "INVOICE": "INVOICE",
    "PRICE_LIST": "PRICE_LIST",
    "OTHER": "OTHER",
}


def normalize_doc_type(v: Optional[str]) -> str:
    if not v:
        return "OTHER"
    key = v.strip().upper()
    return _DOC_TYPE_MAP.get(key, "OTHER")


# ============================================================
# DOCUMENT ROLE (Decision-facing)
# ============================================================

def infer_document_role(doc_type: str, contract_id: Optional[str]) -> str:
    """
    Minimal deterministic logic for SENSE v1.
    """
    if doc_type == "AMENDMENT":
        return "AMENDMENT"

    if doc_type == "CONTRACT":
        return "MASTER" if contract_id else "SUPPORTING"

    return "SUPPORTING"


# ============================================================
# SIGNAL FLAGS (Deterministic only — no LLM)
# ============================================================

def build_signal_flags(pages: List[Dict[str, Any]]) -> Dict[str, bool]:
    """
    Deterministic signals only.
    Uses first 3 pages.
    Designed for:
        - classification signal
        - price precedence gate
        - contract detection

    JSON safe.
    """

    text_blocks: List[str] = []

    for p in pages[:3]:
        text_blocks.append(p.get("text", "") or "")

    full_text = "\n".join(text_blocks)
    text = full_text.lower()

    def has(pattern: str) -> bool:
        return re.search(pattern, text, re.IGNORECASE) is not None

    # ----------------------------
    # Contract-like indicators
    # ----------------------------

    has_this_agreement = has(r"\bthis\s+agreement\b")

    has_signature_block = has(
        r"\bin\s+witness\s+whereof\b"
        r"|\bsigned\s+by\b"
        r"|\bsignature\b"
        r"|\bauthorized\s+signatory\b"
    )

    has_effective_date_phrase = has(
        r"\beffective\s+date\b"
        r"|\beffective\s+as\s+of\b"
        r"|\bcommencement\s+date\b"
        r"|\bvalid\s+from\b"
        r"|\bvalidity\s+period\b"
        r"|\beffective\s+from\b"
    )

    has_governing_law = has(
        r"\bgoverning\s+law\b"
        r"|\bjurisdiction\b"
        r"|\blaws\s+of\b"
    )

    # ----------------------------
    # Price table detection (enterprise conservative)
    # ----------------------------

    money_pattern = (
        r"\b(?:thb|usd|baht|บาท)\b"
        r"|"
        r"\b\d{1,3}(?:,\d{3})+(?:\.\d{1,2})?\b"
        r"|"
        r"\b\d+\.\d{2}\b"
        r"|"
        r"\b\d{4,}\b"
    )

    money_hits = len(re.findall(money_pattern, text))

    has_table_header = has(
        r"\bsku\b"
        r"|\bitem\b"
        r"|\bitem\s+code\b"
        r"|\bdescription\b"
        r"|\bunit\s*price\b"
        r"|\bprice\b"
        r"|\buom\b"
        r"|\bqty\b"
        r"|\bquantity\b"
    )

    table_like_lines = 0
    for line in full_text.splitlines():
        numbers = re.findall(r"\d+(?:,\d{3})*(?:\.\d+)?", line)
        if len(numbers) >= 3:
            table_like_lines += 1

    has_price_table = False
    if (
        money_hits >= 4
        and has_table_header
        and table_like_lines >= 2
    ):
        has_price_table = True

    return {
        "has_this_agreement": bool(has_this_agreement),
        "has_signature_block": bool(has_signature_block),
        "has_effective_date_phrase": bool(has_effective_date_phrase),
        "has_governing_law": bool(has_governing_law),
        "has_price_table": bool(has_price_table),
    }


# ============================================================
# CLASSIFICATION (Decision-facing JSON)
# ============================================================

def build_classification_trace(
    *,
    method: str,
    final_type: str,
    final_role: str,
    confidence: float,
    signals: Dict[str, bool],
    evidence: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Used by Decision Engine.
    Must be lightweight.
    """

    return {
        "method": method,
        "final_type": final_type,
        "final_role": final_role,
        "confidence": round(float(confidence), 3),
        "signals": signals,
        "evidence": {
            "page_numbers": evidence.get("page_numbers", []),
        },
    }


# ============================================================
# EXTRACTION SUMMARY (Validity + Supersession only)
# ============================================================

def _safe_date_str(v: Optional[date]) -> Optional[str]:
    if not v:
        return None
    return v.isoformat()


def build_extraction_summary(
    *,
    effective_from: Optional[date],
    effective_to: Optional[date],
    extraction_method: str,
    confidence: float,
    page_number: Optional[int] = None,
    raw_from: Optional[str] = None,
    raw_to: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Minimal, policy-aligned extraction summary.

    Used for:
        - validity gate
        - supersession resolver

    NEVER returns date objects.
    """

    out: Dict[str, Any] = {}

    if effective_from:
        out["effective_from"] = {
            "value": _safe_date_str(effective_from),
            "raw": raw_from,
            "page_number": page_number,
            "method": extraction_method,
            "confidence": round(float(confidence), 3),
        }

    if effective_to:
        out["effective_to"] = {
            "value": _safe_date_str(effective_to),
            "raw": raw_to,
            "page_number": page_number,
            "method": extraction_method,
            "confidence": round(float(confidence), 3),
        }

    return out
