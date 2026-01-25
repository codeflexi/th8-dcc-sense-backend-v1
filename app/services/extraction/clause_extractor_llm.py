"""Production-ready ClauseExtractor (LLM + validation, conservative).

Principles:
- Extract ONLY what is explicitly stated.
- If uncertain, keep structured fields null.
- Never invent numeric values.
- Output is audit-friendly: method + confidence + validation notes.
"""

from __future__ import annotations
import re
from typing import List, Dict, Any, Tuple
from langchain_openai import ChatOpenAI
from pydantic import BaseModel
from app.core.config import settings
from app.services.extraction.clause_models import ClauseList, ClauseItem, ClauseRules

_ALLOWED_TYPES = {"PRICE","PAYMENT_TERM","SLA","PENALTY","REBATE","TERMINATION","OTHER","UNKNOWN"}
_ALLOWED_UNITS = {"DAY","WEEK","MONTH"}

class ClauseExtractionResult(BaseModel):
    clauses: List[Dict[str, Any]]
    warnings: List[str] = []
    rejected: List[Dict[str, Any]] = []

class ClauseExtractor:
    def __init__(self):
        self.llm = ChatOpenAI(
            model=settings.OPENAI_CHAT_MODEL,
            api_key=settings.OPENAI_API_KEY,
            temperature=0,
        ).with_structured_output(ClauseList)

    def extract_from_pages(self, pages: List[Dict[str, Any]]) -> ClauseExtractionResult:
        out: List[Dict[str, Any]] = []
        warnings: List[str] = []
        rejected: List[Dict[str, Any]] = []

        for p in pages:
            page_no = p["page_number"]
            md = p.get("text") or ""
            if not md.strip():
                continue

            candidates = self._detect_candidates(md)
            if not candidates:
                continue

            # Call LLM once per page (bounded input)
            page_text = self._build_prompt_block(candidates)
            try:
                resp: ClauseList = self.llm.invoke(
                    """You are extracting contract clauses into structured data for audit/compliance.
Return ONLY what is explicitly stated in the text.
If you cannot find a value, set it to null.
Do NOT infer or guess.
Use clause_type from: PRICE, PAYMENT_TERM, SLA, PENALTY, REBATE, TERMINATION, OTHER.

Text:
""" + page_text
                )
            except Exception as e:
                warnings.append(f"LLM_CLAUSE_PAGE_FAILED:{page_no}:{e}")
                continue

            for c in resp.clauses:
                norm = self._normalize_and_validate(c)
                norm["page_number"] = page_no
                out.append(norm)

        return ClauseExtractionResult(clauses=out, warnings=warnings, rejected=rejected)

    # ---------- candidate detection (deterministic) ----------
    _sec_pat = re.compile(r"^(?:\s*(?:ข้อ\s*)?)(\d+(?:\.\d+)*|\([a-z]\))\s+", re.IGNORECASE)

    def _detect_candidates(self, md: str) -> List[str]:
        # Split by newlines; group consecutive lines into blocks when section numbering found.
        lines = [ln.strip() for ln in md.splitlines() if ln.strip()]
        blocks: List[str] = []
        cur: List[str] = []

        for ln in lines:
            if self._sec_pat.match(ln) and cur:
                blocks.append("\n".join(cur)[:2500])
                cur = [ln]
            else:
                cur.append(ln)
            if sum(len(x) for x in cur) > 2600:
                blocks.append("\n".join(cur)[:2500])
                cur = []
        if cur:
            blocks.append("\n".join(cur)[:2500])

        # Filter: keep blocks that look like policy-bearing clauses (keywords or numbering)
        keep = []
        for b in blocks:
            if self._sec_pat.match(b) or self._has_clause_keywords(b):
                keep.append(b)
        # cap per page to avoid runaway
        return keep[:12]

    def _has_clause_keywords(self, text: str) -> bool:
        t = text.lower()
        keys = [
            "penalty","rebate","payment","sla","delivery","terminate","price",
            "ค่าปรับ","ส่วนลด","ชำระ","ส่งมอบ","ยกเลิก","ราคา"
        ]
        return any(k in t for k in keys)

    def _build_prompt_block(self, blocks: List[str]) -> str:
        # Provide numbered blocks for traceability.
        out = []
        for i, b in enumerate(blocks, 1):
            out.append(f"[BLOCK {i}]\n{b}")
        return "\n\n".join(out)[:12000]

    # ---------- validation / normalization ----------
    def _normalize_and_validate(self, c: ClauseItem) -> Dict[str, Any]:
        ct = (c.clause_type or "OTHER").upper().strip()
        if ct not in _ALLOWED_TYPES:
            ct = "OTHER"

        sd: ClauseRules = c.structured_data or ClauseRules()

        # Validate numeric ranges; fail-closed to null if invalid.
        penalty_rate = sd.penalty_rate
        if penalty_rate is not None and not (0 < penalty_rate < 1):
            penalty_rate = None

        grace_days = sd.grace_days
        if grace_days is not None and grace_days < 0:
            grace_days = None

        payment_days = sd.payment_days
        if payment_days is not None and payment_days <= 0:
            payment_days = None

        rebate_threshold = sd.rebate_threshold
        if rebate_threshold is not None and rebate_threshold < 0:
            rebate_threshold = None

        penalty_unit = sd.penalty_unit
        if penalty_unit is not None:
            pu = str(penalty_unit).upper().strip()
            penalty_unit = pu if pu in _ALLOWED_UNITS else None

        # Confidence heuristic: structured fields present improves score.
        score = 0.75
        if ct != "OTHER" and ct != "UNKNOWN": score += 0.05
        if penalty_rate is not None: score += 0.05
        if payment_days is not None: score += 0.05
        if rebate_threshold is not None: score += 0.05
        score = min(score, 0.95)

        return {
            "clause_type": ct,
            "clause_title": (c.clause_title or "").strip()[:200] or "Untitled",
            "clause_text": (c.clause_text or "").strip()[:6000],
            "structured_data": {
                "penalty_rate": penalty_rate,
                "grace_days": grace_days,
                "penalty_unit": penalty_unit,
                "rebate_threshold": rebate_threshold,
                "payment_days": payment_days,
            },
            "extraction_method": "LLM_STRUCTURED",
            "extraction_confidence": round(score, 3),
        }
