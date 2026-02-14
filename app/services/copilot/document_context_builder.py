from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from app.services.document.document_service import DocumentPageService


# -------------------------
# Clause types (simple + deterministic)
# -------------------------
CLAUSE_TYPES = (
    "PAYMENT",
    "PRICING",
    "TERMINATION",
    "LIABILITY",
    "CONFIDENTIALITY",
    "GOVERNING_LAW",
    "WARRANTY",
    "SCOPE",
)

# keyword heuristics (fast, deterministic)
CLAUSE_KEYWORDS: Dict[str, List[str]] = {
    "PAYMENT": ["payment", "invoice", "within", "days", "net"],
    "PRICING": ["price", "pricing", "fixed", "rate", "fee"],
    "TERMINATION": ["terminate", "termination", "notice", "breach"],
    "LIABILITY": ["liability", "indirect", "consequential", "damages", "negligence"],
    "CONFIDENTIALITY": ["confidential", "non-public", "disclose"],
    "GOVERNING_LAW": ["governing law", "laws of", "jurisdiction", "kingdom of thailand"],
    "WARRANTY": ["warranty", "warrants", "defects", "fit for"],
    "SCOPE": ["scope", "agreement governs", "services", "procurement"],
}

# section heading regex: "# 3. PRICING AND PAYMENT"
HEADING_RE = re.compile(r"^\s*#\s*\d+(\.\d+)?\s+(.+?)\s*$", re.MULTILINE)


@dataclass
class EvidenceRef:
    document_id: str
    page: int
    highlight_text: str
    chunk_id: Optional[str] = None


@dataclass
class ClauseRef:
    clause_type: str
    text: str
    page: int
    chunk_id: Optional[str] = None
    score: float = 0.0


class CopilotDocumentContextBuilder:
    """
    Build compact, deterministic context pack for Copilot from DocumentPageService outputs.

    Goals:
    - Evidence-first
    - Token-budget aware
    - Deterministic extraction (no LLM)
    """

    def __init__(self, sb):
        self.sb = sb
        self.page_service = DocumentPageService(sb)

    # -------------------------
    # Public API
    # -------------------------
    def build_from_pages(
        self,
        *,
        document_id: str,
        pages: List[int],
        case_id: Optional[str] = None,
        group_id: Optional[str] = None,
        max_chunks_per_page: int = 4,
        max_clause_chars: int = 900,
        max_snippet_chars: int = 400,
    ) -> Dict[str, Any]:
        """
        Fetch multiple pages via DocumentPageService.get_page(), then build context pack.
        """
        pages = self._dedupe_ints(pages)
        if not pages:
            # fallback to page 1 if nothing provided
            pages = [1]

        page_payloads: List[Dict[str, Any]] = []
        for p in pages:
            payload = self.page_service.get_page(
                document_id=document_id,
                page_number=p,
                case_id=case_id,
                group_id=group_id,
            )
            page_payloads.append(payload)

        return self._build_context_pack(
            document_id=document_id,
            page_payloads=page_payloads,
            max_chunks_per_page=max_chunks_per_page,
            max_clause_chars=max_clause_chars,
            max_snippet_chars=max_snippet_chars,
        )

    def build_from_evidence_refs(
        self,
        *,
        evidence_refs: List[Dict[str, Any]],
        case_id: Optional[str] = None,
        group_id: Optional[str] = None,
        max_pages: int = 4,
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Evidence-driven mode:
        - evidence_refs items contain {document_id, page, ...}
        - We group by document_id and load most relevant pages first.
        """
        # group pages per document
        pages_by_doc: Dict[str, List[int]] = {}
        for r in evidence_refs or []:
            doc_id = r.get("document_id")
            page = r.get("page")
            if not doc_id or not page:
                continue
            pages_by_doc.setdefault(doc_id, []).append(int(page))

        # pick first doc_id by appearance
        if not pages_by_doc:
            return {}

        doc_id = next(iter(pages_by_doc.keys()))
        pages = self._dedupe_ints(pages_by_doc[doc_id])[:max_pages]

        return self.build_from_pages(
            document_id=doc_id,
            pages=pages,
            case_id=case_id,
            group_id=group_id,
            **kwargs,
        )

    # -------------------------
    # Core builders
    # -------------------------
    def _build_context_pack(
        self,
        *,
        document_id: str,
        page_payloads: List[Dict[str, Any]],
        max_chunks_per_page: int,
        max_clause_chars: int,
        max_snippet_chars: int,
    ) -> Dict[str, Any]:

        # Use first payload as primary for header/meta
        primary = page_payloads[0] if page_payloads else {}

        doc = primary.get("document") or {}
        header = primary.get("document_header") or {}

        doc_facts = self._extract_doc_facts(doc=doc, header=header)

        # Extract clauses + evidence refs from all pages
        clauses: List[ClauseRef] = []
        evidence: List[EvidenceRef] = []
        raw_pages: List[Dict[str, Any]] = []

        for payload in page_payloads:
            page_obj = payload.get("page") or {}
            page_no = int(page_obj.get("page_number") or 0) or None
            if not page_no:
                continue

            chunks = (payload.get("content") or {}).get("chunks") or []
            chunks = [c for c in chunks if isinstance(c, dict)]

            # raw pages (trim chunks to budget)
            raw_pages.append(
                {
                    "page": page_no,
                    "chunks": self._slim_chunks(chunks, max_chunks_per_page=max_chunks_per_page),
                }
            )

            # deterministic clause extraction
            clauses.extend(self._extract_clauses_from_chunks(chunks, page_no, max_clause_chars=max_clause_chars))

            # evidence refs (if service enriched them)
            ev_pack = (payload.get("evidence_context") or {}).get("evidences") or []
            evidence.extend(self._extract_evidence_refs(ev_pack, default_doc_id=document_id, max_snippet_chars=max_snippet_chars))

        # de-dup clauses & evidence
        clauses = self._dedupe_clauses(clauses)
        evidence = self._dedupe_evidence(evidence)

        # Also create a compact "contract_brief" string for LLM
        contract_brief = self._render_contract_brief(doc_facts=doc_facts, clauses=clauses)

        return {
            "document_id": document_id,
            "doc_facts": doc_facts,
            "clauses": [self._clause_to_dict(c) for c in clauses],
            "evidence_refs": [self._evidence_to_dict(e) for e in evidence],
            "raw_pages": raw_pages,
            "contract_brief": contract_brief,
        }

    # -------------------------
    # Extractors
    # -------------------------
    def _extract_doc_facts(self, *, doc: Dict[str, Any], header: Dict[str, Any]) -> Dict[str, Any]:
        extraction = doc.get("extraction_summary") or {}
        parties = header.get("parties") or {}
        extracted_fields = header.get("extracted_fields") or {}

        vendor = None
        if isinstance(parties, dict):
            vendor = parties.get("vendor") or parties.get("supplier")
        if not vendor and isinstance(extracted_fields, dict):
            p2 = extracted_fields.get("parties") or {}
            if isinstance(p2, dict):
                vendor = p2.get("vendor") or p2.get("supplier")

        effective_from = header.get("effective_from") or (extraction.get("effective_from") or {}).get("value") or doc.get("effective_from")
        effective_to = header.get("effective_to") or (extraction.get("effective_to") or {}).get("value") or doc.get("effective_to")

        return {
            "doc_type": header.get("doc_type") or doc.get("document_type"),
            "doc_title": header.get("doc_title") or extracted_fields.get("document_title") or None,
            "doc_number": header.get("doc_number") or extracted_fields.get("document_number") or None,
            "vendor": vendor,
            "status": doc.get("status"),
            "effective_from": effective_from,
            "effective_to": effective_to,
            "confidence": header.get("confidence") or (doc.get("classification") or {}).get("confidence"),
        }

    def _extract_clauses_from_chunks(self, chunks: List[Dict[str, Any]], page_no: int, max_clause_chars: int) -> List[ClauseRef]:
        out: List[ClauseRef] = []
        for ch in chunks:
            content = (ch.get("content") or "").strip()
            if not content:
                continue

            chunk_id = ch.get("chunk_id")
            text_lc = content.lower()

            # quick classify by keywords
            for ctype in CLAUSE_TYPES:
                kws = CLAUSE_KEYWORDS.get(ctype, [])
                score = 0
                for kw in kws:
                    if kw in text_lc:
                        score += 1

                # require at least 2 hits to reduce noise
                if score >= 2:
                    out.append(
                        ClauseRef(
                            clause_type=ctype,
                            text=content[:max_clause_chars],
                            page=page_no,
                            chunk_id=chunk_id,
                            score=float(score),
                        )
                    )

        # extra: if headings exist, try to infer clause types from headings
        # (optional but deterministic)
        return out

    def _extract_evidence_refs(self, evidences: List[Dict[str, Any]], default_doc_id: str, max_snippet_chars: int) -> List[EvidenceRef]:
        out: List[EvidenceRef] = []
        for ev in evidences or []:
            if not isinstance(ev, dict):
                continue

            doc_id = ev.get("document_id") or default_doc_id
            page = ev.get("source_page")
            snippet = (ev.get("source_snippet") or "").strip()

            if (not page or not snippet) and isinstance(ev.get("price_items"), list) and ev["price_items"]:
                pi0 = ev["price_items"][0] if isinstance(ev["price_items"][0], dict) else {}
                page = page or pi0.get("page_number")
                snippet = snippet or (pi0.get("snippet") or "").strip()

            if not page:
                continue

            out.append(
                EvidenceRef(
                    document_id=doc_id,
                    page=int(page),
                    highlight_text=snippet[:max_snippet_chars],
                    chunk_id=ev.get("chunk_id"),
                )
            )

        return out

    # -------------------------
    # Renderers
    # -------------------------
    def _render_contract_brief(self, *, doc_facts: Dict[str, Any], clauses: List[ClauseRef]) -> str:
        """
        Compact string designed for LLM input (small + clear + citeable).
        """
        lines: List[str] = []
        lines.append("CONTRACT FACTS")
        lines.append(f"- Vendor: {doc_facts.get('vendor')}")
        lines.append(f"- Title: {doc_facts.get('doc_title')}")
        lines.append(f"- Contract No: {doc_facts.get('doc_number')}")
        lines.append(f"- Effective: {doc_facts.get('effective_from')} -> {doc_facts.get('effective_to')}")
        lines.append(f"- Status: {doc_facts.get('status')}")
        lines.append("")

        # pick top clauses per type
        by_type: Dict[str, ClauseRef] = {}
        for c in sorted(clauses, key=lambda x: x.score, reverse=True):
            if c.clause_type not in by_type:
                by_type[c.clause_type] = c

        lines.append("KEY CLAUSES (evidence-first)")
        for ctype in ("PAYMENT", "PRICING", "TERMINATION", "GOVERNING_LAW", "LIABILITY", "CONFIDENTIALITY", "WARRANTY", "SCOPE"):
            c = by_type.get(ctype)
            if not c:
                continue
            snippet = (c.text or "").replace("\n", " ").strip()
            snippet = snippet[:260]
            lines.append(f"- {ctype}: {snippet} (page {c.page})")

        return "\n".join(lines).strip()

    # -------------------------
    # Slim / dedupe helpers
    # -------------------------
    def _slim_chunks(self, chunks: List[Dict[str, Any]], *, max_chunks_per_page: int) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        for ch in (chunks or [])[: max_chunks_per_page * 2]:
            if len(out) >= max_chunks_per_page:
                break
            content = (ch.get("content") or "").strip()
            if not content:
                continue
            out.append(
                {
                    "chunk_id": ch.get("chunk_id"),
                    "chunk_type": ch.get("chunk_type"),
                    "page_number": ch.get("page_number"),
                    "content": content[:900],
                }
            )
        return out

    def _dedupe_ints(self, xs: List[int]) -> List[int]:
        seen = set()
        out = []
        for x in xs or []:
            try:
                xi = int(x)
            except Exception:
                continue
            if xi in seen:
                continue
            seen.add(xi)
            out.append(xi)
        return out

    def _dedupe_clauses(self, clauses: List[ClauseRef]) -> List[ClauseRef]:
        seen = set()
        out: List[ClauseRef] = []
        for c in clauses:
            key = (c.clause_type, c.page, (c.text or "")[:120])
            if key in seen:
                continue
            seen.add(key)
            out.append(c)
        # keep best 12 total
        out = sorted(out, key=lambda x: (x.clause_type, -x.score, x.page))[:12]
        return out

    def _dedupe_evidence(self, refs: List[EvidenceRef]) -> List[EvidenceRef]:
        seen = set()
        out: List[EvidenceRef] = []
        for r in refs:
            key = (r.document_id, r.page, (r.highlight_text or "")[:120])
            if key in seen:
                continue
            seen.add(key)
            out.append(r)
        return out[:12]

    def _clause_to_dict(self, c: ClauseRef) -> Dict[str, Any]:
        return {
            "clause_type": c.clause_type,
            "page": c.page,
            "chunk_id": c.chunk_id,
            "score": c.score,
            "text": c.text,
        }

    def _evidence_to_dict(self, e: EvidenceRef) -> Dict[str, Any]:
        return {
            "document_id": e.document_id,
            "page": e.page,
            "chunk_id": e.chunk_id,
            "highlight_text": e.highlight_text,
        }
