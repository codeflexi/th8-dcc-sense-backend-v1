from __future__ import annotations
from typing import List, Dict, Any
from pydantic import BaseModel, Field
from langchain_openai import ChatOpenAI

from app.core.config import settings
from app.services.extraction.document_header_models import DocumentHeader
from app.services.extraction.contract_header_models import ContractHeader


_ALLOWED_DOC_TYPES = {"CONTRACT", "INVOICE", "SLA", "AMENDMENT", "OTHER"}


class HeaderExtractionResult(BaseModel):
    header: Dict[str, Any]
    extraction_method: str
    confidence: float
    warnings: List[str] = Field(default_factory=list)


class HeaderExtractor:
    """
    LLM-first, conservative header extractor.

    Principles:
    - Extract ONLY what is explicitly stated
    - Fail-closed to null
    - No inference / no guessing
    - Output matches DB schema directly
    """

    def __init__(self):
        self.doc_llm = ChatOpenAI(
            model=settings.OPENAI_CHAT_MODEL,
            api_key=settings.OPENAI_API_KEY,
            temperature=0,
        ).with_structured_output(
            DocumentHeader,
            method="function_calling",
        )

        self.contract_llm = ChatOpenAI(
            model=settings.OPENAI_CHAT_MODEL,
            api_key=settings.OPENAI_API_KEY,
            temperature=0,
        ).with_structured_output(
            ContractHeader,
            method="function_calling",
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def extract_document_header(
        self, pages: List[Dict[str, Any]]
    ) -> HeaderExtractionResult:
        text = self._build_prompt_text(pages)

        try:
            raw: DocumentHeader = self.doc_llm.invoke(
                self._document_prompt(text)
            )
        except Exception as e:
            return HeaderExtractionResult(
                header={},
                extraction_method="LLM_HEADER",
                confidence=0.0,
                warnings=[f"LLM_DOCUMENT_HEADER_FAILED:{e}"],
            )

        header, score = self._normalize_document_header(raw)

        return HeaderExtractionResult(
            header=header,
            extraction_method="LLM_HEADER",
            confidence=score,
        )

    def extract_contract_header(
        self, pages: List[Dict[str, Any]]
    ) -> HeaderExtractionResult:
        text = self._build_prompt_text(pages)

        try:
            raw: ContractHeader = self.contract_llm.invoke(
                self._contract_prompt(text)
            )
        except Exception as e:
            return HeaderExtractionResult(
                header={},
                extraction_method="LLM_HEADER",
                confidence=0.0,
                warnings=[f"LLM_CONTRACT_HEADER_FAILED:{e}"],
            )

        header, score = self._normalize_contract_header(raw)

        return HeaderExtractionResult(
            header=header,
            extraction_method="LLM_HEADER",
            confidence=score,
        )

    # ------------------------------------------------------------------
    # Prompt builders
    # ------------------------------------------------------------------

    def _build_prompt_text(self, pages: List[Dict[str, Any]]) -> str:
        blocks: List[str] = []
        for p in pages[:2]:
            blocks.append(
                f"[PAGE {p['page_number']}]\n{p.get('text', '')}"
            )
        return "\n\n".join(blocks)[:8000]

    def _document_prompt(self, text: str) -> str:
        return (
            "You are extracting DOCUMENT HEADER information.\n"
            "Rules:\n"
            "- Extract ONLY what is explicitly stated.\n"
            "- If uncertain, return null.\n"
            "- Do NOT infer dates, parties, or numbers.\n"
            "- doc_type must be one of: CONTRACT, INVOICE, SLA, AMENDMENT, OTHER.\n\n"
            "Text:\n"
            + text
        )

    def _contract_prompt(self, text: str) -> str:
        return (
            "You are extracting CONTRACT HEADER information.\n"
            "Rules:\n"
            "- Extract ONLY what is explicitly stated.\n"
            "- If uncertain, return null.\n"
            "- Do NOT infer vendor, buyer, or dates.\n\n"
            "Text:\n"
            + text
        )

    # ------------------------------------------------------------------
    # Normalization (DB-schema aligned)
    # ------------------------------------------------------------------

    def _normalize_document_header(
        self, h: DocumentHeader
    ) -> tuple[Dict[str, Any], float]:
        score = 0.7

        doc_type = (h.doc_type or "OTHER").upper()
        if doc_type not in _ALLOWED_DOC_TYPES:
            doc_type = "OTHER"

        header: Dict[str, Any] = {
            "doc_type": doc_type,
            "doc_title": h.doc_title,
            "doc_number": h.doc_number,
            "language": h.language,
            "effective_from": h.effective_from,
            "effective_to": h.effective_to,
            "parties": h.parties,
            "extracted_fields": h.extracted_fields or {},
        }

        if h.doc_title:
            score += 0.05
        if h.doc_number:
            score += 0.05
        if h.effective_from:
            score += 0.05
        if h.effective_to:
            score += 0.05
        if h.parties:
            score += 0.05

        return header, round(min(score, 0.95), 3)

    def _normalize_contract_header(
        self, h: ContractHeader
    ) -> tuple[Dict[str, Any], float]:
        score = 0.7

        header: Dict[str, Any] = {
            "contract_code": h.contract_code,
            "vendor_name": h.vendor_name,
            "buyer_name": h.buyer_name,
            "effective_from": h.effective_from,
            "effective_to": h.effective_to,
            "status": h.status,
            "metadata": h.metadata or {},
        }

        if h.contract_code:
            score += 0.05
        if h.vendor_name:
            score += 0.05
        if h.buyer_name:
            score += 0.05
        if h.effective_from:
            score += 0.05
        if h.effective_to:
            score += 0.05

        return header, round(min(score, 0.95), 3)
