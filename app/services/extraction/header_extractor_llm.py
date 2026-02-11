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
    Enterprise-safe header extractor
    - schema aligned with DB
    - always produces extracted_fields
    - deterministic fallback if LLM doesn't send
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
    # PUBLIC
    # ------------------------------------------------------------------

    def extract_document_header(
        self, pages: List[Dict[str, Any]]
    ) -> HeaderExtractionResult:
        text = self._build_prompt_text(pages)

        try:
            raw: DocumentHeader = self.doc_llm.invoke(
                self._document_prompt(text)
            )
            print("RAW LLM HEADER =", raw)
            print("RAW extracted_fields =", raw.extracted_fields)

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
    # PROMPT
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
            "Extract DOCUMENT HEADER.\n"
            "Only explicit values.\n"
            "Return null if not found.\n\n"
            "Return JSON with fields:\n"
            "- doc_type\n"
            "- doc_title\n"
            "- doc_number\n"
            "- language\n"
            "- effective_from\n"
            "- effective_to\n"
            "- parties (buyer/vendor if explicitly written)\n"
            "- extracted_fields (ALL explicit fields found)\n\n"
            "extracted_fields must include any explicit:\n"
            "- contract number\n"
            "- vendor\n"
            "- buyer\n"
            "- reference numbers\n"
            "- currency\n"
            "- other key header values\n\n"
            "If none found → return empty object {}\n\n"
            "Text:\n"
            + text
        )

    def _contract_prompt(self, text: str) -> str:
        return (
            "Extract CONTRACT HEADER.\n"
            "Only explicit values.\n"
            "Return null if not found.\n\n"
            "Text:\n"
            + text
        )

    # ------------------------------------------------------------------
    # NORMALIZE
    # ------------------------------------------------------------------

    def _normalize_document_header(
        self, h: DocumentHeader
    ) -> tuple[Dict[str, Any], float]:

        score = 0.7

        doc_type = (getattr(h, "doc_type", None) or "OTHER").upper()
        if doc_type not in _ALLOWED_DOC_TYPES:
            doc_type = "OTHER"

        doc_title = getattr(h, "doc_title", None)
        doc_number = getattr(h, "doc_number", None)
        language = getattr(h, "language", None)
        eff_from = getattr(h, "effective_from", None)
        eff_to = getattr(h, "effective_to", None)
        parties = getattr(h, "parties", None)

        extracted = getattr(h, "extracted_fields", None) or {}
        
        print("RAW HEADER:", h)
        print("EXTRACTED:", getattr(h,"extracted_fields",None))


        # -------- deterministic fallback --------
        if not extracted:
            extracted = {}

            if doc_number:
                extracted["document_number"] = doc_number

            if doc_title:
                extracted["document_title"] = doc_title

            if eff_from:
                extracted["effective_from"] = eff_from

            if eff_to:
                extracted["effective_to"] = eff_to

            if parties:
                extracted["parties"] = parties

        header: Dict[str, Any] = {
            "doc_type": doc_type,
            "doc_title": doc_title,
            "doc_number": doc_number,
            "language": language,
            "effective_from": eff_from,
            "effective_to": eff_to,
            "parties": parties,
            "extracted_fields": extracted,
        }

        if doc_title:
            score += 0.05
        if doc_number:
            score += 0.05
        if eff_from:
            score += 0.05
        if eff_to:
            score += 0.05
        if parties:
            score += 0.05

        return header, round(min(score, 0.95), 3)

    def _normalize_contract_header(
        self, h: ContractHeader
    ) -> tuple[Dict[str, Any], float]:

        score = 0.7

        header: Dict[str, Any] = {
            "contract_code": getattr(h, "contract_code", None),
            "vendor_name": getattr(h, "vendor_name", None),
            "buyer_name": getattr(h, "buyer_name", None),
            "effective_from": getattr(h, "validity_start", None),
            "effective_to": getattr(h, "validity_end", None),
            "metadata": {},
        }

        if header["contract_code"]:
            score += 0.05
        if header["vendor_name"]:
            score += 0.05
        if header["buyer_name"]:
            score += 0.05
        if header["effective_from"]:
            score += 0.05
        if header["effective_to"]:
            score += 0.05

        return header, round(min(score, 0.95), 3)
