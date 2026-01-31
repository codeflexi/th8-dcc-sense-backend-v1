from __future__ import annotations
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any, List
from datetime import date


class DocumentHeader(BaseModel):
    """
    Canonical header extracted from a document.
    Conservative by design: null if uncertain.
    """

    doc_type: Optional[str] = Field(
        None, description="e.g. CONTRACT, SLA, INVOICE, AMENDMENT"
    )
    doc_title: Optional[str] = None
    doc_number: Optional[str] = None
    language: Optional[str] = None

    effective_from: Optional[date] = None
    effective_to: Optional[date] = None

    parties: Optional[List[Dict[str, Any]]] = None
    extracted_fields: Dict[str, Any] = Field(default_factory=dict)

    extraction_method: str = "LLM_HEADER"
    confidence: float = 0.75
