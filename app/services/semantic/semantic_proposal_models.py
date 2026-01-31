from __future__ import annotations
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import date


class SemanticField(BaseModel):
    canonical: str = Field(..., description="Canonical semantic key")

    value: Optional[str] = None
    value_from: Optional[date] = None
    value_to: Optional[date] = None

    source_text: Optional[str] = None
    confidence: Optional[float] = Field(None, ge=0.0, le=1.0)


class SemanticProposal(BaseModel):
    """
    STEP 2.5 semantic proposal
    - nullable
    - non-blocking
    """
    language: Optional[SemanticField] = None
    vendor: Optional[SemanticField] = None
    buyer: Optional[SemanticField] = None
    effective_period: Optional[SemanticField] = None

    warnings: List[str] = []
