from __future__ import annotations
from typing import Optional
from pydantic import BaseModel, Field


# ---------- Document Header ----------

class DocumentHeader(BaseModel):
    document_type: Optional[str] = Field(
        default=None, description="CONTRACT | INVOICE | SLA | OTHER"
    )
    title: Optional[str] = None
    language: Optional[str] = None
    issuer_name: Optional[str] = None
    effective_from: Optional[str] = Field(
        default=None, description="ISO date string if explicitly stated"
    )
    effective_to: Optional[str] = Field(
        default=None, description="ISO date string if explicitly stated"
    )


# ---------- Contract Header ----------

class ContractHeader(BaseModel):
    contract_code: Optional[str] = None
    contract_type: Optional[str] = Field(
        default=None, description="MSA | SUPPLY | SLA | OTHER"
    )
    vendor_name: Optional[str] = None
    buyer_name: Optional[str] = None
    currency: Optional[str] = None
    validity_start: Optional[str] = None
    validity_end: Optional[str] = None
