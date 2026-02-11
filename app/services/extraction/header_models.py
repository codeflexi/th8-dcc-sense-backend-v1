from __future__ import annotations
from typing import Optional, Dict, Any
from pydantic import BaseModel


class DocumentHeader(BaseModel):
    doc_type: Optional[str] = None
    doc_title: Optional[str] = None
    doc_number: Optional[str] = None
    language: Optional[str] = None

    effective_from: Optional[str] = None
    effective_to: Optional[str] = None

    # IMPORTANT: must be dict not list
    parties: Optional[Dict[str, Any]] = None

    # for downstream decision engine
    extracted_fields: Optional[Dict[str, Any]] = None


# class DocumentHeader(BaseModel):
#     document_type: Optional[str] = None
#     title: Optional[str] = None
#     document_number: Optional[str] = None
#     language: Optional[str] = None

#     issuer_name: Optional[str] = None
#     counterparty_name: Optional[str] = None

#     effective_from: Optional[str] = None
#     effective_to: Optional[str] = None

#     extracted_fields: Optional[Dict[str, Any]] = None

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
