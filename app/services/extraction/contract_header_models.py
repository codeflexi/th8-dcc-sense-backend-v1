# app/services/extraction/contract_header_models.py
from pydantic import BaseModel
from typing import Optional, Dict, Any
from datetime import date

class ContractHeader(BaseModel):
    contract_code: Optional[str]
    vendor_name: Optional[str]
    buyer_name: Optional[str]
    effective_from: Optional[date]
    effective_to: Optional[date]
    raw_fields: Dict[str, Any] = {}
    confidence: float = 0.75
