from typing import List, Optional ,Any, Dict, Optional, List
from pydantic import BaseModel, Field , ConfigDict
from datetime import datetime


class POLineItemInput(BaseModel):
    source_line_ref: Optional[str] = None
    sku: Optional[str] = None
    item_name: Optional[str] = None
    description: Optional[str] = None
    quantity: Optional[float] = None
    uom: Optional[str] = None
    unit_price: Optional[float] = None
    currency: Optional[str] = None


class CreateCaseFromPORequest(BaseModel):
    reference_type: str = Field(..., example="ERP_PO")
    reference_id: str = Field(..., example="PO-2026-000123")

    entity_id: str = Field(..., example="VENDOR-001")
    entity_type: str = Field(..., example="VENDOR")
    domain: str = Field(..., example="PROCUREMENT")

    currency: Optional[str] = "THB"
    amount_total: Optional[float]

    line_items: List[POLineItemInput]


class CaseResponse(BaseModel):
    case_id: str
    reference_type: str
    reference_id: str
    status: str



class CaseListItem(BaseModel):
    """
    1 row จาก vw_case_list
    """
    model_config = ConfigDict(extra="allow")

    case_id: str

    entity_id: Optional[str] = None
    entity_type: Optional[str] = None
    domain: Optional[str] = None

    reference_type: Optional[str] = None
    reference_id: Optional[str] = None

    contract_id: Optional[str] = None
    amount_total: Optional[float] = None
    currency: Optional[str] = None

    status: Optional[str] = None
    decision: Optional[str] = None
    risk_level: Optional[str] = None
    confidence_score: Optional[float] = None

    case_detail: Optional[Dict[str, Any]] = None

    created_by: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None


class CaseListResponse(BaseModel):
    items: List[CaseListItem]
    page: int
    limit: int
    total: Optional[int] = None
