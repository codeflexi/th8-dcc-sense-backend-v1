from typing import List, Optional
from pydantic import BaseModel, Field


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
