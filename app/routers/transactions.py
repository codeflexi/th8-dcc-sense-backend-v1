# app/api/v1/transactions_ingestion_router.py
from __future__ import annotations

from typing import Any, Dict, List, Optional
from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from app.services.transactions.transaction_ingestion_service import TransactionIngestionService


router = APIRouter()


class LineIn(BaseModel):
    sku: Optional[str] = None
    uom: Optional[str] = None
    quantity: float = 0
    unit_price: float = 0
    currency: Optional[str] = None
    amount: Optional[float] = None
    item_name: Optional[str] = None
    description: Optional[str] = None
    source_line_ref: Optional[str] = None
    document_id: Optional[str] = None
    metadata_json: Optional[Dict[str, Any]] = None


class GRNIn(BaseModel):
    entity_id: str
    po_number: str
    grn_number: str
    currency: str = "THB"
    lines: List[LineIn] = Field(default_factory=list)


class InvoiceIn(BaseModel):
    entity_id: str
    invoice_number: str
    currency: str = "THB"
    po_number: Optional[str] = None
    lines: List[LineIn] = Field(default_factory=list)


@router.post("/grn")
def ingest_grn(request: Request, body: GRNIn, actor_id: str = "SYSTEM"):
    sb = request.state.sb
    svc = TransactionIngestionService(sb)

    try:
        out = svc.ingest_grn(
            actor_id=actor_id,
            entity_id=body.entity_id,
            po_number=body.po_number,
            grn_number=body.grn_number,
            currency=body.currency,
            lines=[x.model_dump() for x in body.lines],
        )
        return out
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"GRN ingestion failed: {e}")


@router.post("/invoice")
def ingest_invoice(request: Request, body: InvoiceIn, actor_id: str = "SYSTEM"):
    sb = request.state.sb
    svc = TransactionIngestionService(sb)

    try:
        out = svc.ingest_invoice(
            actor_id=actor_id,
            entity_id=body.entity_id,
            invoice_number=body.invoice_number,
            currency=body.currency,
            po_number=body.po_number,
            lines=[x.model_dump() for x in body.lines],
        )
        return out
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Invoice ingestion failed: {e}")
