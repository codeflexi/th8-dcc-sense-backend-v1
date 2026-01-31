# app/services/signal/signal_models.py

from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field


class CounterpartySignal(BaseModel):
    """
    Generic counterparty (not vendor-specific)
    e.g. vendor, customer, partner
    """
    counterparty_id: Optional[str]
    counterparty_type: Optional[str]  # VENDOR | CUSTOMER | PARTNER | UNKNOWN
    confidence: float = 1.0
    source: str = "CASE_CONTEXT"       # CASE_CONTEXT | DERIVED | USER_INPUT


class ItemSignal(BaseModel):
    sku: Optional[str]
    item_name: Optional[str]
    quantity: Optional[float]
    uom: Optional[str]
    unit_price: Optional[float]
    currency: Optional[str]


class TimeWindowSignal(BaseModel):
    """
    Used for historical / document filtering
    """
    lookback_months: int = 12
    start_date: Optional[str] = None
    end_date: Optional[str] = None


class QueryContextSignal(BaseModel):
    """
    Used by vector discovery
    """
    text: str
    keywords: List[str] = []


class CaseSignal(BaseModel):
    """
    Canonical signal bundle (in-memory, recomputable)
    """
    case_id: str

    counterparty: CounterpartySignal
    items: List[ItemSignal]

    time_window: TimeWindowSignal
    query_context: QueryContextSignal

    meta: Dict[str, Any] = {}
