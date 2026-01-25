from typing import Optional, Any, Dict, List
from pydantic import BaseModel, Field

class EvidenceRef(BaseModel):
    evidence_id: str
    case_id: str
    rule_id: Optional[str] = None

    source_type: str = Field(..., description="PRICE_ITEM | CLAUSE | DOCUMENT_CHUNK | CASE_FACT")
    source_ref_id: Optional[str] = None

    document_id: Optional[str] = None
    page_number: Optional[int] = None

    snippet: str
    confidence_score: float = 0.9

    open_url: Optional[str] = None
    viewer_url: Optional[str] = None
    meta: Dict[str, Any] = {}

class EvidenceResponse(BaseModel):
    case_id: str
    entity_id: Optional[str] = None
    entity_type: Optional[str] = None
    contract_id: Optional[str] = None
    evidence_groups: Dict[str, List[EvidenceRef]] = {}
    generated_from: str = "db"
