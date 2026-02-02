# app/services/evidence/evidence_models.py
from pydantic import BaseModel
from typing import Optional, Dict, Any


class Evidence(BaseModel):
    case_id: str
    document_id: str
    chunk_id: Optional[str]

    evidence_type: str
    extracted_value: Dict[str, Any]

    source_snippet: str
    source_page: Optional[int]

    confidence: float = 1.0
    extraction_method: str
