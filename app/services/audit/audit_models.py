from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime


class AuditTimelineEvent(BaseModel):
    id: str
    timestamp: datetime
    type: str
    actor: str
    title: str
    severity: str
    run_id: Optional[str] = None
    group_id: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None


class AuditTimelineContext(BaseModel):
    case_id: str
    events: List[AuditTimelineEvent]