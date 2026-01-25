from typing import Optional, List
from pydantic import BaseModel, Field

class ClauseRules(BaseModel):
    penalty_rate: Optional[float] = Field(None, description="Penalty rate e.g. 0.02")
    grace_days: Optional[int] = Field(None, description="Grace days before penalty")
    penalty_unit: Optional[str] = Field(None, description="Unit e.g. DAY, WEEK, MONTH")
    rebate_threshold: Optional[float] = Field(None, description="Threshold for rebate")
    payment_days: Optional[int] = Field(None, description="Payment terms in days")

class ClauseItem(BaseModel):
    clause_type: str = Field(..., description="PRICE, PAYMENT_TERM, SLA, PENALTY, REBATE, TERMINATION, OTHER")
    clause_title: str = Field(..., description="Clause title/section heading")
    clause_text: str = Field(..., description="Raw clause text")
    structured_data: ClauseRules = Field(default_factory=ClauseRules)

class ClauseList(BaseModel):
    clauses: List[ClauseItem]
