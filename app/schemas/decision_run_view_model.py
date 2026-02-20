from __future__ import annotations

from typing import List, Optional, Literal
from datetime import datetime
from pydantic import BaseModel


DecisionStatus = Literal["APPROVE", "REVIEW", "ESCALATE", "REJECT"]
RiskLevel = Literal["LOW", "MED", "HIGH", "CRITICAL"]


class PolicyInfo(BaseModel):
    policy_id: str
    policy_version: str


class ExposureInfo(BaseModel):
    currency: str = "THB"
    unit_variance_sum: float = 0.0


class TopReasonCode(BaseModel):
    code: str
    count: int


class RunSummary(BaseModel):
    overall_decision: DecisionStatus
    risk_level: RiskLevel
    confidence_avg: float
    item_count: int
    exposure: ExposureInfo
    top_reason_codes: List[TopReasonCode]


class StatusInfo(BaseModel):
    decision: DecisionStatus
    risk: RiskLevel
    confidence: float


class ItemInfo(BaseModel):
    sku: str
    name: str
    uom: str


class QuantityFlags(BaseModel):
    gr_exceeds_po: bool = False
    inv_exceeds_gr: bool = False
    inv_without_gr: bool = False


class QuantityInfo(BaseModel):
    po: float = 0.0
    gr: float = 0.0
    inv: float = 0.0
    over_gr_qty: float = 0.0
    over_inv_qty: float = 0.0
    flags: QuantityFlags


# 🔵 ENTERPRISE UNIFIED PRICE MODEL
class PriceInfo(BaseModel):
    context: Literal["BASELINE", "3WAY_MATCH", "UNKNOWN"]

    po_unit: float
    inv_unit: Optional[float] = None
    baseline_unit: Optional[float] = None

    variance_pct: Optional[float] = None
    variance_abs: float = 0.0

    tolerance_abs: float = 0.0
    currency: str = "THB"

    within_tolerance: bool = True
    has_baseline: bool = False


class RuleCalc(BaseModel):
    field: str
    actual: Optional[float | bool] = None
    expected: Optional[float | bool] = None
    operator: Optional[str] = None


class RuleView(BaseModel):
    rule_id: str
    group: str
    domain: str
    result: str
    severity: str
    exec_message: Optional[str] = None
    audit_message: Optional[str] = None
    calculation: Optional[RuleCalc] = None
    fail_actions: List[dict] = []


class DriverInfo(BaseModel):
    rule_id: str
    label: str
    severity: str


class ArtifactFlags(BaseModel):
    po: bool = False
    grn: bool = False
    invoice: bool = False


class DecisionRunItemView(BaseModel):
    group_id: str
    status: StatusInfo
    item: ItemInfo
    quantity: QuantityInfo
    price: PriceInfo
    drivers: List[DriverInfo]
    next_action: Optional[str] = None
    rules: List[RuleView]
    artifacts: ArtifactFlags
    created_at: Optional[datetime]


class DecisionRunViewContext(BaseModel):
    case_id: str
    run_id: str
    policy: PolicyInfo
    technique: Optional[str] = None
    created_at: Optional[datetime]
    summary: RunSummary
    items: List[DecisionRunItemView]