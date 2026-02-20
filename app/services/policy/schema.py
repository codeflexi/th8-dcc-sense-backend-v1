from typing import Dict, List, Optional, Any
from pydantic import BaseModel


# ---------- META ----------

class PolicyMetaDefaults(BaseModel):
    currency: Optional[str] = "THB"
    rounding: Dict[str, Any] = {}
    tolerances: Dict[str, Any] = {}


class PolicyMeta(BaseModel):
    policy_id: str
    version: str
    description: Optional[str] = None
    defaults: Optional[PolicyMetaDefaults] = None
    discovery: Optional[Dict[str, Any]] = None


# ---------- RULE ----------

class RuleExplanation(BaseModel):
    exec: Optional[str] = None
    audit: Optional[str] = None


class RuleSpec(BaseModel):
    rule_id: str
    group: Optional[str] = None
    severity: Optional[str] = None
    description: Optional[str] = None
    uses: Optional[List[str]] = []
    logic: Dict[str, Any] = {}
    fail_actions: Optional[List[Any]] = []
    explanation: Optional[RuleExplanation] = None


# ---------- DOMAIN ----------

class DomainSpec(BaseModel):
    description: Optional[str] = None
    profile: Optional[Dict[str, Any]] = {}
    calculations: Optional[Dict[str, Any]] = {}
    rules: List[RuleSpec] = []


# ---------- ROOT ----------

class PolicyBundle(BaseModel):
    meta: PolicyMeta
    domains: Dict[str, DomainSpec]
    decision_logic: Optional[Dict[str, Any]] = {}
