from typing import Dict, List, Optional, Any
from pydantic import BaseModel


class PolicyMeta(BaseModel):
    policy_name: str
    version: str
    description: Optional[str] = None
    currency_default: str


class DomainProfile(BaseModel):
    description: Optional[str]
    baseline_priority: List[str]


class TechniqueSpec(BaseModel):
    id: str
    domain: Optional[str] = None
    category: str
    description: Optional[str] = None
    required_facts: List[str] = []
    required_artifacts: List[str] = []
    gates: Dict[str, Any] = {}
    derive: Dict[str, Any] = {}
    fallback_chain: List[str] = []


class RuleSpec(BaseModel):
    rule_id: str
    domain: str
    group: str
    severity: str
    description: Optional[str] = None
    preconditions: Dict[str, Any] = {}
    inputs: List[str] = []
    logic: Dict[str, Any]
    thresholds: Dict[str, Any] = {}
    fail_actions: List[Any] = []
    explanation: Dict[str, str]


class PolicyBundle(BaseModel):
    meta: PolicyMeta
    domain_profiles: Dict[str, DomainProfile]
    techniques: List[TechniqueSpec]
    rules: List[RuleSpec]
    decision_logic: Dict[str, Any]
