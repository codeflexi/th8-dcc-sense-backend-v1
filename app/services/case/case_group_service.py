from typing import Dict, Any, List

from app.repositories.decision_run_repo import DecisionRunRepository
from app.repositories.case_decision_result_repo import CaseDecisionResultRepository
from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.base import json_safe


class CaseGroupService:
    """
    CaseGroupService (READ-ONLY / AUDIT-GRADE)

    RULES:
    - MUST receive sb from request layer
    - No Repository() without sb
    - Read-only (no side effects)
    """

    def __init__(self, sb):
        if sb is None:
            raise ValueError("CaseGroupService requires sb")
        
        self.sb = sb
        self.run_repo = DecisionRunRepository(sb)
        self.result_repo = CaseDecisionResultRepository(sb)
        self.group_repo = CaseEvidenceGroupRepository(sb)
        self.line_repo = CaseLineItemRepository(sb)

    # =====================================================
    # Group summary (used by /cases/{case_id}/groups)
    # =====================================================
    def get_groups(self, case_id: str) -> Dict[str, Any]:
        run = self.run_repo.get_latest_completed_by_case(case_id)
        if not run:
            return {
                "case_id": case_id,
                "groups": [],
            }

        run_id = run["run_id"]
        results = self.result_repo.list_by_run(run_id)

        # preload immutable PO snapshot
        items = self.line_repo.list_by_case(case_id)
        item_by_id = {
            str(i["item_id"]): i
            for i in items
            if i.get("item_id")
        }

        groups: List[Dict[str, Any]] = []

        for r in results:
            trace = r.get("trace") or {}
            inputs = trace.get("inputs") or {}

            anchor_id = inputs.get("anchor_id")
            po_item = item_by_id.get(str(anchor_id)) if anchor_id else None

            groups.append(json_safe({
                "group_id": r.get("group_id"),

                "decision": r.get("decision_status"),
                "risk_level": r.get("risk_level"),
                "confidence": r.get("confidence"),

                # PO snapshot (audit-grade, immutable)
                "sku": po_item,          # ← frontend ใช้ตัวนี้
                "po_item": po_item,

                # failed rules only (exec-level)
                "reasons": [
                    {
                        "rule_id": rule.get("rule_id"),
                        "severity": rule.get("severity"),
                        "exec": (rule.get("explanation") or {}).get("exec"),
                    }
                    for rule in (trace.get("rules") or [])
                    if rule.get("result") == "FAIL"
                ],

                # baseline chosen by C3.5
                "baseline": (
                    (trace.get("selection") or {}).get("baseline")
                ),

                # fact / evidence refs for drill-down
                "evidence_refs": r.get("evidence_refs"),
            }))
        res =  {
            "case_id": case_id,
            "run_id": run_id,
            "groups": groups,
        }
      
        return res

    # =====================================================
    # Rule drill-down (used by /groups/{group_id}/rules)
    # =====================================================
    def get_group_rules(self, group_id: str) -> Dict[str, Any]:
        result = self.result_repo.get_latest_by_group(group_id=group_id)
        if not result:
            return {
                "group_id": group_id,
                "rules": [],
            }

        trace = result.get("trace") or {}
        rules = trace.get("rules") or []

        return json_safe({
            "group_id": group_id,
            "decision": result.get("decision_status"),
            "risk_level": result.get("risk_level"),
            "confidence": result.get("confidence"),
            "rules": [
                {
                    "rule_id": r.get("rule_id"),
                    "severity": r.get("severity"),
                    "result": r.get("result"),
                    "explanation": (r.get("explanation") or {}).get("exec"),
                    "calculation": r.get("calculation"),
                }
                for r in rules
            ],
        })
