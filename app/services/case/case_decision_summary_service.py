from typing import Dict, Any

from app.repositories.decision_run_repo import DecisionRunRepository
from app.repositories.base import json_safe


class CaseDecisionSummaryService:
    """
    Read-only service
    - Uses injected Supabase client (sb)
    """

    def __init__(self, sb):
        self.sb = sb
        self.run_repo = DecisionRunRepository(sb)

    def get_decision_summary(self, case_id: str) -> Dict[str, Any]:
        run = self.run_repo.get_latest_completed_by_case(case_id)

        if not run:
            return {
                "case_id": case_id,
                "status": "NO_COMPLETED_RUN",
            }

        return json_safe({
            "case_id": case_id,
            "run_id": run.get("run_id"),

            "decision": run.get("decision"),
            "risk_level": run.get("risk_level"),
            "confidence": run.get("confidence"),

            "summary": run.get("summary"),

            "policy": {
                "policy_id": run.get("policy_id"),
                "policy_version": run.get("policy_version"),
            },

            "timing": {
                "started_at": run.get("created_at"),
                "completed_at": run.get("completed_at"),
            },
        })
