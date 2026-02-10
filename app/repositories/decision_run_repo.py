from __future__ import annotations

from datetime import datetime
from typing import Any, Dict

from fastapi.encoders import jsonable_encoder

from app.repositories.base import BaseRepository


class DecisionRunRepository(BaseRepository):
    """
    Repository for dcc_decision_runs
    """

    TABLE = "dcc_decision_runs"

    def __init__(self, sb):
        super().__init__(sb)

    def _encode(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Ensure supabase client never sees datetime/Decimal/UUID objects
        return jsonable_encoder(payload)

    def create_run(
        self,
        *,
        case_id: str,
        policy_id: str,
        policy_version: str,
        input_hash: str,
        created_by: str,
        inputs_snapshot: dict,
    ) -> dict:
        payload = {
            "case_id": case_id,
            "policy_id": policy_id,
            "policy_version": policy_version,
            # IMPORTANT: your DB CHECK allows only certain statuses
            # (you told: STARTED/COMPLETED/FAILED/SUBMITTED)
            "run_status": "STARTED",
            "input_hash": input_hash,
            "inputs_snapshot": inputs_snapshot,
            "created_by": created_by,
        }

        payload = self._encode(payload)

        res = self.sb.table(self.TABLE).insert(payload).execute()
        return (res.data or [None])[0]

    def complete_run(
        self,
        *,
        run_id: str,
        decision: str,
        risk_level: str,
        confidence: float,
        summary: dict,
    ) -> None:
        payload = {
            "run_status": "COMPLETED",
            "decision": decision,
            "risk_level": risk_level,
            "confidence": confidence,
            "summary": summary,
            "completed_at": datetime.utcnow().isoformat(),
        }

        payload = self._encode(payload)

        self.sb.table(self.TABLE).update(payload).eq("run_id", run_id).execute()

    def fail_run(self, *, run_id: str, error: str) -> None:
        payload = {
            "run_status": "FAILED",
            "summary": {"error": error},
            "completed_at": datetime.utcnow().isoformat(),
        }

        payload = self._encode(payload)

        self.sb.table(self.TABLE).update(payload).eq("run_id", run_id).execute()

    # -------------------------------------------------
    # Read
    # -------------------------------------------------
    def get_latest_completed_by_case(self, case_id: str) -> dict | None:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .eq("run_status", "COMPLETED")
            .order("completed_at", desc=True)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None