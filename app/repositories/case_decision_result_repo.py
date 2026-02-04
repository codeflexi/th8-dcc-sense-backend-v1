from __future__ import annotations

from typing import Any, Dict

from fastapi.encoders import jsonable_encoder

from app.repositories.base import BaseRepository


class CaseDecisionResultRepository(BaseRepository):
    """
    Repository for dcc_case_decision_results
    1 row = 1 (run_id, group_id)
    """

    TABLE = "dcc_case_decision_results"

    def __init__(self):
        super().__init__()

    def _encode(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Ensure supabase client never sees datetime/Decimal/UUID objects
        return jsonable_encoder(payload)

    def upsert_result(
        self,
        *,
        run_id: str,
        group_id: str,
        decision_status: str,
        risk_level: str,
        confidence: float,
        reason_codes: list,
        fail_actions: list,
        trace: dict,
        evidence_refs: dict,
        created_by: str,
    ) -> None:
        payload = {
            "run_id": run_id,
            "group_id": group_id,
            "decision_status": decision_status,
            "risk_level": risk_level or "LOW",
            "confidence": confidence,
            "reason_codes": reason_codes or [],
            "fail_actions": fail_actions or [],
            "trace": trace or {},
            "evidence_refs": evidence_refs or {"fact_ids": [], "evidence_ids": []},
            "created_by": created_by,
        }

        payload = self._encode(payload)

        self.sb.table(self.TABLE).upsert(
            payload,
            on_conflict="run_id,group_id"
        ).execute()

    def list_by_run(self, run_id: str) -> list[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("run_id", run_id)
            .execute()
        )
        return res.data or []
