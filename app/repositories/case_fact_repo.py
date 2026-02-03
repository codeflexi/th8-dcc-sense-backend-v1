# app/repositories/case_fact_repo.py

from app.repositories.base import BaseRepository
from typing import List
from datetime import datetime


class CaseFactRepository(BaseRepository):
    """
    Case Fact = derived, deterministic fact
    Contract:
    - Fact MUST belong to exactly ONE evidence_group
    - group_id is the ONLY anchor used downstream
    """

    TABLE = "dcc_case_facts"

    # =====================================================
    # WRITE
    # =====================================================
    def upsert_fact(self, payload: dict) -> dict:
        # --- required fields (LOCKED, enterprise-grade) ---
        required = [
            "case_id",
            "group_id",          # ✅ REQUIRED (C3 contract)
            "fact_type",
            "fact_key",
            "confidence",
            "derivation_method",
            "created_by",
        ]

        for k in required:
            if k not in payload:
                raise ValueError(f"Missing required fact field: {k}")

        payload.setdefault("created_at", datetime.utcnow().isoformat())

        # ✅ conflict is GROUP-SCOPED (deterministic)
        res = (
            self.sb
            .table(self.TABLE)
            .upsert(
                payload,
                on_conflict="group_id,fact_type"
            )
            .execute()
        )

        if not res.data:
            raise RuntimeError("Failed to upsert case fact")

        return res.data[0]

    # =====================================================
    # READ
    # =====================================================
    def list_by_group(self, group_id: str) -> List[dict]:
        """
        PRIMARY READ PATH
        Used by:
        - C3.5 Technical Selection
        - C4 Decision Run
        """

        if not group_id:
            raise ValueError("group_id is required")

        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("group_id", group_id)
            .order("created_at")
            .execute()
        )
        return res.data or []

    def list_by_case(self, case_id: str) -> List[dict]:
        """
        Debug / audit only
        NEVER used by decision logic
        """

        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .order("created_at")
            .execute()
        )
        return res.data or []

    def list_unassigned_by_case(self, case_id: str) -> List[dict]:
        """
        Audit helper:
        - facts missing group_id = INVALID C3 state
        """

        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .is_("group_id", None)
            .execute()
        )
        return res.data or []
