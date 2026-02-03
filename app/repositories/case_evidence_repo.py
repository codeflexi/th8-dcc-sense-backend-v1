# app/repositories/case_evidence_repo.py

from app.repositories.base import BaseRepository
from datetime import datetime
from typing import List


class CaseEvidenceRepository(BaseRepository):
    """
    Case Evidence = atomic extracted evidence

    Contracts (LOCKED):
    - Evidence is created WITHOUT group_id
    - Evidence is later attached to exactly ONE group_id
    - group_id is the ONLY linkage used downstream (C3.5+)
    """

    TABLE = "dcc_case_evidences"

    # -------------------------------------------------
    # Insert
    # -------------------------------------------------
    def insert(self, payload: dict) -> dict:
        required = [
            "case_id",
            "document_id",
            "evidence_type",
            "evidence_payload",
            "source",
            "extraction_method",
            "confidence",
            "created_by",
        ]

        for k in required:
            if k not in payload:
                raise ValueError(f"Missing required field: {k}")

        payload.setdefault("created_at", datetime.utcnow().isoformat())

        res = (
            self.sb
            .table(self.TABLE)
            .insert(payload)
            .execute()
        )

        if not res.data:
            raise RuntimeError("Failed to insert case evidence")

        return res.data[0]

    # -------------------------------------------------
    # Attach (C3+ contract)
    # -------------------------------------------------
    def attach_to_group(
        self,
        *,
        evidence_id: str,
        group_id: str,
    ) -> dict:
        """
        Deterministic attach:
        - Evidence belongs to exactly ONE group
        - Overwrite is allowed only here
        """

        res = (
            self.sb
            .table(self.TABLE)
            .update({"group_id": group_id})
            .eq("evidence_id", evidence_id)
            .execute()
        )

        if not res.data:
            raise RuntimeError("Failed to attach evidence to group")

        return res.data[0]

    # -------------------------------------------------
    # Queries
    # -------------------------------------------------
    def list_by_case(self, case_id: str) -> List[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .order("created_at")
            .execute()
        )
        return res.data or []

    def list_by_group(self, group_id: str) -> List[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("group_id", group_id)
            .order("created_at")
            .execute()
        )
        return res.data or []

    def list_unassigned_by_case(self, case_id: str) -> List[dict]:
        """
        Audit helper:
        - evidences not yet attached to any group
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .is_("group_id", None)
            .order("created_at")
            .execute()
        )
        return res.data or []
