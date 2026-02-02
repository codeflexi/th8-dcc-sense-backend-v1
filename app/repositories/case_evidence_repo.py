from app.repositories.base import BaseRepository
from datetime import datetime
from typing import List


class CaseEvidenceRepository(BaseRepository):
    TABLE = "dcc_case_evidences"

    def insert(self, payload: dict) -> dict:
        # --- minimal safety ---
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

    def list_by_case(self, case_id: str) -> List[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .execute()
        )
        return res.data or []

    def list_by_ids(self, evidence_ids: List[str]) -> List[dict]:
        if not evidence_ids:
            return []

        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .in_("evidence_id", evidence_ids)
            .execute()
        )
        return res.data or []

    def attach_to_group(self, evidence_id: str, group_id: str) -> dict:
        res = (
            self.sb
            .table(self.TABLE)
            .update({"group_id": group_id})
            .eq("evidence_id", evidence_id)
            .execute()
        )
        return res.data[0] if res.data else {}

    def list_by_group(self, group_id: str) -> list[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("group_id", group_id)
            .execute()
        )
        return res.data or []
 
