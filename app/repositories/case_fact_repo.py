from app.repositories.base import BaseRepository
from typing import List
from datetime import datetime


class CaseFactRepository(BaseRepository):
    TABLE = "dcc_case_facts"

    def upsert_fact(self, payload: dict) -> dict:
        # --- required fields (LOCKED) ---
        required = [
            "case_id",
            "fact_type",
            "fact_key",          # สำคัญมาก
            "value",
            "confidence",
            "derivation_method",
            "created_by",
        ]

        for k in required:
            if k not in payload:
                raise ValueError(f"Missing required fact field: {k}")

        payload.setdefault("created_at", datetime.utcnow().isoformat())

        res = (
            self.sb
            .table(self.TABLE)
            .upsert(
                payload,
                on_conflict="case_id,fact_type,fact_key"
            )
            .execute()
        )

        return res.data[0] if res.data else {}

    def list_by_case(self, case_id: str) -> List[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .execute()
        )
        return res.data or []

    def list_by_group(self, case_id: str, fact_key: str) -> List[dict]:
        """
        Used by C.4 Decision Run
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .eq("fact_key", fact_key)
            .execute()
        )
        return res.data or []
