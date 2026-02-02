# app/repositories/case_evidence_group_repo.py
from app.repositories.base import BaseRepository


class CaseEvidenceGroupRepository(BaseRepository):
    TABLE = "dcc_case_evidence_groups"

    def get_or_create(
        self,
        case_id: str,
        group_type: str,
        group_key: str,
        actor_id: str = "SYSTEM",
    ) -> dict:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .eq("group_type", group_type)
            .eq("group_key", group_key)
            .maybe_single()
            .execute()
        )

        if res and res.data:
            return res.data

        payload = {
            "case_id": case_id,
            "group_type": group_type,
            "group_key": group_key,
            "semantic_key": group_key,
            "evidence_ids": [],
            "created_by": actor_id,
        }

        ins = self.sb.table(self.TABLE).insert(payload).execute()
        return ins.data[0]

    def update_evidence_ids(self, group_id: str, evidence_ids: list[str]) -> dict:
        res = (
            self.sb.table(self.TABLE)
            .update({"evidence_ids": evidence_ids})
            .eq("group_id", group_id)
            .execute()
        )
        return res.data[0] if res.data else {}

    def list_by_case(self, case_id: str) -> list[dict]:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .execute()
        )
        return res.data or []
