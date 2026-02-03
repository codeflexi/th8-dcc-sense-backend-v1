# app/repositories/case_evidence_group_repo.py

from app.repositories.base import BaseRepository


class CaseEvidenceGroupRepository(BaseRepository):
    """
    Evidence Group = 1 PO Item Anchor

    Contract (LOCKED):
    - anchor_type = 'PO_ITEM'
    - anchor_id   = dcc_case_line_items.item_id (uuid)
    - claim_type  = 'PRICE_BASELINE'
    - 1 group per (case_id, anchor_type, anchor_id, claim_type)
    """

    TABLE = "dcc_case_evidence_groups"

    # =================================================
    # Create / Get
    # =================================================
    def get_or_create(
        self,
        *,
        case_id: str,
        anchor_id: str,                 # MUST be PO item_id (uuid)
        claim_type: str = "PRICE_BASELINE",
        actor_id: str = "SYSTEM",
    ) -> dict:
        """
        Deterministic group creation.

        IMPORTANT:
        - anchor_type is ALWAYS 'PO_ITEM'
        - group_key / semantic_key are informational only
        """

        if not anchor_id:
            raise ValueError("anchor_id (PO item_id) is required")

        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .eq("anchor_type", "PO_ITEM")
            .eq("anchor_id", anchor_id)
            .eq("claim_type", claim_type)
            .maybe_single()
            .execute()
        )

        if res and res.data:
            return res.data

        payload = {
            "case_id": case_id,
            "anchor_type": "PO_ITEM",
            "anchor_id": anchor_id,
            "group_type": "ITEM",
            "claim_type": claim_type,

            # informational only (NO LOGIC DEPENDS ON THIS)
            "group_key": f"PO_ITEM:{anchor_id}",
            "semantic_key": f"PO_ITEM:{anchor_id}",

            "evidence_ids": [],
            "created_by": actor_id,
        }

        ins = self.sb.table(self.TABLE).insert(payload).execute()
        return ins.data[0]

    # =================================================
    # Mutations
    # =================================================
    def update_evidence_ids(self, *, group_id: str, evidence_ids: list[str]) -> dict:
        if not group_id:
            raise ValueError("group_id is required")

        res = (
            self.sb
            .table(self.TABLE)
            .update({"evidence_ids": evidence_ids})
            .eq("group_id", group_id)
            .execute()
        )
        return res.data[0] if res.data else {}

    # =================================================
    # Queries
    # =================================================
    def list_by_case(self, case_id: str) -> list[dict]:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .order("created_at")
            .execute()
        )
        return res.data or []
