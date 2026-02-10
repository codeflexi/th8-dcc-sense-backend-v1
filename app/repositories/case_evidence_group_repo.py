# app/repositories/case_evidence_group_repo.py

from app.repositories.base import BaseRepository, json_safe
from typing import Dict, Any, List
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.document_repo import DocumentRepository


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
        anchor_id: str,
        claim_type: str = "PRICE_BASELINE",
        actor_id: str = "SYSTEM",
    ) -> dict:

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

            # informational only
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

    # =================================================
    # Evidence Pack (FIXED)
    # =================================================
    def get_group_evidence(
        self,
        *,
        case_id: str,
        group_id: str,
    ) -> Dict[str, Any]:
        """
        Enterprise-grade evidence pack builder
        - Uses injected sb (NO manual repo creation)
        - Deterministic
        - Audit-safe
        """

        evidence_repo = CaseEvidenceRepository(self.sb)
        doc_repo = DocumentRepository(self.sb)

        evidences = evidence_repo.list_by_group(
            case_id=case_id,
            group_id=group_id,
        )

        documents: Dict[str, Dict[str, Any]] = {}
        items: List[Dict[str, Any]] = []

        for e in evidences:
            document_id = e.get("document_id")

            if document_id and document_id not in documents:
                doc = doc_repo.get(document_id)
                if doc:
                    documents[document_id] = {
                        "document_id": doc.get("document_id"),
                        "file_name": doc.get("file_name"),
                        "document_type": doc.get("document_type"),
                        "metadata": doc.get("metadata"),
                    }

            items.append({
                "evidence_id": e.get("evidence_id"),
                "fact_id": e.get("fact_id"),

                "document_id": document_id,
                "page_number": e.get("page_number"),

                "highlight": e.get("highlight"),
                "content": e.get("content"),

                "created_at": e.get("created_at"),
            })

        return json_safe({
            "case_id": case_id,
            "group_id": group_id,
            "documents": list(documents.values()),
            "evidences": items,
        })
