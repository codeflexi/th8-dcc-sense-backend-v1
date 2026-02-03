from typing import List
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository


class EvidenceGroupingService:
    """
    C3 — Evidence Grouping (FINAL / ENTERPRISE)

    Contract:
    - 1 group = 1 PO line item (item_id)
    - Group is created deterministically from PO snapshot
    - PRICE evidence is attached here
    - CLAUSE evidence is NOT attached here
    """

    def __init__(self):
        self.line_repo = CaseLineItemRepository()
        self.group_repo = CaseEvidenceGroupRepository()
        self.evidence_repo = CaseEvidenceRepository()

    # =====================================================
    # Public API
    # =====================================================
    def group_case(self, case_id: str) -> List[dict]:
        """
        Entry point for C3
        """

        po_lines = self.line_repo.list_by_case(case_id)
        if not po_lines:
            return []

        results: List[dict] = []

        for line in po_lines:
            item_id = line.get("item_id")
            if not item_id:
                raise RuntimeError(
                    f"PO line missing item_id (case_id={case_id})"
                )

            # 1) Ensure group exists
            group = self.group_repo.get_or_create(
                case_id=case_id,
                anchor_id=item_id,
            )
            group_id = group["group_id"]

            # 2) Attach PRICE evidences (anchor-based)
            evidences = (
                self.evidence_repo.sb
                .table("dcc_case_evidences")
                .select("evidence_id")
                .eq("case_id", case_id)
                .eq("anchor_type", "PO_ITEM")
                .eq("anchor_id", item_id)
                .eq("evidence_type", "PRICE")
                .is_("group_id", None)
                .execute()
                .data or []
            )

            if evidences:
                evidence_ids = []

                for ev in evidences:
                    self.evidence_repo.attach_to_group(
                        evidence_id=ev["evidence_id"],
                        group_id=group_id,
                    )
                    evidence_ids.append(ev["evidence_id"])

                # keep denormalized list for audit only
                self.group_repo.update_evidence_ids(
                    group_id=group_id,
                    evidence_ids=list(
                        set((group.get("evidence_ids") or []) + evidence_ids)
                    ),
                )

            results.append(group)

        return results
