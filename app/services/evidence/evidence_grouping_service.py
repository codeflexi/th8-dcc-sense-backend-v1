# app/services/evidence/evidence_grouping_service.py
from collections import defaultdict
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository


class EvidenceGroupingService:
    """
    C.2 Evidence Grouping (LOCKED)

    - Group evidences by business key
    - EvidenceGroup OWNS evidence_ids[]
    """

    @staticmethod
    def group(case_id: str, actor_id: str = "SYSTEM"):
        ev_repo = CaseEvidenceRepository()
        group_repo = CaseEvidenceGroupRepository()

        evidences = ev_repo.list_by_case(case_id)

        if not evidences:
            return {
                "case_id": case_id,
                "status": "no_evidence",
                "groups_created": 0,
            }

        groups = defaultdict(list)

        # -------- grouping rule (deterministic) --------
        for ev in evidences:
            payload = ev.get("evidence_payload") or {}

            sku = payload.get("sku")
            item_name = payload.get("item_name")

            if sku:
                group_key = f"SKU:{sku}"
            elif item_name:
                group_key = f"NAME:{item_name}"
            else:
                group_key = "UNGROUPED"

            groups[group_key].append(ev)

        groups_created = 0

        for group_key, evs in groups.items():
            group = group_repo.get_or_create(
                case_id=case_id,
                group_type="ITEM",
                group_key=group_key,
                actor_id=actor_id,
            )

            evidence_ids = [e["evidence_id"] for e in evs]

            # persist ownership
            group_repo.update_evidence_ids(
                group_id=group["group_id"],
                evidence_ids=evidence_ids,
            )

            # optional backlink (trace only)
            for ev_id in evidence_ids:
                ev_repo.attach_to_group(ev_id, group["group_id"])

            groups_created += 1

        return {
            "case_id": case_id,
            "status": "evidence_grouped",
            "groups_created": groups_created,
        }
