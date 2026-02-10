from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.case_evidence_group_repo import CaseEvidenceGroupRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.price_repo import PriceItemRepository

from typing import Dict, Any, List
from app.repositories.document_repo import DocumentRepository
from app.repositories.base import json_safe


class EvidenceGroupingService:
    """
    C3 — Evidence Grouping (FINAL / ENTERPRISE)

    Contract:
    - 1 group = 1 PO line item (item_id)
    - Group is created deterministically from PO snapshot
    - PRICE evidence is attached here
    - CLAUSE evidence is NOT attached here

    ENTERPRISE CONSTRAINT (ADDED):
    - Repositories MUST be constructed with sb
    - No Repo() without sb
    """

    # ------------------------------------------------------------------
    # CHANGED:
    #   __init__ now requires sb
    #
    # WHY:
    # - Prevent Repo() without Supabase client
    # - Enforce single DB lifecycle across C3
    # ------------------------------------------------------------------
    def __init__(self, *, sb):
        self.sb = sb

        # CHANGED: inject sb into all repositories
        self.line_repo = CaseLineItemRepository(sb)
        self.group_repo = CaseEvidenceGroupRepository(sb)
        self.evidence_repo = CaseEvidenceRepository(sb)
        self.doc_repo = DocumentRepository(sb)
        self.price_repo = PriceItemRepository(sb)
        self.line_item_repo = CaseLineItemRepository(sb)

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

            # 1) Ensure group exists (deterministic)
            group = self.group_repo.get_or_create(
                case_id=case_id,
                anchor_id=item_id,
            )
            group_id = group["group_id"]

            # -------------------------------------------------
            # 2) Attach PRICE evidences (anchor-based)
            #
            # NOTE:
            # - Use direct query for deterministic attach
            # - Only ungrouped PRICE evidences are attached
            # -------------------------------------------------
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

    # =====================================================
    # Read-only helpers (group_id owner)
    # =====================================================
    def get_group_only_evidence(self, *, group_id: str) -> Dict[str, Any]:
        """
        Read-only view:
        - group_id is sole owner
        - no case_id dependency
        """

        # CHANGED:
        #   use list_by_group_id(group_id)
        # WHY:
        # - contract: evidence OWNED by group_id
        evidences = self.evidence_repo.list_by_group_id(group_id=group_id)

        documents: Dict[str, Dict[str, Any]] = {}
        out_evidences: List[Dict[str, Any]] = []

        for e in evidences:
            document_id = e.get("document_id")

            # -------------------------
            # Document header
            # -------------------------
            if document_id and document_id not in documents:
                doc = self.doc_repo.get(document_id)
                if doc:
                    documents[document_id] = {
                        "document_id": doc.get("document_id"),
                        "file_name": doc.get("filename"),
                        "document_type": doc.get("document_type"),
                        "created_at": doc.get("created_at"),
                    }

            # -------------------------
            # Price items (from contract table)
            # -------------------------
            price_items = []
            if e.get("evidence_type") == "PRICE":
                price_items = self.price_repo.list_by_document(
                    document_id=document_id
                )

            out_evidences.append({
                "evidence_id": e.get("evidence_id"),
                "evidence_type": e.get("evidence_type"),
                "anchor_type": e.get("anchor_type"),
                "anchor_id": e.get("anchor_id"),

                "document_id": document_id,
                "source_page": e.get("source_page"),
                "source_snippet": e.get("source_snippet"),

                "confidence": float(e.get("confidence")) if e.get("confidence") else None,
                "extraction_method": e.get("extraction_method"),

                "evidence_payload": e.get("evidence_payload"),
                "price_items": price_items,

                "created_at": e.get("created_at"),
            })

        return json_safe({
            "group_id": group_id,
            "documents": list(documents.values()),
            "evidences": out_evidences,
        })

    # =====================================================
    # Legacy-compatible read (case_id + group_id)
    # =====================================================
    def get_group_evidence(
        self,
        *,
        case_id: str,
        group_id: str,
    ) -> Dict[str, Any]:
        """
        Legacy-compatible read:
        - supports case_id + group_id
        - used by older UI paths
        """

        evidences = self.evidence_repo.list_by_group(
            case_id=case_id,
            group_id=group_id,
        )

        documents: Dict[str, Dict[str, Any]] = {}
        items: List[Dict[str, Any]] = []

        for e in evidences:
            document_id = e.get("document_id")
            anchor_id = e.get("anchor_id")
            anchor_type = e.get("anchor_type")

            # =====================================================
            # 1) Document header (dcc_documents)
            # =====================================================
            if document_id and document_id not in documents:
                doc = self.doc_repo.get(document_id)
                if doc:
                    documents[document_id] = {
                        "document_id": doc.get("document_id"),
                        "file_name": doc.get("filename"),
                        "document_type": doc.get("document_type"),
                        "metadata": doc.get("metadata"),
                        "created_at": doc.get("created_at"),
                    }

            # =====================================================
            # 2) Contract PO line (from PO snapshot)
            # =====================================================
            po_items = None
            if anchor_type == "PO_ITEM" and anchor_id:
                po_items = self.line_item_repo.list_by_id(anchor_id)

            # =====================================================
            # 3) Evidence item (audit-grade)
            # =====================================================
            items.append({
                "evidence_id": e.get("evidence_id"),
                "evidence_type": e.get("evidence_type"),

                "anchor_type": anchor_type,
                "anchor_id": anchor_id,

                "document_id": document_id,
                "source_page": e.get("source_page"),
                "source_snippet": e.get("source_snippet"),

                "confidence": e.get("confidence"),
                "extraction_method": e.get("extraction_method"),

                # raw audit payload (do not interpret)
                "evidence_payload": e.get("evidence_payload"),

                # structured PO snapshot rows (for UI)
                "po_items": po_items,

                "created_at": e.get("created_at"),
            })

        return json_safe({
            "case_id": case_id,
            "group_id": group_id,
            "documents": list(documents.values()),
            "evidences": items,
        })

    # =====================================================
    # Context builder (legacy helper)
    # =====================================================
    def build_group_evidence_context(
        self,
        *,
        case_id: str,
        group_id: str,
    ) -> dict:
        """
        Legacy helper:
        - retained for backward compatibility
        - NOT used in C3.5+ decision flow
        """

        # ------------------------------------------------------------------
        # CHANGED:
        #   remove Repo() inside method
        #
        # WHY:
        # - enforce single sb lifecycle
        # - avoid hidden DB clients
        # ------------------------------------------------------------------
        evidences = self.evidence_repo.list_by_group(
            case_id=case_id,
            group_id=group_id,
        )

        price_items_by_sku = {}

        for e in evidences:
            if e["evidence_type"] == "PRICE":
                payload = e.get("evidence_payload") or {}
                sku = payload.get("sku")
                if sku:
                    price_items_by_sku[sku] = payload

        return {
            "case_id": case_id,
            "group_id": group_id,
            "evidences": evidences,
            "price_items": price_items_by_sku,
        }
