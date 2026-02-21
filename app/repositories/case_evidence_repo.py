# app/repositories/case_evidence_repo.py

from app.repositories.base import BaseRepository
from datetime import datetime , timezone
from typing import List, Dict, Any


class CaseEvidenceRepository(BaseRepository):
    """
    Case Evidence = atomic extracted evidence

    Contracts (LOCKED):
    - Evidence is created WITHOUT group_id
    - Evidence is later attached to exactly ONE group_id
    - group_id is the ONLY linkage used downstream (C3.5+)

    NOTE (compatibility):
    - Some older callers may invoke list_by_group(case_id, group_id) positionally.
      This repo enforces keyword-only in list_by_group(*, case_id, group_id).
      To avoid runtime breakage, we provide a positional-compatible wrapper.
    """

    TABLE = "dcc_case_evidences"
    
    # =====================================================
    # Constructor (REQUIRED)
    # =====================================================
    def __init__(self, sb):
        super().__init__(sb)


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

        payload.setdefault("created_at", datetime.now(timezone.utc).isoformat())
        
        payload = self._encode(payload)

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
        payload = self._encode({"group_id": group_id})
        res = (
            self.sb
            .table(self.TABLE)
            .update(payload)
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

    def list_by_group_only(self, group_id: str) -> List[dict]:
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

    # -------------------------------------------------
    # Primary API (enterprise-grade, keyword-only)
    # -------------------------------------------------
    def list_by_group(self, *args, **kwargs) -> List[Dict[str, Any]]:
        """
        BACKWARD-COMPAT list_by_group dispatcher.

        Supports:
        1) list_by_group(group_id)                       # legacy callers (group linkage only)
        2) list_by_group(case_id, group_id)              # positional (some older services)
        3) list_by_group(case_id=..., group_id=...)      # enterprise keyword-only (preferred)

        Deterministic behavior:
        - If only group_id is provided -> query by group_id (matches locked contract: group_id is the linkage)
        - If case_id + group_id -> query by both (strong isolation)
        """
        # -------- Case A: keyword form (preferred) --------
        if "case_id" in kwargs and "group_id" in kwargs:
            case_id = kwargs["case_id"]
            group_id = kwargs["group_id"]
            return self._list_by_case_and_group(case_id=case_id, group_id=group_id)

        # -------- Case B: positional forms --------
        if len(args) == 1 and not kwargs:
            # list_by_group(group_id)
            group_id = args[0]
            return self.list_by_group_id(group_id=str(group_id))

        if len(args) == 2 and not kwargs:
            # list_by_group(case_id, group_id)
            case_id, group_id = args
            return self._list_by_case_and_group(case_id=str(case_id), group_id=str(group_id))

        raise TypeError(
            "list_by_group() supports (group_id) or (case_id, group_id) or (case_id=..., group_id=...)"
        )

    def _list_by_case_and_group(self, *, case_id: str, group_id: str) -> List[Dict[str, Any]]:
        """
        Enterprise-grade read:
        - Strong isolation: (case_id, group_id)
        - Predictable for audit/replay
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("""
                evidence_id,
                case_id,
                group_id,
                document_id,
                chunk_id,
                evidence_type,
                source,
                source_page,
                source_snippet,
                extraction_method,
                confidence,
                evidence_payload,
                anchor_type,
                anchor_id,
                created_at
            """)
            .eq("case_id", case_id)
            .eq("group_id", group_id)
            .order("created_at", desc=False)
            .execute()
        )
        return res.data or []

    # -------------------------------------------------
    # Compatibility wrappers (DO NOT REMOVE)
    # -------------------------------------------------
    def list_by_group_compat(self, case_id: str, group_id: str) -> List[Dict[str, Any]]:
        """
        Backward-compatible wrapper.

        Some services may call list_by_group(case_id, group_id) positionally.
        That will FAIL because list_by_group is keyword-only.

        Use this wrapper in older call sites or keep it to prevent breakage
        while you refactor callers to keyword args.
        """
        return self.list_by_group(case_id=case_id, group_id=group_id)

    # Optional alias if you prefer clearer naming
    def list_by_group_positional(self, case_id: str, group_id: str) -> List[Dict[str, Any]]:
        return self.list_by_group(case_id=case_id, group_id=group_id)

    # -------------------------------------------------
    # Existing read-only (group_id only)
    # -------------------------------------------------
    def list_by_group_id(
        self,
        group_id: str,
    ) -> List[Dict[str, Any]]:
        """
        Read-only
        - Return ALL evidences attached to a group
        - No derivation
        - No enrichment
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("""
                evidence_id,
                case_id,
                group_id,
                
                source,

                evidence_type,
                anchor_type,
                anchor_id,

                document_id,
                chunk_id,
                source_page,
                source_snippet,

                confidence,
                extraction_method,
                evidence_payload,

                created_by,
                created_at
            """)
            .eq("group_id", group_id)
            .order("created_at", desc=False)
            .execute()
        )
        return res.data or []
