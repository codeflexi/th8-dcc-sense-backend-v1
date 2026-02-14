# app/repositories/document_repo.py

from __future__ import annotations

from typing import Any, Dict, Optional,List

from app.repositories.base import BaseRepository
from app.repositories.page_repo import PageRepository
from app.repositories.document_open_repo import DocumentOpenRepository


class DocumentRepository(BaseRepository):
    TABLE = "dcc_documents"

    def __init__(self, sb):
        super().__init__(sb)

        # ✅ inject sb into dependent repos
        self.page_repo = PageRepository(sb)
        self.doc_open_repo = DocumentOpenRepository(sb)

    # -------------------------------------------------
    # Read
    # -------------------------------------------------
    def get(self, document_id: str) -> dict | None:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("document_id", document_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

    # -------------------------------------------------
    # Write / Upsert
    # -------------------------------------------------
    def upsert_by_hash(
        self,
        *,
        entity_id: str,
        contract_id: str | None = None,
        file_hash: str,
        filename: str,
        content_type: str,
        storage_key: str,
        source_system: str | None = None,
    ) -> dict:
        """
        NOTE:
        - dcc_documents schema (current) does NOT include entity_type.
        - Keep payload schema-safe only.
        """
        payload = {
            "entity_id": entity_id,
            "contract_id": contract_id,
            "file_hash": file_hash,
            "filename": filename,
            "content_type": content_type,
            "storage_key": storage_key,
        }
        if source_system is not None:
            payload["source_system"] = source_system

        res = (
            self.sb
            .table(self.TABLE)
            .upsert(payload, on_conflict="entity_id,file_hash")
            .execute()
        )

        return res.data[0] if res.data else payload

    def update_storage_key(self, document_id: str, storage_key: str) -> None:
        self.sb.table(self.TABLE).update(
            {"storage_key": storage_key}
        ).eq("document_id", document_id).execute()

    # -------------------------------------------------
    # Meta update (enterprise-grade)
    # -------------------------------------------------
    @staticmethod
    def _deep_merge_json(base: Dict[str, Any], overlay: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge overlay into base.
        - dict merges recursively
        - non-dict overwrites
        """
        out = dict(base or {})
        for k, v in (overlay or {}).items():
            if isinstance(v, dict) and isinstance(out.get(k), dict):
                out[k] = DocumentRepository._deep_merge_json(out[k], v)
            else:
                out[k] = v
        return out

    def update_meta(
        self,
        *,
        document_id: str,
        document_type: str | None = None,
        document_role: str | None = None,
        effective_from: str | None = None,
        effective_to: str | None = None,
        superseded_by: str | None = None,
        source_system: str | None = None,
        classification: dict | None = None,
        extraction_summary: dict | None = None,
    ):
        payload = {}

        if document_type is not None:
            payload["document_type"] = document_type

        if document_role is not None:
            payload["document_role"] = document_role

        if effective_from is not None:
            payload["effective_from"] = effective_from

        if effective_to is not None:
            payload["effective_to"] = effective_to

        if superseded_by is not None:
            payload["superseded_by"] = superseded_by

        if source_system is not None:
            payload["source_system"] = source_system

        if classification is not None:
            payload["classification"] = classification

        if extraction_summary is not None:
            payload["extraction_summary"] = extraction_summary

        if not payload:
            return

        res = (
            self.sb.table("dcc_documents")
            .update(payload)
            .eq("document_id", document_id)
            .execute()
        )

        # supabase v2: check via data not .error
        if not res.data:
            raise Exception("UPDATE_META_FAILED")



    # -------------------------------------------------
    # Discovery support
    # -------------------------------------------------
    def list_by_ids(self, document_ids: List[str]) -> List[Dict[str, Any]]:
        if not document_ids:
            return []
        r = (
            self.sb.table("dcc_documents")
            .select("*")
            .in_("document_id", document_ids)
            .execute()
        )
        return getattr(r, "data", None) or []
    
    def list_by_ids(self, document_ids: List[str]) -> List[Dict[str, Any]]:
        if not document_ids:
            return []
        r = (
            self.sb.table("dcc_documents")
            .select("*")
            .in_("document_id", document_ids)
            .execute()
        )
        return getattr(r, "data", None) or []

    def find_relational_candidates(
        self,
        *,
        entity_id: str,
        contract_id: Optional[str] = None,
        allow_vendor_fallback: bool = True,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        BUGFIX (minimal):
        - ถ้ามี contract_id: ต้องเป็น AND ไม่ใช่ OR
          entity_id == X AND contract_id == Y
        - ถ้าไม่เจอและ allow_vendor_fallback=True -> fallback เป็น vendor-level (entity_id == X)

        NOTE:
        ตอนนี้ contract number จริงอยู่ที่ dcc_document_headers.doc_number
        ดังนั้น service layer จะเป็นตัว enforce "requested_contract_number" อีกชั้น
        """
        base = (
            self.sb.table("dcc_documents")
            .select("*")
            .eq("entity_id", entity_id)
            .eq("status", "ACTIVE")
            .is_("superseded_by", None)
            .order("created_at", desc=True)
        )

        if contract_id:
            r = base.eq("contract_id", contract_id).limit(limit).execute()
            data = getattr(r, "data", None) or []
            if data:
                return data

            if allow_vendor_fallback:
                r2 = base.limit(limit).execute()
                return getattr(r2, "data", None) or []
            return []

        r = base.limit(limit).execute()
        return getattr(r, "data", None) or []

    

    def list_active_docs_for_supersession(
        self,
        *,
        entity_id: str,
        contract_id: str | None,
        document_type: str,
        exclude_document_id: str,
    ) -> list[dict]:
        """
        Deterministic query for supersession resolver.
        """
        q = (
            self.sb
            .table(self.TABLE)
            .select("document_id, entity_id, contract_id, document_type, document_role, effective_from, effective_to, superseded_by, status")
            .eq("status", "ACTIVE")
            .eq("entity_id", entity_id)
            .eq("document_type", document_type)
            .neq("document_id", exclude_document_id)
            .is_("superseded_by", "null")
        )

        if contract_id:
            q = q.eq("contract_id", contract_id)

        res = q.execute()
        return res.data or []

    # -------------------------------------------------
    # Viewer support (PDF / Page)
    # -------------------------------------------------
    def get_page(self, document_id: str, page_no: int) -> dict:
        doc = self.get(document_id)
        if not doc:
            raise ValueError("Document not found")

        page = self.page_repo.get_page(document_id, page_no)
        if not page:
            raise ValueError("Page not found")

        pdf_url = self.doc_open_repo.create_signed_url(
            storage_key=doc["storage_key"],
            expires_in=3600,
        )

        return {
            "document_id": document_id,
            "contract_id": doc.get("contract_id"),
            "file_name": doc.get("filename"),

            "page": page_no,
            "page_id": page.get("page_id"),
            "page_text": page.get("page_text"),

            "pdf_url": pdf_url,
            "text_blocks": page.get("text_blocks", []),
        }
