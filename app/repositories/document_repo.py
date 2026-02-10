# app/repositories/document_repo.py

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
        entity_type: str,
        contract_id: str | None = None,
        file_hash: str,
        filename: str,
        content_type: str,
        storage_key: str,
    ) -> dict:
        payload = {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "contract_id": contract_id,
            "file_hash": file_hash,
            "filename": filename,
            "content_type": content_type,
            "storage_key": storage_key,
        }

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
    # Discovery support
    # -------------------------------------------------
    def find_relational_candidates(
        self,
        entity_id: str,
        contract_id: str | None = None,
    ) -> list[dict]:
        q = (
            self.sb
            .table(self.TABLE)
            .select("document_id, entity_id, entity_type, contract_id")
            .eq("status", "ACTIVE")
        )

        if contract_id:
            q = q.or_(
                f"entity_id.eq.{entity_id},contract_id.eq.{contract_id}"
            )
        else:
            q = q.eq("entity_id", entity_id)

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
