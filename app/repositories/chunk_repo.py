# app/repositories/chunk_repo.py

from app.repositories.base import BaseRepository
from typing import List, Dict, Any


class ChunkRepository(BaseRepository):
    TABLE = "dcc_document_chunks"

    # =====================================================
    # Constructor (REQUIRED)
    # =====================================================
    def __init__(self, sb):
        super().__init__(sb)

    # =====================================================
    # Write
    # =====================================================
    def replace_by_document(
        self,
        *,
        document_id: str,
        rows: List[dict],
    ) -> List[dict]:
        """
        Replace ALL chunks for a document (idempotent).
        Used by ingestion pipeline only.
        """
        self.sb.table(self.TABLE).delete().eq(
            "document_id", document_id
        ).execute()

        if not rows:
            return []

        res = self.sb.table(self.TABLE).insert(rows).execute()
        return res.data or []

    def update_embedding(
        self,
        *,
        chunk_id: str,
        embedding: List[float],
    ) -> None:
        """
        Update vector embedding for a single chunk.
        """
        self.sb.table(self.TABLE).update(
            {"embedding": embedding}
        ).eq("chunk_id", chunk_id).execute()

    def delete_by_document(
        self,
        *,
        document_id: str,
    ) -> None:
        """
        Hard delete all chunks for a document.
        """
        self.sb.table(self.TABLE).delete().eq(
            "document_id", document_id
        ).execute()

    # =====================================================
    # Read
    # =====================================================
    def list_by_document_page(
        self,
        *,
        document_id: str,
        page_number: int,
    ) -> List[Dict[str, Any]]:
        """
        Return all text chunks for a given document + page
        (embedding explicitly excluded for performance)
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select(
                """
                chunk_id,
                document_id,
                page_id,
                page_number,
                chunk_type,
                content,
                metadata,
                created_at
                """
            )
            .eq("document_id", document_id)
            .eq("page_number", page_number)
            .order("created_at", desc=False)
            .execute()
        )

        return res.data or []
