from app.repositories.base import BaseRepository

class ChunkRepository(BaseRepository):
    TABLE = "dcc_document_chunks"

    def replace_by_document(self, *, document_id: str, rows: list[dict]) -> int:
        self.sb.table(self.TABLE).delete().eq("document_id", document_id).execute()
        if not rows:
            return []
        res = self.sb.table(self.TABLE).insert(rows).execute()
        return res.data or []
    
    def update_embedding(self, chunk_id: str, embedding: list[float]):
        self.sb.table(self.TABLE).update(
             {"embedding": embedding}
        ).eq("chunk_id", chunk_id).execute()

    def delete_by_document(self, *, document_id: str):
        self.sb.table(self.TABLE).delete().eq("document_id", document_id).execute()