from app.repositories.base import BaseRepository

class DocumentRepository(BaseRepository):
    TABLE = "dcc_documents"

    def upsert_by_hash(self, *, entity_id: str, entity_type: str, contract_id: str | None = None, file_hash: str, filename: str, content_type: str, storage_key: str) -> dict:
        payload = {
            "entity_id": entity_id,
            "entity_type": entity_type,
            "contract_id": contract_id,
            "file_hash": file_hash,
            "filename": filename,
            "content_type": content_type,
            "storage_key": storage_key,
        }
        res = self.sb.table(self.TABLE).upsert(payload, on_conflict="entity_id,file_hash").execute()
        return res.data[0] if res.data else payload

    def get(self, document_id: str) -> dict | None:
        res = self.sb.table(self.TABLE).select("*").eq("document_id", document_id).limit(1).execute()
        return res.data[0] if res.data else None

    def update_storage_key(self, document_id: str, storage_key: str):
        self.sb.table(self.TABLE).update({"storage_key": storage_key}).eq("document_id", document_id).execute()
