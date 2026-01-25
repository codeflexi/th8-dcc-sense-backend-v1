from app.repositories.base import BaseRepository

class ClauseRepository(BaseRepository):
    TABLE = "dcc_contract_clauses"

    def replace_by_contract(self, *, contract_id: str, rows: list[dict]) -> int:
        self.sb.table(self.TABLE).delete().eq("contract_id", contract_id).execute()
        if not rows:
            return 0
        res = self.sb.table(self.TABLE).insert(rows).execute()
        return len(res.data or [])
    
    def delete_by_document(self, *, document_id: str):
        self.sb.table(self.TABLE).delete().eq("document_id", document_id).execute()