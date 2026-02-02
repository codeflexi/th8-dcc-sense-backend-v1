from app.repositories.base import BaseRepository

class PriceItemRepository(BaseRepository):
    TABLE = "dcc_contract_price_items"

    def replace_by_contract(self, *, contract_id: str, rows: list[dict]) -> int:
        self.sb.table(self.TABLE).delete().eq("contract_id", contract_id).execute()
        if not rows:
            return 0
        res = self.sb.table(self.TABLE).insert(rows).execute()
        return len(res.data or [])

    def delete_by_document(self, *, document_id: str):
        self.sb.table(self.TABLE).delete().eq("document_id", document_id).execute()
        
    def list_by_document(self, document_id: str):
        return (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("document_id", document_id)
            .execute()
        ).data
