from app.repositories.base import BaseRepository

class PageRepository(BaseRepository):
    TABLE = "dcc_document_pages"

    def replace_pages(self, *, document_id: str, pages: list[dict]) -> int:
        self.sb.table(self.TABLE).delete().eq("document_id", document_id).execute()
        if not pages:
            return 0
        res = self.sb.table(self.TABLE).insert(pages).execute()
        return len(res.data or [])

    def resolve_page_id(self, *, document_id: str, page_number: int) -> str | None:
        res = self.sb.table(self.TABLE).select("page_id").eq("document_id", document_id).eq("page_number", page_number).limit(1).execute()
        return res.data[0]["page_id"] if res.data else None

    def get_page(self, document_id: str, page_no: int) -> dict | None:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("document_id", document_id)
            .eq("page_number", page_no)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None