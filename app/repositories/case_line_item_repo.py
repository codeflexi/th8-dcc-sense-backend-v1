from app.repositories.base import BaseRepository


from app.repositories.base import BaseRepository


class CaseLineItemRepository(BaseRepository):
    TABLE = "dcc_case_line_items"

    def bulk_insert(self, items: list[dict]):
        if not items:
            return

        # ถ้า insert fail → supabase client จะ raise exception เอง
        self.sb.table(self.TABLE).insert(items).execute()

    def list_by_case(self, case_id: str) -> list[dict]:
        """
        Return immutable PO snapshot line items for a case
        """
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .order("created_at")
            .execute()
        )
        return res.data or []