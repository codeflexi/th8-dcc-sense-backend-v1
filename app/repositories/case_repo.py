from app.repositories.base import BaseRepository

class CaseRepository(BaseRepository):
    TABLE = "dcc_cases"

    def get(self, case_id: str) -> dict | None:
        res = self.sb.table(self.TABLE).select("*").eq("case_id", case_id).limit(1).execute()
        return res.data[0] if res.data else None
