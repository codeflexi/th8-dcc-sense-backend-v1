from app.repositories.base import BaseRepository


class CaseRepository(BaseRepository):
    TABLE = "dcc_cases"

    def get(self, case_id: str) -> dict | None:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

    def find_by_reference(self, reference_type: str, reference_id: str) -> dict | None:
        res = (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("reference_type", reference_type)
            .eq("reference_id", reference_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

    def create(self, payload: dict) -> dict:
        res = (
            self.sb
            .table(self.TABLE)
            .insert(payload)
            .execute()
        )
        return res.data[0]
