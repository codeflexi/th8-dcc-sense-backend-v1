from app.repositories.base import BaseRepository

class CaseDocumentLinkRepository(BaseRepository):
    TABLE = "dcc_case_document_links"

    def exists(self, case_id: str, document_id: str) -> bool:
        res = (
            self.sb.table(self.TABLE)
            .select("link_id")
            .eq("case_id", case_id)
            .eq("document_id", document_id)
            .limit(1)
            .execute()
        )
        return bool(res.data)

    def insert_inferred(
        self,
        case_id: str,
        document_id: str,
        inferred_by: str,
        match_score: float,
        explain: dict
    ):
        payload = {
            "case_id": case_id,
            "document_id": document_id,
            "link_status": "INFERRED",
            "inferred_by": inferred_by,
            "match_score": match_score,
            "match_explain_json": explain,
        }
        return self.sb.table(self.TABLE).insert(payload).execute()
