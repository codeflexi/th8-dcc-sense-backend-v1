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

    def list_by_case(
        self,
        case_id: str,
        status: str | None = None,
        inferred_by: str | None = None
    ):
        q = (
            self.sb
            .table(self.TABLE)
            .select(
                """
                link_id,
                case_id,
                document_id,
                link_status,
                inferred_by,
                match_score,
                match_explain_json,
                confirmed_by,
                confirmed_at,
                created_at,
                dcc_documents (
                    filename,
                    entity_id,
                    entity_type,
                    contract_id
                )
                """
            )
            .eq("case_id", case_id)
        )

        if status:
            q = q.eq("link_status", status)

        if inferred_by:
            q = q.eq("inferred_by", inferred_by)

        res = q.execute()
        return res.data or []
