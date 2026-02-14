from app.repositories.base import BaseRepository
from datetime import datetime , timezone

class CaseDocumentLinkRepository(BaseRepository):
    TABLE = "dcc_case_document_links"
     # =====================================================
    # Constructor
    # =====================================================
    def __init__(self, sb):
        super().__init__(sb)

    def get(self, link_id: str) -> dict | None:
        res = (
            self.sb.table(self.TABLE)
            .select("*")
            .eq("link_id", link_id)
            .limit(1)
            .execute()
        )
        return res.data[0] if res.data else None

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
        *,
        case_id: str,
        document_id: str,
        inferred_by: str,
        match_score: float,
        match_explain_json: dict | None = None,
    ):
        """
        Insert inferred link (idempotent-safe)
        """

        payload = {
            "case_id": case_id,
            "document_id": document_id,
            "link_status": "CONFIRMED",
            "inferred_by": inferred_by,
            "match_score": match_score,
            "match_explain_json": match_explain_json or {},
        }

        return (
            self.sb.table("dcc_case_document_links")
            .upsert(payload, on_conflict="case_id,document_id")
            .execute()
        )


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
                dcc_documents!dcc_case_document_links_document_id_fkey (
                    document_id,
                    filename,
                    entity_id,
                    
                    contract_id,
                    status,
                    created_at
                )
                """
            )
            .eq("case_id", case_id)
            .order("created_at", desc=True)
        )

        if status:
            q = q.eq("link_status", status)

        if inferred_by:
            q = q.eq("inferred_by", inferred_by)

        res = q.execute()
        return res.data or []

    def confirm(self, link_id: str, actor_id: str):
        
        if not actor_id:
            raise ValueError("actor_id required")
        
        res =   (
            self.sb.table(self.TABLE)
            .update({
                "link_status": "CONFIRMED",
                "confirmed_by": actor_id,
                "confirmed_at": datetime.now(timezone.utc).isoformat(),
            })
            .eq("link_id", link_id)
            .eq("link_status", "INFERRED")
            .execute()
        )
        return res.data

    def remove(self, link_id: str, actor_id: str):
        if not actor_id:
            raise ValueError("actor_id required")
        
        res = (
            self.sb.table(self.TABLE)
            .update({
                "link_status": "REMOVED",
                "confirmed_by": actor_id,
                "confirmed_at": datetime.now(timezone.utc).isoformat(),
            })
            .eq("link_id", link_id)
            .eq("link_status", "INFERRED")
            .execute()
        )
        return res.data
    
    def list_confirmed(self, case_id: str):
        return (
            self.sb
            .table(self.TABLE)
            .select("*")
            .eq("case_id", case_id)
            .eq("link_status", "CONFIRMED")
            .execute()
        ).data
