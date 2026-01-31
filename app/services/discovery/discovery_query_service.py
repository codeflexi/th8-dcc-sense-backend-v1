
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository


class DiscoveryQueryService:

    @staticmethod
    def list_discovery_results(
        case_id: str,
        status: str | None = None,
        source: str | None = None
    ):
        link_repo = CaseDocumentLinkRepository()

        rows = link_repo.list_by_case(
            case_id=case_id,
            status=status,
            inferred_by=source
        )

        documents = []
        summary = {
            "total": 0,
            "inferred": 0,
            "confirmed": 0,
            "removed": 0
        }

        for r in rows:
            summary["total"] += 1
            if r["link_status"] == "INFERRED":
                summary["inferred"] += 1
            elif r["link_status"] == "CONFIRMED":
                summary["confirmed"] += 1
            elif r["link_status"] == "REMOVED":
                summary["removed"] += 1

            doc = r.get("dcc_documents") or {}

            documents.append({
                "document_id": r["document_id"],
                "filename": doc.get("filename"),
                "entity_id": doc.get("entity_id"),
                "entity_type": doc.get("entity_type"),
                "contract_id": doc.get("contract_id"),
                "link_status": r["link_status"],
                "inferred_by": r["inferred_by"],
                "match_score": r["match_score"],
                "match_explain": r["match_explain_json"],
                "confirmed_by": r["confirmed_by"],
                "confirmed_at": r["confirmed_at"],
                "created_at": r["created_at"]
            })

        return {
            "case_id": case_id,
            "summary": summary,
            "documents": documents
        }
