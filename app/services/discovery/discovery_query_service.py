from app.repositories.case_document_link_repo import CaseDocumentLinkRepository


class DiscoveryQueryService:
    """
    Read-only query service for discovery results

    Contract:
    - NO mutation
    - Read from inferred / confirmed document links
    - Must share sb with discovery / decision pipeline
    """

    def __init__(self, sb):
        self.sb = sb
        self.link_repo = CaseDocumentLinkRepository(sb)

    def list_discovery_results(
        self,
        *,
        case_id: str,
        status: str | None = None,
        source: str | None = None,
    ):
        rows = self.link_repo.list_by_case(
            case_id=case_id,
            status=status,
            inferred_by=source,
        )

        documents = []
        summary = {
            "total": 0,
            "inferred": 0,
            "confirmed": 0,
            "removed": 0,
        }

        for r in rows:
            summary["total"] += 1

            if r.get("link_status") == "INFERRED":
                summary["inferred"] += 1
            elif r.get("link_status") == "CONFIRMED":
                summary["confirmed"] += 1
            elif r.get("link_status") == "REMOVED":
                summary["removed"] += 1

            doc = r.get("dcc_documents") or {}

            documents.append({
                "document_id": r.get("document_id"),
                "filename": doc.get("filename"),
                "entity_id": doc.get("entity_id"),
                "entity_type": doc.get("entity_type"),
                "contract_id": doc.get("contract_id"),
                "link_status": r.get("link_status"),
                "inferred_by": r.get("inferred_by"),
                "match_score": r.get("match_score"),
                "match_explain": r.get("match_explain_json"),
                "confirmed_by": r.get("confirmed_by"),
                "confirmed_at": r.get("confirmed_at"),
                "created_at": r.get("created_at"),
            })

        return {
            "case_id": case_id,
            "summary": summary,
            "documents": documents,
        }
