from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.chunk_repo import ChunkRepository
from app.services.signal.signal_extraction_service import SignalExtractionService
from app.infra.supabase_client import get_supabase_client


class DiscoveryService:

    @staticmethod
    def discover(case_id: str, actor_id: str = "SYSTEM"):

        case_repo = CaseRepository()
        line_item_repo = CaseLineItemRepository()
        doc_repo = DocumentRepository()
        link_repo = CaseDocumentLinkRepository()
        chunk_repo = ChunkRepository()
        sb = get_supabase_client()

        # 1. Load case + snapshot
        case = case_repo.get(case_id)
        if not case:
            raise ValueError("Case not found")

        line_items = line_item_repo.list_by_case(case_id)

        # 2. Extract signals
        signals = SignalExtractionService.extract(case, line_items)

        inferred = {"relational": 0, "vector": 0}

        # ==================================================
        # 3. Relational Discovery
        # ==================================================
        relational_docs = doc_repo.find_relational_candidates(
            entity_id=signals.counterparty.counterparty_id,
            contract_id=case.get("contract_id")
        )

        for doc in relational_docs:
            if link_repo.exists(case_id, doc["document_id"]):
                continue

            link_repo.insert_inferred(
                case_id=case_id,
                document_id=doc["document_id"],
                inferred_by="RELATIONAL",
                match_score=1.0,
                explain={
                    "counterparty_id": signals.counterparty.counterparty_id,
                    "technique": "RELATIONAL_MATCH"
                }
            )
            inferred["relational"] += 1

        # ==================================================
        # 4. Vector Discovery
        # ==================================================
        query_embedding = chunk_repo.embed_text(signals.query_context.text)
        if not query_embedding:
            return {"case_id": case_id, "status": "no_embedding"}

        res = sb.rpc(
            "dcc_vector_discover_documents_v1",
            {
                "query_embedding": query_embedding,
                "p_top_k_chunks": 50,
                "p_top_k_docs": 15,
                "p_min_similarity": 0.35,
                "p_top_chunks_per_doc": 3
            }
        ).execute()

        for hit in res.data or []:
            if link_repo.exists(case_id, hit["document_id"]):
                continue

            link_repo.insert_inferred(
                case_id=case_id,
                document_id=hit["document_id"],
                inferred_by="VECTOR",
                match_score=hit["match_score"],
                explain={
                    "technique": "VECTOR_SEMANTIC_MATCH",
                    "top_chunks": hit.get("top_chunks", [])
                }
            )
            inferred["vector"] += 1

        return {
            "case_id": case_id,
            "status": "discovery_completed",
            "inferred": inferred
        }
