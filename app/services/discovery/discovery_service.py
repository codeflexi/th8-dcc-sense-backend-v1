from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.chunk_repo import ChunkRepository
from app.repositories.vector_discovery_repo import VectorDiscoveryRepository
from app.services.signal.signal_extraction_service import SignalExtractionService
from app.services.embedding.embedding_service import EmbeddingService


class DiscoveryService:

    @staticmethod
    def discover(case_id: str, actor_id: str = "SYSTEM"):

        case_repo = CaseRepository()
        line_item_repo = CaseLineItemRepository()
        doc_repo = DocumentRepository()
        link_repo = CaseDocumentLinkRepository()
        chunk_repo = ChunkRepository()
        vector_repo = VectorDiscoveryRepository()

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
                    "technique": "RELATIONAL_MATCH",
                    "counterparty_id": signals.counterparty.counterparty_id
                }
            )
            inferred["relational"] += 1

        # ==================================================
        # 4. Vector Discovery (via repo)
        # ==================================================
        # query_embedding = chunk_repo.embed_text(signals.query_context.text)
        
        query_embedding = EmbeddingService.embed(
        signals.query_context.text
)
        if not query_embedding:
            return {"case_id": case_id, "status": "no_embedding"}

        vector_hits = vector_repo.discover_documents(
            query_embedding=query_embedding
        )

        for hit in vector_hits:
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
