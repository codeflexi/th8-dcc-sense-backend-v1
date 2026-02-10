# app/services/discovery/discovery_service.py

from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.chunk_repo import ChunkRepository
from app.repositories.vector_discovery_repo import VectorDiscoveryRepository

from app.services.signal.signal_extraction_service import SignalExtractionService
from app.services.embedding.embedding_service import EmbeddingService


class DiscoveryService:
    """
    C3 — Document Discovery (FINAL / LOCKED)

    Contract:
    - Read-only on Case / PO / Documents
    - Writes inferred links ONLY
    - Must share sb with upstream/downstream services
    """

    def __init__(self, sb):
        self.sb = sb

        # -----------------------------
        # Repositories (sb injected)
        # -----------------------------
        self.case_repo = CaseRepository(sb)
        self.line_item_repo = CaseLineItemRepository(sb)
        self.doc_repo = DocumentRepository(sb)
        self.link_repo = CaseDocumentLinkRepository(sb)
        self.chunk_repo = ChunkRepository(sb)
        self.vector_repo = VectorDiscoveryRepository(sb)

        # -----------------------------
        # Pure services
        # -----------------------------
        self.embedder = EmbeddingService()

    # =====================================================
    # Public API
    # =====================================================
    def discover(self, case_id: str, actor_id: str = "SYSTEM") -> dict:
        # --------------------------------------------------
        # 1. Load case + immutable snapshot
        # --------------------------------------------------
        case = self.case_repo.get(case_id)
        if not case:
            raise ValueError("Case not found")

        line_items = self.line_item_repo.list_by_case(case_id)

        # --------------------------------------------------
        # 2. Extract signals (PURE / deterministic)
        # --------------------------------------------------
        signals = SignalExtractionService.extract(case, line_items)

        inferred = {"relational": 0, "vector": 0}

        # ==================================================
        # 3. Relational Discovery (deterministic)
        # ==================================================
        relational_docs = self.doc_repo.find_relational_candidates(
            entity_id=signals.counterparty.counterparty_id,
            contract_id=case.get("contract_id"),
        )

        # ต้องมีเรื่อง Contract ID ด้วย เพราะบางครั้ง อาจจะมี Entity ID เหมือนกัน แต่เป็นคนละ Contract
        # เช่น บริษัทแม่ กับ บริษัทลูก
        # ดังนั้น การจับคู่เชิงสัมพันธ์ ควรจะต้องพิจารณา Contract ID ร่วมด้วยเสมอ
        # contract validation จะช่วยกรองเอกสารที่ไม่เกี่ยวข้องออกไปได้
        for doc in relational_docs:
            if self.link_repo.exists(case_id, doc["document_id"]):
                continue

            self.link_repo.insert_inferred(
                case_id=case_id,
                document_id=doc["document_id"],
                inferred_by="RELATIONAL",
                match_score=1.0,
                explain={
                    "technique": "RELATIONAL_MATCH",
                    "counterparty_id": signals.counterparty.counterparty_id,
                },
            )
            inferred["relational"] += 1

        # ==================================================
        # 4. Vector Discovery (semantic)
        # ==================================================
        query_text = signals.query_context.text
        if not query_text:
            return {
                "case_id": case_id,
                "status": "no_query_context",
                "inferred": inferred,
            }

        query_embedding = self.embedder.embed(query_text)
        if not query_embedding:
            return {
                "case_id": case_id,
                "status": "no_embedding",
                "inferred": inferred,
            }

        vector_hits = self.vector_repo.discover_documents(
            query_embedding=query_embedding
        )

        for hit in vector_hits:
            document_id = hit.get("document_id")
            if not document_id:
                continue

            if self.link_repo.exists(case_id, document_id):
                continue

            self.link_repo.insert_inferred(
                case_id=case_id,
                document_id=document_id,
                inferred_by="VECTOR",
                match_score=hit.get("match_score", 0),
                explain={
                    "technique": "VECTOR_SEMANTIC_MATCH",
                    "top_chunks": hit.get("top_chunks", []),
                },
            )
            inferred["vector"] += 1

        return {
            "case_id": case_id,
            "status": "discovery_completed",
            "inferred": inferred,
        }
