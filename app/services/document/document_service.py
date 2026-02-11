from typing import Dict, Any, List, Optional

from app.repositories.document_repo import DocumentRepository
from app.repositories.page_repo import PageRepository
from app.repositories.chunk_repo import ChunkRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.price_repo import PriceItemRepository
from app.repositories.document_header_repo import DocumentHeaderRepository
from app.repositories.base import json_safe


class DocumentPageService:
    """
    Compose document + header + page + chunk + evidence + price-item context

    Used by:
    - Evidence Modal
    - PDF Viewer
    - Copilot Context Builder
    """

    def __init__(self, sb):
        self.sb = sb
        self.document_repo = DocumentRepository(sb)
        self.page_repo = PageRepository(sb)
        self.chunk_repo = ChunkRepository(sb)
        self.evidence_repo = CaseEvidenceRepository(sb)
        self.price_repo = PriceItemRepository(sb)
        self.header_repo = DocumentHeaderRepository(sb)  # NEW

    def get_page(
        self,
        *,
        document_id: str,
        page_number: int,
        case_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> Dict[str, Any]:

        # =====================================================
        # 1) Document meta (dcc_documents)
        # =====================================================
        document = self.document_repo.get(document_id)
        if not document:
            raise ValueError("Document not found")

        # =====================================================
        # 2) Document header (dcc_document_headers)
        # =====================================================
        header = self.header_repo.get_by_document(document_id)

        # =====================================================
        # 3) Page metadata
        # =====================================================
        page = self.page_repo.get_page(
            document_id=document_id,
            page_no=page_number,
        )
        if not page:
            raise ValueError("Page not found")

        # =====================================================
        # 4) Chunks
        # =====================================================
        chunks = self.chunk_repo.list_by_document_page(
            document_id=document_id,
            page_number=page_number,
        )

        # =====================================================
        # 5) Contract price items
        # =====================================================
        price_items = self.price_repo.list_by_document_page(
            document_id=document_id,
            page_number=page_number,
        )

        price_items_by_sku: Dict[str, Dict[str, Any]] = {}
        for p in price_items:
            sku = p.get("sku")
            if sku:
                price_items_by_sku[sku] = p

        # =====================================================
        # 6) Case evidences (optional)
        # =====================================================
        evidences: List[Dict[str, Any]] = []
        if case_id and group_id:
            evidences = self.evidence_repo.list_by_group(
                case_id=case_id,
                group_id=group_id,
            )

        enriched_evidences: List[Dict[str, Any]] = []
        for e in evidences:
            enriched = dict(e)

            if e.get("evidence_type") == "PRICE":
                payload = e.get("evidence_payload") or {}
                sku = payload.get("sku")

                if sku and sku in price_items_by_sku:
                    enriched["price_item"] = price_items_by_sku[sku]

            enriched_evidences.append(enriched)

        # =====================================================
        # FINAL RESPONSE
        # =====================================================
        return json_safe(
            {
                "document": {
                    "document_id": document.get("document_id"),
                    "entity_id": document.get("entity_id"),
                    "file_name": document.get("file_name"),
                    "document_type": document.get("document_type"),
                    "document_role": document.get("document_role"),
                    "status": document.get("status"),
                    "effective_from": document.get("effective_from"),
                    "effective_to": document.get("effective_to"),
                    "superseded_by": document.get("superseded_by"),
                    "source_system": document.get("source_system"),
                    "classification": document.get("classification"),
                    "extraction_summary": document.get("extraction_summary"),
                    "created_at": document.get("created_at"),
                },
                "document_header": header or {},
                
                "page": {
                    "page_number": page_number,
                    "page_id": page.get("page_id"),
                    "image_url": page.get("image_url"),
                },
                "content": {
                    "chunks": chunks or [],
                    "price_items": price_items or [],
                },
                "evidence_context": {
                    "case_id": case_id,
                    "group_id": group_id,
                    "evidences": enriched_evidences,
                },
            }
        )
