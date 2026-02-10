from typing import Dict, Any, List, Optional

from app.repositories.document_repo import DocumentRepository
from app.repositories.page_repo import PageRepository
from app.repositories.chunk_repo import ChunkRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.price_repo import PriceItemRepository
from app.repositories.base import json_safe


class DocumentPageService:
    """
    Compose document + page + chunk + evidence + price-item context

    Used by:
    - Evidence Modal
    - PDF Viewer
    - Copilot Context Builder

    Design principles:
    - Evidence-first
    - Audit-grade
    - No schema invention
    - FE-friendly but backend-neutral
    """

    def __init__(self):
        self.document_repo = DocumentRepository()
        self.page_repo = PageRepository()
        self.chunk_repo = ChunkRepository()
        self.evidence_repo = CaseEvidenceRepository()
        self.price_repo = PriceItemRepository()

    def get_page(
        self,
        *,
        document_id: str,
        page_number: int,
        case_id: Optional[str] = None,
        group_id: Optional[str] = None,
    ) -> Dict[str, Any]:

        # =====================================================
        # 1) Document header (dcc_documents)
        # =====================================================
        document = self.document_repo.get(document_id)
        if not document:
            raise ValueError("Document not found")

        # =====================================================
        # 2) Page metadata (image / pdf pointer)
        # =====================================================
        page = self.page_repo.get_page(
            document_id=document_id,
            page_number=page_number,
        )
        if not page:
            raise ValueError("Page not found")

        # =====================================================
        # 3) Chunks (dcc_document_chunks)
        # =====================================================
        chunks = self.chunk_repo.list_by_document_page(
            document_id=document_id,
            page_number=page_number,
        )

        # =====================================================
        # 4) Contract price items (dcc_contract_price_items)
        # =====================================================
        price_items = self.price_repo.list_by_document_page(
            document_id=document_id,
            page_number=page_number,
        )

        # index by SKU for fast lookup (used by FE / Copilot)
        price_items_by_sku: Dict[str, Dict[str, Any]] = {}
        for p in price_items:
            sku = p.get("sku")
            if sku:
                price_items_by_sku[sku] = p

        # =====================================================
        # 5) Case evidences (dcc_case_evidence) – optional
        # =====================================================
        evidences: List[Dict[str, Any]] = []
        if case_id and group_id:
            evidences = self.evidence_repo.list_by_group(
                case_id=case_id,
                group_id=group_id,
            )

        # enrich evidence with related price item (if PRICE)
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
        # 6) Final response (NO opinion, NO formatting logic)
        # =====================================================
        return json_safe({
            "document": {
                "document_id": document.get("document_id"),
                "file_name": document.get("file_name"),
                "document_type": document.get("document_type"),
                "page_count": document.get("page_count"),
                "created_at": document.get("created_at"),
            },
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
            }
        })
