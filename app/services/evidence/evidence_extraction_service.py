from app.repositories.case_document_link_repo import CaseDocumentLinkRepository
from app.repositories.price_repo import PriceItemRepository
from app.repositories.clause_repo import ClauseRepository
from app.repositories.case_evidence_repo import CaseEvidenceRepository
from app.repositories.document_repo import DocumentRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository   # ✅ ADD


class EvidenceExtractionService:

    @staticmethod
    def extract(case_id: str, actor_id: str = "SYSTEM"):
        link_repo = CaseDocumentLinkRepository()
        price_repo = PriceItemRepository()
        clause_repo = ClauseRepository()
        evidence_repo = CaseEvidenceRepository()
        doc_repo = DocumentRepository()
        line_repo = CaseLineItemRepository()   # ✅ ADD

        # -------------------------
        # Build PO index (authoritative)
        # -------------------------
        po_lines = line_repo.list_by_case(case_id)
        sku_to_item_id = {
            li.get("sku"): li.get("item_id")
            for li in po_lines
            if li.get("sku") and li.get("item_id")
        }

        confirmed_links = link_repo.list_confirmed(case_id)

        if not confirmed_links:
            return {
                "case_id": case_id,
                "status": "no_confirmed_documents",
                "evidence_created": 0
            }

        evidence_count = 0

        for link in confirmed_links:
            document_id = link["document_id"]

            document = doc_repo.get(document_id)
            if not document:
                continue

            source = "CONTRACT" if document.get("contract_id") else "OTHER"

            # =========================
            # PRICE EVIDENCE (ANCHOR REQUIRED)
            # =========================
            for item in price_repo.list_by_document(document_id):
                sku = item.get("sku")
                item_id = sku_to_item_id.get(sku)

                if not item_id:
                    # 🔒 strict: do not guess
                    continue

                evidence_repo.insert({
                    "case_id": case_id,
                    "document_id": document_id,
                    "evidence_type": "PRICE",
                    "extraction_method": "STRUCTURED_TABLE",
                    "source": source,

                    # ✅ ANCHOR (THIS IS THE FIX)
                    "anchor_type": "PO_ITEM",
                    "anchor_id": item_id,

                    "evidence_payload": {
                        "sku": sku,
                        "unit_price": item.get("unit_price"),
                        "currency": item.get("currency"),
                        "uom": item.get("uom")
                    },
                    "source_snippet": item.get("raw_text"),
                    "source_page": item.get("page_no"),
                    "confidence": 1.0,
                    "created_by": actor_id
                })
                evidence_count += 1

            # =========================
            # CLAUSE EVIDENCE (NO ANCHOR)
            # =========================
            for clause in clause_repo.list_by_document(document_id):
                evidence_repo.insert({
                    "case_id": case_id,
                    "document_id": document_id,
                    "evidence_type": "CLAUSE",
                    "extraction_method": "CLAUSE_PARSE",
                    "source": source,
                    "evidence_payload": {
                        "clause_id": clause.get("clause_id"),
                        "title": clause.get("title")
                    },
                    "source_snippet": clause.get("text"),
                    "source_page": clause.get("page_no"),
                    "confidence": 1.0,
                    "created_by": actor_id
                })
                evidence_count += 1

        return {
            "case_id": case_id,
            "status": "evidence_extracted",
            "evidence_created": evidence_count
        }
