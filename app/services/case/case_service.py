# app/services/case/case_service.py

from typing import List, Dict, Any

from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.audit_repo import AuditRepository
from app.repositories.base import json_safe


class CaseService:
    """
    CaseService (enterprise wiring)
    - All repos must share the same Supabase client (sb) per request.
    - No global repo instances.
    """

    def __init__(self, sb):
        self.sb = sb
        self.case_repo = CaseRepository(sb)
        self.line_item_repo = CaseLineItemRepository(sb)
        self.audit_repo = AuditRepository(sb)

    def create_case_from_po(self, po_payload: dict, actor_id: str = "SYSTEM"):
        """
        Create case + immutable PO snapshot + audit event.

        Notes:
        - Idempotency is enforced by (reference_type, reference_id).
        - Line items are inserted as immutable snapshot.
        - Audit event is append-only.
        """

        # 1) Idempotency check
        existing = self.case_repo.find_by_reference(
            po_payload["reference_type"],
            po_payload["reference_id"],
        )
        if existing:
            return existing

        # 2) Create case header
        case = self.case_repo.create({
            "entity_id": po_payload["entity_id"],
            "entity_type": po_payload["entity_type"],
            "domain": po_payload["domain"],
            "reference_type": po_payload["reference_type"],
            "reference_id": po_payload["reference_id"],
            "amount_total": po_payload.get("amount_total"),
            "currency": po_payload.get("currency"),
            "status": "OPEN",
            "created_by": actor_id,
        })

        case_id = case["case_id"]

        # 3) Snapshot line items (IMMUTABLE)
        line_items_payload = []
        for item in po_payload.get("line_items", []) or []:
            qty = item.get("quantity") or 0
            unit = item.get("unit_price") or 0

            line_items_payload.append({
                "case_id": case_id,
                "source_line_ref": item.get("source_line_ref"),
                "sku": item.get("sku"),
                "item_name": item.get("item_name"),
                "description": item.get("description"),
                "quantity": qty,
                "uom": item.get("uom"),
                "unit_price": unit,
                "currency": item.get("currency"),
                "total_price": qty * unit
                
            })

        if line_items_payload:
            self.line_item_repo.bulk_insert(line_items_payload)

        # 4) Audit event (append-only)
        self.audit_repo.emit(
            case_id=case_id,
            event_type="CASE_CREATED_FROM_PO",
            actor=actor_id,
            payload={
                "reference_type": po_payload["reference_type"],
                "reference_id": po_payload["reference_id"],
                "entity_id": po_payload["entity_id"],
                "entity_type": po_payload["entity_type"],
                "domain": po_payload["domain"],
            },
        )

        print("Case created:", case_id )
        return case

    def get_case_list(
        self,
        *,
        page: int = 1,
        page_size: int = 20,
    ) -> Dict[str, Any]:

        if page < 1:
            page = 1
        if page_size < 1:
            page_size = 20

        offset = (page - 1) * page_size

        rows = self.case_repo.list_cases_paginated(
            offset=offset,
            limit=page_size,
        )
        total = self.case_repo.count_cases()

        items: List[Dict[str, Any]] = []
        for r in rows or []:
            items.append(json_safe({
                "case_id": r.get("case_id"),
                "domain": r.get("domain"),
                "reference_type": r.get("reference_type"),
                "reference_id": r.get("reference_id"),

                "entity_id": r.get("entity_id"),
                "entity_type": r.get("entity_type"),
                "entity_name": r.get("entity_name"),

                "amount_total": r.get("amount_total"),
                "currency": r.get("currency"),

                "status": r.get("status"),
                "decision": r.get("decision"),
                "risk_level": r.get("risk_level"),
                "confidence": r.get("confidence_score"),

                "created_at": r.get("created_at"),
                "updated_at": r.get("updated_at"),
            }))

        return {
            "items": items,
            "page": page,
            "page_size": page_size,
            "total": total,
        }

    def get_case_detail(self, case_id: str) -> Dict[str, Any]:
        case = self.case_repo.get_case(case_id)
        if not case:
            raise ValueError("Case not found")

        line_items = self.line_item_repo.list_by_case(case_id)

        return json_safe({
            "case": {
                "case_id": case.get("case_id"),
                "entity_id": case.get("entity_id"),
                "entity_type": case.get("entity_type"),
                "domain": case.get("domain"),

                "reference_type": case.get("reference_type"),
                "reference_id": case.get("reference_id"),

                "amount_total": case.get("amount_total"),
                "currency": case.get("currency"),

                "status": case.get("status"),
                "created_by": case.get("created_by"),
                "created_at": case.get("created_at"),
                "updated_at": case.get("updated_at"),
            },
            "line_items": [
                {
                    "item_id": li.get("item_id"),
                    "source_line_ref": li.get("source_line_ref"),
                    "sku": li.get("sku"),
                    "item_name": li.get("item_name"),
                    "description": li.get("description"),
                    "quantity": li.get("quantity"),
                    "uom": li.get("uom"),
                    "unit_price": li.get("unit_price"),
                    "currency": li.get("currency"),
                    "total_price": li.get("total_price"),
                    "created_at": li.get("created_at"),
                }
                for li in (line_items or [])
            ],
        })
