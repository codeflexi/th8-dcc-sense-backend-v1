from app.repositories.case_repo import CaseRepository
from app.repositories.case_line_item_repo import CaseLineItemRepository
from app.repositories.audit_repo import AuditRepository


class CaseService:

    @staticmethod
    def create_case_from_po(po_payload: dict, actor_id: str = "SYSTEM"):
        # Instantiate repositories (IMPORTANT)
        case_repo = CaseRepository()
        line_item_repo = CaseLineItemRepository()

        # 1. Idempotency check
        existing = case_repo.find_by_reference(
            po_payload["reference_type"],
            po_payload["reference_id"]
        )
        if existing:
            return existing

        # 2. Create case header
        case = case_repo.create({
            "entity_id": po_payload["entity_id"],
            "entity_type": po_payload["entity_type"],
            "domain": po_payload["domain"],
            "reference_type": po_payload["reference_type"],
            "reference_id": po_payload["reference_id"],
            "amount_total": po_payload.get("amount_total"),
            "currency": po_payload.get("currency"),
            "status": "OPEN",
            "created_by": actor_id
        })

        case_id = case["case_id"]

        # 3. Snapshot line items (IMMUTABLE)
        line_items_payload = []
        for item in po_payload.get("line_items", []):
            line_items_payload.append({
                "case_id": case_id,
                "source_line_ref": item.get("source_line_ref"),
                "sku": item.get("sku"),
                "item_name": item.get("item_name"),
                "description": item.get("description"),
                "quantity": item.get("quantity"),
                "uom": item.get("uom"),
                "unit_price": item.get("unit_price"),
                "currency": item.get("currency"),
                "total_price": (
                    (item.get("quantity") or 0)
                    * (item.get("unit_price") or 0)
                )
            })
        print("------LINE ITEMS PAYLOAD:", line_items_payload)
        if line_items_payload:
            line_item_repo.bulk_insert(line_items_payload)

        # 4. Audit event
        audit_repo = AuditRepository()

        audit_repo.emit(
            event_type="CASE_CREATED_FROM_PO",
            case_id=case_id,
           
            actor=actor_id,
            payload={
                "reference_type": po_payload["reference_type"],
                "reference_id": po_payload["reference_id"]
            }
        )

        return case
